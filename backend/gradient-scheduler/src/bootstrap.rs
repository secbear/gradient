/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use std::collections::HashSet;

use anyhow::{Context, Result, ensure};
use chrono::NaiveDateTime;
use gradient_db::WorkerDb;
use gradient_entity::evaluation::EvaluationStatus;
use gradient_entity::exec_job::ExecJobStatus;
use gradient_pool::{EvaluationRequirement, WorkerCaps, WorkerJob, WorkerProfile};
use gradient_types::waiting_reason::EvalCapability;
use gradient_types::{ProjectId, WaitingReason};
use gradient_wire::types::{BuildSpecKind, GradientCapabilities};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QueryOrder, QuerySelect,
    RelationTrait, TransactionTrait,
};
use serde::{Deserialize, Serialize};

use crate::capacity::{BuildCapacityProfile, BuildCapacitySnapshot};
use crate::jobs::PendingJob;

pub(crate) const MAX_BOOTSTRAP_JOBS: usize = 128;
pub(crate) const MAX_BOOTSTRAP_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CapabilityProfile {
    pub name: String,
    pub worker_id: String,
    pub capabilities: GradientCapabilities,
    pub resources: WorkerProfile,
}

impl From<BuildCapacityProfile> for CapabilityProfile {
    fn from(profile: BuildCapacityProfile) -> Self {
        Self {
            name: profile.name,
            worker_id: profile.worker_id,
            capabilities: profile.capabilities,
            resources: profile.resources,
        }
    }
}

impl CapabilityProfile {
    pub fn validate_all(profiles: &[Self]) -> Result<()> {
        Self::validate(profiles, true)
    }

    pub(crate) fn validate_tracked(profiles: &[Self]) -> Result<()> {
        Self::validate(profiles, false)
    }

    fn validate(profiles: &[Self], require_operation: bool) -> Result<()> {
        let mut names = HashSet::new();
        for profile in profiles {
            ensure!(
                !profile.name.trim().is_empty(),
                "capability profile name is empty"
            );
            ensure!(
                names.insert(&profile.name),
                "duplicate capability profile name"
            );
            ensure!(
                !profile.worker_id.trim().is_empty(),
                "capability profile has no worker identity"
            );
            ensure!(
                !require_operation
                    || profile.capabilities.build
                    || profile.capabilities.eval
                    || profile.capabilities.network,
                "capability profile provides no supported operation"
            );
            if profile.capabilities.build {
                BuildCapacityProfile::validate_all(&[BuildCapacityProfile {
                    name: profile.name.clone(),
                    worker_id: profile.worker_id.clone(),
                    capabilities: profile.capabilities.clone(),
                    resources: profile.resources.clone(),
                }])?;
            }
            if profile.capabilities.network {
                ensure!(
                    profile.resources.max_concurrent_builds > 0,
                    "network profile has no fetch slots"
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct BootstrapCapacityProfiles {
    pub profiles: Vec<CapabilityProfile>,
    pub tracked_profiles: Vec<CapabilityProfile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapDemandKind {
    Download,
    Substitution,
    Evaluation,
    InputFetch,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct BootstrapCapacityDemand {
    pub kind: BootstrapDemandKind,
    pub available: bool,
    pub profiles: Vec<bool>,
    pub tracked_workers: Vec<bool>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct BootstrapCapacityProfile {
    pub profile: CapabilityProfile,
    pub build_profile_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct BootstrapTrackedWorker {
    pub profile: CapabilityProfile,
    pub connected: bool,
    pub build_tracked_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct BootstrapCapacityAssessment {
    /// Observation time of the separately captured waiting and input-fetch sources.
    pub observed_at: NaiveDateTime,
    pub complete: bool,
    pub demands: Vec<BootstrapCapacityDemand>,
    pub profiles: Vec<BootstrapCapacityProfile>,
    pub tracked_workers: Vec<BootstrapTrackedWorker>,
}

#[derive(Debug, Clone)]
pub(crate) struct BootstrapWorker {
    pub id: String,
    pub caps: WorkerCaps,
    pub authorized: Option<HashSet<ProjectId>>,
    pub ready: bool,
    pub max_concurrent_builds: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct BootstrapSnapshot {
    pub workers: Vec<BootstrapWorker>,
    pub pending: Vec<BootstrapJob>,
    pub pending_evaluations: HashSet<gradient_types::EvaluationId>,
    pub complete: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct BootstrapObservation {
    pub stages: Vec<StageDemand>,
    pub inputs: Vec<crate::input_waits::InputCapacityDemand>,
    pub complete: bool,
    pub observed_at: NaiveDateTime,
}

#[derive(Debug, Clone)]
pub(crate) struct StageDemand {
    pub evaluation: gradient_types::EvaluationId,
    pub project: ProjectId,
    pub requirement: EvaluationRequirement,
}

#[derive(Debug, Clone)]
pub(crate) enum BootstrapJob {
    Evaluation {
        project: ProjectId,
        requirement: EvaluationRequirement,
    },
    Download {
        project: ProjectId,
    },
    Substitution {
        project: ProjectId,
        verifying_keys: Vec<Option<String>>,
    },
}

impl BootstrapJob {
    pub(crate) fn payload_bytes(job: &PendingJob, limit: usize) -> Option<usize> {
        match job {
            PendingJob::Build(build) if build.job.build.kind == BuildSpecKind::Substitute => {
                let storage = build
                    .verifying_keys
                    .len()
                    .checked_mul(std::mem::size_of::<Option<String>>())?;
                if storage > limit {
                    return None;
                }
                build.verifying_keys.iter().try_fold(storage, |bytes, key| {
                    bytes
                        .checked_add(key.as_ref().map_or(0, String::len))
                        .filter(|bytes| *bytes <= limit)
                })
            }
            _ => Some(0),
        }
    }

    pub(crate) fn of(job: &PendingJob) -> Option<Self> {
        match job {
            PendingJob::Eval(eval) => Some(Self::Evaluation {
                project: eval.project_id,
                requirement: EvaluationRequirement::of(&eval.job),
            }),
            PendingJob::Build(build) => match build.job.build.kind {
                BuildSpecKind::Build => None,
                BuildSpecKind::Download => Some(Self::Download {
                    project: build.project_id,
                }),
                BuildSpecKind::Substitute => Some(Self::Substitution {
                    project: build.project_id,
                    verifying_keys: build.verifying_keys.clone(),
                }),
            },
            PendingJob::Exec(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BootstrapInputs {
    pub observation: BootstrapObservation,
    pub profiles: Vec<BootstrapCapacityProfile>,
    pub tracked_workers: Vec<BootstrapTrackedWorker>,
    pub projects: HashSet<ProjectId>,
    pub policy: std::sync::Arc<dyn gradient_pool::WorkerJobPolicy>,
}

impl BootstrapObservation {
    pub(crate) async fn capture(
        db: &WorkerDb,
        projects: &HashSet<ProjectId>,
        inputs: &crate::input_waits::InputWaits,
        scan_limit: usize,
        excluded_limit: usize,
    ) -> Result<Self> {
        use gradient_types::{CEvaluation, CExecJob, CTask, EEvaluation, EExecJob};
        use sea_orm::sea_query::Expr;

        if projects.is_empty() {
            return Ok(Self {
                stages: vec![],
                inputs: vec![],
                complete: true,
                observed_at: gradient_types::now(),
            });
        }
        let (total_inputs, inputs) =
            inputs.capacity_demands(projects, MAX_BOOTSTRAP_JOBS, scan_limit, excluded_limit)?;
        let mut complete = total_inputs <= inputs.len();
        let mut stages = Vec::new();
        let transaction = tokio::time::timeout(std::time::Duration::from_secs(2), db.begin())
            .await
            .context("waiting-worker query could not acquire a transaction")??;
        transaction
            .execute_unprepared("SET LOCAL statement_timeout = '2s'")
            .await
            .context("set waiting-worker query deadline")?;
        let rows = EEvaluation::find()
                .select_only()
                .column_as(CEvaluation::Id, "evaluation")
            .column_as(CTask::Project, "project")
                .expr_as(Expr::cust("CASE WHEN octet_length(evaluation.repository) <= 65536 THEN evaluation.repository END"), "repository")
            .expr_as(Expr::cust("CASE WHEN octet_length(evaluation.waiting_reason::text) <= 65536 THEN evaluation.waiting_reason END"), "reason")
                .join(JoinType::InnerJoin, gradient_entity::evaluation::Relation::Task.def())
                .filter(CEvaluation::Status.eq(EvaluationStatus::Waiting))
                .filter(CTask::Project.is_in(projects.iter().copied()))
                .filter(sea_orm::sea_query::Expr::cust(
                    "evaluation.waiting_reason ->> 'kind' = 'eval_workers'",
                ))
                .order_by_asc(CEvaluation::UpdatedAt)
                .order_by_asc(CEvaluation::Id)
                .limit((MAX_BOOTSTRAP_JOBS + 1) as u64)
                .into_model::<WaitingStageRow>()
                .all(&transaction)
                .await
                .context("read scoped evaluations waiting for workers")?;
        complete &= rows.len() <= MAX_BOOTSTRAP_JOBS;
        let rows = rows
            .into_iter()
            .take(MAX_BOOTSTRAP_JOBS)
            .collect::<Vec<_>>();
        let queued_exec: Option<gradient_types::ExecJobId> = EExecJob::find()
            .select_only()
            .column(CExecJob::Id)
            .join(
                JoinType::InnerJoin,
                gradient_entity::exec_job::Relation::Evaluation.def(),
            )
            .join(
                JoinType::InnerJoin,
                gradient_entity::evaluation::Relation::Task.def(),
            )
            .filter(CExecJob::Status.eq(ExecJobStatus::Queued))
            .filter(CTask::Project.is_in(projects.iter().copied()))
            .limit(1)
            .into_tuple()
            .one(&transaction)
            .await
            .context("read scoped queued exec jobs")?;
        complete &= queued_exec.is_none();
        transaction
            .rollback()
            .await
            .context("finish read-only waiting-worker query")?;
        for row in rows {
            let reason = row.reason.as_ref().and_then(WaitingReason::from_json);
            match reason {
                Some(WaitingReason::EvalWorkers { capability, .. })
                    if capability != EvalCapability::Exec =>
                {
                    let Some(repository) = row.repository else {
                        complete = false;
                        continue;
                    };
                    let (job, _) = crate::loops::flake_job_for_eval_source(
                        &repository,
                        String::new(),
                        vec![],
                        capability != EvalCapability::Eval,
                        vec![],
                        None,
                    );
                    let needed = EvaluationRequirement::of(&job);
                    if needed.eval {
                        stages.push(StageDemand {
                            evaluation: row.evaluation,
                            project: row.project,
                            requirement: EvaluationRequirement {
                                eval: true,
                                ..Default::default()
                            },
                        });
                    }
                    if needed.network {
                        stages.push(StageDemand {
                            evaluation: row.evaluation,
                            project: row.project,
                            requirement: EvaluationRequirement {
                                network: true,
                                secrets: needed.secrets,
                                ..Default::default()
                            },
                        });
                    }
                }
                Some(WaitingReason::EvalWorkers {
                    capability: EvalCapability::Exec,
                    ..
                })
                | None => complete = false,
                _ => {}
            }
        }
        Ok(Self {
            stages,
            inputs,
            complete,
            observed_at: gradient_types::now(),
        })
    }
}

#[derive(sea_orm::FromQueryResult)]
struct WaitingStageRow {
    evaluation: gradient_types::EvaluationId,
    project: ProjectId,
    repository: Option<String>,
    reason: Option<serde_json::Value>,
}

enum Requirement<'a> {
    Job(&'a BootstrapJob),
    Stage(&'a StageDemand),
    Input(&'a crate::input_waits::InputCapacityDemand),
}

impl Requirement<'_> {
    fn project(&self) -> ProjectId {
        match self {
            Self::Job(
                BootstrapJob::Evaluation { project, .. }
                | BootstrapJob::Download { project }
                | BootstrapJob::Substitution { project, .. },
            ) => *project,
            Self::Stage(stage) => stage.project,
            Self::Input(input) => input.project,
        }
    }

    fn kind(&self) -> BootstrapDemandKind {
        match self {
            Self::Job(BootstrapJob::Download { .. }) => BootstrapDemandKind::Download,
            Self::Job(BootstrapJob::Substitution { .. }) => BootstrapDemandKind::Substitution,
            Self::Input(_) => BootstrapDemandKind::InputFetch,
            Self::Job(BootstrapJob::Evaluation { .. }) | Self::Stage(_) => {
                BootstrapDemandKind::Evaluation
            }
        }
    }

    fn fits(&self, worker: &BootstrapWorker) -> bool {
        if !crate::jobs::authorized_for(self.project(), worker.authorized.as_ref()) {
            return false;
        }
        match self {
            Self::Job(BootstrapJob::Evaluation { requirement, .. }) => {
                worker.caps.allowed_jobs.contains(WorkerJob::Evaluation)
                    && worker.caps.can_eval_requirement(*requirement)
            }
            Self::Job(BootstrapJob::Download { .. }) => {
                worker.max_concurrent_builds > 0
                    && worker.caps.allowed_jobs.contains(WorkerJob::Download)
                    && worker.caps.can_fetch_input(false)
            }
            Self::Job(BootstrapJob::Substitution { verifying_keys, .. }) => {
                worker.max_concurrent_builds > 0
                    && worker
                        .caps
                        .allowed_jobs
                        .contains(WorkerJob::Substitution { verifying_keys })
                    && worker.caps.can_fetch_input(false)
            }
            Self::Input(input) => {
                worker.max_concurrent_builds > 0
                    && !input.excluded.contains(&worker.id)
                    && worker.caps.allowed_jobs.contains(WorkerJob::InputFetch)
                    && worker.caps.can_fetch_input(input.needs_secrets)
            }
            Self::Stage(stage) => {
                worker.caps.allowed_jobs.contains(WorkerJob::Evaluation)
                    && worker.caps.can_eval_requirement(stage.requirement)
            }
        }
    }
}

pub(crate) fn assess(
    snapshot: &BuildCapacitySnapshot,
    observation: &BootstrapObservation,
    profiles: &[BootstrapCapacityProfile],
    tracked_workers: &[BootstrapTrackedWorker],
    projects: &HashSet<ProjectId>,
    policy: &std::sync::Arc<dyn gradient_pool::WorkerJobPolicy>,
) -> BootstrapCapacityAssessment {
    let captured = snapshot
        .bootstrap
        .as_ref()
        .expect("bootstrap capture was requested");
    let prospective = |profile: &CapabilityProfile| BootstrapWorker {
        id: profile.worker_id.clone(),
        caps: WorkerCaps {
            architectures: profile.resources.architectures.clone(),
            system_features: profile.resources.system_features.clone(),
            capabilities: profile.capabilities.clone(),
            allowed_jobs: gradient_pool::AllowedJobs::of(&profile.worker_id, policy),
            ..WorkerCaps::default()
        },
        authorized: Some(projects.clone()),
        ready: false,
        max_concurrent_builds: profile.resources.max_concurrent_builds,
    };
    let candidates = profiles
        .iter()
        .map(|profile| prospective(&profile.profile))
        .collect::<Vec<_>>();
    let starting = tracked_workers
        .iter()
        .map(|profile| prospective(&profile.profile))
        .collect::<Vec<_>>();
    let requirements = captured
        .pending
        .iter()
        .map(Requirement::Job)
        .chain(
            observation
                .stages
                .iter()
                .filter(|stage| !captured.pending_evaluations.contains(&stage.evaluation))
                .map(Requirement::Stage),
        )
        .chain(observation.inputs.iter().map(Requirement::Input));
    let demands = requirements
        .map(|requirement| BootstrapCapacityDemand {
            kind: requirement.kind(),
            available: captured
                .workers
                .iter()
                .any(|worker| worker.ready && requirement.fits(worker)),
            profiles: candidates
                .iter()
                .map(|worker| requirement.fits(worker))
                .collect(),
            tracked_workers: starting
                .iter()
                .zip(tracked_workers.iter())
                .map(|(worker, tracked)| !tracked.connected && requirement.fits(worker))
                .collect(),
        })
        .collect();
    BootstrapCapacityAssessment {
        observed_at: observation.observed_at,
        complete: observation.complete
            && captured.complete
            && snapshot.total_ready_builds == snapshot.builds.len(),
        demands,
        profiles: profiles.to_vec(),
        tracked_workers: tracked_workers.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_jobs::{
        RESTRICTED_WORKER, TRUSTED_KEY, TrustedSubstitutionsOnly, eval_job, substitution_job,
    };
    use gradient_types::{DerivationBuildId, EvaluationId};
    use gradient_wire::types::FlakeStep;
    use std::sync::Arc;

    fn snapshot(project: ProjectId, pending: Vec<PendingJob>) -> BuildCapacitySnapshot {
        BuildCapacitySnapshot {
            captured_at: gradient_types::now(),
            server_draining: false,
            builds: Vec::new(),
            total_ready_builds: 0,
            remaining_ready_builds: 0,
            workers: Vec::new(),
            connected_workers: HashSet::new(),
            instance: Default::default(),
            bootstrap: Some(BootstrapSnapshot {
                pending_evaluations: pending
                    .iter()
                    .filter_map(|job| match job {
                        PendingJob::Eval(eval) if eval.project_id == project => {
                            Some(eval.evaluation_id)
                        }
                        _ => None,
                    })
                    .collect(),
                pending: pending.iter().filter_map(BootstrapJob::of).collect(),
                workers: Vec::new(),
                complete: true,
            }),
        }
    }

    fn profile(id: &str, eval: bool, network: bool, secrets: bool) -> BootstrapCapacityProfile {
        BootstrapCapacityProfile {
            profile: CapabilityProfile {
                name: id.into(),
                worker_id: id.into(),
                capabilities: GradientCapabilities {
                    eval,
                    network,
                    secrets,
                    ..Default::default()
                },
                resources: WorkerProfile {
                    max_concurrent_builds: 1,
                    ..Default::default()
                },
            },
            build_profile_index: None,
        }
    }

    fn observation() -> BootstrapObservation {
        BootstrapObservation {
            stages: Vec::new(),
            inputs: Vec::new(),
            complete: true,
            observed_at: gradient_types::now(),
        }
    }

    #[test]
    fn bootstrap_reuses_ready_eval_only_workers_without_build_resources() {
        let project = ProjectId::now_v7();
        let mut snapshot = snapshot(project, Vec::new());
        snapshot
            .bootstrap
            .as_mut()
            .unwrap()
            .workers
            .push(BootstrapWorker {
                id: "eval-only".into(),
                caps: WorkerCaps {
                    capabilities: GradientCapabilities {
                        eval: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                authorized: Some(HashSet::from([project])),
                ready: true,
                max_concurrent_builds: 0,
            });
        let mut observed = observation();
        observed.stages.push(StageDemand {
            evaluation: EvaluationId::now_v7(),
            project,
            requirement: EvaluationRequirement {
                eval: true,
                ..Default::default()
            },
        });
        let profiles = [profile("candidate", true, false, false)];
        let policy: Arc<dyn gradient_pool::WorkerJobPolicy> =
            Arc::new(gradient_pool::EveryJobAllowed);
        for ready in [true, false] {
            snapshot.bootstrap.as_mut().unwrap().workers[0].ready = ready;
            let assessed = assess(
                &snapshot,
                &observed,
                &profiles,
                &[],
                &HashSet::from([project]),
                &policy,
            );
            assert_eq!(assessed.demands[0].available, ready);
            assert_eq!(assessed.demands[0].profiles, vec![true]);
            assert!(snapshot.workers.is_empty());
            assert_eq!(assessed.profiles[0].profile.resources.ram_total_mb, 0);
        }
    }

    #[test]
    fn bootstrap_preserves_substitution_signing_policy_and_transfer_semantics() {
        let project = ProjectId::now_v7();
        let jobs = [vec![Some(TRUSTED_KEY.into())], vec![None]]
            .into_iter()
            .map(|keys| {
                PendingJob::Build(substitution_job(project, DerivationBuildId::now_v7(), keys))
            })
            .collect();
        let snapshot = snapshot(project, jobs);
        let profiles = [
            profile(RESTRICTED_WORKER, false, true, false),
            profile("offline", false, false, false),
        ];
        let policy: Arc<dyn gradient_pool::WorkerJobPolicy> = Arc::new(TrustedSubstitutionsOnly);
        let assessed = assess(
            &snapshot,
            &observation(),
            &profiles,
            &[],
            &HashSet::from([project]),
            &policy,
        );
        assert_eq!(assessed.demands[0].kind, BootstrapDemandKind::Substitution);
        assert_eq!(assessed.demands[0].profiles, vec![true, false]);
        assert_eq!(assessed.demands[1].profiles, vec![false, false]);
        assert!(!assessed.profiles[0].profile.capabilities.build);
        assert!(
            assessed.profiles[0]
                .profile
                .resources
                .architectures
                .is_empty()
        );
    }

    #[test]
    fn admitted_eval_requirements_override_coarser_waiting_stage_capabilities() {
        let project = ProjectId::now_v7();
        let mut eval = eval_job(project);
        eval.job.steps.push(FlakeStep::FetchFlake);
        let stage = StageDemand {
            evaluation: eval.evaluation_id,
            project,
            requirement: EvaluationRequirement {
                eval: true,
                ..Default::default()
            },
        };
        let snapshot = snapshot(project, vec![PendingJob::Eval(eval)]);
        let mut observed = observation();
        observed.stages.push(stage);
        let profiles = [
            profile("eval-only", true, false, false),
            profile("both", true, true, false),
        ];
        let policy: Arc<dyn gradient_pool::WorkerJobPolicy> =
            Arc::new(gradient_pool::EveryJobAllowed);
        let assessed = assess(
            &snapshot,
            &observed,
            &profiles,
            &[],
            &HashSet::from([project]),
            &policy,
        );
        assert_eq!(assessed.demands.len(), 1);
        assert_eq!(assessed.demands[0].profiles, vec![false, true]);
    }

    #[test]
    fn input_requirements_preserve_secrets_exclusions_and_connected_credit_guards() {
        let project = ProjectId::now_v7();
        let snapshot = snapshot(project, Vec::new());
        let mut observed = observation();
        observed
            .inputs
            .push(crate::input_waits::InputCapacityDemand {
                project,
                needs_secrets: true,
                excluded: HashSet::from(["excluded".into()]),
            });
        let profiles = [
            profile("network", false, true, false),
            profile("allowed", false, true, true),
            profile("excluded", false, true, true),
        ];
        let tracked = [BootstrapTrackedWorker {
            profile: profiles[1].profile.clone(),
            connected: true,
            build_tracked_index: None,
        }];
        let policy: Arc<dyn gradient_pool::WorkerJobPolicy> =
            Arc::new(gradient_pool::EveryJobAllowed);
        let assessed = assess(
            &snapshot,
            &observed,
            &profiles,
            &tracked,
            &HashSet::from([project]),
            &policy,
        );
        assert_eq!(assessed.demands[0].profiles, vec![false, true, false]);
        assert_eq!(assessed.demands[0].tracked_workers, vec![false]);
        observed.complete = false;
        assert!(
            !assess(
                &snapshot,
                &observed,
                &profiles,
                &tracked,
                &HashSet::from([project]),
                &policy
            )
            .complete
        );
    }
}
