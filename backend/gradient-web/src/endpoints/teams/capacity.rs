/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use super::refusal::TeamRefusal;
use crate::api_spec::TeamPath;
use crate::error::{ErrorCode, WebError, WebResult};
use crate::helpers::{OptionExt, ok_json};
use crate::refusal::SharedRefusal;
use crate::resource::Resource;
use crate::scope::{Admin, TeamScope};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::NaiveDateTime;
use gradient_scheduler::Scheduler;
use gradient_scheduler::capacity::{
    BuildCapacityAssessment, BuildCapacityLimits, BuildCapacityProfile, BuildProfileAssessment,
    BuildResourceAssessment,
};
use gradient_types::ProjectId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(crate) const REQUEST_BYTES: usize = 64 * 1024;
const MAX_PROFILES: usize = 16;
const MAX_TRACKED_PROFILES: usize = 256;
const MAX_PROJECTS: usize = 256;
const MAX_BUILDS: usize = 128;
const MAX_WORKERS: usize = 256;
const MAX_SCANNED_JOBS: usize = 262_144;
const MAX_SCANNED_WORKERS: usize = 1_024;
const MAX_ASSIGNED_JOBS_PER_WORKER: usize = 1_024;

#[derive(Clone)]
pub struct CapacityAssessmentAdmission(Arc<Semaphore>);

impl Default for CapacityAssessmentAdmission {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(2)))
    }
}

impl CapacityAssessmentAdmission {
    fn try_acquire(&self) -> WebResult<OwnedSemaphorePermit> {
        Arc::clone(&self.0)
            .try_acquire_owned()
            .map_err(|_| unavailable())
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CapacityProfileRequest {
    pub profile: BuildCapacityProfile,
    /// Whether the intended registration will permit network access.
    pub enable_network: bool,
    /// Whether the intended registration will permit builds.
    pub enable_build: bool,
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CapacityAssessmentRequest {
    /// Omit to assess all projects granting the team its workers; an empty list assesses none.
    pub projects: Option<Vec<ProjectId>>,
    pub profiles: Vec<CapacityProfileRequest>,
    /// Registered team workers whose planned resources can represent capacity still starting.
    #[serde(default)]
    pub tracked_profiles: Vec<CapacityProfileRequest>,
    /// Include once-only RAM credit for a waiting build in the selected batch.
    #[serde(default)]
    pub include_waiting_builds: bool,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct CapacityWaitingBuild {
    pub build_index: usize,
    pub ram_credit_mb: u64,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct CapacityWorker {
    pub free_slots: u32,
    pub available_ram_mb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiting_build: Option<CapacityWaitingBuild>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct TrackedCapacityWorker {
    pub profile: BuildCapacityProfile,
    pub connected: bool,
    /// Index in workers when this connection also supplies scoped ordinary-build capacity.
    pub worker_index: Option<usize>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct CapacityBuild {
    pub architecture: String,
    pub required_features: Vec<String>,
    pub is_fixed_output: bool,
    pub ready_at: NaiveDateTime,
    /// Entries correspond to the response's workers; null means no eligible resource assessment.
    pub workers: Vec<Option<BuildResourceAssessment>>,
    /// Entries correspond to the response's profiles, which are conditional sizing alternatives.
    pub profiles: Vec<Option<BuildProfileAssessment>>,
    /// Entries correspond to tracked_workers; connected workers never receive starting credit.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tracked_workers: Vec<Option<BuildProfileAssessment>>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct CapacityAssessmentResponse {
    /// Capture time, not the age of the underlying worker metrics or model samples.
    pub captured_at: NaiveDateTime,
    pub server_draining: bool,
    pub total_ready_builds: usize,
    pub builds_truncated: bool,
    pub workers: Vec<CapacityWorker>,
    pub profiles: Vec<BuildCapacityProfile>,
    pub builds: Vec<CapacityBuild>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tracked_workers: Vec<TrackedCapacityWorker>,
}

impl CapacityAssessmentResponse {
    fn from_assessment(
        assessment: BuildCapacityAssessment,
        profiles: Vec<BuildCapacityProfile>,
        tracked_profiles: Vec<BuildCapacityProfile>,
        include_waiting_builds: bool,
    ) -> Self {
        let snapshot = &assessment.snapshot;
        let builds = snapshot
            .builds
            .iter()
            .map(|build| CapacityBuild {
                architecture: build.requirement.architecture.clone(),
                required_features: build.requirement.required_features.clone(),
                is_fixed_output: build.is_fixed_output,
                ready_at: build.ready_at,
                workers: snapshot
                    .workers
                    .iter()
                    .map(|worker| snapshot.assess(build, worker))
                    .collect(),
                profiles: assessment
                    .profiles
                    .iter()
                    .map(|profile| assessment.assess_profile(build, profile))
                    .collect(),
                tracked_workers: assessment
                    .tracked_profiles
                    .iter()
                    .map(|profile| assessment.assess_tracked_profile(build, profile))
                    .collect(),
            })
            .collect();
        Self {
            captured_at: snapshot.captured_at,
            server_draining: snapshot.server_draining,
            total_ready_builds: snapshot.total_ready_builds,
            builds_truncated: snapshot.total_ready_builds > snapshot.builds.len(),
            workers: snapshot
                .workers
                .iter()
                .map(|worker| CapacityWorker {
                    free_slots: worker.free_slots,
                    available_ram_mb: worker
                        .caps
                        .metrics
                        .and_then(|metrics| metrics.ram_available_mb()),
                    waiting_build: include_waiting_builds
                        .then(|| snapshot.waiting_build(worker))
                        .flatten()
                        .map(|(build_index, ram_credit_mb)| CapacityWaitingBuild {
                            build_index,
                            ram_credit_mb,
                        }),
                })
                .collect(),
            profiles,
            builds,
            tracked_workers: tracked_profiles
                .into_iter()
                .map(|profile| {
                    let connected = snapshot.connected_workers.contains(&profile.worker_id);
                    let worker_index = snapshot
                        .workers
                        .iter()
                        .position(|worker| worker.id == profile.worker_id);
                    TrackedCapacityWorker {
                        profile,
                        connected,
                        worker_index,
                    }
                })
                .collect(),
        }
    }
}

fn unavailable() -> WebError {
    WebError::ServiceUnavailable(
        ErrorCode::SERVICE_UNAVAILABLE,
        "capacity assessment unavailable; retry later".into(),
    )
}

#[utoipa::path(
    post,
    path = "/teams/{team}/workers/capacity-assessment",
    operation_id = "assessTeamBuildCapacity",
    tag = "teams",
    summary = "Assess local ordinary-build capacity",
    description = "Needs Admin in the team. Only projects granting the team its workers are assessed. This is one scheduler instance's resource snapshot, not placement or a reservation. Builds and live workers use response-local indexes.\n\nReturns the oldest 128 ready ordinary builds with the total backlog and a truncation flag. Live workers are never sampled. Limits are 16 prospective profiles, 256 tracked profiles, 256 project IDs and 64 KiB. Capture, scan or worker-limit failures return 503.\n\nProspective profiles require unregistered UUIDs and planned build/network enable flags. Tracked profiles require active, build-enabled registrations owned by this team. Saved registration flags additionally restrict their network capability. A connected tracked worker never receives a starting-profile assessment, even when it has no scoped live-worker index. All profile fits remain conditional on valid worker tokens, project caches and matching advertised resources.\n\ninclude_waiting_builds adds once-only RAM credit for a waiting build in the selected batch. The credit is the native usable-budget difference, not raw reserved RAM. Fit does not imply a free slot. Unknown predictions remain distinct from fallback reservations. captured_at is not a metrics sample timestamp.",
    params(TeamPath),
    request_body = CapacityAssessmentRequest,
    responses((status = OK, description = "OK", body = CapacityAssessmentResponse), WebError)
)]
#[tracing::instrument(skip_all)]
pub async fn post_team_capacity_assessment(
    TeamScope { team, db, .. }: TeamScope<Admin>,
    Extension(scheduler): Extension<Arc<Scheduler>>,
    Extension(admission): Extension<CapacityAssessmentAdmission>,
    Json(body): Json<CapacityAssessmentRequest>,
) -> WebResult<Response> {
    let permit = admission.try_acquire()?;
    if body.profiles.len() > MAX_PROFILES
        || body.tracked_profiles.len() > MAX_TRACKED_PROFILES
        || body
            .projects
            .as_ref()
            .is_some_and(|projects| projects.len() > MAX_PROJECTS)
    {
        return Err(TeamRefusal::CapacityRequestTooLarge.into());
    }

    let profiles = requested_profiles(body.profiles)?;
    let mut tracked_profiles = requested_profiles(body.tracked_profiles)?;

    let granting: HashSet<ProjectId> =
        gradient_db::teams::workers::projects_granted_with_workers(&db.web_db, team.id)
            .await?
            .into_iter()
            .collect();
    let projects = match body.projects {
        Some(projects) => {
            let projects: HashSet<_> = projects.into_iter().collect();
            if !projects.is_subset(&granting) {
                return Err(TeamRefusal::CapacityProjectNotGranted.into());
            }
            projects
        }
        None => granting,
    };
    if projects.len() > MAX_PROJECTS {
        return Err(unavailable());
    }

    let mut tracked_ids = HashSet::new();
    for profile in &tracked_profiles {
        if !tracked_ids.insert(profile.worker_id.clone()) {
            return Err(TeamRefusal::DuplicateTrackedWorker.into());
        }
    }
    let tracked_rows: HashMap<_, _> = gradient_db::teams::workers::workers_of_ids(
        &db.web_db,
        team.id,
        &tracked_profiles
            .iter()
            .map(|profile| profile.worker_id.clone())
            .collect::<Vec<_>>(),
    )
    .await?
    .into_iter()
    .map(|worker| (worker.worker_id.clone(), worker))
    .collect();
    for profile in &mut tracked_profiles {
        let registered = tracked_rows
            .get(&profile.worker_id)
            .or_not_found(Resource::TeamWorker)?;
        if !registered.active || !registered.enable_build {
            return Err(TeamRefusal::TrackedWorkerCannotBuild.into());
        }
        profile.capabilities.network &= registered.enable_network;
    }

    let mut checked = HashSet::new();
    for profile in &profiles {
        if !checked.insert(&profile.worker_id) {
            continue;
        }
        if gradient_db::worker_registrations::by_worker_id(&db.web_db, &profile.worker_id)
            .await?
            .is_some()
            || gradient_db::teams::workers::is_team_worker(&db.web_db, &profile.worker_id).await?
        {
            return Err(TeamRefusal::WorkerIdRegistered.into());
        }
    }

    assess_capacity(
        scheduler,
        projects,
        profiles,
        tracked_profiles,
        body.include_waiting_builds,
        permit,
    )
    .await
}

fn requested_profiles(
    requests: Vec<CapacityProfileRequest>,
) -> WebResult<Vec<BuildCapacityProfile>> {
    let profiles = requests
        .into_iter()
        .map(|request| {
            let mut profile = request.profile;
            profile.worker_id = Uuid::parse_str(&profile.worker_id)
                .map_err(|_| SharedRefusal::InvalidWorkerId)?
                .to_string();
            profile.capabilities.build &= request.enable_build;
            profile.capabilities.network &= request.enable_network;
            Ok(profile)
        })
        .collect::<WebResult<Vec<_>>>()?;
    BuildCapacityProfile::validate_all(&profiles)
        .map_err(|err| TeamRefusal::InvalidCapacityProfile(err.to_string()))?;
    Ok(profiles)
}

#[tracing::instrument(skip_all)]
async fn assess_capacity(
    scheduler: Arc<Scheduler>,
    projects: HashSet<ProjectId>,
    profiles: Vec<BuildCapacityProfile>,
    tracked_profiles: Vec<BuildCapacityProfile>,
    include_waiting_builds: bool,
    permit: OwnedSemaphorePermit,
) -> WebResult<Response> {
    // The task owns admission even if the HTTP request is cancelled.
    let shutdown = scheduler.db.shutdown.clone();
    shutdown
        .spawn(async move {
            let assessment = scheduler
                .assess_build_capacity_with_tracked_workers(
                    projects,
                    profiles.clone(),
                    tracked_profiles.clone(),
                    BuildCapacityLimits {
                        builds: MAX_BUILDS,
                        workers: MAX_WORKERS,
                        scanned_jobs: MAX_SCANNED_JOBS,
                        scanned_workers: MAX_SCANNED_WORKERS,
                        assigned_jobs_per_worker: MAX_ASSIGNED_JOBS_PER_WORKER,
                    },
                )
                .await
                .map_err(|err| {
                    tracing::warn!(error = %err, "capacity snapshot unavailable");
                    unavailable()
                })?;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                ok_json(CapacityAssessmentResponse::from_assessment(
                    assessment,
                    profiles,
                    tracked_profiles,
                    include_waiting_builds,
                ))
                .into_response()
            })
            .await
            .map_err(|err| WebError::Internal(err.into()))
        })
        .await
        .map_err(|err| WebError::Internal(err.into()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use gradient_test_support::state::TestState;

    #[test]
    fn capacity_admission_is_shared_and_exhaustion_returns_unavailable() {
        let admission = CapacityAssessmentAdmission::default();
        let first = admission.try_acquire().unwrap();
        let second = admission.clone().try_acquire().unwrap();
        assert_eq!(
            admission.try_acquire().unwrap_err().status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        drop(first);
        assert!(admission.try_acquire().is_ok());
        drop(second);
        assert_eq!(admission.0.available_permits(), 2);
    }

    #[tokio::test]
    async fn cancelled_capacity_work_keeps_admission_until_assessment_finishes() {
        let scheduler = Arc::new(Scheduler::new(TestState::new().build()));
        let admission = CapacityAssessmentAdmission::default();
        let mut caller = Box::pin(assess_capacity(
            Arc::clone(&scheduler),
            HashSet::new(),
            vec![],
            vec![],
            false,
            admission.try_acquire().unwrap(),
        ));
        assert!(futures::poll!(caller.as_mut()).is_pending());
        drop(caller);
        assert_eq!(admission.0.available_permits(), 1);
        assert_eq!(scheduler.db.shutdown.pending(), 1);
        let core = scheduler.spawn_core(None).await.unwrap();
        let restored = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            Arc::clone(&admission.0).acquire_many_owned(2),
        )
        .await
        .unwrap()
        .unwrap();
        drop(restored);
        core.stop_and_wait(None, None).await.unwrap();
        assert!(
            scheduler
                .db
                .shutdown
                .cancel_and_drain(std::time::Duration::from_secs(5))
                .await
        );
    }
}
