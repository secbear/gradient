/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use std::collections::HashSet;

use chrono::NaiveDateTime;
use gradient_pool::estimate::{BuildEstimate, MemoryAssumption};
use gradient_pool::score::rules::estimated_time::{self, TimeEstimate};
use gradient_pool::score::{
    InstanceContext, JobContext, RamFit, RamNeed, ScoredJob, WorkerContext,
};
use gradient_pool::{WorkerCaps, WorkerJob};
use gradient_types::ids::{DerivationBuildId, ProjectId};
use gradient_wire::types::{BuildRequirement, BuildSpecKind};

#[derive(Debug, Clone)]
pub struct ReadyBuild {
    pub id: DerivationBuildId,
    pub project: ProjectId,
    pub requirement: BuildRequirement,
    pub is_fixed_output: bool,
    pub prefer_local_build: bool,
    pub ready_at: NaiveDateTime,
    pub estimate: BuildEstimate,
}

#[derive(Debug, Clone)]
pub struct BuildCapacityWorker {
    pub id: String,
    pub caps: WorkerCaps,
    pub free_slots: u32,
    pub(crate) authorized: Option<HashSet<ProjectId>>,
    pub(crate) waiting_build: Option<DerivationBuildId>,
}

/// One instance's ready ordinary builds and worker resources, not a placement or reservation.
#[derive(Debug, Clone)]
pub struct BuildCapacitySnapshot {
    pub captured_at: NaiveDateTime,
    pub server_draining: bool,
    pub builds: Vec<ReadyBuild>,
    pub workers: Vec<BuildCapacityWorker>,
    pub instance: InstanceContext,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BuildResourceAssessment {
    pub cores: u32,
    /// The policy reservation, not a measured peak; absent when no memory estimate exists.
    pub reserved_ram_mb: Option<u64>,
    /// The original prediction at the chosen cores, before any proportional fallback.
    pub predicted_ram_mb: Option<u64>,
    pub ram_fit: Option<RamFit>,
    pub memory_assumption: MemoryAssumption,
    pub own_samples: u32,
    pub time: TimeEstimate,
}

impl BuildCapacitySnapshot {
    /// Evaluate captured resources without claiming a job or changing its worker.
    /// Worker-local store scores are not included.
    pub fn assess(
        &self,
        build: &ReadyBuild,
        worker: &BuildCapacityWorker,
    ) -> Option<BuildResourceAssessment> {
        if !crate::jobs::authorized_for(build.project, worker.authorized.as_ref())
            || !worker.caps.allowed_jobs.contains(WorkerJob::Build)
            || !worker.caps.can_run_build_kind(
                &build.requirement,
                BuildSpecKind::Build,
                build.is_fixed_output,
            )
        {
            return None;
        }

        let mut metrics = worker.caps.metrics?;
        if worker.waiting_build == Some(build.id) {
            metrics.ram_reserved_for_waiting_build_mb = 0;
        }
        let job = ScoredJob::new_build(
            "",
            build.project,
            &build.requirement.architecture,
            build.prefer_local_build,
            build.is_fixed_output,
            None,
            None,
            &build.estimate,
        );
        let context = JobContext {
            job: &job,
            ready_at: build.ready_at,
            now: self.captured_at,
            ..JobContext::default()
        };
        let unmeasured_mb = self.instance.unmeasured_build_ram_mb();
        let assessed = context.core_assessment(&metrics, unmeasured_mb);
        let reserved_ram_mb = match assessed.choice.need {
            RamNeed::Predicted(mb) => Some(mb),
            RamNeed::Negligible => Some(0),
            RamNeed::Unmeasured => self
                .instance
                .peak_ram_mb
                .w24h
                .or(self.instance.peak_ram_mb.w1h)
                .map(|_| unmeasured_mb),
        };
        let ram_fit = reserved_ram_mb.and_then(|mb| {
            (assessed.memory_assumption != MemoryAssumption::Negligible
                && metrics.ram_available_mb().is_some())
            .then(|| metrics.ram_fit(mb))
        });
        let worker_context = WorkerContext {
            architectures: &worker.caps.architectures,
            system_features: &worker.caps.system_features,
            fetches_flakes: worker.caps.capabilities.network,
            metrics: Some(metrics),
            exec: None,
        };

        Some(BuildResourceAssessment {
            cores: assessed.choice.cores,
            reserved_ram_mb,
            predicted_ram_mb: build.estimate.ram_mb(assessed.choice.cores),
            ram_fit,
            memory_assumption: assessed.memory_assumption,
            own_samples: build.estimate.own_samples,
            time: estimated_time::estimate(&context, &worker_context, &self.instance),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gradient_pool::score::{Windowed, WorkerMetricsView};
    use gradient_wire::types::GradientCapabilities;

    fn snapshot(estimate: BuildEstimate, metrics: WorkerMetricsView) -> BuildCapacitySnapshot {
        let project = ProjectId::now_v7();
        BuildCapacitySnapshot {
            captured_at: gradient_types::now(),
            server_draining: false,
            builds: vec![ReadyBuild {
                id: DerivationBuildId::now_v7(),
                project,
                requirement: BuildRequirement {
                    architecture: "x86_64-linux".into(),
                    required_features: vec![],
                },
                is_fixed_output: false,
                prefer_local_build: false,
                ready_at: gradient_types::now(),
                estimate,
            }],
            workers: vec![BuildCapacityWorker {
                id: "worker".into(),
                caps: WorkerCaps {
                    architectures: vec!["x86_64-linux".into()],
                    capabilities: GradientCapabilities {
                        build: true,
                        ..Default::default()
                    },
                    metrics: Some(metrics),
                    ..Default::default()
                },
                free_slots: 2,
                authorized: Some(HashSet::from([project])),
                waiting_build: None,
            }],
            instance: InstanceContext::default(),
        }
    }

    fn worker() -> WorkerMetricsView {
        WorkerMetricsView {
            cpu_count: 64,
            cpu_core_score: 4_000,
            ram_total_mb: 60_000,
            ram_free_mb: Some(60_000),
            ..Default::default()
        }
    }

    #[test]
    fn a_proportional_fit_keeps_the_original_prediction_visible() {
        let snapshot = snapshot(
            BuildEstimate::fixed(Some(100_000), Some(60_000), 5),
            worker(),
        );
        let assessed = snapshot
            .assess(&snapshot.builds[0], &snapshot.workers[0])
            .unwrap();
        assert_eq!(assessed.memory_assumption, MemoryAssumption::Proportional);
        assert_eq!(assessed.cores, 29);
        assert_eq!(assessed.reserved_ram_mb, Some(45_313));
        assert_eq!(assessed.predicted_ram_mb, Some(100_000));
        assert_eq!(assessed.ram_fit, Some(RamFit::Fits));
        assert_eq!(assessed.own_samples, 5);
        assert_eq!(assessed.time.cores, Some(assessed.cores));
        assert!((assessed.time.build_secs - 60.0).abs() < 1e-9);
        assert!(
            assessed
                .time
                .fallbacks
                .contains(&estimated_time::Fallback::MissingNarSize)
        );
    }

    #[test]
    fn missing_memory_history_or_worker_capacity_is_not_a_known_fit() {
        for (estimate, metrics, memory) in [
            (BuildEstimate::default(), worker(), None),
            (
                BuildEstimate::fixed(Some(1_000), None, 1),
                WorkerMetricsView {
                    ram_total_mb: 0,
                    ram_free_mb: None,
                    ..worker()
                },
                Some(1_000),
            ),
        ] {
            let snapshot = snapshot(estimate, metrics);
            let assessed = snapshot
                .assess(&snapshot.builds[0], &snapshot.workers[0])
                .unwrap();
            assert_eq!(assessed.reserved_ram_mb, memory);
            assert_eq!(assessed.ram_fit, None);
        }
    }

    #[test]
    fn unmeasured_builds_keep_the_instance_fallback_distinct() {
        let mut snapshot = snapshot(BuildEstimate::default(), worker());
        snapshot.instance.peak_ram_mb = Windowed {
            w24h: Some(2_000.0),
            ..Default::default()
        };
        snapshot.instance.build_time_median_ms = Windowed {
            w24h: Some(60_000.0),
            ..Default::default()
        };
        let assessed = snapshot
            .assess(&snapshot.builds[0], &snapshot.workers[0])
            .unwrap();
        assert_eq!(assessed.reserved_ram_mb, Some(2_000));
        assert_eq!(assessed.predicted_ram_mb, None);
        assert_eq!(assessed.memory_assumption, MemoryAssumption::Unmeasured);
        assert_eq!(assessed.ram_fit, Some(RamFit::Fits));
        assert_eq!(assessed.time.build_secs, 60.0);
        assert!(
            assessed
                .time
                .fallbacks
                .contains(&estimated_time::Fallback::BuildHistory)
        );
    }

    #[test]
    fn a_zero_policy_reservation_is_not_a_known_memory_requirement() {
        let mut snapshot = snapshot(BuildEstimate::default(), worker());
        snapshot.builds[0].requirement.architecture = "builtin".into();
        let assessed = snapshot
            .assess(&snapshot.builds[0], &snapshot.workers[0])
            .unwrap();
        assert_eq!(assessed.memory_assumption, MemoryAssumption::Negligible);
        assert_eq!(assessed.reserved_ram_mb, Some(0));
        assert_eq!(assessed.predicted_ram_mb, None);
        assert_eq!(assessed.ram_fit, None);
    }

    #[test]
    fn a_waiting_build_does_not_count_its_own_reserved_memory_twice() {
        let mut snapshot = snapshot(
            BuildEstimate::fixed(Some(40_000), None, 3),
            WorkerMetricsView {
                ram_reserved_for_waiting_build_mb: 40_000,
                ..worker()
            },
        );
        snapshot.workers[0].waiting_build = Some(snapshot.builds[0].id);
        let own = snapshot
            .assess(&snapshot.builds[0], &snapshot.workers[0])
            .unwrap();
        assert_eq!(own.ram_fit, Some(RamFit::Fits));
        let mut other = snapshot.builds[0].clone();
        other.id = DerivationBuildId::now_v7();
        let beside = snapshot.assess(&other, &snapshot.workers[0]).unwrap();
        assert_eq!(beside.ram_fit, Some(RamFit::FitsOnceRamIsReleased));
    }

    #[test]
    fn build_capabilities_features_and_project_permissions_gate_assessment() {
        let snapshot = snapshot(BuildEstimate::fixed(Some(1_000), None, 3), worker());
        for (architecture, features, build, network, fixed_output, authorized, expected) in [
            ("x86_64-linux", vec![], true, false, false, true, true),
            ("builtin", vec![], true, false, false, true, true),
            ("aarch64-linux", vec![], true, false, false, true, false),
            (
                "x86_64-linux",
                vec!["kvm".into()],
                true,
                false,
                false,
                true,
                false,
            ),
            ("x86_64-linux", vec![], false, true, false, true, false),
            ("x86_64-linux", vec![], true, false, true, true, false),
            ("x86_64-linux", vec![], true, true, true, true, true),
            ("x86_64-linux", vec![], true, true, false, false, false),
        ] {
            let mut job = snapshot.builds[0].clone();
            job.requirement.architecture = architecture.into();
            job.requirement.required_features = features;
            job.is_fixed_output = fixed_output;
            let mut worker = snapshot.workers[0].clone();
            worker.caps.capabilities.build = build;
            worker.caps.capabilities.network = network;
            if !authorized {
                worker.authorized = Some(HashSet::new());
            }
            assert_eq!(snapshot.assess(&job, &worker).is_some(), expected);
        }
    }
}
