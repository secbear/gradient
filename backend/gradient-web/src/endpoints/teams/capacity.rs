/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use super::refusal::TeamRefusal;
use crate::api_spec::TeamPath;
use crate::error::{ErrorCode, WebError, WebResult};
use crate::helpers::ok_json;
use crate::refusal::SharedRefusal;
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
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(crate) const REQUEST_BYTES: usize = 64 * 1024;
const MAX_PROFILES: usize = 16;
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
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct CapacityWorker {
    pub free_slots: u32,
    pub available_ram_mb: Option<u64>,
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
}

impl CapacityAssessmentResponse {
    fn from_assessment(
        assessment: BuildCapacityAssessment,
        profiles: Vec<BuildCapacityProfile>,
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
                })
                .collect(),
            profiles,
            builds,
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
    description = "Needs Admin in the team. Assesses only projects granting the team its workers. This is one scheduler instance's resource snapshot, not placement or a reservation. Workers and builds have response-local indexes, not stable identities. The oldest 128 ready builds are returned with the total backlog and an explicit truncation flag; workers are never sampled. Accepts at most 16 profiles, 256 project IDs and 64 KiB. Returns 503 when capture, scan or worker limits prevent assessment. Profiles must use unregistered UUIDs; their advertised build/network capabilities are intersected with the intended registration's enable flags. Profile results are conditional on registering that exact ID under this team and satisfying worker token and project cache requirements. Profiles are alternatives, not live or starting supply. Fit does not imply a free build slot; unknown predictions and fallback reservations remain distinct. captured_at is not a metrics sample timestamp.",
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
        || body
            .projects
            .as_ref()
            .is_some_and(|projects| projects.len() > MAX_PROJECTS)
    {
        return Err(TeamRefusal::CapacityRequestTooLarge.into());
    }

    let mut profiles = Vec::with_capacity(body.profiles.len());
    for request in body.profiles {
        let mut profile = request.profile;
        profile.worker_id = Uuid::parse_str(&profile.worker_id)
            .map_err(|_| SharedRefusal::InvalidWorkerId)?
            .to_string();
        profile.capabilities.build &= request.enable_build;
        profile.capabilities.network &= request.enable_network;
        profiles.push(profile);
    }
    BuildCapacityProfile::validate_all(&profiles)
        .map_err(|err| TeamRefusal::InvalidCapacityProfile(err.to_string()))?;

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

    assess_capacity(scheduler, projects, profiles, permit).await
}

#[tracing::instrument(skip_all)]
async fn assess_capacity(
    scheduler: Arc<Scheduler>,
    projects: HashSet<ProjectId>,
    profiles: Vec<BuildCapacityProfile>,
    permit: OwnedSemaphorePermit,
) -> WebResult<Response> {
    // The task owns admission even if the HTTP request is cancelled.
    let shutdown = scheduler.db.shutdown.clone();
    shutdown
        .spawn(async move {
            let assessment = scheduler
                .assess_build_capacity_with_limits(
                    projects,
                    profiles.clone(),
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
                    assessment, profiles,
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
