/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use crate::refusal::{Refusal, RefusalStatus};

#[derive(Debug, thiserror::Error)]
pub(crate) enum TeamRefusal {
    #[error("A grant includes users, workers or both.")]
    EmptyGrant,
    #[error("A grant includes users, workers or both. Remove the grant instead.")]
    EmptyGrantAfterUpdate,
    #[error("A role is required when the grant includes users.")]
    RoleRequiredForUsers,
    #[error("Turning on a team's workers needs Admin in the team.")]
    WorkersNeedTeamAdmin,
    #[error("new_project_role must be Admin, Write or View")]
    InvalidNewProjectRole,
    #[error("member is managed by server state")]
    MemberManagedByState,
    #[error("the worker id is already registered")]
    WorkerIdRegistered,
    #[error(
        "capacity assessment accepts at most 16 prospective profiles, 256 tracked profiles and 256 project IDs"
    )]
    CapacityRequestTooLarge,
    #[error("invalid build capacity profile: {0}")]
    InvalidCapacityProfile(String),
    #[error("capacity assessment requires a worker grant for every requested project")]
    CapacityProjectNotGranted,
    #[error("a tracked worker identity is included more than once")]
    DuplicateTrackedWorker,
    #[error("a tracked team worker is inactive or does not permit builds")]
    TrackedWorkerCannotBuild,
    #[error("invalid capacity cursor; restart the build-page assessment")]
    InvalidCapacityCursor,
    #[error("bootstrap assessment needs explicit prospective registration permissions")]
    BootstrapRegistrationRequired,
    #[error("a prospective worker identity is included more than once")]
    DuplicateCapacityWorker,
    #[error("a tracked team worker is inactive")]
    TrackedWorkerInactive,
}

impl Refusal for TeamRefusal {
    fn status(&self) -> RefusalStatus {
        match self {
            Self::EmptyGrant
            | Self::EmptyGrantAfterUpdate
            | Self::RoleRequiredForUsers
            | Self::InvalidNewProjectRole
            | Self::CapacityRequestTooLarge
            | Self::InvalidCapacityProfile(_) => RefusalStatus::BadRequest,
            Self::DuplicateTrackedWorker
            | Self::InvalidCapacityCursor
            | Self::BootstrapRegistrationRequired
            | Self::DuplicateCapacityWorker => RefusalStatus::BadRequest,
            Self::WorkersNeedTeamAdmin | Self::CapacityProjectNotGranted => {
                RefusalStatus::Forbidden
            }
            Self::MemberManagedByState
            | Self::WorkerIdRegistered
            | Self::TrackedWorkerCannotBuild => RefusalStatus::Conflict,
            Self::TrackedWorkerInactive => RefusalStatus::Conflict,
        }
    }
}
