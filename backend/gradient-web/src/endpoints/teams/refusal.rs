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
    #[error("capacity assessment accepts at most 16 profiles and 256 project IDs")]
    CapacityRequestTooLarge,
    #[error("invalid build capacity profile: {0}")]
    InvalidCapacityProfile(String),
    #[error("capacity assessment requires a worker grant for every requested project")]
    CapacityProjectNotGranted,
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
            Self::WorkersNeedTeamAdmin | Self::CapacityProjectNotGranted => {
                RefusalStatus::Forbidden
            }
            Self::MemberManagedByState | Self::WorkerIdRegistered => RefusalStatus::Conflict,
        }
    }
}
