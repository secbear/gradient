/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::codec::Proto;

#[derive(Proto, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
pub struct WorkerHealth {
    pub checks: Vec<WorkerHealthCheck>,
    pub valid_for_secs: u32,
}

impl WorkerHealth {
    pub fn validate(&self) -> Result<(), WorkerHealthError> {
        if self.checks.is_empty() {
            return Err(WorkerHealthError::EmptyChecks);
        }
        if self.checks.len() > 32 {
            return Err(WorkerHealthError::TooManyChecks);
        }
        if self.valid_for_secs > 86_400 {
            return Err(WorkerHealthError::InvalidValidity);
        }
        let mut names = HashSet::new();
        for check in &self.checks {
            if check.name.is_empty()
                || check.name.len() > 80
                || !check.name.bytes().all(|byte| (b' '..=b'~').contains(&byte))
            {
                return Err(WorkerHealthError::InvalidName);
            }
            if !names.insert(&check.name) {
                return Err(WorkerHealthError::DuplicateName);
            }
        }
        Ok(())
    }

    pub fn ready(&self) -> bool {
        self.valid_for_secs > 0
            && !self.checks.is_empty()
            && self.checks.iter().all(|check| check.healthy)
    }
}

#[derive(Proto, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, utoipa::ToSchema)]
pub struct WorkerHealthCheck {
    pub name: String,
    /// Result of the latest attempt.
    pub status: WorkerCheckStatus,
    /// Whether the configured success and failure thresholds permit new work.
    pub healthy: bool,
}

#[derive(Proto, Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkerCheckStatus {
    Pending,
    Passed,
    Failed,
    TimedOut,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WorkerHealthError {
    #[error("worker health must contain at least one check")]
    EmptyChecks,
    #[error("worker health has more than 32 checks")]
    TooManyChecks,
    #[error("worker health validity exceeds 86400 seconds")]
    InvalidValidity,
    #[error("worker health check names must be 1 to 80 printable ASCII characters")]
    InvalidName,
    #[error("worker health check names must be unique")]
    DuplicateName,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health(names: &[&str]) -> WorkerHealth {
        WorkerHealth {
            checks: names
                .iter()
                .map(|name| WorkerHealthCheck {
                    name: (*name).into(),
                    status: WorkerCheckStatus::Passed,
                    healthy: true,
                })
                .collect(),
            valid_for_secs: 86_400,
        }
    }

    #[test]
    fn validates_report_limits_and_names() {
        let names: Vec<_> = (0..32).map(|n| format!("check-{n}")).collect();
        let mut report = health(&names.iter().map(String::as_str).collect::<Vec<_>>());
        assert_eq!(report.validate(), Ok(()));
        report.checks.push(health(&["extra"]).checks.remove(0));
        assert_eq!(report.validate(), Err(WorkerHealthError::TooManyChecks));
        for name in ["", "\n", "\x7f", "non-ascii-\u{e9}", &"x".repeat(81)] {
            assert_eq!(
                health(&[name]).validate(),
                Err(WorkerHealthError::InvalidName)
            );
        }
        assert_eq!(health(&[&"x".repeat(80)]).validate(), Ok(()));
        assert_eq!(
            health(&["same", "same"]).validate(),
            Err(WorkerHealthError::DuplicateName)
        );
        assert_eq!(health(&[]).validate(), Err(WorkerHealthError::EmptyChecks));
        let mut report = health(&["check"]);
        report.valid_for_secs += 1;
        assert_eq!(report.validate(), Err(WorkerHealthError::InvalidValidity));
        report.valid_for_secs = 0;
        assert_eq!(report.validate(), Ok(()));
    }

    #[test]
    fn readiness_uses_healthy_not_diagnostic_status() {
        let mut report = health(&["first", "second"]);
        report.checks[0].status = WorkerCheckStatus::Failed;
        assert!(report.ready());
        report.checks[0].healthy = false;
        report.checks[0].status = WorkerCheckStatus::Passed;
        assert!(!report.ready());
        report.checks[0].healthy = true;
        report.valid_for_secs = 0;
        assert!(!report.ready());
        assert!(!health(&[]).ready());
    }

    #[test]
    fn statuses_use_snake_case_json() {
        for (status, expected) in [
            (WorkerCheckStatus::Pending, "\"pending\""),
            (WorkerCheckStatus::Passed, "\"passed\""),
            (WorkerCheckStatus::Failed, "\"failed\""),
            (WorkerCheckStatus::TimedOut, "\"timed_out\""),
            (WorkerCheckStatus::Error, "\"error\""),
        ] {
            assert_eq!(serde_json::to_string(&status).unwrap(), expected);
            assert_eq!(
                serde_json::from_str::<WorkerCheckStatus>(expected).unwrap(),
                status
            );
        }
    }
}
