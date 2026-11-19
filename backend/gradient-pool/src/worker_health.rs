/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use std::time::{Duration, Instant};

use gradient_types::now;
use gradient_wire::types::{WorkerHealth, WorkerHealthCheck};

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct WorkerHealthInfo {
    pub ready: bool,
    pub checks: Vec<WorkerHealthCheck>,
    /// When the server received the report, not when each check ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<chrono::NaiveDateTime>,
    /// When the report stops permitting new assignments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::NaiveDateTime>,
}

#[derive(Debug, Clone)]
struct ReportedHealth {
    health: WorkerHealth,
    observed_at: chrono::NaiveDateTime,
    expires_at: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerReadiness {
    required: bool,
    report: Option<ReportedHealth>,
}

impl WorkerReadiness {
    pub fn is_required(&self) -> bool {
        self.required
    }

    pub fn pending(required: bool) -> Self {
        Self {
            required,
            report: None,
        }
    }

    pub fn update(&mut self, health: WorkerHealth) -> anyhow::Result<()> {
        anyhow::ensure!(self.required, "worker did not declare readiness checks");
        health.validate()?;
        let expires_at = Instant::now() + Duration::from_secs(u64::from(health.valid_for_secs));
        self.report = Some(ReportedHealth {
            health,
            observed_at: now(),
            expires_at,
        });
        Ok(())
    }

    pub fn permits_jobs(&self) -> bool {
        !self.required
            || self
                .report
                .as_ref()
                .is_some_and(|report| Instant::now() < report.expires_at && report.health.ready())
    }

    pub fn info(&self) -> Option<WorkerHealthInfo> {
        self.required.then(|| WorkerHealthInfo {
            ready: self.permits_jobs(),
            checks: self
                .report
                .as_ref()
                .map(|report| report.health.checks.clone())
                .unwrap_or_default(),
            observed_at: self.report.as_ref().map(|report| report.observed_at),
            expires_at: self.report.as_ref().map(|report| {
                report.observed_at
                    + chrono::Duration::seconds(i64::from(report.health.valid_for_secs))
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gradient_wire::types::WorkerCheckStatus;

    fn report(healthy: bool, valid_for_secs: u32) -> WorkerHealth {
        WorkerHealth {
            checks: vec![WorkerHealthCheck {
                name: "nix-store".into(),
                status: if healthy {
                    WorkerCheckStatus::Passed
                } else {
                    WorkerCheckStatus::Failed
                },
                healthy,
            }],
            valid_for_secs,
        }
    }

    #[test]
    fn legacy_workers_need_no_health_report() {
        let mut readiness = WorkerReadiness::default();
        assert!(readiness.permits_jobs());
        assert!(readiness.info().is_none());
        assert!(readiness.update(report(true, 30)).is_err());
    }

    #[test]
    fn readiness_is_required_before_the_first_result_and_can_recover() {
        let mut readiness = WorkerReadiness::pending(true);
        assert!(!readiness.permits_jobs());
        let info = readiness.info().unwrap();
        assert!(info.checks.is_empty());
        assert!(info.observed_at.is_none());
        readiness.update(report(true, 30)).unwrap();
        assert!(readiness.permits_jobs());
        readiness.update(report(false, 30)).unwrap();
        assert!(!readiness.permits_jobs());
        readiness.update(report(true, 30)).unwrap();
        assert!(readiness.permits_jobs());
    }

    #[test]
    fn copying_a_report_does_not_renew_its_deadline() {
        let mut readiness = WorkerReadiness::pending(true);
        readiness.update(report(true, 30)).unwrap();
        readiness.report.as_mut().unwrap().expires_at = Instant::now();
        let restored = readiness.clone();
        assert!(!restored.permits_jobs());
        assert!(!restored.info().unwrap().ready);
    }
}
