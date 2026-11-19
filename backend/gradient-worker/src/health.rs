/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use std::io::Read as _;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, ensure};
use gradient_util::sync::Mutex;
use gradient_wire::messages::{WorkerCheckStatus, WorkerHealth, WorkerHealthCheck};
use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use serde::Deserialize;
use tokio::process::Command;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const MAX_FILE_BYTES: u64 = 64 * 1024;
const CONCURRENCY: usize = 4;
const VALID_FOR: Duration = Duration::from_secs(120);
const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);
const REAP_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Check {
    name: String,
    command: Vec<String>,
    #[serde(default = "default_timeout")]
    timeout_secs: u32,
    #[serde(default = "default_interval")]
    interval_secs: Option<u32>,
    #[serde(default = "default_threshold")]
    failure_threshold: u32,
    #[serde(default = "default_threshold")]
    success_threshold: u32,
}

fn default_timeout() -> u32 {
    5
}

fn default_interval() -> Option<u32> {
    Some(30)
}

fn default_threshold() -> u32 {
    1
}

fn parse(raw: &[u8]) -> Result<Vec<Check>> {
    let checks: Vec<Check> = serde_json::from_slice(raw).context("parse worker health checks")?;
    WorkerHealth {
        checks: checks
            .iter()
            .map(|check| WorkerHealthCheck {
                name: check.name.clone(),
                status: WorkerCheckStatus::Pending,
                healthy: false,
            })
            .collect(),
        valid_for_secs: 120,
    }
    .validate()
    .context("validate worker health checks")?;
    for check in &checks {
        ensure!(
            !check.command.is_empty()
                && !check.command[0].is_empty()
                && check.command.iter().all(|arg| !arg.contains('\0')),
            "health check command must contain an executable and valid arguments"
        );
        ensure!(
            (1..=300).contains(&check.timeout_secs),
            "health check timeoutSecs must be between 1 and 300"
        );
        ensure!(
            check
                .interval_secs
                .is_none_or(|secs| (1..=3600).contains(&secs)),
            "health check intervalSecs must be null or between 1 and 3600"
        );
        ensure!(
            (1..=100).contains(&check.failure_threshold)
                && (1..=100).contains(&check.success_threshold),
            "health check thresholds must be between 1 and 100"
        );
    }
    Ok(checks)
}

#[derive(Clone)]
struct Snapshot {
    checks: Vec<WorkerHealthCheck>,
    periodic: Vec<bool>,
    expires: Option<Instant>,
    checkpoint: Arc<Mutex<Instant>>,
}

#[derive(Clone, Default)]
pub(crate) struct HealthChecks {
    snapshot: Option<watch::Receiver<Snapshot>>,
    report_interval: Option<Duration>,
}

impl HealthChecks {
    pub(crate) fn start(
        path: Option<&Path>,
        stop: CancellationToken,
        tasks: &TaskTracker,
    ) -> Result<Self> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let mut raw = Vec::new();
        std::fs::File::open(path)
            .context("open worker health checks file")?
            .take(MAX_FILE_BYTES + 1)
            .read_to_end(&mut raw)
            .context("read worker health checks file")?;
        ensure!(
            raw.len() as u64 <= MAX_FILE_BYTES,
            "health checks file exceeds 64 KiB"
        );
        Ok(Self::start_checks(parse(&raw)?, stop, tasks))
    }

    fn start_checks(checks: Vec<Check>, stop: CancellationToken, tasks: &TaskTracker) -> Self {
        let report_interval = checks
            .iter()
            .filter_map(|check| {
                check.interval_secs.map(|interval| {
                    Duration::from_secs(u64::from(interval) + u64::from(check.timeout_secs)) / 2
                })
            })
            .fold(DEFAULT_REPORT_INTERVAL, Duration::min);
        let snapshot = Snapshot {
            checks: checks
                .iter()
                .map(|check| WorkerHealthCheck {
                    name: check.name.clone(),
                    status: WorkerCheckStatus::Pending,
                    healthy: false,
                })
                .collect(),
            periodic: checks
                .iter()
                .map(|check| check.interval_secs.is_some())
                .collect(),
            expires: None,
            checkpoint: Arc::new(Mutex::new(Instant::now())),
        };
        let checkpoint = Arc::clone(&snapshot.checkpoint);
        let (tx, rx) = watch::channel(snapshot);
        tasks.spawn(run(checks, tx, stop, checkpoint));
        Self {
            snapshot: Some(rx),
            report_interval: Some(report_interval),
        }
    }

    pub(crate) fn required(&self) -> bool {
        self.snapshot.is_some()
    }

    pub(crate) fn report_interval(&self) -> Duration {
        self.report_interval.unwrap_or(DEFAULT_REPORT_INTERVAL)
    }

    pub(crate) async fn changed(&mut self) {
        let Some(snapshot) = self.snapshot.as_mut() else {
            return std::future::pending().await;
        };
        if snapshot.changed().await.is_err() {
            std::future::pending().await
        }
    }

    pub(crate) fn ready(&self) -> bool {
        self.report().is_none_or(|report| report.ready())
    }

    pub(crate) fn report(&self) -> Option<WorkerHealth> {
        let rx = self.snapshot.as_ref()?;
        let snapshot = rx.borrow();
        let checkpoint_expiry = *snapshot.checkpoint.lock() + VALID_FOR;
        let remaining = snapshot
            .expires
            .unwrap_or(checkpoint_expiry)
            .min(checkpoint_expiry)
            .saturating_duration_since(Instant::now());
        let valid_for_secs = if rx.has_changed().is_err() {
            0
        } else {
            u32::try_from(remaining.as_secs()).unwrap_or(120)
        };
        let mut checks = snapshot.checks.clone();
        if valid_for_secs == 0 {
            for (check, periodic) in checks.iter_mut().zip(&snapshot.periodic) {
                if *periodic {
                    check.status = WorkerCheckStatus::Pending;
                }
                check.healthy = false;
            }
        }
        Some(WorkerHealth {
            checks,
            valid_for_secs,
        })
    }

    #[cfg(test)]
    pub(crate) fn controlled(healthy: bool) -> (Self, impl Fn(bool)) {
        let (tx, rx) = watch::channel(Snapshot {
            checks: vec![WorkerHealthCheck {
                name: "probe".into(),
                status: if healthy {
                    WorkerCheckStatus::Passed
                } else {
                    WorkerCheckStatus::Failed
                },
                healthy,
            }],
            periodic: vec![true],
            expires: Some(Instant::now() + VALID_FOR),
            checkpoint: Arc::new(Mutex::new(Instant::now())),
        });
        (
            Self {
                snapshot: Some(rx),
                ..Self::default()
            },
            move |healthy| {
                tx.send_modify(|snapshot| {
                    snapshot.checks[0].healthy = healthy;
                    snapshot.checks[0].status = if healthy {
                        WorkerCheckStatus::Passed
                    } else {
                        WorkerCheckStatus::Failed
                    };
                    snapshot.expires = Some(Instant::now() + VALID_FOR);
                    *snapshot.checkpoint.lock() = Instant::now();
                })
            },
        )
    }
}

struct State {
    definition: Check,
    report: WorkerHealthCheck,
    successes: u32,
    failures: u32,
    next: Instant,
    expires: Instant,
    running: bool,
}

impl State {
    fn completed(&mut self, status: WorkerCheckStatus) {
        if status == WorkerCheckStatus::Passed {
            self.failures = 0;
            self.successes = self.successes.saturating_add(1);
            if self.successes >= self.definition.success_threshold {
                self.report.healthy = true;
            }
        } else {
            self.successes = 0;
            self.failures = self.failures.saturating_add(1);
            if self.failures >= self.definition.failure_threshold {
                self.report.healthy = false;
            }
        }
        self.report.status = status;
        self.running = false;
        self.next = Instant::now()
            + Duration::from_secs(u64::from(self.definition.interval_secs.unwrap_or(30)));
        // Allow a polling tick and whole-second validity rounding.
        self.expires = self.next + Duration::from_secs(u64::from(self.definition.timeout_secs) + 2);
    }

    fn due(&self, now: Instant) -> bool {
        !self.running
            && self.next <= now
            && (self.definition.interval_secs.is_some() || !self.report.healthy)
    }
}

async fn run(
    checks: Vec<Check>,
    tx: watch::Sender<Snapshot>,
    stop: CancellationToken,
    checkpoint: Arc<Mutex<Instant>>,
) {
    let mut states: Vec<_> = checks
        .into_iter()
        .map(|definition| State {
            report: WorkerHealthCheck {
                name: definition.name.clone(),
                status: WorkerCheckStatus::Pending,
                healthy: false,
            },
            expires: Instant::now() + Duration::from_secs(u64::from(definition.timeout_secs) + 1),
            definition,
            successes: 0,
            failures: 0,
            next: Instant::now(),
            running: false,
        })
        .collect();
    let mut running = JoinSet::<(usize, WorkerCheckStatus)>::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            Some(result) = running.join_next(), if !running.is_empty() => {
                match result {
                    Ok((index, status)) => states[index].completed(status),
                    Err(_) => break,
                }
            }
            _ = tick.tick() => {},
        }
        let now = Instant::now();
        *checkpoint.lock() = now;
        while running.len() < CONCURRENCY {
            let Some((index, state)) = states
                .iter_mut()
                .enumerate()
                .filter(|(_, state)| state.due(now))
                .min_by_key(|(_, state)| state.next)
            else {
                break;
            };
            state.running = true;
            let check = state.definition.clone();
            let stop = stop.clone();
            running.spawn(async move { (index, execute(&check, &stop).await) });
        }
        tx.send_if_modified(|snapshot| {
            let checks: Vec<_> = states.iter().map(|state| state.report.clone()).collect();
            // Runner liveness cannot extend a check's observation deadline.
            let expires = states
                .iter()
                .filter(|state| state.definition.interval_secs.is_some())
                .map(|state| state.expires)
                .min();
            if snapshot.checks == checks && snapshot.expires == expires {
                return false;
            }
            snapshot.checks = checks;
            snapshot.expires = expires;
            true
        });
    }
    stop.cancel();
    while running.join_next().await.is_some() {}
}

struct ProcessGroup(Pid);

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = killpg(self.0, Signal::SIGKILL);
    }
}

async fn execute(check: &Check, stop: &CancellationToken) -> WorkerCheckStatus {
    let mut command = Command::new(&check.command[0]);
    command
        .args(&check.command[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
    let Ok(mut child) = command.spawn() else {
        return WorkerCheckStatus::Error;
    };
    let group = child
        .id()
        .map(|pid| ProcessGroup(Pid::from_raw(pid as i32)));
    let status = tokio::select! {
        biased;
        () = stop.cancelled() => WorkerCheckStatus::Error,
        result = tokio::time::timeout(
            Duration::from_secs(u64::from(check.timeout_secs)),
            child.wait(),
        ) => match result {
            Ok(Ok(status)) if status.success() => WorkerCheckStatus::Passed,
            Ok(Ok(_)) => WorkerCheckStatus::Failed,
            Ok(Err(_)) => WorkerCheckStatus::Error,
            Err(_) => WorkerCheckStatus::TimedOut,
        },
    };
    drop(group);
    if let Err(error) = child.start_kill() {
        tracing::warn!(%error, "health check child could not be killed");
    }
    match tokio::time::timeout(REAP_TIMEOUT, child.wait()).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => tracing::warn!(%error, "health check child could not be reaped"),
        Err(_) => tracing::warn!("health check child did not exit within cleanup timeout"),
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(command: &[&str]) -> Check {
        Check {
            name: "probe".into(),
            command: command.iter().map(|arg| (*arg).into()).collect(),
            timeout_secs: 5,
            interval_secs: Some(1),
            failure_threshold: 1,
            success_threshold: 1,
        }
    }

    async fn eventually(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("condition reached within ten seconds");
    }

    async fn shutdown(stop: CancellationToken, tasks: TaskTracker) {
        stop.cancel();
        tasks.close();
        tokio::time::timeout(Duration::from_secs(5), tasks.wait())
            .await
            .expect("checks stop and reap their children");
    }

    #[test]
    fn strict_definitions_apply_defaults_and_reject_invalid_values() {
        let minimal = r#"[{"name":"probe","command":["true"]}]"#;
        let parsed = parse(minimal.as_bytes()).unwrap().remove(0);
        assert_eq!(parsed.timeout_secs, 5);
        assert_eq!(parsed.interval_secs, Some(30));
        assert_eq!(parsed.failure_threshold, 1);
        assert_eq!(parsed.success_threshold, 1);
        let startup = parse(br#"[{"name":"probe","command":["true"],"intervalSecs":null}]"#)
            .unwrap()
            .remove(0);
        assert_eq!(startup.interval_secs, None);
        for invalid in [
            "[]".to_owned(),
            r#"[{"name":"","command":["true"]}]"#.to_owned(),
            r#"[{"name":"probe","command":[]}]"#.to_owned(),
            r#"[{"name":"probe","command":[""]}]"#.to_owned(),
            r#"[{"name":"probe","command":"true"}]"#.to_owned(),
            r#"[{"name":"probe","command":["true"],"required":false}]"#.to_owned(),
            format!(
                "[{},{}]",
                &minimal[1..minimal.len() - 1],
                &minimal[1..minimal.len() - 1]
            ),
            format!(r#"[{{"name":"{}","command":["true"]}}]"#, "x".repeat(81)),
            format!(
                "[{}]",
                (0..33)
                    .map(|n| format!(r#"{{"name":"{n}","command":["true"]}}"#))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ] {
            assert!(parse(invalid.as_bytes()).is_err(), "{invalid}");
        }
        for (field, values) in [
            ("timeoutSecs", [0, 301]),
            ("intervalSecs", [0, 3601]),
            ("failureThreshold", [0, 101]),
            ("successThreshold", [0, 101]),
        ] {
            for value in values {
                let invalid =
                    format!(r#"[{{"name":"probe","command":["true"],"{field}":{value}}}]"#);
                assert!(parse(invalid.as_bytes()).is_err(), "{invalid}");
            }
        }
    }

    #[tokio::test]
    async fn thresholds_gate_initial_readiness_and_allow_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("healthy");
        std::fs::write(&marker, "").unwrap();
        let mut definition = check(&[
            "sh",
            "-c",
            "test -f \"$1\"",
            "check",
            marker.to_str().unwrap(),
        ]);
        definition.success_threshold = 2;
        definition.failure_threshold = 2;
        let tasks = TaskTracker::new();
        let stop = CancellationToken::new();
        let health = HealthChecks::start_checks(vec![definition], stop.clone(), &tasks);
        assert!(!health.ready());
        assert_eq!(
            health.report().unwrap().checks[0].status,
            WorkerCheckStatus::Pending
        );
        eventually(|| health.report().unwrap().checks[0].status == WorkerCheckStatus::Passed).await;
        assert!(
            !health.ready(),
            "one success is below the startup threshold"
        );
        eventually(|| health.ready()).await;
        std::fs::remove_file(&marker).unwrap();
        eventually(|| health.report().unwrap().checks[0].status == WorkerCheckStatus::Failed).await;
        assert!(health.ready(), "one failure is below the failure threshold");
        eventually(|| !health.ready()).await;
        std::fs::write(marker, "").unwrap();
        eventually(|| health.report().unwrap().checks[0].status == WorkerCheckStatus::Passed).await;
        assert!(!health.ready(), "recovery also requires two successes");
        eventually(|| health.ready()).await;
        shutdown(stop, tasks).await;
    }

    #[tokio::test]
    async fn startup_only_retries_until_passed_then_stays_passed() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("healthy");
        let mut definition = check(&[
            "sh",
            "-c",
            "test -f \"$1\"",
            "check",
            marker.to_str().unwrap(),
        ]);
        definition.interval_secs = None;
        let tasks = TaskTracker::new();
        let stop = CancellationToken::new();
        let health = HealthChecks::start_checks(vec![definition], stop.clone(), &tasks);
        eventually(|| health.report().unwrap().checks[0].status == WorkerCheckStatus::Failed).await;
        std::fs::write(&marker, "").unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::time::resume();
        eventually(|| health.ready()).await;
        std::fs::remove_file(marker).unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(
            health.ready(),
            "startup-only success does not expire or rerun"
        );
        tokio::time::resume();
        shutdown(stop, tasks).await;
    }

    #[tokio::test]
    async fn periodic_snapshots_expire_without_a_runner() {
        let snapshot = Snapshot {
            checks: vec![WorkerHealthCheck {
                name: "probe".into(),
                status: WorkerCheckStatus::Passed,
                healthy: true,
            }],
            periodic: vec![true],
            expires: Some(Instant::now() + VALID_FOR),
            checkpoint: Arc::new(Mutex::new(Instant::now())),
        };
        let (_tx, rx) = watch::channel(snapshot);
        let health = HealthChecks {
            snapshot: Some(rx),
            ..HealthChecks::default()
        };
        assert!(health.ready());
        tokio::time::pause();
        tokio::time::advance(VALID_FOR).await;
        assert!(!health.ready());
        assert_eq!(health.report().unwrap().valid_for_secs, 0);
        assert_eq!(
            health.report().unwrap().checks[0].status,
            WorkerCheckStatus::Pending
        );
    }

    #[tokio::test]
    async fn command_arguments_do_not_implicitly_use_a_shell_and_spawn_errors_fail() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("must-not-exist");
        assert_eq!(
            execute(
                &check(&["echo", &format!("$(touch {})", marker.display())]),
                &CancellationToken::new()
            )
            .await,
            WorkerCheckStatus::Passed,
        );
        assert!(!marker.exists());
        assert_eq!(
            execute(
                &check(&["/nonexistent/gradient-health-check"]),
                &CancellationToken::new()
            )
            .await,
            WorkerCheckStatus::Error,
        );
    }

    fn process_running(pid: i32) -> bool {
        #[cfg(target_os = "linux")]
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            && stat
                .rsplit_once(") ")
                .is_some_and(|(_, tail)| tail.starts_with('Z'))
        {
            return false;
        }
        nix::sys::signal::kill(Pid::from_raw(pid), None).is_ok()
    }

    #[tokio::test]
    #[expect(
        clippy::disallowed_methods,
        reason = "test-only child process rendezvous, not worker configuration"
    )]
    async fn timeout_cancellation_and_success_clean_up_process_groups() {
        if let Ok(path) = std::env::var("GRADIENT_HEALTH_TEST_ESCAPED_PID") {
            let parent_group = nix::unistd::getpgid(Some(nix::unistd::getppid())).unwrap();
            nix::unistd::setpgid(Pid::from_raw(0), parent_group).unwrap();
            std::fs::write(path, std::process::id().to_string()).unwrap();
            std::future::pending::<()>().await;
            return;
        }
        for mode in ["timeout", "cancel", "success", "escaped"] {
            let dir = tempfile::tempdir().unwrap();
            let pids_file = dir.path().join("pids");
            let script = if mode == "success" {
                "sleep 60 & echo \"$$ $!\" > \"$1\""
            } else {
                "sleep 60 & echo \"$$ $!\" > \"$1\"; wait"
            };
            let executable = std::env::current_exe().unwrap();
            let escaped_env = format!("GRADIENT_HEALTH_TEST_ESCAPED_PID={}", pids_file.display());
            let mut definition = if mode == "escaped" {
                check(&[
                    "env",
                    &escaped_env,
                    executable.to_str().unwrap(),
                    "--exact",
                    "health::tests::timeout_cancellation_and_success_clean_up_process_groups",
                ])
            } else {
                check(&["sh", "-c", script, "check", pids_file.to_str().unwrap()])
            };
            definition.timeout_secs = 1;
            let stop = CancellationToken::new();
            let control = async {
                eventually(|| pids_file.exists()).await;
                if mode == "cancel" {
                    stop.cancel();
                }
            };
            let (status, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(execute(&definition, &stop), control)
            })
            .await
            .expect("termination stays bounded even when the child leaves its process group");
            assert_eq!(
                status,
                match mode {
                    "timeout" | "escaped" => WorkerCheckStatus::TimedOut,
                    "cancel" => WorkerCheckStatus::Error,
                    _ => WorkerCheckStatus::Passed,
                }
            );
            let pids: Vec<i32> = std::fs::read_to_string(pids_file)
                .unwrap()
                .split_whitespace()
                .map(|pid| pid.parse().unwrap())
                .collect();
            assert_eq!(pids.len(), if mode == "escaped" { 1 } else { 2 });
            eventually(|| pids.iter().all(|pid| !process_running(*pid))).await;
        }
    }

    #[tokio::test]
    async fn a_shared_runner_bounds_concurrency_and_never_overlaps_a_check() {
        let dir = tempfile::tempdir().unwrap();
        let definitions = (0..8)
            .map(|index| {
                let marker = dir.path().join(index.to_string());
                let mut definition = check(&[
                    "sh",
                    "-c",
                    "echo start >> \"$1\"; sleep 60",
                    "check",
                    marker.to_str().unwrap(),
                ]);
                definition.name = index.to_string();
                definition.timeout_secs = 5;
                definition
            })
            .collect();
        let tasks = TaskTracker::new();
        let stop = CancellationToken::new();
        let health = HealthChecks::start_checks(definitions, stop.clone(), &tasks);
        let other_session = health.clone();
        eventually(|| std::fs::read_dir(dir.path()).unwrap().count() == CONCURRENCY).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), CONCURRENCY);
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            assert_eq!(
                std::fs::read_to_string(entry.unwrap().path()).unwrap(),
                "start\n"
            );
        }
        assert_eq!(
            health.report().unwrap().checks,
            other_session.report().unwrap().checks
        );
        assert_eq!(tasks.len(), 1, "sessions share a single runner task");
        shutdown(stop, tasks).await;
    }

    #[tokio::test]
    async fn refills_do_not_starve_a_startup_check_behind_busy_periodic_checks() {
        let mut definitions: Vec<_> = (0..8)
            .map(|index| {
                let mut definition = check(&["sleep", "2"]);
                definition.name = format!("periodic-{index}");
                definition
            })
            .collect();
        let mut startup = check(&["true"]);
        startup.name = "startup".into();
        startup.interval_secs = None;
        definitions.push(startup);
        let tasks = TaskTracker::new();
        let stop = CancellationToken::new();
        let health = HealthChecks::start_checks(definitions, stop.clone(), &tasks);

        eventually(|| health.report().unwrap().checks[8].healthy).await;
        assert_eq!(
            health.report().unwrap().checks[8].status,
            WorkerCheckStatus::Passed
        );
        shutdown(stop, tasks).await;
    }

    #[test]
    fn unchecked_workers_need_no_reports_and_remain_ready() {
        let health = HealthChecks::default();
        assert!(!health.required());
        assert!(health.ready());
        assert!(health.report().is_none());
    }

    #[tokio::test]
    async fn reporting_does_not_extend_observation_deadlines() {
        let definition = check(&["true"]);
        let mut state = State {
            report: WorkerHealthCheck {
                name: definition.name.clone(),
                status: WorkerCheckStatus::Pending,
                healthy: false,
            },
            definition,
            successes: 0,
            failures: 0,
            next: Instant::now(),
            expires: Instant::now(),
            running: false,
        };
        state.completed(WorkerCheckStatus::Passed);
        let snapshot = Snapshot {
            checks: vec![state.report],
            periodic: vec![true],
            expires: Some(state.expires),
            checkpoint: Arc::new(Mutex::new(Instant::now())),
        };
        let (_tx, rx) = watch::channel(snapshot);
        let health = HealthChecks {
            snapshot: Some(rx),
            ..HealthChecks::default()
        };
        tokio::time::pause();
        let initial = health.report().unwrap().valid_for_secs;
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(health.report().unwrap().valid_for_secs, initial - 1);
        assert!(health.ready());
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(health.report().unwrap().valid_for_secs, 0);
        assert!(!health.ready());
    }

    #[tokio::test]
    async fn report_cadence_follows_the_shortest_periodic_freshness_budget() {
        for (interval, timeout, expected) in [
            (Some(30), 5, 5_000),
            (Some(1), 1, 1_000),
            (Some(3), 2, 2_500),
            (None, 1, 5_000),
        ] {
            let mut normal = check(&["true"]);
            normal.name = "normal".into();
            normal.interval_secs = Some(30);
            let mut other = check(&["true"]);
            other.interval_secs = interval;
            other.timeout_secs = timeout;
            let stop = CancellationToken::new();
            let tasks = TaskTracker::new();
            let health = HealthChecks::start_checks(vec![normal, other], stop.clone(), &tasks);
            assert_eq!(health.report_interval(), Duration::from_millis(expected));
            shutdown(stop, tasks).await;
        }
    }

    #[tokio::test]
    async fn checkpoint_only_ticks_do_not_notify_report_subscribers() {
        let mut startup = check(&["true"]);
        startup.interval_secs = None;
        let stop = CancellationToken::new();
        let tasks = TaskTracker::new();
        let mut health = HealthChecks::start_checks(vec![startup], stop.clone(), &tasks);
        eventually(|| health.ready()).await;
        let rx = health.snapshot.as_mut().unwrap();
        let checkpoint = Arc::clone(&rx.borrow_and_update().checkpoint);
        let previous = *checkpoint.lock();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        assert!(
            *checkpoint.lock() > previous,
            "the runner checkpoint advances"
        );
        assert!(
            !rx.has_changed().unwrap(),
            "checkpoint ticks send no result notifications"
        );
        tokio::time::resume();
        shutdown(stop, tasks).await;
    }

    #[tokio::test]
    async fn unreadable_invalid_and_oversized_files_fail_before_starting_a_runner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.json");
        let stop = CancellationToken::new();
        let tasks = TaskTracker::new();
        assert!(HealthChecks::start(Some(&path), stop.clone(), &tasks).is_err());
        std::fs::write(&path, "{}").unwrap();
        assert!(HealthChecks::start(Some(&path), stop.clone(), &tasks).is_err());
        std::fs::write(
            &path,
            vec![b' '; usize::try_from(MAX_FILE_BYTES).unwrap() + 1],
        )
        .unwrap();
        assert!(HealthChecks::start(Some(&path), stop, &tasks).is_err());
        assert!(tasks.is_empty());
    }
}
