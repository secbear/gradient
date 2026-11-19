/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

pub(crate) mod cluster;
mod id;
mod jobs;
mod message_loop;
mod scoring;
mod vm_report;

use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{Context, Result};
use gradient_wire::messages::{
    ClientMessage, JobKind, RUNNING_JOBS_SINCE, RunningAssignment, WORKER_HEALTH_SINCE,
};
use tracing::{info, warn};

use crate::config::WorkerConfig;
use crate::connection_state::{Connected, Disconnected};
use crate::exec::host::ExecHost;
use crate::exec::reattach::ResumedJob;
use crate::executor::{JobExecutor, WorkerEvaluator};
use crate::health::HealthChecks;
use crate::nix::store::LocalNixStore;
use crate::proto::credentials::CredentialStore;
use crate::proto::scorer::JobScorer;
use gradient_worker_client::connection::ProtoConnection;
use gradient_worker_client::connection::handshake::{
    perform_dialed_handshake_with_readiness, perform_handshake_with_readiness,
};
use gradient_worker_client::reconnect::RunOutcome;
use tokio_util::sync::CancellationToken;

pub(crate) use id::load_or_generate_id;
use jobs::WorkerJobs;

pub(crate) use jobs::clear_job_logs;

const WRITER_FLUSH_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

const NO_JOB_SESSION: u64 = 0;

pub struct Worker<S> {
    config: WorkerConfig,
    executor: JobExecutor,
    scorer: JobScorer,
    credentials: CredentialStore,
    jobs: Arc<WorkerJobs>,
    health: HealthChecks,
    conn_state: S,
    _marker: PhantomData<S>,
}

impl<S> Worker<S> {
    pub fn executor_handle(&self) -> JobExecutor {
        self.executor.clone()
    }
}

impl Worker<Connected> {
    pub async fn from_accepted(
        ws: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        config: WorkerConfig,
        exec: Option<Arc<ExecHost>>,
        health: HealthChecks,
    ) -> Result<Self> {
        let mut conn = ProtoConnection::from_accepted(ws);
        perform_setup(
            &mut conn,
            &config,
            Side::Inbound,
            Vec::new(),
            exec.as_deref(),
            &health,
        )
        .await?;
        let (executor, scorer) = build_executor(&config, exec).await?;
        Ok(Self::new_connected(config, conn, executor, scorer, health))
    }

    fn new_connected(
        config: WorkerConfig,
        conn: ProtoConnection,
        executor: JobExecutor,
        scorer: JobScorer,
        health: HealthChecks,
    ) -> Self {
        Self {
            jobs: Arc::new(WorkerJobs::new(&config)),
            config,
            executor,
            scorer,
            credentials: CredentialStore::new(),
            health,
            conn_state: Connected { conn },
            _marker: PhantomData,
        }
    }
}

impl Worker<Disconnected> {
    pub async fn start(
        config: WorkerConfig,
        exec: Option<Arc<ExecHost>>,
        resumed: Vec<ResumedJob>,
        health: HealthChecks,
    ) -> Result<Self> {
        let (executor, scorer) = build_executor(&config, exec).await?;
        let jobs = Arc::new(WorkerJobs::new(&config));
        for job in resumed {
            resume_exec_job(&jobs, &executor, job);
        }
        Ok(Self {
            config,
            executor,
            scorer,
            credentials: CredentialStore::new(),
            jobs,
            health,
            conn_state: Disconnected,
            _marker: PhantomData,
        })
    }

    pub async fn connect(&self, server_url: &str) -> Result<ProtoConnection> {
        let mut conn = ProtoConnection::open(server_url).await?;
        perform_setup(
            &mut conn,
            &self.config,
            Side::Outbound,
            self.jobs.running_assignments(),
            self.executor.exec.as_deref(),
            &self.health,
        )
        .await?;
        Ok(conn)
    }

    pub fn into_connected(self, conn: ProtoConnection) -> Worker<Connected> {
        let Worker {
            config,
            executor,
            scorer,
            credentials,
            jobs,
            health,
            ..
        } = self;

        Worker {
            config,
            executor,
            scorer,
            credentials,
            jobs,
            health,
            conn_state: Connected { conn },
            _marker: PhantomData,
        }
    }
}

impl Worker<Connected> {
    pub async fn run(self, stop: CancellationToken) -> (Worker<Disconnected>, Result<RunOutcome>) {
        let Worker {
            config,
            executor,
            scorer,
            credentials,
            jobs,
            health,
            conn_state: Connected { conn },
            ..
        } = self;

        let (writer, reader, flush) = conn.split();
        let version = writer.version();
        let job_session = jobs
            .start_session(writer.clone())
            .await
            .unwrap_or_else(|e| {
                warn!(error = %e, "the new session closed before the queued job reports went out");
                NO_JOB_SESSION
            });
        let state = message_loop::MessageLoopState::new(
            writer,
            &jobs,
            config.clone(),
            executor.clone(),
            scorer,
            credentials.clone(),
            health.clone(),
        );
        let outcome =
            message_loop::run_message_loop(state, reader, job_session, stop.clone()).await;
        jobs.end_session(version);

        // Only background tasks can still hold a writer clone at this point.
        // A stopping worker must see its last reports leave the queue.
        // A session ending for a reconnect is closing at once.
        if stop.is_cancelled() {
            flush.flush(WRITER_FLUSH_BUDGET).await;
        } else {
            flush.close();
        }

        let disconnected = Worker {
            config,
            executor,
            scorer,
            credentials,
            jobs,
            health,
            conn_state: Disconnected,
            _marker: PhantomData,
        };

        let result = outcome.map(|end| match end {
            _ if end.draining => RunOutcome::Drained,
            _ if end.refused => RunOutcome::Refused,
            _ => RunOutcome::CleanDisconnect,
        });

        (disconnected, result)
    }
}

async fn build_executor(
    config: &WorkerConfig,
    exec: Option<Arc<ExecHost>>,
) -> Result<(JobExecutor, JobScorer)> {
    let store = LocalNixStore::connect(config.nix_daemon.max_connections.get())?;
    let evaluator = WorkerEvaluator::new(
        config.fork_workers(),
        config.eval.max_rss.get(),
        config.memory_guard()?,
        config.eval_cache_dir(),
        config.eval.cache_share,
        config.eval.metrics,
    );
    let gcroots = crate::nix::gcroots::GcRootKeeper::new(
        &config.gcroots_dir,
        std::sync::Arc::new(store.clone()),
    );
    if let Err(e) = gcroots.purge_all().await {
        tracing::warn!(error = %e, "gcroot purge failed at startup; continuing");
    }
    let executor = JobExecutor::new(
        store,
        evaluator,
        gcroots,
        config.ssh_bin.clone(),
        crate::executor::log_limit::LogRateLimits {
            burst_bytes_per_min: config.log.burst_bytes_per_min,
            sustained_bytes_per_hour: config.log.sustained_bytes_per_hour,
        },
        config.log.fetch_from_store,
        crate::executor::BuildHost {
            build_cores: config.build_cores(),
            cpu_core_score: config.cpu_core_score(),
        },
    )
    .with_exec(exec);
    Ok((executor, JobScorer::new()))
}

fn resume_exec_job(jobs: &WorkerJobs, executor: &JobExecutor, resumed: ResumedJob) {
    let job_id = resumed.stored.job_id.clone();
    let assignment_id = resumed.stored.assignment_id.clone();
    let executor = executor.clone();
    jobs.resume_job(job_id, assignment_id, move |mut updater, abort, inbox| {
        Box::pin(async move {
            executor
                .resume_exec_job(resumed, &mut updater, abort, inbox)
                .await
        })
    });
}

#[derive(Clone, Copy, Debug)]
enum Side {
    Outbound,
    Inbound,
}

async fn perform_setup(
    conn: &mut ProtoConnection,
    config: &WorkerConfig,
    side: Side,
    running: Vec<RunningAssignment>,
    exec: Option<&ExecHost>,
    health: &HealthChecks,
) -> Result<()> {
    let peer_id = load_or_generate_id(&config.base_dir, config.id.as_deref())
        .context("load or generate persistent worker ID")?;
    if let Some(exec) = exec {
        exec.start_session();
    }
    let handshake = match side {
        Side::Inbound => {
            perform_dialed_handshake_with_readiness(
                conn,
                peer_id,
                config.accepted_server_tokens(),
                config.capabilities(),
                health.required(),
            )
            .await?
        }
        Side::Outbound => {
            perform_handshake_with_readiness(
                conn,
                peer_id,
                config.peer_tokens(),
                config.capabilities(),
                health.required(),
            )
            .await?
        }
    };
    anyhow::ensure!(
        !health.required() || handshake.version >= WORKER_HEALTH_SINCE,
        "configured health checks require server protocol {WORKER_HEALTH_SINCE}"
    );
    info!(
        ?side,
        negotiated = ?handshake.negotiated,
        version = handshake.version,
        "capabilities negotiated"
    );
    if handshake.version >= RUNNING_JOBS_SINCE {
        conn.send(ClientMessage::RunningJobs {
            assignments: running,
        })
        .await?;
    }
    let exec = exec.filter(|_| handshake.negotiated.exec);
    if handshake.negotiated.build || exec.is_some() {
        let architectures = if config.system.architectures.is_empty() {
            vec![crate::config::host_system()]
        } else {
            config.system.architectures.clone()
        };
        let system_features = if config.system.features.is_empty() {
            crate::nix::system_features::detect().await
        } else {
            config.system.features.clone()
        };
        let host = crate::metrics::host_static();
        let cpu_core_score = config.cpu_core_score();
        info!(
            ?architectures,
            ?system_features,
            max_concurrent_builds = config.build.max_concurrent.get(),
            cpu_count = host.cpu_count,
            ram_total_mb = host.ram_total_mb,
            cpu_core_score,
            "advertising build capabilities"
        );
        conn.send(ClientMessage::WorkerCapabilities {
            architectures,
            system_features,
            max_concurrent_builds: config.build.max_concurrent.get(),
            cpu_count: host.cpu_count,
            ram_total_mb: host.ram_total_mb,
            cpu_core_score,
            zone: config.zone.clone().filter(|z| !z.is_empty()),
            endpoint: config.endpoint.clone().filter(|e| !e.is_empty()),
            build_cores: config.build_cores().map(std::num::NonZeroU32::get),
            exec: exec.map(|exec| Box::new(exec.inventory())),
        })
        .await?;
    }
    if health.required() {
        return Ok(());
    }
    conn.send(ClientMessage::RequestJobList).await?;
    conn.send(ClientMessage::RequestJob {
        kind: JobKind::Flake,
    })
    .await?;
    conn.send(ClientMessage::RequestJob {
        kind: JobKind::Build,
    })
    .await?;
    if exec.is_some() {
        conn.send(ClientMessage::RequestJob {
            kind: JobKind::Exec,
        })
        .await?;
    }
    Ok(())
}

#[cfg(test)]
fn test_executor(gcroots: &std::path::Path) -> JobExecutor {
    let store =
        LocalNixStore::connect_at("/var/empty/gradient-nonexistent.sock", 1).expect("lazy connect");
    let config = WorkerConfig::default();
    JobExecutor::new(
        store.clone(),
        WorkerEvaluator::new(
            std::num::NonZeroUsize::MIN,
            0,
            config.memory_guard().expect("default memory guard"),
            String::new(),
            false,
            false,
        ),
        crate::nix::gcroots::GcRootKeeper::new(
            gcroots.to_str().expect("utf-8 path"),
            Arc::new(store),
        ),
        "ssh".into(),
        crate::executor::log_limit::LogRateLimits {
            burst_bytes_per_min: 0,
            sustained_bytes_per_hour: 0,
        },
        false,
        crate::executor::BuildHost {
            build_cores: None,
            cpu_core_score: 0,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use gradient_test_support::prelude::MockProtoServer;
    use gradient_wire::messages::GradientCapabilities;

    fn disconnected_worker(base: &std::path::Path) -> Worker<Disconnected> {
        let config = WorkerConfig {
            base_dir: base.to_owned(),
            ..WorkerConfig::default()
        };
        Worker {
            jobs: Arc::new(WorkerJobs::new(&config)),
            executor: test_executor(base),
            scorer: JobScorer::new(),
            credentials: CredentialStore::new(),
            health: HealthChecks::default(),
            config,
            conn_state: Disconnected,
            _marker: PhantomData,
        }
    }

    async fn reconnected(
        worker: Worker<Disconnected>,
    ) -> (
        gradient_test_support::prelude::MockServerConn,
        Worker<Connected>,
    ) {
        let server = MockProtoServer::bind().await;
        let (sc, conn) = tokio::join!(
            async {
                let mut sc = server.accept().await;
                sc.handshake(GradientCapabilities::default()).await.unwrap();
                sc
            },
            worker.connect(server.url())
        );
        (sc, worker.into_connected(conn.unwrap()))
    }

    #[tokio::test]
    async fn a_failed_job_report_ends_the_session() {
        let base = tempfile::tempdir().unwrap();
        let worker = disconnected_worker(base.path());
        let jobs = Arc::clone(&worker.jobs);
        let (_sc, worker) = reconnected(worker).await;

        let lose_the_job_lane = async {
            jobs.writer.attached().await;
            jobs.writer.detach();
        };
        let ((_, ended), ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(worker.run(CancellationToken::new()), lose_the_job_lane)
        })
        .await
        .expect("a session that cannot carry job reports ends while the socket stays open");
        ended.unwrap();
    }

    #[tokio::test]
    async fn a_job_finished_while_detached_reports_after_running_jobs() {
        let base = tempfile::tempdir().unwrap();
        let worker = disconnected_worker(base.path());
        let finished = worker.jobs.register(
            "build:1".to_owned(),
            JobKind::Build,
            "dispatch-1".to_owned(),
            None,
        );
        finished
            .done_tx
            .send(("build:1".to_owned(), Ok(())))
            .unwrap();

        let (mut sc, worker) = reconnected(worker).await;

        let script = async {
            let listed = sc
                .recv_until(|msg| match msg {
                    ClientMessage::RunningJobs { assignments } => Some(assignments),
                    ClientMessage::JobCompleted { .. } => panic!("completed before RunningJobs"),
                    _ => None,
                })
                .await
                .unwrap();
            assert_eq!(
                listed,
                vec![RunningAssignment {
                    assignment_id: "dispatch-1".into(),
                    kind: JobKind::Build,
                }]
            );
            let completed = sc
                .recv_until(|msg| match msg {
                    ClientMessage::JobCompleted { job_id, .. } => Some(job_id),
                    _ => None,
                })
                .await
                .unwrap();
            assert_eq!(completed, "build:1");
            sc.close().await;
        };
        let ((_, ended), ()) = tokio::join!(worker.run(CancellationToken::new()), script);
        ended.unwrap();
    }

    #[tokio::test]
    async fn checked_build_setup_stays_quiet_until_dispatch_reports_health() {
        for (initial_ready, queued_completion) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let base = tempfile::tempdir().unwrap();
            let mut worker = disconnected_worker(base.path());
            let (health, _set_ready) = HealthChecks::controlled(initial_ready);
            worker.health = health;
            worker.config.system.architectures = vec!["x86_64-linux".into()];
            worker.config.system.features = vec!["benchmark".into()];
            worker.config.system.cpu_core_score = std::num::NonZeroU32::new(111);
            worker.config.build.max_concurrent = std::num::NonZeroU32::new(7).unwrap();
            let active =
                worker
                    .jobs
                    .register("build:1".into(), JobKind::Build, "dispatch-1".into(), None);
            if queued_completion {
                active.done_tx.send(("build:1".into(), Ok(()))).unwrap();
            }
            let server = MockProtoServer::bind().await;
            let script = async {
                let mut sc = server.accept().await;
                sc.handshake(GradientCapabilities {
                    build: true,
                    ..Default::default()
                })
                .await
                .unwrap();
                assert_eq!(
                    sc.recv().await.unwrap(),
                    ClientMessage::RunningJobs {
                        assignments: vec![RunningAssignment {
                            assignment_id: "dispatch-1".into(),
                            kind: JobKind::Build,
                        }],
                    },
                    "reclamation must precede any readiness report",
                );
                let ClientMessage::WorkerCapabilities {
                    architectures,
                    system_features,
                    max_concurrent_builds,
                    cpu_core_score,
                    ..
                } = sc.recv().await.unwrap()
                else {
                    panic!("capabilities must precede any readiness report");
                };
                assert_eq!(architectures, ["x86_64-linux"]);
                assert_eq!(system_features, ["benchmark"]);
                assert_eq!(max_concurrent_builds, 7);
                assert_eq!(cpu_core_score, 111);
                sc
            };
            let (mut sc, connection) =
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    tokio::join!(script, worker.connect(server.url()))
                })
                .await
                .expect("checked build setup completes in wire order");
            let connection = connection.unwrap();
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), sc.recv())
                    .await
                    .is_err(),
                "checked setup alone sends neither health reports nor job requests",
            );

            let script = async {
                let ClientMessage::WorkerHealth { health } = sc.recv().await.unwrap() else {
                    panic!("dispatch reports health before requesting jobs");
                };
                assert_eq!(health.ready(), initial_ready);
                assert!(health.valid_for_secs > 0);
                if queued_completion {
                    sc.recv_until(|message| match message {
                        ClientMessage::JobCompleted { job_id, .. } => {
                            assert_eq!(job_id, "build:1");
                            Some(())
                        }
                        ClientMessage::RequestJob { .. } | ClientMessage::RequestJobList => {
                            assert!(initial_ready, "an unhealthy worker requests no jobs");
                            None
                        }
                        ClientMessage::WorkerHealth { .. } => {
                            panic!(
                                "the initial interval tick must not duplicate the health report"
                            );
                        }
                        _ => None,
                    })
                    .await
                    .unwrap();
                } else if initial_ready {
                    assert_eq!(sc.recv().await.unwrap(), ClientMessage::RequestJobList);
                } else {
                    assert!(
                        tokio::time::timeout(std::time::Duration::from_millis(100), sc.recv())
                            .await
                            .is_err(),
                        "an initially unhealthy session requests no jobs",
                    );
                }
                sc.close().await;
            };
            let worker = worker.into_connected(connection);
            let ((_, ended), ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(worker.run(CancellationToken::new()), script)
            })
            .await
            .expect("dispatch reports health after completed setup");
            ended.unwrap();
        }
    }

    #[tokio::test]
    async fn checked_sessions_report_initial_health_and_request_jobs_after_recovery() {
        use tokio_util::task::TaskTracker;

        let base = tempfile::tempdir().unwrap();
        let marker = base.path().join("healthy");
        let checks_file = base.path().join("health.json");
        std::fs::write(
            &checks_file,
            serde_json::to_vec(&serde_json::json!([{
                "name": "probe",
                "command": ["sh", "-c", "test -f \"$1\"", "check", marker],
                "intervalSecs": 1,
            }]))
            .unwrap(),
        )
        .unwrap();
        let stop = CancellationToken::new();
        let tasks = TaskTracker::new();
        let mut worker = disconnected_worker(base.path());
        worker.health = HealthChecks::start(Some(&checks_file), stop.clone(), &tasks).unwrap();
        let health = worker.health.clone();
        let finished = worker.jobs.register(
            "build:1".to_owned(),
            JobKind::Build,
            "dispatch-1".to_owned(),
            None,
        );
        let (mut sc, worker) = reconnected(worker).await;
        let script = async {
            let initial = sc
                .recv_until(|msg| match msg {
                    ClientMessage::WorkerHealth { health } => Some(health),
                    ClientMessage::RequestJob { .. } | ClientMessage::RequestJobList => {
                        panic!("requested work before reporting health")
                    }
                    _ => None,
                })
                .await
                .unwrap();
            assert!(!initial.ready());
            assert!(initial.valid_for_secs > 0);

            assert!(
                !sc.assign(
                    "build:2",
                    "dispatch-2",
                    gradient_wire::messages::Job::Build(gradient_wire::messages::BuildJob {
                        build: gradient_wire::messages::BuildSpec::default(),
                        requirement: gradient_wire::messages::BuildRequirement::default(),
                    }),
                )
                .await
                .unwrap(),
                "an unready worker refuses new assignments"
            );

            finished
                .done_tx
                .send(("build:1".to_owned(), Ok(())))
                .unwrap();
            sc.recv_until(|msg| match msg {
                ClientMessage::JobCompleted { .. } => Some(()),
                ClientMessage::Draining => panic!("readiness must not drain active jobs"),
                _ => None,
            })
            .await
            .unwrap();
            std::fs::write(&marker, "").unwrap();
            let passed = sc
                .recv_until(|msg| match msg {
                    ClientMessage::WorkerHealth { health } if health.ready() => Some(health),
                    ClientMessage::RequestJob { .. } | ClientMessage::RequestJobList => {
                        panic!("requested work before reporting recovery")
                    }
                    _ => None,
                })
                .await
                .unwrap();
            assert_eq!(
                passed.checks[0].status,
                gradient_wire::messages::WorkerCheckStatus::Passed
            );
            sc.recv_until(|msg| matches!(msg, ClientMessage::RequestJobList).then_some(()))
                .await
                .unwrap();
            assert!(health.ready());
            sc.close().await;
        };
        let ((_, ended), ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(worker.run(CancellationToken::new()), script)
        })
        .await
        .expect("health is reported and recovery requests work");
        ended.unwrap();
        stop.cancel();
        tasks.close();
        tasks.wait().await;
    }

    #[tokio::test]
    async fn short_interval_checks_keep_server_facing_health_reports_fresh() {
        use std::time::Duration;
        use tokio::time::Instant;
        use tokio_util::task::TaskTracker;

        let base = tempfile::tempdir().unwrap();
        let path = base.path().join("health.json");
        std::fs::write(
            &path,
            br#"[{"name":"probe","command":["true"],"intervalSecs":1,"timeoutSecs":1}]"#,
        )
        .unwrap();
        let stop = CancellationToken::new();
        let tasks = TaskTracker::new();
        let mut worker = disconnected_worker(base.path());
        worker.health = HealthChecks::start(Some(&path), stop.clone(), &tasks).unwrap();
        let (mut server, worker) = reconnected(worker).await;
        let reports = async {
            let started = Instant::now();
            let mut server_expiry = None;
            let mut received = 0;
            loop {
                let health = server
                    .recv_until(|message| match message {
                        ClientMessage::WorkerHealth { health } => Some(health),
                        _ => None,
                    })
                    .await
                    .unwrap();
                let now = Instant::now();
                if let Some(expiry) = server_expiry {
                    assert!(
                        now < expiry,
                        "the previous server-facing report has not expired"
                    );
                    assert!(
                        health.ready(),
                        "successful checks stay healthy across reporting cycles"
                    );
                } else if !health.ready() {
                    continue;
                }
                assert!(health.valid_for_secs > 0);
                server_expiry = Some(now + Duration::from_secs(u64::from(health.valid_for_secs)));
                received += 1;
                if received >= 6 && started.elapsed() >= Duration::from_secs(4) {
                    break;
                }
            }
            server.close().await;
        };
        let ((_, ended), ()) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(worker.run(CancellationToken::new()), reports)
        })
        .await
        .expect("short-interval health reports cover successive server validity windows");
        ended.unwrap();
        stop.cancel();
        tasks.close();
        tasks.wait().await;
    }

    #[tokio::test]
    async fn legacy_servers_keep_unchecked_workers_but_refuse_configured_checks() {
        use futures::{SinkExt as _, StreamExt as _};
        use gradient_wire::codec::agreement::version_frame;
        use gradient_wire::codec::{from_bytes, to_bytes};
        use gradient_wire::messages::ServerMessage;
        use tokio_tungstenite::tungstenite::Message;
        use tokio_util::task::TaskTracker;

        for checked in [false, true] {
            let base = tempfile::tempdir().unwrap();
            let stop = CancellationToken::new();
            let tasks = TaskTracker::new();
            let mut worker = disconnected_worker(base.path());
            worker.config.system.architectures = vec!["x86_64-linux".into()];
            worker.config.system.features = vec!["benchmark".into()];
            if checked {
                let path = base.path().join("health.json");
                std::fs::write(&path, br#"[{"name":"probe","command":["true"]}]"#).unwrap();
                worker.health = HealthChecks::start(Some(&path), stop.clone(), &tasks).unwrap();
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server = async {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                socket
                    .send(Message::Binary(version_frame(&(60..=60))))
                    .await
                    .unwrap();
                assert!(matches!(socket.next().await, Some(Ok(Message::Binary(_)))));
                if checked {
                    assert!(
                        !matches!(socket.next().await, Some(Ok(Message::Binary(_)))),
                        "configured worker refuses the protocol before declaring capabilities"
                    );
                    return;
                }
                let Some(Ok(Message::Binary(init))) = socket.next().await else {
                    panic!("unchecked legacy worker sends InitConnection");
                };
                assert!(matches!(
                    from_bytes::<ClientMessage>(init, 60).unwrap(),
                    ClientMessage::InitConnection { .. }
                ));
                for message in [
                    ServerMessage::AuthChallenge { peers: vec![] },
                    ServerMessage::InitAck {
                        capabilities: GradientCapabilities::default(),
                        authorized_peers: vec![],
                        failed_peers: vec![],
                    },
                ] {
                    socket
                        .send(Message::Binary(to_bytes(&message, 60).unwrap()))
                        .await
                        .unwrap();
                    if matches!(message, ServerMessage::AuthChallenge { .. }) {
                        assert!(matches!(socket.next().await, Some(Ok(Message::Binary(_)))));
                    }
                }
                loop {
                    let Some(Ok(Message::Binary(message))) = socket.next().await else {
                        panic!("unchecked legacy worker requests work");
                    };
                    if matches!(
                        from_bytes::<ClientMessage>(message, 60).unwrap(),
                        ClientMessage::RequestJobList
                    ) {
                        break;
                    }
                }
            };
            let ((), connection) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                tokio::join!(server, worker.connect(&url))
            })
            .await
            .unwrap();
            assert_eq!(connection.is_err(), checked);
            stop.cancel();
            tasks.close();
            tasks.wait().await;
        }
    }
}
