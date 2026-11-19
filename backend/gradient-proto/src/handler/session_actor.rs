/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! The reader is delivering one frame per call and is stamping liveness while a frame is in flight.
//! A handler waiting on a slow graph would otherwise look like a silent worker and get a healthy
//! connection unregistered.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use gradient_pool::session_port::{SessionPort, SessionSignal};
use gradient_scheduler::Scheduler;
use gradient_storage::admission::UploadAdmission;
use gradient_types::RuntimeConfig;
use gradient_types::ids::ProjectId;
use gradient_util::shutdown::Shutdown;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::SessionHandles;
use super::dialed::{DialedSession, refresh_dialed_peers};
use super::guest_channel::{WORKER_TOO_OLD, outbound_guest_channel};
use super::inbound::{ActiveJobs, InboundContext, RpcContext};
use super::job_events::{JobEvents, SchedulerJobEvents};
use super::log_lane::LogLane;
use super::session::on_reauth_notify;
use super::socket::{
    InputFetch, ProtoSocket, ProtoWriter, job_offer_chunks, send_input_fetch, send_server_msg,
};
use super::store_paths::{outbound_message, outbound_offer, project_mapper};
use super::upload::{UploadSession, UploadTable, abandon_transfer};
use gradient_wire::Rejection;
use gradient_wire::messages::{
    ClientMessage, EXEC_KEEP_VM_RUNNING_SINCE, GradientCapabilities, RUNNING_JOBS_SINCE,
    ServerMessage,
};
use gradient_wire::session::frame::ProtoReader;

const IN_FLIGHT_STAMP: Duration = Duration::from_secs(5);

pub enum SessionMsg {
    Frame(ClientMessage, RpcReplyPort<bool>),
    Signal(SessionSignal),
    Reattach,
    ReaderClosed,
    Admitted(gradient_storage::admission::Admitted),
    SweepUploads,
}

#[derive(Clone)]
pub struct SessionRef(pub ActorRef<SessionMsg>);

impl SessionPort for SessionRef {
    fn signal(&self, signal: SessionSignal) {
        let _ = self.0.send_message(SessionMsg::Signal(signal));
    }
}

pub struct SessionArgs {
    pub peer_id: String,
    pub handles: SessionHandles,
    pub scheduler: Arc<Scheduler>,
    pub socket: ProtoSocket,
    pub capabilities: GradientCapabilities,
    pub authorized_peers: HashSet<ProjectId>,
    pub dialed: Option<DialedSession>,
    pub readiness_required: bool,
}

pub struct SessionState {
    peer_id: String,
    handles: SessionHandles,
    scheduler: Arc<Scheduler>,
    port: Arc<dyn SessionPort>,
    writer: ProtoWriter,
    capabilities: GradientCapabilities,
    authorized_peers: HashSet<ProjectId>,
    dialed: Option<DialedSession>,
    readiness_required: bool,
    reclaims_jobs: bool,
    uploads: UploadSession,
    nar_serve_semaphore: Arc<Semaphore>,
    offers_seen: u64,
    active: ActiveJobs,
    job_events: JobEvents,
    logs: LogLane,
    reader: JoinHandle<()>,
}

pub struct SessionActor;

impl Actor for SessionActor {
    type Msg = SessionMsg;
    type State = SessionState;
    type Arguments = SessionArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let SessionArgs {
            peer_id,
            handles,
            scheduler,
            mut socket,
            capabilities,
            authorized_peers,
            dialed,
            readiness_required,
        } = args;
        let uploads = open_uploads(
            &handles.upload_admission,
            &handles.db.config,
            &handles.db.shutdown,
            &peer_id,
            &myself,
        )
        .map_err(|e| ActorProcessingErr::from(format!("{e:#}")))?;
        let port: Arc<dyn SessionPort> = Arc::new(SessionRef(myself.clone()));
        let reclaims_jobs = dialed.is_none()
            && socket
                .version()
                .is_some_and(|version| version >= RUNNING_JOBS_SINCE);
        let registered = match scheduler
            .register_worker_with_readiness(
                &peer_id,
                capabilities.clone(),
                authorized_peers.clone(),
                port.clone(),
                reclaims_jobs,
                readiness_required,
            )
            .await
        {
            Ok(registered) => registered,
            Err(e) => {
                warn!(%peer_id, error = %e, "registration failed; scheduler unavailable");
                socket.send_reject(Rejection::SchedulerUnavailable).await;
                return Err(ActorProcessingErr::from(e.to_string()));
            }
        };

        let nar_cfg = &handles.db.config.nar;
        let send_chunk_timeout = Duration::from_secs(nar_cfg.send_chunk_timeout_secs.get());
        let max_serves = nar_cfg.max_concurrent_serves.get();
        let (reader, writer) = socket.split(send_chunk_timeout, &handles.db.shutdown);
        let tap = Arc::new(super::tap::ProtoTap {
            bus: handles.db.events.clone(),
            worker_id: peer_id.clone(),
        });
        let reader = reader.with_observer(tap.clone());
        let writer = writer.with_observer(tap);
        let active = ActiveJobs::default();
        let rpc = RpcContext::new(
            handles.clone(),
            Arc::clone(&scheduler),
            writer.clone(),
            peer_id.clone(),
            active.clone(),
        );
        let reader = handles.db.shutdown.spawn(read_loop(
            reader,
            myself,
            registered.last_seen,
            IN_FLIGHT_STAMP,
            move |msg| rpc.serve(msg),
        ));
        let job_events = JobEvents::spawn(
            &handles.db.shutdown,
            &peer_id,
            SchedulerJobEvents {
                scheduler: Arc::clone(&scheduler),
                writer: writer.clone(),
                peer_id: peer_id.clone(),
            },
        );

        let logs = LogLane::to_storage(
            &handles.db.shutdown,
            handles.db.worker_db.clone(),
            Arc::clone(&handles.db.storage.log_storage),
        );

        Ok(SessionState {
            peer_id,
            handles,
            scheduler,
            port,
            writer,
            capabilities,
            authorized_peers,
            dialed,
            readiness_required,
            reclaims_jobs,
            uploads,
            nar_serve_semaphore: Arc::new(Semaphore::new(max_serves)),
            offers_seen: 0,
            active,
            logs,
            job_events,
            reader,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        st: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            SessionMsg::Frame(msg, reply) => {
                let keep = if let ClientMessage::WorkerHealth { health } = msg {
                    if !st.readiness_required {
                        let _ = send_server_msg(
                            &st.writer,
                            &ServerMessage::Error {
                                code: 400,
                                message: "unsolicited worker health".into(),
                            },
                        )
                        .await;
                        false
                    } else if let Err(error) = st
                        .scheduler
                        .update_worker_health(&st.peer_id, st.port.clone(), health)
                        .await
                    {
                        warn!(peer_id = %st.peer_id, %error, "worker health rejected");
                        let _ = send_server_msg(
                            &st.writer,
                            &ServerMessage::Error {
                                code: 400,
                                message: "invalid worker health".into(),
                            },
                        )
                        .await;
                        false
                    } else {
                        true
                    }
                } else {
                    let mut ctx = InboundContext {
                        writer: &st.writer,
                        handles: &st.handles,
                        scheduler: &st.scheduler,
                        peer_id: &st.peer_id,
                        nar_serve_semaphore: &st.nar_serve_semaphore,
                        active: &st.active,
                        job_events: &st.job_events,
                        logs: &st.logs,
                        dialed: st.dialed.as_ref(),
                    };

                    ctx.handle(msg, &mut st.uploads).await
                };
                let _ = reply.send(keep);

                if !keep {
                    myself.stop(Some("peer closed".into()));
                }
            }
            SessionMsg::Admitted(admitted) => {
                let (mut ctx, uploads) = split_uploads(st);
                ctx.on_upload_admitted(admitted, uploads).await;
            }
            SessionMsg::SweepUploads => {
                let (mut ctx, uploads) = split_uploads(st);
                ctx.sweep_uploads(uploads).await;
            }
            SessionMsg::Signal(SessionSignal::Offers(generation)) => {
                if generation > st.offers_seen && !offer_jobs(st).await {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::Reauth) => {
                let kept = match &st.dialed {
                    Some(dialed) => {
                        refresh_dialed_peers(
                            &st.writer,
                            &st.handles.db.worker_db,
                            &st.scheduler,
                            &st.peer_id,
                            dialed,
                        )
                        .await
                    }
                    None => {
                        on_reauth_notify(&st.writer, &st.handles.db.worker_db, &st.peer_id).await
                    }
                };
                if !kept {
                    myself.stop(Some("deactivated".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::Abort { job_id, reason }) => {
                info!(peer_id = %st.peer_id, %job_id, %reason, "sending AbortJob to worker");
                {
                    let (mut ctx, uploads) = split_uploads(st);
                    ctx.forget_uploads(&job_id, uploads).await;
                }
                if send_server_msg(&st.writer, &ServerMessage::AbortJob { job_id, reason })
                    .await
                    .is_err()
                {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::PreWarmVms { count, sizes }) => {
                if send_server_msg(&st.writer, &ServerMessage::PreWarmVms { count, sizes })
                    .await
                    .is_err()
                {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::ImportResult {
                job_id,
                request_id,
                outcome,
            }) => {
                let Some(project) = st.active.project(&job_id) else {
                    debug!(peer_id = %st.peer_id, %job_id, "import result for a job this session no longer runs");
                    return Ok(());
                };

                let msg = ServerMessage::ImportResult {
                    job_id,
                    request_id,
                    outcome,
                };
                let Some(msg) = outbound_to(st, project, msg).await else {
                    return Ok(());
                };
                if send_server_msg(&st.writer, &msg).await.is_err() {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::InputResult {
                job_id,
                request_id,
                outcome,
            }) => {
                let Some(project) = st.active.project(&job_id) else {
                    debug!(peer_id = %st.peer_id, %job_id, "input result for a job this session no longer runs");
                    return Ok(());
                };

                let msg = ServerMessage::InputResult {
                    job_id,
                    request_id,
                    outcome,
                };
                let Some(msg) = outbound_to(st, project, msg).await else {
                    return Ok(());
                };
                if send_server_msg(&st.writer, &msg).await.is_err() {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::InputProgress {
                job_id,
                request_id,
                bytes_done,
                bytes_total,
            }) => {
                if !st.active.contains(&job_id) {
                    return Ok(());
                }

                let msg = ServerMessage::InputProgress {
                    job_id,
                    request_id,
                    bytes_done,
                    bytes_total,
                };
                if send_server_msg(&st.writer, &msg).await.is_err() {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::FetchInput {
                fetch_id,
                project,
                request,
            }) => {
                st.active.insert_fetch(fetch_id.clone(), project);
                let fetch = InputFetch {
                    fetch_id,
                    project,
                    request,
                };
                if send_input_fetch(
                    &st.writer,
                    &st.handles.db,
                    &st.scheduler,
                    &st.peer_id,
                    fetch,
                )
                .await
                .is_err()
                {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::StartCluster { attempt, roster }) => {
                let msg = ServerMessage::StartCluster { attempt, roster };
                if send_server_msg(&st.writer, &msg).await.is_err() {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::ClusterSignal {
                attempt,
                from,
                payload,
            }) => {
                let msg = ServerMessage::ClusterSignal {
                    attempt,
                    from,
                    payload: payload.into(),
                };
                if send_server_msg(&st.writer, &msg).await.is_err() {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::AbortCluster { attempt, reason }) => {
                info!(peer_id = %st.peer_id, %attempt, %reason, "sending AbortCluster to worker");
                if let Ok(id) = attempt.parse() {
                    let members = st.active.remove_attempt(id);
                    let (mut ctx, uploads) = split_uploads(st);
                    for job_id in &members {
                        ctx.forget_uploads(job_id, uploads).await;
                    }
                }
                let msg = ServerMessage::AbortCluster { attempt, reason };
                if send_server_msg(&st.writer, &msg).await.is_err() {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::GuestChannel(signal)) => {
                match outbound_guest_channel(signal, st.writer.version()) {
                    Ok(Some(msg)) => {
                        if send_server_msg(&st.writer, &msg).await.is_err() {
                            myself.stop(Some("write failed".into()));
                        }
                    }
                    Ok(None) => {}
                    Err(channel_id) => st.scheduler.guest_channels.closed_by_worker(
                        &st.peer_id,
                        channel_id,
                        None,
                        Some(WORKER_TOO_OLD.into()),
                    ),
                }
            }
            SessionMsg::Signal(SessionSignal::ExecKeepVmRunning {
                job_id,
                keep_vm_running,
            }) => {
                if st.writer.version() < EXEC_KEEP_VM_RUNNING_SINCE {
                    debug!(peer_id = %st.peer_id, %job_id, "the worker is too old to keep a VM running");
                    return Ok(());
                }
                let msg = ServerMessage::ExecKeepVmRunning {
                    job_id,
                    keep_vm_running,
                };
                if send_server_msg(&st.writer, &msg).await.is_err() {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::ClusterAssign { job_id }) => {
                let Some(prepared) = st.scheduler.cluster.prepared.take(&job_id).await else {
                    return Ok(());
                };
                let (mut ctx, _) = split_uploads(st);
                if !ctx
                    .hand_out(prepared.assignment, Some(prepared.membership))
                    .await
                {
                    myself.stop(Some("write failed".into()));
                }
            }
            SessionMsg::Signal(SessionSignal::Close { reason }) => {
                warn!(peer_id = %st.peer_id, %reason, "closing session at the scheduler's request");
                myself.stop(Some(reason));
            }
            SessionMsg::Reattach => {
                let active = st.active.pending();
                if let Err(e) = st
                    .scheduler
                    .reattach_worker_with_readiness(
                        &st.peer_id,
                        st.capabilities.clone(),
                        st.authorized_peers.clone(),
                        st.port.clone(),
                        active,
                        st.reclaims_jobs,
                        st.readiness_required,
                    )
                    .await
                {
                    warn!(peer_id = %st.peer_id, error = %e, "re-registration after scheduler restart failed; closing");
                    myself.stop(Some("reattach failed".into()));
                }
            }
            SessionMsg::ReaderClosed => myself.stop(Some("peer closed".into())),
        }

        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        st: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        st.reader.abort();
        for granted in st.uploads.table.drain_granted() {
            let nar_storage = st.handles.db.storage.nar_storage.clone();
            st.handles
                .db
                .shutdown
                .spawn(async move { abandon_transfer(&nar_storage, granted).await });
        }
        st.job_events.finish().await;
        st.scheduler.guest_channels.close_worker(&st.peer_id);
        st.scheduler.unregister_worker(&st.peer_id).await;
        info!(peer_id = %st.peer_id, "WebSocket connection closed");
        Ok(())
    }
}

async fn outbound_to(
    st: &SessionState,
    project: ProjectId,
    msg: ServerMessage,
) -> Option<ServerMessage> {
    let mapper = project_mapper(&st.handles.db, project).await?;
    Some(outbound_message(msg, &mapper))
}

#[tracing::instrument(level = "debug", skip_all, fields(candidates = tracing::field::Empty))]
async fn offer_jobs(st: &mut SessionState) -> bool {
    let offer = st.scheduler.get_new_job_candidates(&st.peer_id).await;
    st.offers_seen = offer.generation;
    let candidates = outbound_offer(offer, &st.handles.db).await;
    tracing::Span::current().record("candidates", candidates.len());
    if candidates.is_empty() {
        return true;
    }

    debug!(peer_id = %st.peer_id, count = candidates.len(), "pushing job offer (delta)");
    for chunk in job_offer_chunks(&candidates) {
        if send_server_msg(
            &st.writer,
            &ServerMessage::JobOffer {
                candidates: chunk.to_vec(),
            },
        )
        .await
        .is_err()
        {
            return false;
        }
    }

    true
}

/// Unanswered frames are budgeted in bytes, not in count: a graph that lags behind holds up many
/// small reports and a frame cap stopped the reader before a lookup the worker awaits.
const READ_AHEAD_BYTES: usize = 32 * 1024 * 1024;

async fn read_loop(
    mut reader: ProtoReader,
    session: ActorRef<SessionMsg>,
    last_seen: Arc<AtomicI64>,
    stamp_every: Duration,
    serve: impl Fn(ClientMessage) -> Option<ClientMessage> + Send + 'static,
) {
    let stamp = || {
        last_seen.store(
            gradient_types::now().and_utc().timestamp_millis(),
            Ordering::Relaxed,
        )
    };
    let mut unanswered: VecDeque<(oneshot::Receiver<bool>, usize)> = VecDeque::new();
    let mut unanswered_bytes = 0;
    let mut in_flight =
        tokio::time::interval_at(tokio::time::Instant::now() + stamp_every, stamp_every);
    loop {
        tokio::select! {
            biased;
            answered = async { (&mut unanswered.front_mut().expect("guarded").0).await }, if !unanswered.is_empty() => {
                if let Some((_, bytes)) = unanswered.pop_front() {
                    unanswered_bytes -= bytes;
                }
                if !matches!(answered, Ok(true)) {
                    return;
                }
            }
            inbound = reader.recv_sized(), if unanswered_bytes < READ_AHEAD_BYTES => {
                let Some((inbound, len)) = inbound else { break };
                stamp();
                let Some(msg) = serve(inbound) else { continue };
                let (reply, answered) = oneshot::channel();
                if session.send_message(SessionMsg::Frame(msg, reply.into())).is_err() {
                    return;
                }
                let bytes = len + size_of::<SessionMsg>();
                unanswered_bytes += bytes;
                unanswered.push_back((answered, bytes));
            }
            _ = in_flight.tick(), if !unanswered.is_empty() => stamp(),
        }
    }

    let _ = session.send_message(SessionMsg::ReaderClosed);
}

fn open_uploads(
    upload_admission: &Arc<UploadAdmission>,
    config: &RuntimeConfig,
    shutdown: &Shutdown,
    peer_id: &str,
    myself: &ActorRef<SessionMsg>,
) -> anyhow::Result<UploadSession> {
    let (admission, mut admitted) = upload_admission.open_session(peer_id);
    let uploads = UploadSession {
        admission,
        table: UploadTable::default(),
        partials: gradient_storage::PartialStore::new(config.server.nar_partial_dir())?,
        retain_up_to: config.nar.small_bytes,
        idle_lease: Duration::from_secs(config.upload.lease_idle_secs.get()),
    };
    let forward = myself.clone();
    shutdown.spawn(async move {
        while let Some(a) = admitted.recv().await {
            if forward.send_message(SessionMsg::Admitted(a)).is_err() {
                break;
            }
        }
    });
    let sweep = myself.clone();
    shutdown.spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tick.tick().await;
            if sweep.send_message(SessionMsg::SweepUploads).is_err() {
                break;
            }
        }
    });
    Ok(uploads)
}

fn split_uploads(st: &mut SessionState) -> (InboundContext<'_>, &mut UploadSession) {
    (
        InboundContext {
            writer: &st.writer,
            handles: &st.handles,
            scheduler: &st.scheduler,
            peer_id: &st.peer_id,
            nar_serve_semaphore: &st.nar_serve_semaphore,
            active: &st.active,
            job_events: &st.job_events,
            logs: &st.logs,
            dialed: st.dialed.as_ref(),
        },
        &mut st.uploads,
    )
}

#[cfg(test)]
pub(in crate::handler) mod tests {
    use super::super::sessions::{AttachedSession, Sessions, SessionsArgs, SessionsMsg};
    use super::*;
    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};
    use gradient_fixtures::database::TestDatabase;
    use gradient_fixtures::mock::empty_db;
    use gradient_test_support::prelude::*;
    use gradient_wire::PROTO_VERSIONS;
    use gradient_wire::codec::{from_bytes, to_bytes};
    use gradient_wire::types::{WorkerCheckStatus, WorkerHealth, WorkerHealthCheck};
    use ractor::rpc::CallResult;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Notify;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    struct DecliningSession;

    impl Actor for DecliningSession {
        type Msg = SessionMsg;
        type State = ();
        type Arguments = ();

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            _args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(())
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            msg: Self::Msg,
            _st: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            if let SessionMsg::Frame(_, reply) = msg {
                let _ = reply.send(false);
            }

            Ok(())
        }
    }

    struct HoldingSession;

    impl Actor for HoldingSession {
        type Msg = SessionMsg;
        type State = (Arc<Notify>, Option<RpcReplyPort<bool>>);
        type Arguments = Arc<Notify>;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            received: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok((received, None))
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            msg: Self::Msg,
            (received, held): &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match msg {
                SessionMsg::Frame(_, reply) => {
                    *held = Some(reply);
                    received.notify_one();
                }
                SessionMsg::Reattach => {
                    if let Some(reply) = held.take() {
                        let _ = reply.send(false);
                    }
                }
                _ => {}
            }

            Ok(())
        }
    }

    struct BacklogSession;

    impl Actor for BacklogSession {
        type Msg = SessionMsg;
        type State = tokio::sync::watch::Receiver<bool>;
        type Arguments = tokio::sync::watch::Receiver<bool>;

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            graph_caught_up: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(graph_caught_up)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            msg: Self::Msg,
            graph_caught_up: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            if let SessionMsg::Frame(_, reply) = msg {
                let _ = graph_caught_up.wait_for(|caught_up| *caught_up).await;
                let _ = reply.send(false);
            }

            Ok(())
        }
    }

    pub(in crate::handler) async fn connected_pair()
    -> (ProtoSocket, WebSocketStream<MaybeTlsStream<TcpStream>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dial = tokio_tungstenite::connect_async(format!("ws://{addr}/proto"));
        let accept = async {
            let (tcp, _) = listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(MaybeTlsStream::Plain(tcp))
                .await
                .unwrap()
        };
        let (client, server) = tokio::join!(dial, accept);

        (
            ProtoSocket::tungstenite(server).with_version(*PROTO_VERSIONS.end()),
            client.unwrap().0,
        )
    }

    async fn await_workers(
        scheduler: &Scheduler,
        predicate: impl Fn(&[gradient_pool::WorkerInfo]) -> bool,
    ) -> Vec<gradient_pool::WorkerInfo> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let workers = scheduler.board_workers().await;
                if predicate(&workers) {
                    return workers;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the session updates the workers")
    }

    #[tokio::test]
    async fn required_sessions_start_not_ready_and_apply_wire_health_reports() {
        let (socket, mut client) = connected_pair().await;
        let db = TestDatabase::create().await;
        let state = test_state(db.connection());
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
        let core = scheduler.spawn_core(None).await.unwrap();
        let (actor, join) = Actor::spawn(
            None,
            SessionActor,
            SessionArgs {
                peer_id: "w1".into(),
                handles: SessionHandles::from_state(&state),
                scheduler: Arc::clone(&scheduler),
                socket,
                capabilities: GradientCapabilities::default(),
                authorized_peers: HashSet::new(),
                dialed: None,
                readiness_required: true,
            },
        )
        .await
        .unwrap();
        let workers = scheduler.board_workers().await;
        let initial = workers[0].health.as_ref().expect("required health");
        assert!(!initial.ready);
        assert!(initial.checks.is_empty());
        assert_eq!(initial.observed_at, None);
        assert_eq!(initial.expires_at, None);

        actor.send_message(SessionMsg::Reattach).unwrap();
        for (healthy, valid_for_secs, ready) in [
            (false, 60, false),
            (true, 60, true),
            (true, 0, false),
            (false, 60, false),
        ] {
            let health = WorkerHealth {
                checks: vec![WorkerHealthCheck {
                    name: "hook".into(),
                    status: WorkerCheckStatus::Passed,
                    healthy,
                }],
                valid_for_secs,
            };
            client
                .send(Message::Binary(
                    to_bytes(
                        &ClientMessage::WorkerHealth {
                            health: health.clone(),
                        },
                        61,
                    )
                    .unwrap(),
                ))
                .await
                .unwrap();
            let workers = await_workers(&scheduler, |workers| {
                workers
                    .first()
                    .and_then(|worker| worker.health.as_ref())
                    .is_some_and(|observed| {
                        observed.checks == health.checks && observed.ready == ready
                    })
            })
            .await;
            let observed = workers[0].health.as_ref().unwrap();
            assert!(observed.observed_at.is_some());
            assert!(observed.expires_at.is_some());
        }
        actor.stop(None);
        tokio::time::timeout(gradient_scheduler::actor::CALL_TIMEOUT, join)
            .await
            .expect("required session cleanup completes")
            .unwrap();
        core.stop_and_wait(None, Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert!(
            state
                .shutdown
                .cancel_and_drain(Duration::from_secs(35))
                .await
        );
        drop((actor, core, client, scheduler, state));
        db.finish().await;
    }

    #[tokio::test]
    async fn health_before_reattach_survives_core_restart_without_renewing_expiry() {
        let (socket, mut client) = connected_pair().await;
        let db = TestDatabase::create().await;
        let state = test_state(db.connection());
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
        let core = scheduler.spawn_core(None).await.unwrap();
        let (actor, join) = Actor::spawn(
            None,
            SessionActor,
            SessionArgs {
                peer_id: "w1".into(),
                handles: SessionHandles::from_state(&state),
                scheduler: Arc::clone(&scheduler),
                socket,
                capabilities: GradientCapabilities::default(),
                authorized_peers: HashSet::new(),
                dialed: None,
                readiness_required: true,
            },
        )
        .await
        .unwrap();

        core.stop_and_wait(None, Some(Duration::from_secs(5)))
            .await
            .unwrap();
        let core = scheduler.spawn_core(None).await.unwrap();
        assert!(scheduler.board_workers().await.is_empty());
        let health = WorkerHealth {
            checks: vec![WorkerHealthCheck {
                name: "hook".into(),
                status: WorkerCheckStatus::Passed,
                healthy: true,
            }],
            valid_for_secs: 1,
        };
        let sent_at = gradient_types::now();
        for message in [
            ClientMessage::WorkerHealth {
                health: health.clone(),
            },
            ClientMessage::RequestJobList,
        ] {
            client
                .send(Message::Binary(to_bytes(&message, 61).unwrap()))
                .await
                .unwrap();
        }
        // The ordered reply proves the health frame was accepted while the new pool was empty.
        let frame = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("health must not close the session before reattachment")
            .unwrap()
            .unwrap();
        let Message::Binary(bytes) = frame else {
            panic!("expected a job-list reply, got {frame:?}");
        };
        assert_eq!(
            from_bytes::<ServerMessage>(bytes, 61).unwrap(),
            ServerMessage::JobListChunk {
                candidates: vec![],
                is_final: true
            },
        );
        let accepted_at = gradient_types::now();
        assert!(!join.is_finished());
        assert!(scheduler.board_workers().await.is_empty());

        actor.send_message(SessionMsg::Reattach).unwrap();
        let workers = await_workers(&scheduler, |workers| {
            workers
                .first()
                .and_then(|worker| worker.health.as_ref())
                .is_some_and(|observed| observed.checks == health.checks && observed.ready)
        })
        .await;
        let original = workers[0].health.as_ref().unwrap();
        let observed_at = original.observed_at.expect("the report's observation time");
        let expires_at = original.expires_at.expect("the report's expiry time");
        assert!(observed_at >= sent_at && observed_at <= accepted_at);
        assert_eq!(expires_at, observed_at + chrono::Duration::seconds(1));

        tokio::time::sleep(Duration::from_millis(1100)).await;
        core.stop_and_wait(None, Some(Duration::from_secs(5)))
            .await
            .unwrap();
        let core = scheduler.spawn_core(None).await.unwrap();
        assert!(scheduler.board_workers().await.is_empty());
        actor.send_message(SessionMsg::Reattach).unwrap();
        let workers = await_workers(&scheduler, |workers| {
            workers
                .first()
                .and_then(|worker| worker.health.as_ref())
                .is_some_and(|observed| observed.checks == health.checks && !observed.ready)
        })
        .await;
        let restored = workers[0].health.as_ref().unwrap();
        assert_eq!(restored.observed_at, Some(observed_at));
        assert_eq!(restored.expires_at, Some(expires_at));
        assert!(!join.is_finished());

        actor.stop(None);
        tokio::time::timeout(gradient_scheduler::actor::CALL_TIMEOUT, join)
            .await
            .expect("restarted session cleanup completes")
            .unwrap();
        core.stop_and_wait(None, Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert!(
            state
                .shutdown
                .cancel_and_drain(Duration::from_secs(35))
                .await
        );
        drop((actor, core, client, scheduler, state));
        db.finish().await;
    }

    #[tokio::test]
    async fn invalid_unsolicited_or_stale_health_closes_the_session() {
        for (readiness_required, superseded) in [(false, false), (true, false), (true, true)] {
            let (socket, mut client) = connected_pair().await;
            let db = TestDatabase::create().await;
            let state = test_state(db.connection());
            let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
            let core = scheduler.spawn_core(None).await.unwrap();
            let (_actor, join) = Actor::spawn(
                None,
                SessionActor,
                SessionArgs {
                    peer_id: "w1".into(),
                    handles: SessionHandles::from_state(&state),
                    scheduler: Arc::clone(&scheduler),
                    socket,
                    capabilities: GradientCapabilities::default(),
                    authorized_peers: HashSet::new(),
                    dialed: None,
                    readiness_required,
                },
            )
            .await
            .unwrap();
            let replacement = if superseded {
                let (socket, client) = connected_pair().await;
                let (actor, join) = Actor::spawn(
                    None,
                    SessionActor,
                    SessionArgs {
                        peer_id: "w1".into(),
                        handles: SessionHandles::from_state(&state),
                        scheduler: Arc::clone(&scheduler),
                        socket,
                        capabilities: GradientCapabilities::default(),
                        authorized_peers: HashSet::new(),
                        dialed: None,
                        readiness_required: true,
                    },
                )
                .await
                .unwrap();
                let workers = scheduler.board_workers().await;
                let health = workers[0].health.as_ref().expect("replacement health");
                assert!(!health.ready);
                assert!(health.checks.is_empty());
                Some((actor, join, client))
            } else {
                None
            };
            let checks = if readiness_required && !superseded {
                vec![]
            } else {
                vec![WorkerHealthCheck {
                    name: "hook".into(),
                    status: WorkerCheckStatus::Passed,
                    healthy: true,
                }]
            };
            client
                .send(Message::Binary(
                    to_bytes(
                        &ClientMessage::WorkerHealth {
                            health: WorkerHealth {
                                checks,
                                valid_for_secs: 60,
                            },
                        },
                        61,
                    )
                    .unwrap(),
                ))
                .await
                .unwrap();
            let frame = tokio::time::timeout(Duration::from_secs(5), client.next())
                .await
                .expect("the report is rejected")
                .unwrap()
                .unwrap();
            let Message::Binary(bytes) = frame else {
                panic!("expected a rejection, got {frame:?}");
            };
            assert_eq!(
                from_bytes::<ServerMessage>(bytes, 61).unwrap(),
                ServerMessage::Error {
                    code: 400,
                    message: if readiness_required {
                        "invalid worker health"
                    } else {
                        "unsolicited worker health"
                    }
                    .into(),
                },
            );
            tokio::time::timeout(Duration::from_secs(5), join)
                .await
                .expect("the session closes")
                .unwrap();
            if let Some((actor, join, _client)) = replacement {
                actor.stop(None);
                tokio::time::timeout(gradient_scheduler::actor::CALL_TIMEOUT, join)
                    .await
                    .expect("replacement session cleanup completes after stale health rejection")
                    .unwrap();
            } else {
                assert!(!scheduler.is_worker_connected("w1").await);
            }
            core.stop_and_wait(None, Some(Duration::from_secs(5)))
                .await
                .unwrap();
            assert!(
                state
                    .shutdown
                    .cancel_and_drain(Duration::from_secs(35))
                    .await
            );
            drop((_actor, core, client, scheduler, state));
            db.finish().await;
        }
    }

    #[tokio::test]
    async fn an_abort_cluster_signal_reaches_the_worker() {
        let (socket, mut client) = connected_pair().await;
        let state = test_state(empty_db());
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
        scheduler.spawn_core(None).await.unwrap();
        let (actor, _join) = Actor::spawn(
            None,
            SessionActor,
            SessionArgs {
                peer_id: "w1".into(),
                handles: SessionHandles::from_state(&state),
                scheduler: Arc::clone(&scheduler),
                socket,
                capabilities: GradientCapabilities::default(),
                authorized_peers: HashSet::new(),
                dialed: None,
                readiness_required: false,
            },
        )
        .await
        .unwrap();

        actor
            .send_message(SessionMsg::Signal(SessionSignal::AbortCluster {
                attempt: "a1".into(),
                reason: "member lost".into(),
            }))
            .unwrap();

        let frame = client
            .next()
            .await
            .expect("a frame")
            .expect("no transport error");
        let Message::Binary(bytes) = frame else {
            panic!("expected a binary frame, got {frame:?}");
        };
        assert_eq!(
            from_bytes::<ServerMessage>(bytes, *PROTO_VERSIONS.end()).unwrap(),
            ServerMessage::AbortCluster {
                attempt: "a1".into(),
                reason: "member lost".into(),
            }
        );
    }

    #[tokio::test]
    async fn pre_warm_vms_reaches_the_worker() {
        let (socket, mut client) = connected_pair().await;
        let state = test_state(empty_db());
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
        scheduler.spawn_core(None).await.unwrap();
        let (actor, _join) = Actor::spawn(
            None,
            SessionActor,
            SessionArgs {
                peer_id: "w1".into(),
                handles: SessionHandles::from_state(&state),
                scheduler: Arc::clone(&scheduler),
                socket,
                capabilities: GradientCapabilities::default(),
                authorized_peers: HashSet::new(),
                dialed: None,
                readiness_required: false,
            },
        )
        .await
        .unwrap();

        actor
            .send_message(SessionMsg::Signal(SessionSignal::PreWarmVms {
                count: 2,
                sizes: Vec::new(),
            }))
            .unwrap();

        let frame = client
            .next()
            .await
            .expect("a frame")
            .expect("no transport error");
        let Message::Binary(bytes) = frame else {
            panic!("expected a binary frame, got {frame:?}");
        };
        assert_eq!(
            from_bytes::<ServerMessage>(bytes, *PROTO_VERSIONS.end()).unwrap(),
            ServerMessage::PreWarmVms {
                count: 2,
                sizes: Vec::new()
            }
        );
    }

    #[tokio::test]
    async fn replacing_a_same_id_session_waits_for_old_cleanup_before_admission() {
        for readiness_required in [false, true] {
            let db = TestDatabase::create().await;
            let state = test_state(db.connection());
            let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
            let core = scheduler.spawn_core(None).await.unwrap();
            let (sessions, sessions_join) = Actor::spawn(
                None,
                Sessions,
                SessionsArgs {
                    scheduler: Arc::clone(&scheduler),
                    shutdown: state.shutdown.clone(),
                },
            )
            .await
            .unwrap();
            let mut previous: Option<AttachedSession> = None;
            let mut clients = Vec::new();
            for generation in 1..=2 {
                let (socket, client) = connected_pair().await;
                clients.push(client);
                let args = SessionArgs {
                    peer_id: "w1".into(),
                    handles: SessionHandles::from_state(&state),
                    scheduler: Arc::clone(&scheduler),
                    socket,
                    capabilities: GradientCapabilities::default(),
                    authorized_peers: HashSet::new(),
                    dialed: None,
                    readiness_required,
                };
                let CallResult::Success(Ok((actor, join))) = sessions
                    .call(
                        |reply| SessionsMsg::Attach(Box::new(args), reply),
                        Some(gradient_scheduler::actor::CALL_TIMEOUT),
                    )
                    .await
                    .unwrap()
                else {
                    panic!("the session attaches");
                };
                if let Some((old, old_join)) = previous.take() {
                    assert_ne!(old.get_id(), actor.get_id());
                    tokio::time::timeout(Duration::from_secs(5), old_join)
                        .await
                        .expect("old session cleanup completes before replacement admission")
                        .unwrap();
                }
                let workers = scheduler.board_workers().await;
                assert_eq!(workers.len(), 1);
                let connections = db
                    .rows::<gradient_entity::worker_connection::Entity>()
                    .await;
                assert_eq!(connections.len(), generation);
                assert_eq!(
                    connections
                        .iter()
                        .filter(|connection| connection.disconnected_at.is_none())
                        .count(),
                    1,
                );
                if readiness_required {
                    let initial = workers[0].health.as_ref().expect("required health");
                    assert!(!initial.ready);
                    assert!(initial.checks.is_empty());
                    assert!(initial.observed_at.is_none());
                    let result = actor
                        .call(
                            |reply| {
                                SessionMsg::Frame(
                                    ClientMessage::WorkerHealth {
                                        health: WorkerHealth {
                                            checks: vec![WorkerHealthCheck {
                                                name: "hook".into(),
                                                status: WorkerCheckStatus::Passed,
                                                healthy: true,
                                            }],
                                            valid_for_secs: 60,
                                        },
                                    },
                                    reply,
                                )
                            },
                            Some(Duration::from_secs(5)),
                        )
                        .await
                        .unwrap();
                    assert!(matches!(result, CallResult::Success(true)));
                    assert!(
                        scheduler.board_workers().await[0]
                            .health
                            .as_ref()
                            .unwrap()
                            .ready
                    );
                } else {
                    assert!(workers[0].health.is_none());
                }
                previous = Some((actor, join));
            }
            sessions.stop(None);
            tokio::time::timeout(gradient_scheduler::actor::CALL_TIMEOUT, sessions_join)
                .await
                .expect("handoff supervisor cleanup completes")
                .unwrap();
            tokio::time::timeout(gradient_scheduler::actor::CALL_TIMEOUT, previous.unwrap().1)
                .await
                .expect("final handoff session cleanup completes")
                .unwrap();
            assert!(!scheduler.is_worker_connected("w1").await);
            assert!(
                db.rows::<gradient_entity::worker_connection::Entity>()
                    .await
                    .iter()
                    .all(|connection| connection.disconnected_at.is_some())
            );
            core.stop_and_wait(None, Some(Duration::from_secs(5)))
                .await
                .unwrap();
            assert!(
                state
                    .shutdown
                    .cancel_and_drain(Duration::from_secs(35))
                    .await
            );
            drop((sessions, core, clients, scheduler, state));
            db.finish().await;
        }
    }

    #[tokio::test]
    async fn stopping_the_sessions_closes_every_socket_without_a_draining_frame() {
        let (socket, mut client) = connected_pair().await;
        let state = test_state(empty_db());
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
        scheduler.spawn_core(None).await.unwrap();
        let (sessions, sessions_join) = Actor::spawn(
            None,
            Sessions,
            SessionsArgs {
                scheduler: Arc::clone(&scheduler),
                shutdown: state.shutdown.clone(),
            },
        )
        .await
        .unwrap();
        let args = SessionArgs {
            peer_id: "w1".into(),
            handles: SessionHandles::from_state(&state),
            scheduler: Arc::clone(&scheduler),
            socket,
            capabilities: GradientCapabilities::default(),
            authorized_peers: HashSet::new(),
            dialed: None,
            readiness_required: false,
        };
        let CallResult::Success(Ok((_session, session_join))) = sessions
            .call(|reply| SessionsMsg::Attach(Box::new(args), reply), None)
            .await
            .unwrap()
        else {
            panic!("the session attaches");
        };
        assert!(scheduler.is_worker_connected("w1").await);

        sessions.stop(None);
        sessions_join.await.unwrap();
        session_join.await.unwrap();

        let frames = tokio::time::timeout(Duration::from_secs(5), async {
            let mut frames = Vec::new();
            while let Some(Ok(Message::Binary(bytes))) = client.next().await {
                frames.push(from_bytes::<ServerMessage>(bytes, *PROTO_VERSIONS.end()).unwrap());
            }
            frames
        })
        .await
        .expect("the server closes the socket");
        assert!(!frames.contains(&ServerMessage::Draining), "{frames:?}");
        assert!(!scheduler.is_worker_connected("w1").await);
    }

    #[tokio::test]
    async fn the_reader_stamps_liveness_on_receipt() {
        let (socket, mut client) = connected_pair().await;
        let state = test_state(empty_db());
        let (reader, _writer) = socket.split(Duration::from_secs(5), &state.shutdown);
        let (session, join) = Actor::spawn(None, DecliningSession, ()).await.unwrap();
        let last_seen = Arc::new(AtomicI64::new(0));

        client
            .send(Message::Binary(
                to_bytes(&ClientMessage::ReauthRequest, *PROTO_VERSIONS.end()).unwrap(),
            ))
            .await
            .unwrap();
        read_loop(
            reader,
            session.clone(),
            Arc::clone(&last_seen),
            IN_FLIGHT_STAMP,
            Some,
        )
        .await;
        session.stop(None);
        join.await.unwrap();

        assert!(
            last_seen.load(Ordering::Relaxed) > 0,
            "the reader, not the handler, stamps the frame it just received"
        );
    }

    #[tokio::test]
    async fn the_reader_keeps_stamping_while_a_frame_is_in_flight() {
        let (socket, mut client) = connected_pair().await;
        let state = test_state(empty_db());
        let (reader, _writer) = socket.split(Duration::from_secs(5), &state.shutdown);
        let received = Arc::new(Notify::new());
        let (session, join) = Actor::spawn(None, HoldingSession, Arc::clone(&received))
            .await
            .unwrap();
        let last_seen = Arc::new(AtomicI64::new(0));

        client
            .send(Message::Binary(
                to_bytes(&ClientMessage::ReauthRequest, *PROTO_VERSIONS.end()).unwrap(),
            ))
            .await
            .unwrap();
        let reading = state.shutdown.spawn(read_loop(
            reader,
            session.clone(),
            Arc::clone(&last_seen),
            Duration::from_millis(20),
            Some,
        ));
        received.notified().await;
        last_seen.store(0, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let stamped = last_seen.load(Ordering::Relaxed);

        session.send_message(SessionMsg::Reattach).unwrap();
        reading.await.unwrap();
        session.stop(None);
        join.await.unwrap();

        assert!(
            stamped > 0,
            "a worker whose frame is still being handled is not silent"
        );
    }

    #[tokio::test]
    async fn a_cache_query_is_served_while_an_earlier_frame_is_held() {
        let (socket, mut client) = connected_pair().await;
        let state = test_state(empty_db());
        let (reader, _writer) = socket.split(Duration::from_secs(5), &state.shutdown);
        let received = Arc::new(Notify::new());
        let (session, join) = Actor::spawn(None, HoldingSession, Arc::clone(&received))
            .await
            .unwrap();
        let (served_tx, mut served) = tokio::sync::mpsc::unbounded_channel();

        for msg in [
            ClientMessage::ReauthRequest,
            ClientMessage::CacheQuery {
                job_id: "j1".into(),
                query_id: "q1".into(),
                paths: Vec::new(),
                nar_sizes: Vec::new(),
                mode: gradient_wire::types::QueryMode::Pull,
                external: false,
            },
        ] {
            client
                .send(Message::Binary(
                    to_bytes(&msg, *PROTO_VERSIONS.end()).unwrap(),
                ))
                .await
                .unwrap();
        }
        let reading = state.shutdown.spawn(read_loop(
            reader,
            session.clone(),
            Arc::new(AtomicI64::new(0)),
            IN_FLIGHT_STAMP,
            move |msg| match msg {
                ClientMessage::CacheQuery { query_id, .. } => {
                    let _ = served_tx.send(query_id);
                    None
                }
                other => Some(other),
            },
        ));
        received.notified().await;
        let query = tokio::time::timeout(Duration::from_secs(5), served.recv())
            .await
            .expect("the lookup is not queued behind the held frame");

        session.send_message(SessionMsg::Reattach).unwrap();
        reading.await.unwrap();
        session.stop(None);
        join.await.unwrap();

        assert_eq!(query.as_deref(), Some("q1"));
    }

    #[tokio::test]
    async fn a_cache_query_behind_hundreds_of_unanswered_log_chunks_is_served() {
        let (socket, mut client) = connected_pair().await;
        let state = test_state(empty_db());
        let (reader, _writer) = socket.split(Duration::from_secs(5), &state.shutdown);
        let (catch_up, graph_caught_up) = tokio::sync::watch::channel(false);
        let (session, join) = Actor::spawn(None, BacklogSession, graph_caught_up)
            .await
            .unwrap();
        let (served_tx, mut served) = tokio::sync::mpsc::unbounded_channel();

        let log_chunks = (0..500).map(|_| ClientMessage::LogChunk {
            job_id: "j1".into(),
            data: Bytes::from_static(b"building\n"),
        });
        let query = ClientMessage::CacheQuery {
            job_id: "j1".into(),
            query_id: "q1".into(),
            paths: Vec::new(),
            nar_sizes: Vec::new(),
            mode: gradient_wire::types::QueryMode::Pull,
            external: false,
        };
        for msg in log_chunks.chain([query]) {
            client
                .feed(Message::Binary(
                    to_bytes(&msg, *PROTO_VERSIONS.end()).unwrap(),
                ))
                .await
                .unwrap();
        }
        client.flush().await.unwrap();
        let reading = state.shutdown.spawn(read_loop(
            reader,
            session.clone(),
            Arc::new(AtomicI64::new(0)),
            IN_FLIGHT_STAMP,
            move |msg| match msg {
                ClientMessage::CacheQuery { query_id, .. } => {
                    let _ = served_tx.send(query_id);
                    None
                }
                other => Some(other),
            },
        ));
        let query = tokio::time::timeout(Duration::from_secs(5), served.recv())
            .await
            .expect("the lookup is read while the session is behind the graph");

        catch_up.send(true).unwrap();
        reading.await.unwrap();
        session.stop(None);
        join.await.unwrap();

        assert_eq!(query.as_deref(), Some("q1"));
    }
}
