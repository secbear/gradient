/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use gradient_scheduler::Scheduler;
use gradient_scheduler::actor::CALL_TIMEOUT;
use gradient_util::shutdown::Shutdown;
use gradient_util::supervision::{ChildCtx, ChildSpec};
use ractor::rpc::CallResult;
use ractor::{Actor, ActorId, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::session_actor::{SessionActor, SessionArgs, SessionMsg};

pub type AttachedSession = (ActorRef<SessionMsg>, JoinHandle<()>);

pub enum SessionsMsg {
    Attach(
        Box<SessionArgs>,
        RpcReplyPort<Result<AttachedSession, String>>,
    ),
    Reattach,
}

pub struct SessionsHandle {
    actor: ArcSwapOption<ActorRef<SessionsMsg>>,
}

impl SessionsHandle {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            actor: ArcSwapOption::empty(),
        })
    }

    pub fn child_spec(self: &Arc<Self>, scheduler: Arc<Scheduler>) -> ChildSpec {
        let handle = Arc::clone(self);
        ChildSpec::Custom {
            stop_last: false,
            name: "sessions",
            spawn: Arc::new(move |ctx: ChildCtx| {
                let handle = Arc::clone(&handle);
                let scheduler = Arc::clone(&scheduler);
                Box::pin(async move {
                    let args = SessionsArgs {
                        shutdown: scheduler.db.shutdown.clone(),
                        scheduler,
                    };
                    let (actor, _) = Actor::spawn_linked(None, Sessions, args, ctx.parent).await?;
                    handle.actor.store(Some(Arc::new(actor.clone())));
                    Ok(actor.get_cell())
                })
            }),
        }
    }

    pub async fn attach(&self, args: SessionArgs) -> Result<AttachedSession, String> {
        let Some(actor) = self.actor.load_full() else {
            return Err("sessions supervisor not running".into());
        };

        match actor
            .call(
                |reply| SessionsMsg::Attach(Box::new(args), reply),
                Some(CALL_TIMEOUT),
            )
            .await
        {
            Ok(CallResult::Success(result)) => result,
            Ok(CallResult::Timeout) => Err("sessions supervisor timed out".into()),
            Ok(CallResult::SenderError) => Err("sessions supervisor dropped the reply".into()),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub struct Sessions;

pub struct SessionsArgs {
    pub scheduler: Arc<Scheduler>,
    pub shutdown: Shutdown,
}

pub struct SessionsState {
    scheduler: Arc<Scheduler>,
    live: HashMap<ActorId, (String, ActorRef<SessionMsg>)>,
    core_watch: JoinHandle<()>,
}

impl Actor for Sessions {
    type Msg = SessionsMsg;
    type State = SessionsState;
    type Arguments = SessionsArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let mut cores = args.scheduler.core_changes();
        let core_watch = args.shutdown.spawn(async move {
            while cores.changed().await.is_ok() {
                if cores.borrow_and_update().is_some()
                    && myself.send_message(SessionsMsg::Reattach).is_err()
                {
                    return;
                }
            }
        });

        Ok(SessionsState {
            scheduler: args.scheduler,
            live: HashMap::new(),
            core_watch,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        st: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            SessionsMsg::Attach(args, reply) => {
                let peer_id = args.peer_id.clone();
                let previous: Vec<_> = st
                    .live
                    .iter()
                    .filter(|(_, (peer, _))| peer == &peer_id)
                    .map(|(id, (_, actor))| (*id, actor.clone()))
                    .collect();
                for (id, actor) in previous {
                    if let Err(error) = actor
                        .stop_and_wait(
                            Some("replaced by new connection".into()),
                            Some(CALL_TIMEOUT),
                        )
                        .await
                    {
                        let _ = reply.send(Err(format!(
                            "previous worker session did not stop: {error}"
                        )));
                        return Ok(());
                    }
                    // A failed child's post_stop may have skipped scheduler cleanup.
                    if let Err(error) = st
                        .scheduler
                        .unregister_worker_after_session_exit(&peer_id)
                        .await
                    {
                        let _ = reply.send(Err(format!(
                            "previous worker session cleanup failed: {error}"
                        )));
                        return Ok(());
                    }
                    // A queued failure event must not unregister the replacement.
                    st.live.remove(&id);
                }
                let result =
                    match Actor::spawn_linked(None, SessionActor, *args, myself.get_cell()).await {
                        Ok((actor, join)) => {
                            st.live.insert(actor.get_id(), (peer_id, actor.clone()));
                            Ok((actor, join))
                        }
                        Err(e) => Err(e.to_string()),
                    };
                let _ = reply.send(result);
            }
            SessionsMsg::Reattach => {
                info!(
                    sessions = st.live.len(),
                    "scheduler core republished; re-registering sessions"
                );
                for (_, actor) in st.live.values() {
                    let _ = actor.send_message(SessionMsg::Reattach);
                }
            }
        }

        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        evt: SupervisionEvent,
        st: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let (cell, failed) = match evt {
            SupervisionEvent::ActorFailed(cell, err) => {
                warn!(error = %err, "session actor panicked");
                (cell, true)
            }
            SupervisionEvent::ActorTerminated(cell, _, _) => (cell, false),
            _ => return Ok(()),
        };
        if let Some((peer_id, _)) = st.live.remove(&cell.get_id())
            && failed
        {
            st.scheduler.unregister_worker(&peer_id).await;
        }

        Ok(())
    }

    async fn post_stop(
        &self,
        myself: ActorRef<Self::Msg>,
        st: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        st.core_watch.abort();
        info!(sessions = st.live.len(), "closing sessions");
        // Ractor kills children still running once this returns, which skips their `post_stop`.
        myself
            .stop_children_and_wait(Some("shutdown".into()), Some(CALL_TIMEOUT))
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use gradient_fixtures::database::TestDatabase;
    use gradient_pool::session_port::SessionPort;
    use gradient_test_support::prelude::test_state;
    use gradient_types::events::Event;
    use gradient_wire::types::GradientCapabilities;
    use tokio::sync::Notify;

    use super::*;
    use crate::handler::SessionHandles;
    use crate::handler::session_actor::{SessionRef, tests::connected_pair};

    struct PanicsBeforeUnregister;

    impl Actor for PanicsBeforeUnregister {
        type Msg = SessionMsg;
        type State = (Arc<Notify>, Arc<Notify>);
        type Arguments = Self::State;

        async fn pre_start(
            &self,
            _: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(args)
        }

        async fn post_stop(
            &self,
            _: ActorRef<Self::Msg>,
            st: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            st.0.notify_one();
            st.1.notified().await;
            panic!("session failed before scheduler cleanup");
        }
    }

    struct SessionsWithFailingChild {
        failure_handled: Arc<Notify>,
        cleanup_entered: Arc<Notify>,
        cleanup_release: Arc<Notify>,
    }

    impl Actor for SessionsWithFailingChild {
        type Msg = SessionsMsg;
        type State = SessionsState;
        type Arguments = SessionsArgs;

        async fn pre_start(
            &self,
            myself: ActorRef<Self::Msg>,
            args: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            let mut st = Sessions.pre_start(myself.clone(), args).await?;
            let (child, _) = Actor::spawn_linked(
                None,
                PanicsBeforeUnregister,
                (
                    Arc::clone(&self.cleanup_entered),
                    Arc::clone(&self.cleanup_release),
                ),
                myself.get_cell(),
            )
            .await?;
            let port: Arc<dyn SessionPort> = Arc::new(SessionRef(child.clone()));
            st.scheduler
                .register_worker_with_readiness(
                    "w1",
                    GradientCapabilities::default(),
                    HashSet::new(),
                    port,
                    false,
                    true,
                )
                .await?;
            st.live.insert(child.get_id(), ("w1".into(), child));
            Ok(st)
        }

        async fn handle(
            &self,
            myself: ActorRef<Self::Msg>,
            msg: Self::Msg,
            st: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            Sessions.handle(myself, msg, st).await
        }

        async fn handle_supervisor_evt(
            &self,
            myself: ActorRef<Self::Msg>,
            evt: SupervisionEvent,
            st: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            let failed = matches!(&evt, SupervisionEvent::ActorFailed(..));
            Sessions.handle_supervisor_evt(myself, evt, st).await?;
            if failed {
                self.failure_handled.notify_one();
            }
            Ok(())
        }

        async fn post_stop(
            &self,
            myself: ActorRef<Self::Msg>,
            st: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            Sessions.post_stop(myself, st).await
        }
    }

    #[tokio::test]
    async fn a_failed_child_is_unregistered_before_its_replacement_is_admitted() {
        for restart_core in [false, true] {
            let db = TestDatabase::create().await;
            let state = test_state(db.connection());
            let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
            let mut core = scheduler.spawn_core(None).await.unwrap();
            let mut events = state.events.subscribe();
            let failure_handled = Arc::new(Notify::new());
            let cleanup_entered = Arc::new(Notify::new());
            let cleanup_release = Arc::new(Notify::new());
            let (sessions, sessions_join) = Actor::spawn(
                None,
                SessionsWithFailingChild {
                    failure_handled: Arc::clone(&failure_handled),
                    cleanup_entered: Arc::clone(&cleanup_entered),
                    cleanup_release: Arc::clone(&cleanup_release),
                },
                SessionsArgs {
                    scheduler: Arc::clone(&scheduler),
                    shutdown: state.shutdown.clone(),
                },
            )
            .await
            .unwrap();
            let (socket, client) = connected_pair().await;
            let args = SessionArgs {
                peer_id: "w1".into(),
                handles: SessionHandles::from_state(&state),
                scheduler: Arc::clone(&scheduler),
                socket,
                capabilities: GradientCapabilities::default(),
                authorized_peers: HashSet::new(),
                dialed: None,
                readiness_required: true,
            };
            let attached = {
                let attach = sessions.call(
                    |reply| SessionsMsg::Attach(Box::new(args), reply),
                    Some(CALL_TIMEOUT),
                );
                tokio::pin!(attach);
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        _ = cleanup_entered.notified() => {},
                        result = &mut attach => {
                            panic!("attachment finished before old cleanup: {result:?}");
                        }
                    }
                })
                .await
                .expect("Attach waits for the old child's cleanup");
                if restart_core {
                    core.stop_and_wait(None, Some(Duration::from_secs(5)))
                        .await
                        .unwrap();
                    core = scheduler.spawn_core(None).await.unwrap();
                    assert!(scheduler.board_workers().await.is_empty());
                }
                cleanup_release.notify_one();
                attach.await.unwrap()
            };
            let CallResult::Success(Ok((replacement, join))) = attached else {
                panic!("the replacement attaches");
            };
            tokio::time::timeout(Duration::from_secs(5), failure_handled.notified())
                .await
                .expect("the old child's failure event is handled after replacement");
            assert!(scheduler.is_worker_connected("w1").await);
            assert!(!join.is_finished());
            let workers = scheduler.board_workers().await;
            assert_eq!(workers.len(), 1);
            assert!(!workers[0].health.as_ref().unwrap().ready);
            let connections = db
                .rows::<gradient_entity::worker_connection::Entity>()
                .await;
            assert_eq!(connections.len(), 2);
            assert_eq!(
                connections
                    .iter()
                    .filter(|connection| connection.disconnected_at.is_none())
                    .count(),
                1,
            );
            let mut disconnects = 0;
            while let Ok(envelope) = events.try_recv() {
                if let Event::WorkerDisconnected(event) = &envelope.event
                    && event.worker_id == "w1"
                {
                    disconnects += 1;
                }
            }
            assert_eq!(disconnects, 1, "old session cleanup emits one disconnect");

            sessions.stop(None);
            tokio::time::timeout(CALL_TIMEOUT, sessions_join)
                .await
                .expect("failed-child supervisor cleanup completes")
                .unwrap();
            tokio::time::timeout(CALL_TIMEOUT, join)
                .await
                .expect("failed-child replacement cleanup completes")
                .unwrap();
            scheduler
                .unregister_worker_after_session_exit("w1")
                .await
                .unwrap();
            let mut disconnects = 0;
            while let Ok(envelope) = events.try_recv() {
                if let Event::WorkerDisconnected(event) = &envelope.event
                    && event.worker_id == "w1"
                {
                    disconnects += 1;
                }
            }
            assert_eq!(disconnects, 1, "normal session cleanup is not repeated");
            core.stop_and_wait(None, Some(Duration::from_secs(5)))
                .await
                .unwrap();
            assert!(
                state
                    .shutdown
                    .cancel_and_drain(Duration::from_secs(35))
                    .await
            );
            drop((sessions, replacement, core, client, scheduler, state));
            db.finish().await;
        }
    }
}
