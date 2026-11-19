/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gradient_wire::messages::{CandidateScore, ClientMessage, JobCandidate, JobKind};
use gradient_wire::traits::WorkerStore;
use tracing::{Instrument as _, warn};

use crate::health::HealthChecks;
use crate::proto::scorer::JobScorer;
use gradient_worker_client::connection::ProtoWriter;

pub(super) struct ScoringReadiness {
    pub(super) health: HealthChecks,
    pub(super) refresh_pending: Arc<AtomicBool>,
}

impl ScoringReadiness {
    fn ready(&self) -> bool {
        if self.health.ready() {
            return true;
        }
        self.refresh_pending.store(true, Ordering::Release);
        false
    }
}

pub(super) fn spawn_scoring_task<S: WorkerStore + ?Sized + 'static>(
    scorer: JobScorer,
    store: Arc<S>,
    writer: ProtoWriter,
    candidates: Vec<JobCandidate>,
    is_final: bool,
    request_after: Vec<JobKind>,
    readiness: ScoringReadiness,
) {
    #[expect(
        clippy::disallowed_methods,
        reason = "reports through the writer; the connection going away drops it"
    )]
    tokio::spawn(async move {
        let started = std::time::Instant::now();
        let count = candidates.len();
        let to_send = match scorer
            .score_candidates(&*store, &candidates)
            .instrument(tracing::debug_span!("score_candidates", count))
            .await
        {
            Ok(s) => s,
            Err(e) => {
                readiness.refresh_pending.store(true, Ordering::Release);
                warn!(error = %e, count, "score_candidates failed in spawned task");
                return;
            }
        };

        tracing::debug!(
            scored = count,
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            is_final,
            "scoring task complete"
        );

        if let Err(e) = send_score_chunks(
            move |message| {
                let writer = writer.clone();
                async move { writer.send(message).await }
            },
            to_send,
            is_final,
            request_after,
            &readiness,
        )
        .instrument(tracing::debug_span!("send_scores"))
        .await
        {
            readiness.refresh_pending.store(true, Ordering::Release);
            warn!(error = %e, "send_score_chunks failed");
        }
    });
}

/// A ready worker sends a final sentinel even when it has no scores.
pub(super) async fn send_score_chunks<F, Fut>(
    send: F,
    scores: Vec<CandidateScore>,
    is_final: bool,
    request_after: Vec<JobKind>,
    readiness: &ScoringReadiness,
) -> anyhow::Result<()>
where
    F: Fn(ClientMessage) -> Fut + Send,
    Fut: Future<Output = anyhow::Result<()>> + Send,
{
    if scores.is_empty() && is_final {
        if !readiness.ready() {
            return Ok(());
        }
        send(ClientMessage::RequestJobChunk {
            scores: vec![],
            is_final: true,
        })
        .await?;
    }
    let total = scores.len().div_ceil(1_000);
    for (i, chunk) in scores.chunks(1_000).enumerate() {
        if !readiness.ready() {
            return Ok(());
        }
        send(ClientMessage::RequestJobChunk {
            scores: chunk.to_vec(),
            is_final: is_final && i + 1 == total,
        })
        .await?;
    }
    for kind in request_after {
        if !readiness.ready() {
            return Ok(());
        }
        send(ClientMessage::RequestJob { kind: kind.clone() })
            .instrument(tracing::debug_span!("request_job", ?kind))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gradient_test_support::prelude::{FakeWorkerStore, MockProtoServer};
    use gradient_worker_client::connection::ProtoConnection;

    #[tokio::test]
    async fn readiness_loss_during_a_send_stops_later_chunks_and_requests() {
        for (is_final, count) in [(true, 2_001), (false, 2_001), (true, 0), (false, 0)] {
            let (health, set_ready) = HealthChecks::controlled(true);
            let readiness = ScoringReadiness {
                health,
                refresh_pending: Arc::new(AtomicBool::new(false)),
            };
            let scores = (0..count)
                .map(|index| CandidateScore {
                    job_id: format!("build:{index}"),
                    missing_count: 0,
                    missing_nar_size: 0,
                    outputs_present: false,
                })
                .collect();
            let permits = Arc::new(tokio::sync::Semaphore::new(0));
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut sending = Box::pin(send_score_chunks(
                |message| {
                    let tx = tx.clone();
                    let permits = Arc::clone(&permits);
                    async move {
                        tx.send(message).unwrap();
                        permits.acquire().await.unwrap().forget();
                        Ok(())
                    }
                },
                scores,
                is_final,
                vec![JobKind::Build, JobKind::Flake],
                &readiness,
            ));
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let first = tokio::select! {
                    message = rx.recv() => message.unwrap(),
                    result = &mut sending => panic!("send completed without a permit: {result:?}"),
                };
                match first {
                    ClientMessage::RequestJobChunk {
                        scores,
                        is_final: final_chunk,
                    } => {
                        assert_eq!(scores.len(), count.min(1_000));
                        assert_eq!(final_chunk, is_final && count == 0);
                    }
                    ClientMessage::RequestJob {
                        kind: JobKind::Build,
                    } => {
                        assert_eq!((is_final, count), (false, 0));
                    }
                    message => panic!("unexpected first action: {message:?}"),
                }
                set_ready(false);
                permits.add_permits(8);
                sending.await.unwrap();
                assert!(readiness.refresh_pending.load(Ordering::Acquire));
                assert!(
                    rx.try_recv().is_err(),
                    "no further scores or job requests after readiness loss"
                );
            })
            .await
            .expect("the in-flight send finishes without continuing after readiness loss");
        }
    }

    #[tokio::test]
    async fn a_candidate_offered_again_is_scored_again() {
        let server = MockProtoServer::bind().await;
        let (mut sc, conn) = tokio::join!(server.accept(), ProtoConnection::open(server.url()));
        let (writer, _reader, _flush) = conn.expect("the mock server accepts").split();
        let store = Arc::new(FakeWorkerStore::new());
        let candidate = JobCandidate {
            job_id: "build:1".to_owned(),
            drv_paths: vec!["/nix/store/zzzz-target.drv".to_owned()],
            ..Default::default()
        };

        for offer in ["first", "repeated"] {
            spawn_scoring_task(
                JobScorer::new(),
                Arc::clone(&store),
                writer.clone(),
                vec![candidate.clone()],
                true,
                Vec::new(),
                ScoringReadiness {
                    health: HealthChecks::default(),
                    refresh_pending: Arc::new(AtomicBool::new(false)),
                },
            );
            let scores = sc.scores().await.expect("the worker answers");
            let ids: Vec<&str> = scores.iter().map(|s| s.job_id.as_str()).collect();
            assert_eq!(ids, ["build:1"], "the {offer} offer is scored");
        }
    }
}
