/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use gradient_types::ids::{EvaluationId, ProjectId};
use gradient_util::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputWaiter {
    pub worker: String,
    pub job_id: String,
    pub request_id: String,
    pub evaluation: EvaluationId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedFetch {
    pub fetch_id: String,
    pub worker: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnassignedFetch {
    pub fetch_key: String,
    pub project: ProjectId,
    pub request: String,
    pub needs_secrets: bool,
    pub excluded: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputCapacityDemand {
    pub project: ProjectId,
    pub needs_secrets: bool,
    pub excluded: HashSet<String>,
}

#[derive(Debug)]
struct InputWait {
    request: String,
    project: ProjectId,
    needs_secrets: bool,
    waiters: Vec<InputWaiter>,
    fetch: Option<AssignedFetch>,
    excluded: HashSet<String>,
    announced: HashSet<EvaluationId>,
}

#[derive(Debug, Clone)]
pub struct NewInputWait {
    pub fetch_key: String,
    pub request: String,
    pub project: ProjectId,
    pub needs_secrets: bool,
    pub waiter: InputWaiter,
}

#[derive(Clone, Debug, Default)]
pub struct InputWaits(Arc<Mutex<HashMap<String, InputWait>>>);

impl InputWaits {
    pub fn register(&self, new: NewInputWait) {
        self.0
            .lock()
            .entry(new.fetch_key)
            .or_insert_with(|| InputWait {
                request: new.request,
                project: new.project,
                needs_secrets: new.needs_secrets,
                waiters: Vec::new(),
                fetch: None,
                excluded: HashSet::new(),
                announced: HashSet::new(),
            })
            .waiters
            .push(new.waiter);
    }

    pub fn unassigned(&self) -> Vec<UnassignedFetch> {
        let mut unassigned: Vec<UnassignedFetch> = self
            .0
            .lock()
            .iter()
            .filter(|(_, wait)| wait.fetch.is_none())
            .map(|(key, wait)| UnassignedFetch {
                fetch_key: key.clone(),
                project: wait.project,
                request: wait.request.clone(),
                needs_secrets: wait.needs_secrets,
                excluded: wait.excluded.clone(),
            })
            .collect();
        unassigned.sort_by(|a, b| a.fetch_key.cmp(&b.fetch_key));
        unassigned
    }

    pub(crate) fn capacity_demands(
        &self,
        projects: &HashSet<ProjectId>,
        limit: usize,
        scan_limit: usize,
        excluded_limit: usize,
    ) -> anyhow::Result<(usize, Vec<InputCapacityDemand>)> {
        let waits = self.0.lock();
        anyhow::ensure!(
            waits.capacity() <= scan_limit,
            "too many input waits to scan for a capacity assessment"
        );
        let mut first = BTreeMap::new();
        let mut total = 0;
        for (key, wait) in waits.iter() {
            if wait.fetch.is_some() || !projects.contains(&wait.project) {
                continue;
            }
            total += 1;
            if limit == 0
                || (first.len() == limit
                    && first.last_key_value().is_some_and(|(last, _)| key >= *last))
            {
                continue;
            }
            first.insert(key, wait);
            if first.len() > limit {
                first.pop_last();
            }
        }
        for wait in first.values() {
            anyhow::ensure!(
                wait.excluded.capacity() <= excluded_limit,
                "too many excluded workers in an input wait for a capacity assessment"
            );
        }
        let demands = first
            .into_values()
            .map(|wait| InputCapacityDemand {
                project: wait.project,
                needs_secrets: wait.needs_secrets,
                excluded: wait.excluded.clone(),
            })
            .collect();
        Ok((total, demands))
    }

    pub fn assigned_per_worker(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for fetch in self.0.lock().values().filter_map(|w| w.fetch.as_ref()) {
            *counts.entry(fetch.worker.clone()).or_default() += 1;
        }
        counts
    }

    pub fn assign(&self, fetch_key: &str, fetch: AssignedFetch) -> bool {
        match self.0.lock().get_mut(fetch_key) {
            Some(wait) if wait.fetch.is_none() => {
                wait.fetch = Some(fetch);
                true
            }
            _ => false,
        }
    }

    pub fn take_fetched(&self, worker: &str, fetch_id: &str) -> Vec<InputWaiter> {
        let mut waits = self.0.lock();
        let Some(key) = key_of_fetch(&waits, worker, fetch_id) else {
            return Vec::new();
        };
        waits.remove(&key).map(|w| w.waiters).unwrap_or_default()
    }

    pub fn unplace(&self, worker: &str, fetch_id: &str) -> bool {
        let mut waits = self.0.lock();
        let Some(key) = key_of_fetch(&waits, worker, fetch_id) else {
            return false;
        };
        let Some(wait) = waits.get_mut(&key) else {
            return false;
        };
        wait.fetch = None;
        wait.excluded.insert(worker.to_owned());
        true
    }

    pub fn waiters_of_fetch(&self, worker: &str, fetch_id: &str) -> Vec<InputWaiter> {
        let waits = self.0.lock();
        key_of_fetch(&waits, worker, fetch_id)
            .and_then(|key| waits.get(&key))
            .map(|w| w.waiters.clone())
            .unwrap_or_default()
    }

    pub fn release_worker(&self, worker: &str) {
        let mut waits = self.0.lock();
        for wait in waits.values_mut() {
            wait.waiters.retain(|w| w.worker != worker);
            wait.excluded.remove(worker);
            if wait.fetch.as_ref().is_some_and(|f| f.worker == worker) {
                wait.fetch = None;
            }
        }
        waits.retain(|_, wait| !wait.waiters.is_empty());
    }

    pub fn drop_evaluations(&self, evaluations: &HashSet<EvaluationId>) -> usize {
        let mut waits = self.0.lock();
        let mut dropped = 0;
        for wait in waits.values_mut() {
            let before = wait.waiters.len();
            wait.waiters
                .retain(|w| !evaluations.contains(&w.evaluation));
            dropped += before - wait.waiters.len();
        }
        waits.retain(|_, wait| !wait.waiters.is_empty());
        dropped
    }

    pub fn waiting_evaluations(&self) -> HashSet<EvaluationId> {
        self.0
            .lock()
            .values()
            .flat_map(|w| w.waiters.iter().map(|waiter| waiter.evaluation))
            .collect()
    }

    pub fn announce_unplaced(&self, fetch_key: &str) -> Vec<EvaluationId> {
        let mut waits = self.0.lock();
        let Some(wait) = waits.get_mut(fetch_key).filter(|w| w.fetch.is_none()) else {
            return Vec::new();
        };
        let mut fresh: Vec<EvaluationId> = wait
            .waiters
            .iter()
            .map(|waiter| waiter.evaluation)
            .filter(|evaluation| wait.announced.insert(*evaluation))
            .collect();
        fresh.dedup();
        fresh
    }

    #[cfg(test)]
    pub fn waiting_requests(&self) -> usize {
        self.0.lock().values().map(|w| w.waiters.len()).sum()
    }
}

pub(crate) fn input_fetch_key(fetch_id: &str) -> String {
    format!("input:{fetch_id}")
}

fn key_of_fetch(
    waits: &HashMap<String, InputWait>,
    worker: &str,
    fetch_id: &str,
) -> Option<String> {
    waits
        .iter()
        .find(|(_, wait)| {
            wait.fetch
                .as_ref()
                .is_some_and(|f| f.fetch_id == fetch_id && f.worker == worker)
        })
        .map(|(key, _)| key.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_demands_are_scoped_unassigned_and_read_only() {
        let waits = InputWaits::default();
        let project = ProjectId::now_v7();
        let peer = ProjectId::now_v7();
        let outside = ProjectId::now_v7();
        for (key, project, needs_secrets) in [
            ("a", project, true),
            ("b", peer, false),
            ("assigned", project, true),
            ("outside", outside, true),
        ] {
            waits.register(NewInputWait {
                fetch_key: key.into(),
                request: format!("request:{key}"),
                project,
                needs_secrets,
                waiter: InputWaiter {
                    worker: "eval-worker".into(),
                    job_id: format!("eval:{key}"),
                    request_id: key.into(),
                    evaluation: EvaluationId::now_v7(),
                },
            });
        }
        assert!(waits.assign(
            "assigned",
            AssignedFetch {
                fetch_id: "fetch".into(),
                worker: "fetch-worker".into(),
            },
        ));
        assert_eq!(waits.announce_unplaced("a").len(), 1);
        let unassigned = waits.unassigned();
        let assigned = waits.assigned_per_worker();
        let waiters = waits.waiters_of_fetch("fetch-worker", "fetch");
        let evaluations = waits.waiting_evaluations();
        let scope = HashSet::from([project, peer]);
        let expected = (
            2,
            vec![
                InputCapacityDemand {
                    project,
                    needs_secrets: true,
                    excluded: HashSet::new(),
                },
                InputCapacityDemand {
                    project: peer,
                    needs_secrets: false,
                    excluded: HashSet::new(),
                },
            ],
        );
        for _ in 0..2 {
            assert_eq!(
                waits.capacity_demands(&scope, 4, usize::MAX, 0).unwrap(),
                expected,
            );
        }
        assert_eq!(
            waits
                .capacity_demands(&HashSet::new(), 4, usize::MAX, 0)
                .unwrap(),
            (0, vec![]),
        );
        assert!(waits.capacity_demands(&scope, 4, 0, 0).is_err());
        assert_eq!(waits.unassigned(), unassigned);
        assert_eq!(waits.assigned_per_worker(), assigned);
        assert_eq!(waits.waiters_of_fetch("fetch-worker", "fetch"), waiters);
        assert_eq!(waits.waiting_evaluations(), evaluations);
        assert_eq!(waits.waiting_requests(), 4);
        assert!(waits.announce_unplaced("a").is_empty());
        assert_eq!(waits.announce_unplaced("b").len(), 1);
    }

    #[test]
    fn capacity_demands_count_shared_native_fetch_keys_once() {
        let waits = InputWaits::default();
        let project = ProjectId::now_v7();
        let peer = ProjectId::now_v7();
        let first = r#"{"builtin":"fetchTree","args":{"type":"git","url":"git+ssh://git@example.invalid/r","rev":"abc"}}"#;
        let second = r#"{"args":{"rev":"abc","url":"git+ssh://git@example.invalid/r","type":"git"},"builtin":"fetchTree"}"#;
        for (project, request) in [(project, first), (peer, second)] {
            let parsed = gradient_pool::InputRequest::parse(request).unwrap();
            waits.register(NewInputWait {
                fetch_key: parsed.fetch_key,
                request: request.into(),
                project,
                needs_secrets: parsed.needs_secrets,
                waiter: InputWaiter {
                    worker: "eval-worker".into(),
                    job_id: project.to_string(),
                    request_id: project.to_string(),
                    evaluation: EvaluationId::now_v7(),
                },
            });
        }
        assert_eq!(
            waits
                .capacity_demands(&HashSet::from([project, peer]), 2, usize::MAX, 0)
                .unwrap(),
            (
                1,
                vec![InputCapacityDemand {
                    project,
                    needs_secrets: true,
                    excluded: HashSet::new(),
                }],
            ),
        );
        assert_eq!(
            waits
                .capacity_demands(&HashSet::from([peer]), 2, usize::MAX, 0)
                .unwrap(),
            (0, vec![]),
        );
        assert_eq!(waits.waiting_requests(), 2);
        assert_eq!(waits.unassigned()[0].request, first);
    }

    #[test]
    fn capacity_demands_keep_fetch_key_order_and_the_full_count() {
        let entries = [
            ("a", ProjectId::now_v7()),
            ("b", ProjectId::now_v7()),
            ("c", ProjectId::now_v7()),
        ];
        let scope = entries.iter().map(|(_, project)| *project).collect();
        for order in [[2, 0, 1], [1, 2, 0]] {
            let waits = InputWaits::default();
            for index in order {
                let (key, project) = entries[index];
                waits.register(NewInputWait {
                    fetch_key: key.into(),
                    request: format!("request:{key}"),
                    project,
                    needs_secrets: false,
                    waiter: InputWaiter {
                        worker: "eval-worker".into(),
                        job_id: key.into(),
                        request_id: key.into(),
                        evaluation: EvaluationId::now_v7(),
                    },
                });
            }
            for limit in 0..=4 {
                let (total, demands) = waits
                    .capacity_demands(&scope, limit, usize::MAX, 0)
                    .unwrap();
                assert_eq!(total, entries.len());
                assert_eq!(
                    demands.iter().map(|d| d.project).collect::<Vec<_>>(),
                    entries[..limit.min(entries.len())]
                        .iter()
                        .map(|(_, project)| *project)
                        .collect::<Vec<_>>(),
                );
            }
        }
    }

    #[test]
    fn capacity_scan_guard_covers_retained_storage_capacity() {
        let waits = InputWaits::default();
        let project = ProjectId::now_v7();
        let scope = HashSet::from([project]);
        assert_eq!(
            waits.capacity_demands(&scope, 0, 0, 0).unwrap(),
            (0, vec![]),
        );
        let evaluation = EvaluationId::now_v7();
        waits.register(NewInputWait {
            fetch_key: "a".into(),
            request: "request".into(),
            project,
            needs_secrets: false,
            waiter: InputWaiter {
                worker: "eval-worker".into(),
                job_id: "eval".into(),
                request_id: "request-id".into(),
                evaluation,
            },
        });
        let capacity = waits.0.lock().capacity();
        assert_eq!(waits.capacity_demands(&scope, 1, capacity, 0).unwrap().0, 1);
        assert!(waits.capacity_demands(&scope, 1, capacity - 1, 0).is_err());
        assert!(
            waits
                .capacity_demands(&HashSet::new(), 0, capacity - 1, 0)
                .is_err()
        );
        assert_eq!(waits.drop_evaluations(&HashSet::from([evaluation])), 1);
        assert!(waits.capacity_demands(&scope, 1, capacity - 1, 0).is_err());
        assert_eq!(
            waits.capacity_demands(&scope, 1, capacity, 0).unwrap(),
            (0, vec![]),
        );
    }

    #[test]
    fn capacity_excluded_guard_checks_only_final_selected_demands() {
        let waits = InputWaits::default();
        let project = ProjectId::now_v7();
        let outside = ProjectId::now_v7();
        for (key, project, workers) in [
            ("a", project, &["w1"][..]),
            ("z", project, &["w1", "w2", "w3", "w4"][..]),
            ("0-assigned", project, &["w1", "w2", "w3"][..]),
            ("0-outside", outside, &["w1", "w2", "w3"][..]),
        ] {
            waits.register(NewInputWait {
                fetch_key: key.into(),
                request: format!("request:{key}"),
                project,
                needs_secrets: true,
                waiter: InputWaiter {
                    worker: "eval-worker".into(),
                    job_id: key.into(),
                    request_id: key.into(),
                    evaluation: EvaluationId::now_v7(),
                },
            });
            for worker in workers {
                assert!(waits.assign(
                    key,
                    AssignedFetch {
                        fetch_id: key.into(),
                        worker: (*worker).into(),
                    },
                ));
                assert!(waits.unplace(worker, key));
            }
        }
        assert!(waits.assign(
            "0-assigned",
            AssignedFetch {
                fetch_id: "assigned-fetch".into(),
                worker: "fetch-worker".into(),
            },
        ));
        let unassigned = waits.unassigned();
        let assigned = waits.assigned_per_worker();
        let scope = HashSet::from([project]);
        assert_eq!(
            waits.capacity_demands(&scope, 1, usize::MAX, 3).unwrap(),
            (
                2,
                vec![InputCapacityDemand {
                    project,
                    needs_secrets: true,
                    excluded: HashSet::from(["w1".into()]),
                }],
            ),
        );
        assert!(waits.capacity_demands(&scope, 2, usize::MAX, 3).is_err());
        assert!(waits.capacity_demands(&scope, 1, usize::MAX, 0).is_err());
        assert_eq!(
            waits.capacity_demands(&scope, 0, usize::MAX, 0).unwrap(),
            (2, vec![]),
        );
        let (total, demands) = waits.capacity_demands(&scope, 2, usize::MAX, 7).unwrap();
        assert_eq!(total, 2);
        assert_eq!(demands[0].excluded, HashSet::from(["w1".into()]));
        assert_eq!(
            demands[1].excluded,
            HashSet::from(["w1".into(), "w2".into(), "w3".into(), "w4".into()]),
        );
        assert_eq!(waits.unassigned(), unassigned);
        assert_eq!(waits.assigned_per_worker(), assigned);
        {
            let mut pending = waits.0.lock();
            let excluded = &mut pending.get_mut("a").unwrap().excluded;
            excluded.reserve(1024);
            assert_eq!(excluded.len(), 1);
        }
        assert!(waits.capacity_demands(&scope, 1, usize::MAX, 7).is_err());
    }
}
