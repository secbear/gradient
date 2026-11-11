/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use axum::http::StatusCode;
use axum_test::{TestResponse, TestServer};
use chrono::Duration;
use gradient_entity::ids::*;
use gradient_entity::team_user::TeamRole;
use gradient_entity::{team_project, team_worker, worker_registration};
use gradient_fixtures::ids::{project_id, team_id};
use gradient_fixtures::mock::{all_statements, empty_db};
use gradient_fixtures::rows::{team, team_member, user};
use gradient_fixtures::test_date;
use gradient_pool::estimate::BuildEstimate;
use gradient_pool::session_port::SessionSignal;
use gradient_pool::{WorkerJob, WorkerJobPolicy, WorkerProfile};
use gradient_scheduler::Scheduler;
use gradient_scheduler::actor::WorkerMetrics;
use gradient_scheduler::jobs::{PendingBuildJob, build_job_key};
use gradient_test_support::state::TestState;
use gradient_test_support::web::{signed_in, signed_in_as};
use gradient_util::shutdown::Shutdown;
use gradient_wire::types::{
    BuildJob, BuildRequirement, BuildSpec, BuildSpecKind, GradientCapabilities,
};
use sea_orm::{DatabaseConnection, MockDatabase};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;

const URL: &str = "/api/v1/teams/platform/workers/capacity-assessment";
const INTENDED: &str = "7e000000-0000-0000-0000-0000000000fa";
const OTHER_INTENDED: &str = "7e000000-0000-0000-0000-0000000000fb";
const LIVE: &str = "7e000000-0000-0000-0000-0000000000fc";

fn scoped_db(role: TeamRole, superuser: bool) -> (MockDatabase, String) {
    let (db, auth) = if superuser {
        signed_in_as(gradient_entity::user::Model {
            superuser: true,
            ..user()
        })
    } else {
        signed_in()
    };
    (
        db.append_query_results([vec![team()]])
            .append_query_results([vec![team_member(role)]]),
        auth,
    )
}

fn grants(db: MockDatabase, projects: &[ProjectId]) -> MockDatabase {
    db.append_query_results([projects
        .iter()
        .map(|project| team_project::Model {
            id: TeamProjectId::now_v7(),
            team: team_id(),
            project: *project,
            includes_users: false,
            includes_workers: true,
            ..Default::default()
        })
        .collect::<Vec<_>>()])
}

fn unregistered(mut db: MockDatabase, identities: usize) -> MockDatabase {
    for _ in 0..identities {
        db = db
            .append_query_results([Vec::<worker_registration::Model>::new()])
            .append_query_results([Vec::<team_worker::Model>::new()]);
    }
    db
}

fn profile(name: &str, identity: &str, ram_mb: u64) -> Value {
    json!({
        "profile": {
            "name": name,
            "worker_id": identity,
            "capabilities": {
                "core": true,
                "federate": false,
                "eval": false,
                "build": true,
                "cache": true,
                "network": true,
                "secrets": false,
                "exec": false
            },
            "resources": {
                "architectures": ["x86_64-linux"],
                "system_features": [],
                "max_concurrent_builds": 2,
                "cpu_count": 64,
                "ram_total_mb": ram_mb,
                "cpu_core_score": 4000,
                "zone": null,
                "endpoint": null,
                "build_cores": null,
                "exec": null
            }
        },
        "enable_build": true,
        "enable_network": true
    })
}

fn build(project: ProjectId, seconds: i64, estimate: BuildEstimate) -> PendingBuildJob {
    let derivation_build = DerivationBuildId::now_v7();
    let ready_at = test_date() + Duration::seconds(seconds);
    PendingBuildJob {
        derivation_build,
        derivation: DerivationId::now_v7(),
        evaluation_id: EvaluationId::now_v7(),
        project_id: project,
        job: BuildJob {
            build: BuildSpec {
                build_id: derivation_build.to_string(),
                drv_path: "aaaa-private-build.drv".into(),
                kind: BuildSpecKind::Build,
                is_fixed_output: false,
                outputs: vec![],
                timeout_secs: None,
                max_silent_secs: None,
                build_cores: None,
                input_realisations: vec![],
            },
            requirement: BuildRequirement {
                architecture: "x86_64-linux".into(),
                required_features: vec![],
            },
        },
        required_paths: vec![],
        dependency_count: 0,
        closure_size: None,
        prefer_local_build: false,
        is_fixed_output: false,
        estimate,
        history_key: None,
        queued_at: ready_at,
        ready_at,
        rescore_count: 0,
        prioritized: false,
        build_request: false,
        ifd: false,
        pname: Some("private-build-name".into()),
        substitute: false,
        verifying_keys: vec![],
    }
}

struct CapacityTest {
    server: TestServer,
    scheduler: Arc<Scheduler>,
    auth: String,
    web_db: DatabaseConnection,
    worker_db: DatabaseConnection,
    shutdown: Shutdown,
}

impl CapacityTest {
    async fn start(db: MockDatabase, auth: String) -> Self {
        Self::with_state(TestState::new(), db, auth).await
    }

    async fn with_state(builder: TestState, db: MockDatabase, auth: String) -> Self {
        let web_db = db.into_connection();
        let worker_db = empty_db();
        let state = builder
            .web_db(web_db.clone())
            .worker_db(worker_db.clone())
            .build();
        let shutdown = state.shutdown.clone();
        let scheduler = Arc::new(Scheduler::new(Arc::clone(&state)));
        let server = TestServer::new(
            gradient_web::create_router_with_scheduler(state, Arc::clone(&scheduler))
                .expect("router"),
        );
        scheduler
            .build_capacity_snapshot(HashSet::new())
            .await
            .expect("router scheduler core ready");
        Self {
            server,
            scheduler,
            auth,
            web_db,
            worker_db,
            shutdown,
        }
    }

    async fn post(&self, body: &Value) -> TestResponse {
        self.server
            .post(URL)
            .add_header("authorization", self.auth.clone())
            .json(body)
            .await
    }

    async fn enqueue(&self, job: PendingBuildJob) {
        self.scheduler
            .enqueue_build_job(build_job_key(job.derivation_build), job)
            .await
            .expect("enqueue build");
    }

    async fn worker(
        &self,
        identity: &str,
        projects: HashSet<ProjectId>,
    ) -> mpsc::UnboundedReceiver<SessionSignal> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.scheduler
            .reattach_worker(
                identity,
                GradientCapabilities {
                    build: true,
                    network: true,
                    ..Default::default()
                },
                projects,
                Arc::new(tx),
                vec![],
                false,
            )
            .await
            .expect("attach worker");
        self.scheduler
            .update_worker_capabilities(
                identity,
                WorkerProfile {
                    architectures: vec!["x86_64-linux".into()],
                    max_concurrent_builds: 2,
                    cpu_count: 64,
                    cpu_core_score: 4_000,
                    ram_total_mb: 60_000,
                    zone: Some("private-zone".into()),
                    endpoint: Some("private-endpoint".into()),
                    ..Default::default()
                },
            )
            .await;
        self.scheduler
            .update_worker_metrics(
                identity,
                WorkerMetrics {
                    cpu_usage_pct: 0.0,
                    ram_free_mb: 60_000,
                    disk_speed_mbps: None,
                    upload_speed_mbps: None,
                    download_speed_mbps: None,
                    build_peak_ram_mb: None,
                    exits_at: None,
                    exec: None,
                },
            )
            .await;
        rx
    }

    async fn stop(&self) {
        assert!(
            self.shutdown
                .cancel_and_drain(std::time::Duration::from_secs(5))
                .await,
            "router background tasks drained"
        );
    }
}

impl Drop for CapacityTest {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[tokio::test]
async fn a_member_cannot_assess_capacity() {
    let (db, auth) = scoped_db(TeamRole::Member, false);
    let test = CapacityTest::start(db, auth).await;
    test.post(&json!({ "profiles": [] }))
        .await
        .assert_status(StatusCode::FORBIDDEN);
    test.stop().await;
}

#[tokio::test]
async fn requested_projects_require_worker_grants_even_for_a_superuser() {
    for superuser in [false, true] {
        let (db, auth) = scoped_db(TeamRole::Admin, superuser);
        let test = CapacityTest::start(grants(db, &[project_id()]), auth).await;
        let outside = ProjectId::now_v7();
        let res = test
            .post(&json!({ "projects": [project_id(), outside], "profiles": [] }))
            .await;
        res.assert_status(StatusCode::FORBIDDEN);
        assert!(!res.text().contains(&outside.to_string()));
        test.stop().await;
    }
}

#[tokio::test]
async fn omitted_projects_mean_worker_grants_and_an_empty_list_means_none() {
    for explicit_empty in [false, true] {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let test = CapacityTest::start(grants(db, &[project_id()]), auth).await;
        test.enqueue(build(project_id(), 0, BuildEstimate::default()))
            .await;
        test.enqueue(build(ProjectId::now_v7(), 1, BuildEstimate::default()))
            .await;
        let _signals = test.worker(LIVE, HashSet::from([project_id()])).await;
        let mut body = json!({ "profiles": [] });
        if explicit_empty {
            body["projects"] = json!([]);
        }
        let res = test.post(&body).await;
        res.assert_status_ok();
        let body: Value = res.json();
        let count = usize::from(!explicit_empty);
        assert_eq!(body["error"], false);
        assert_eq!(body["message"]["total_ready_builds"], count);
        assert_eq!(body["message"]["builds"].as_array().unwrap().len(), count);
        assert_eq!(body["message"]["workers"].as_array().unwrap().len(), count);
        assert_eq!(body["message"]["builds_truncated"], false);
        test.stop().await;
        let statements = all_statements(test.web_db.clone().into_transaction_log());
        let grant_query = statements
            .iter()
            .find(|statement| statement.sql.contains(r#"FROM "team_project""#))
            .expect("worker grants queried");
        assert!(grant_query.sql.contains(r#""includes_workers" ="#));
        assert!(
            grant_query
                .values
                .as_ref()
                .unwrap()
                .0
                .contains(&sea_orm::Value::Bool(Some(true)))
        );
        assert!(!grant_query.sql.contains(r#""includes_users" ="#));
    }
}

#[tokio::test]
async fn invalid_profiles_and_unknown_fields_are_refused_before_registration_lookup() {
    let valid = json!({ "profiles": [profile("builder", INTENDED, 60_000)] });
    let mut cases = Vec::new();
    for pointer in [
        "/profiles/0/profile/resources/cpu_count",
        "/profiles/0/profile/resources/ram_total_mb",
        "/profiles/0/profile/resources/max_concurrent_builds",
    ] {
        let mut body = valid.clone();
        *body.pointer_mut(pointer).unwrap() = json!(0);
        cases.push((body, StatusCode::BAD_REQUEST));
    }
    for (pointer, value) in [
        ("/profiles/0/profile/worker_id", json!("not-a-uuid")),
        ("/profiles/0/profile/name", json!(" ")),
        ("/profiles/0/enable_build", json!(false)),
        ("/profiles/0/profile/capabilities/build", json!(false)),
    ] {
        let mut body = valid.clone();
        *body.pointer_mut(pointer).unwrap() = value;
        cases.push((body, StatusCode::BAD_REQUEST));
    }
    let mut duplicates = valid.clone();
    duplicates["profiles"] = json!([
        profile("builder", INTENDED, 60_000),
        profile("builder", OTHER_INTENDED, 60_000)
    ]);
    cases.push((duplicates, StatusCode::BAD_REQUEST));
    let too_many: Vec<_> = (0..17)
        .map(|index| profile(&format!("builder-{index}"), INTENDED, 60_000))
        .collect();
    cases.push((json!({ "profiles": too_many }), StatusCode::BAD_REQUEST));
    cases.push((
        json!({ "projects": ["not-a-uuid"], "profiles": [] }),
        StatusCode::UNPROCESSABLE_ENTITY,
    ));
    cases.push((
        json!({ "projects": vec![project_id(); 257], "profiles": [] }),
        StatusCode::BAD_REQUEST,
    ));
    for pointer in [
        "",
        "/profiles/0",
        "/profiles/0/profile",
        "/profiles/0/profile/resources",
    ] {
        let mut body = valid.clone();
        body.pointer_mut(pointer).unwrap()["unknown_capacity_field"] = json!(true);
        cases.push((body, StatusCode::UNPROCESSABLE_ENTITY));
    }
    for (body, expected) in cases {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let test = CapacityTest::start(db, auth).await;
        let res = test.post(&body).await;
        res.assert_status(expected);
        assert_eq!(test.scheduler.pending_job_count().await, 0);
        test.stop().await;
    }
}

#[tokio::test]
async fn native_capabilities_ignore_extensions_but_require_known_booleans() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(unregistered(grants(db, &[project_id()]), 1), auth).await;
    let mut body = json!({ "profiles": [profile("builder", INTENDED, 60_000)] });
    body["profiles"][0]["profile"]["capabilities"]["future_capability"] = json!(true);
    let res = test.post(&body).await;
    res.assert_status_ok();
    let response: Value = res.json();
    let capabilities = &response["message"]["profiles"][0]["capabilities"];
    assert_eq!(capabilities["network"], true);
    assert!(capabilities.get("future_capability").is_none());
    test.stop().await;

    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(db, auth).await;
    let capabilities = body["profiles"][0]["profile"]["capabilities"]
        .as_object_mut()
        .unwrap();
    capabilities.remove("network");
    capabilities.insert("netwrok".into(), json!(true));
    test.post(&body)
        .await
        .assert_status(StatusCode::UNPROCESSABLE_ENTITY);
    test.stop().await;
}

#[tokio::test]
async fn sixteen_profiles_are_accepted_but_the_request_body_is_bounded() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(unregistered(grants(db, &[project_id()]), 1), auth).await;
    let profiles: Vec<_> = (0..16)
        .map(|index| profile(&format!("builder-{index}"), INTENDED, 60_000))
        .collect();
    let res = test.post(&json!({ "profiles": profiles })).await;
    res.assert_status_ok();
    let body: Value = res.json();
    assert_eq!(body["message"]["profiles"].as_array().unwrap().len(), 16);
    test.stop().await;

    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(db, auth).await;
    let res = test
        .post(&json!({ "profiles": [profile(&"x".repeat(64 * 1024), INTENDED, 60_000)] }))
        .await;
    res.assert_status(StatusCode::PAYLOAD_TOO_LARGE);
    test.stop().await;
}

#[tokio::test]
async fn a_failed_scheduler_capture_returns_http_service_unavailable() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(grants(db, &[project_id()]), auth).await;
    test.stop().await;
    let res = test.post(&json!({ "profiles": [] })).await;
    res.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = res.json();
    assert_eq!(body["error"], true);
    assert_eq!(body["code"], "service_unavailable");
    assert_eq!(
        body["message"],
        "capacity assessment unavailable; retry later"
    );
    assert!(body.get("total_ready_builds").is_none());
}

#[tokio::test]
async fn every_existing_registration_conflicts_including_inactive_and_foreign_workers() {
    for kind in ["project", "team", "other-team"] {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let db = grants(db, &[project_id()]);
        let db = if kind == "project" {
            db.append_query_results([vec![worker_registration::Model {
                worker_id: INTENDED.into(),
                peer_id: ProjectId::now_v7(),
                active: false,
                ..Default::default()
            }]])
        } else {
            db.append_query_results([Vec::<worker_registration::Model>::new()])
                .append_query_results([vec![team_worker::Model {
                    worker_id: INTENDED.into(),
                    team: if kind == "team" {
                        team_id()
                    } else {
                        TeamId::now_v7()
                    },
                    active: false,
                    ..Default::default()
                }]])
        };
        let test = CapacityTest::start(db, auth).await;
        test.post(&json!({ "profiles": [profile("builder", INTENDED, 60_000)] }))
            .await
            .assert_status(StatusCode::CONFLICT);
        assert_eq!(test.scheduler.worker_count().await, 0);
        test.stop().await;
        let lookups = all_statements(test.web_db.clone().into_transaction_log());
        for lookup in lookups.iter().filter(|statement| {
            statement.sql.contains(r#"FROM "worker_registration""#)
                || statement.sql.contains(r#"FROM "team_worker""#)
        }) {
            assert!(!lookup.sql.contains(r#""active" ="#));
            assert!(!lookup.sql.contains(r#""peer_id" ="#));
            assert!(!lookup.sql.contains(r#""team" ="#));
        }
    }
}

#[tokio::test]
async fn fixed_output_fits_require_both_advertised_and_planned_network_access() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(unregistered(grants(db, &[project_id()]), 1), auth).await;
    test.enqueue(build(project_id(), 0, BuildEstimate::default()))
        .await;
    let mut fixed = build(project_id(), 1, BuildEstimate::default());
    fixed.is_fixed_output = true;
    fixed.job.build.is_fixed_output = true;
    test.enqueue(fixed).await;
    let mut planned_off = profile("planned-off", INTENDED, 60_000);
    planned_off["enable_network"] = json!(false);
    let mut advertised_off = profile("advertised-off", INTENDED, 60_000);
    advertised_off["profile"]["capabilities"]["network"] = json!(false);
    let res = test
        .post(&json!({ "profiles": [
            planned_off,
            advertised_off,
            profile("network", INTENDED, 60_000)
        ] }))
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    let message = &body["message"];
    for index in 0..3 {
        assert!(message["builds"][0]["profiles"][index].is_object());
    }
    assert_eq!(message["profiles"][0]["capabilities"]["network"], false);
    assert_eq!(message["profiles"][1]["capabilities"]["network"], false);
    assert!(message["builds"][1]["profiles"][0].is_null());
    assert!(message["builds"][1]["profiles"][1].is_null());
    assert!(message["builds"][1]["profiles"][2].is_object());
    test.stop().await;
}

#[derive(Debug)]
struct OnlyIntendedWorker;

impl WorkerJobPolicy for OnlyIntendedWorker {
    fn allows(&self, worker: &str, job: WorkerJob<'_>) -> bool {
        worker == INTENDED && matches!(job, WorkerJob::Build)
    }
}

#[tokio::test]
async fn job_policy_uses_the_canonical_intended_identity_not_the_profile_name() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::with_state(
        TestState::new().worker_job_policy(Arc::new(OnlyIntendedWorker)),
        unregistered(grants(db, &[project_id()]), 2),
        auth,
    )
    .await;
    test.enqueue(build(project_id(), 0, BuildEstimate::default()))
        .await;
    let res = test
        .post(&json!({ "profiles": [
            profile("arbitrary-sizing-name", &INTENDED.to_uppercase(), 60_000),
            profile(INTENDED, OTHER_INTENDED, 60_000)
        ] }))
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    assert_eq!(body["message"]["profiles"][0]["worker_id"], INTENDED);
    assert!(body["message"]["builds"][0]["profiles"][0].is_object());
    assert!(body["message"]["builds"][0]["profiles"][1].is_null());
    test.stop().await;
}

#[tokio::test]
async fn wire_keeps_unknown_predictions_and_proportional_reservations_distinct() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(unregistered(grants(db, &[project_id()]), 1), auth).await;
    test.enqueue(build(project_id(), 0, BuildEstimate::default()))
        .await;
    test.enqueue(build(
        project_id(),
        1,
        BuildEstimate::fixed(Some(100_000), Some(60_000), 5),
    ))
    .await;
    let res = test
        .post(&json!({ "profiles": [
            profile("small", INTENDED, 60_000),
            profile("large", INTENDED, 128_000)
        ] }))
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    let unknown = &body["message"]["builds"][0]["profiles"][0];
    assert_eq!(unknown.get("prediction_fit"), Some(&Value::Null));
    for field in ["predicted_ram_mb", "reserved_ram_mb", "ram_fit"] {
        assert_eq!(unknown["resources"].get(field), Some(&Value::Null));
    }
    assert_eq!(unknown["resources"]["memory_assumption"], "unmeasured");
    assert_eq!(unknown["resources"]["own_samples"], 0);
    assert_eq!(unknown["resources"]["build_time_known"], false);
    assert!(
        unknown["resources"]["time"]["fallbacks"]
            .as_array()
            .unwrap()
            .contains(&json!("build_history"))
    );
    let small = &body["message"]["builds"][1]["profiles"][0];
    assert_eq!(small["prediction_fit"], "does_not_fit");
    assert_eq!(small["resources"]["predicted_ram_mb"], 100_000);
    assert_eq!(small["resources"]["ram_fit"], "fits");
    assert_eq!(small["resources"]["memory_assumption"], "proportional");
    assert_eq!(small["resources"]["cores"], 29);
    assert_eq!(small["resources"]["reserved_ram_mb"], 45_313);
    assert_eq!(small["resources"]["own_samples"], 5);
    assert_eq!(small["resources"]["build_time_known"], true);
    assert_eq!(small["resources"]["time"]["cores"], 29);
    let large = &body["message"]["builds"][1]["profiles"][1];
    assert_eq!(large["prediction_fit"], "fits");
    assert_eq!(large["resources"]["predicted_ram_mb"], 100_000);
    assert_eq!(large["resources"]["reserved_ram_mb"], 100_000);
    assert_eq!(large["resources"]["memory_assumption"], "predicted");
    test.stop().await;
}

#[tokio::test]
async fn an_assessment_leaks_no_live_identities_and_registers_or_assigns_nothing() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(unregistered(grants(db, &[project_id()]), 1), auth).await;
    let outside = ProjectId::now_v7();
    let ready = build(project_id(), 0, BuildEstimate::default());
    let hidden = [
        ready.derivation_build.to_string(),
        ready.derivation.to_string(),
        ready.evaluation_id.to_string(),
        project_id().to_string(),
        outside.to_string(),
        LIVE.into(),
        "private-build-name".into(),
        "aaaa-private-build.drv".into(),
        "private-zone".into(),
        "private-endpoint".into(),
    ];
    test.enqueue(ready).await;
    test.enqueue(build(outside, 1, BuildEstimate::default()))
        .await;
    let mut signals = test
        .worker(LIVE, HashSet::from([project_id(), outside]))
        .await;
    while signals.try_recv().is_ok() {}
    let before = test.scheduler.metrics_snapshot().await;
    let _ = test.worker_db.clone().into_transaction_log();
    let res = test
        .post(&json!({ "profiles": [profile("builder", INTENDED, 60_000)] }))
        .await;
    res.assert_status_ok();
    let text = res.text();
    for hidden in hidden {
        assert!(!text.contains(&hidden), "response disclosed {hidden}");
    }
    let body: Value = res.json();
    assert_eq!(body["message"]["total_ready_builds"], 1);
    assert_eq!(
        body["message"]["workers"],
        json!([{ "free_slots": 2, "available_ram_mb": 60_000 }])
    );
    assert_eq!(body["message"]["profiles"][0]["worker_id"], INTENDED);
    assert!(body["message"]["builds"][0]["workers"][0].is_object());
    assert_eq!(test.scheduler.metrics_snapshot().await, before);
    assert!(!test.scheduler.is_worker_connected(INTENDED).await);
    assert_eq!(test.scheduler.workers_info().await[0].assigned_job_count, 0);
    assert!(signals.try_recv().is_err());
    let worker_statements = all_statements(test.worker_db.clone().into_transaction_log());
    assert!(
        worker_statements
            .iter()
            .all(|statement| statement.sql.starts_with("SELECT")),
        "assessment worker database statements: {worker_statements:#?}"
    );
    let statements = all_statements(test.web_db.clone().into_transaction_log());
    assert!(statements.iter().all(|statement| {
        let mutation = statement.sql.starts_with("INSERT")
            || statement.sql.starts_with("UPDATE")
            || statement.sql.starts_with("DELETE");
        !mutation
            || (!statement.sql.contains(r#""team_worker""#)
                && !statement.sql.contains(r#""worker_registration""#)
                && !statement.sql.contains(r#""dispatched_job""#))
    }));
    test.stop().await;
}

#[tokio::test]
async fn a_bounded_batch_reports_the_total_and_returns_the_oldest_ordinary_builds() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(grants(db, &[project_id()]), auth).await;
    for seconds in (0..130).rev() {
        test.enqueue(build(project_id(), seconds, BuildEstimate::default()))
            .await;
    }
    test.enqueue(build(ProjectId::now_v7(), -2, BuildEstimate::default()))
        .await;
    let mut substitute = build(project_id(), -1, BuildEstimate::default());
    substitute.job.build.kind = BuildSpecKind::Substitute;
    substitute.substitute = true;
    test.enqueue(substitute).await;
    let res = test.post(&json!({ "profiles": [] })).await;
    res.assert_status_ok();
    let body: Value = res.json();
    let message = &body["message"];
    assert_eq!(message["total_ready_builds"], 130);
    assert_eq!(message["builds_truncated"], true);
    let builds = message["builds"].as_array().unwrap();
    assert_eq!(builds.len(), 128);
    assert_eq!(builds[0]["ready_at"], json!(test_date()));
    assert_eq!(
        builds[127]["ready_at"],
        json!(test_date() + Duration::seconds(127))
    );
    assert!(
        builds
            .windows(2)
            .all(|pair| pair[0]["ready_at"].as_str() < pair[1]["ready_at"].as_str())
    );
    assert_eq!(test.scheduler.pending_job_count().await, 132);
    test.stop().await;
}
