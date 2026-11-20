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
use gradient_fixtures::mock::{all_statements, empty_db, exec_ok, mock_db};
use gradient_fixtures::rows::{live_session, team, team_member, user};
use gradient_fixtures::test_date;
use gradient_pool::estimate::BuildEstimate;
use gradient_pool::session_port::SessionSignal;
use gradient_pool::{WorkerJob, WorkerJobPolicy, WorkerProfile};
use gradient_scheduler::Scheduler;
use gradient_scheduler::actor::WorkerMetrics;
use gradient_scheduler::jobs::{PendingBuildJob, PendingJob, Reattached, build_job_key};
use gradient_test_support::state::TestState;
use gradient_test_support::web::{bearer, signed_in, signed_in_as, with_auth};
use gradient_util::shutdown::Shutdown;
use gradient_wire::types::{
    BuildJob, BuildRequirement, BuildSpec, BuildSpecKind, GradientCapabilities, JobKind,
    WorkerCheckStatus, WorkerHealth, WorkerHealthCheck,
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

fn scoped_key_db(role: TeamRole, superuser: bool) -> (MockDatabase, String) {
    let raw = "c".repeat(64);
    let key = gradient_entity::api::Model {
        owned_by: gradient_fixtures::ids::user_id(),
        key: gradient_db::api_keys::team_key_verifier(&gradient_web::authorization::hash_api_key(
            &raw,
        )),
        permission: gradient_types::permissions::Permission::ManageWorkers.bit(),
        team: Some(team_id()),
        ..Default::default()
    };
    let db = mock_db()
        .append_query_results([vec![key.clone()]])
        .append_exec_results([exec_ok()])
        .append_query_results([vec![key]])
        .append_query_results([vec![gradient_entity::user::Model {
            superuser,
            ..user()
        }]])
        .append_query_results([vec![team()]])
        .append_query_results([vec![team_member(role)]]);
    (db, format!("Bearer GTEAM{raw}"))
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

fn assert_no_registration_or_dispatch(db: &DatabaseConnection) {
    let statements = all_statements(db.clone().into_transaction_log());
    assert!(
        statements.iter().all(|statement| {
            let sql = statement.sql.trim_start();
            let mutation =
                sql.starts_with("INSERT") || sql.starts_with("UPDATE") || sql.starts_with("DELETE");
            !mutation
                || (!sql.contains(r#""team_worker""#)
                    && !sql.contains(r#""worker_registration""#)
                    && !sql.contains(r#""dispatched_job""#))
        }),
        "assessment registered a worker or dispatched a job: {statements:#?}"
    );
}

fn team_registration(identity: &str) -> team_worker::Model {
    team_worker::Model {
        worker_id: identity.into(),
        team: team_id(),
        active: true,
        enable_build: true,
        enable_network: true,
        token_hash: "private-worker-token-hash".into(),
        url: Some("private-worker-url".into()),
        ..Default::default()
    }
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

fn page_state() -> (tempfile::NamedTempFile, TestState) {
    let key = tempfile::NamedTempFile::new().expect("create capacity cursor key");
    std::fs::write(key.path(), b"capacity-test-key-32-bytes-padding")
        .expect("write capacity cursor key");
    let path = key.path().to_string_lossy().into_owned();
    (
        key,
        TestState::new().config(move |config| config.secrets.crypt_file = path),
    )
}

fn page_db(scopes: &[&[ProjectId]], prospective: usize) -> (MockDatabase, String) {
    let session = SessionId::now_v7();
    let mut db = mock_db();
    for (index, projects) in scopes.iter().enumerate() {
        db = if index == 0 {
            with_auth(db, session)
        } else {
            db.append_query_results([vec![live_session(session)]])
                .append_query_results([vec![user()]])
        };
        db = unregistered(
            grants(
                db.append_query_results([vec![team()]])
                    .append_query_results([vec![team_member(TeamRole::Admin)]]),
                projects,
            ),
            prospective,
        );
    }
    (db, bearer(session))
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
        self.worker_with_active(identity, projects, vec![]).await
    }

    async fn worker_with_active(
        &self,
        identity: &str,
        projects: HashSet<ProjectId>,
        active: Vec<Reattached>,
    ) -> mpsc::UnboundedReceiver<SessionSignal> {
        self.worker_with_active_and_readiness(identity, projects, active, false)
            .await
    }

    async fn worker_with_active_and_readiness(
        &self,
        identity: &str,
        projects: HashSet<ProjectId>,
        active: Vec<Reattached>,
        readiness_required: bool,
    ) -> mpsc::UnboundedReceiver<SessionSignal> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.scheduler
            .reattach_worker_with_readiness(
                identity,
                GradientCapabilities {
                    build: true,
                    network: true,
                    ..Default::default()
                },
                projects,
                Arc::new(tx),
                active,
                false,
                readiness_required,
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
    for (superuser, key) in [(false, false), (true, false), (false, true), (true, true)] {
        let (db, auth) = if key {
            scoped_key_db(TeamRole::Admin, superuser)
        } else {
            scoped_db(TeamRole::Admin, superuser)
        };
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
    for (explicit_empty, key) in [(false, false), (true, false), (false, true), (true, true)] {
        let (db, auth) = if key {
            scoped_key_db(TeamRole::Admin, false)
        } else {
            scoped_db(TeamRole::Admin, false)
        };
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
        test.stop().await;
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
    let tracked = json!({
        "profiles": [],
        "tracked_profiles": [profile("starting", INTENDED, 60_000)]
    });
    for pointer in [
        "/tracked_profiles/0",
        "/tracked_profiles/0/profile",
        "/tracked_profiles/0/profile/resources",
    ] {
        let mut body = tracked.clone();
        body.pointer_mut(pointer).unwrap()["unknown_capacity_field"] = json!(true);
        cases.push((body, StatusCode::UNPROCESSABLE_ENTITY));
    }
    for (pointer, value, expected) in [
        (
            "/tracked_profiles/0/profile/worker_id",
            json!("not-a-uuid"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/tracked_profiles/0/profile/resources/ram_total_mb",
            json!(0),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/tracked_profiles/0/enable_build",
            json!(false),
            StatusCode::BAD_REQUEST,
        ),
        (
            "/tracked_profiles",
            Value::Null,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let mut body = tracked.clone();
        *body.pointer_mut(pointer).unwrap() = value;
        cases.push((body, expected));
    }
    for value in [Value::Null, json!("true"), json!(1)] {
        cases.push((
            json!({ "profiles": [], "include_waiting_builds": value }),
            StatusCode::UNPROCESSABLE_ENTITY,
        ));
    }
    for page in [
        json!(true),
        json!({ "after": 1 }),
        json!({ "after": null, "unknown_capacity_field": true }),
    ] {
        cases.push((
            json!({ "profiles": [], "build_page": page }),
            StatusCode::UNPROCESSABLE_ENTITY,
        ));
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
        let lookups = all_statements(test.web_db.clone().into_transaction_log());
        for lookup in lookups.iter().filter(|statement| {
            statement.sql.contains(r#"FROM "worker_registration""#)
                || statement.sql.contains(r#"FROM "team_worker""#)
        }) {
            assert!(!lookup.sql.contains(r#""active" ="#));
            assert!(!lookup.sql.contains(r#""peer_id" ="#));
            assert!(!lookup.sql.contains(r#""team" ="#));
        }
        test.stop().await;
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
    assert_no_registration_or_dispatch(&test.worker_db);
    assert_no_registration_or_dispatch(&test.web_db);
    test.stop().await;
}

#[tokio::test]
async fn absent_or_empty_tracking_preserves_the_legacy_response_shape() {
    for additions in [
        json!({}),
        json!({ "tracked_profiles": [] }),
        json!({ "tracked_profiles": [], "include_waiting_builds": false }),
        json!({ "include_waiting_builds": true }),
        json!({ "build_page": null }),
    ] {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let test = CapacityTest::start(grants(db, &[project_id()]), auth).await;
        test.enqueue(build(project_id(), 0, BuildEstimate::default()))
            .await;
        let _signals = test.worker(LIVE, HashSet::from([project_id()])).await;
        let mut body = json!({ "profiles": [] });
        body.as_object_mut()
            .unwrap()
            .extend(additions.as_object().unwrap().clone());
        let res = test.post(&body).await;
        res.assert_status_ok();
        let body: Value = res.json();
        let message = &body["message"];
        assert_eq!(
            message
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
            HashSet::from([
                "captured_at",
                "server_draining",
                "total_ready_builds",
                "builds_truncated",
                "workers",
                "profiles",
                "builds",
            ])
        );
        assert_eq!(
            message["builds"][0]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
            HashSet::from([
                "architecture",
                "required_features",
                "is_fixed_output",
                "ready_at",
                "workers",
                "profiles",
            ])
        );
        assert_eq!(
            message["workers"],
            json!([{ "free_slots": 2, "available_ram_mb": 60_000 }])
        );
        assert_eq!(message["profiles"], json!([]));
        assert_eq!(message["total_ready_builds"], 1);
        test.stop().await;
    }
}

#[tokio::test]
async fn tracking_requires_every_registration_to_belong_to_the_team_even_for_superusers() {
    for superuser in [false, true] {
        for include_owned in [false, true] {
            let (db, auth) = scoped_db(TeamRole::Admin, superuser);
            let rows = if include_owned {
                vec![team_registration(INTENDED)]
            } else {
                vec![]
            };
            let db = grants(db, &[project_id()]).append_query_results([rows]);
            let test = CapacityTest::start(db, auth).await;
            let _signals = test
                .worker(OTHER_INTENDED, HashSet::from([project_id()]))
                .await;
            let res = test
                .post(&json!({
                    "profiles": [],
                    "tracked_profiles": [
                        profile("owned", INTENDED, 60_000),
                        profile("foreign-or-unknown", OTHER_INTENDED, 60_000)
                    ]
                }))
                .await;
            res.assert_status(StatusCode::NOT_FOUND);
            for hidden in [INTENDED, OTHER_INTENDED, "private-worker-token-hash"] {
                assert!(!res.text().contains(hidden), "response disclosed {hidden}");
            }
            let statements = all_statements(test.web_db.clone().into_transaction_log());
            let lookups: Vec<_> = statements
                .iter()
                .filter(|statement| statement.sql.contains(r#"FROM "team_worker""#))
                .collect();
            assert_eq!(lookups.len(), 1);
            let lookup = lookups[0];
            assert!(lookup.sql.contains(r#""team" ="#));
            assert!(lookup.sql.contains(r#""worker_id" IN"#));
            let values = &lookup.values.as_ref().unwrap().0;
            assert!(values.contains(&team_id().into_inner().into()));
            for identity in [INTENDED, OTHER_INTENDED] {
                assert!(values.contains(&sea_orm::Value::String(Some(identity.into()))));
            }
            assert!(statements.iter().all(|statement| {
                let mutation = statement.sql.starts_with("INSERT")
                    || statement.sql.starts_with("UPDATE")
                    || statement.sql.starts_with("DELETE");
                !statement.sql.contains(r#"FROM "worker_registration""#)
                    && (!mutation
                        || (!statement.sql.contains(r#""team_worker""#)
                            && !statement.sql.contains(r#""worker_registration""#)
                            && !statement.sql.contains(r#""dispatched_job""#)))
            }));
            test.stop().await;
        }
    }
}

#[tokio::test]
async fn tracked_registrations_must_be_active_and_permit_builds() {
    for (active, enable_build) in [(false, true), (true, false)] {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let db = grants(db, &[project_id()]).append_query_results([vec![team_worker::Model {
            active,
            enable_build,
            ..team_registration(INTENDED)
        }]]);
        let test = CapacityTest::start(db, auth).await;
        test.post(&json!({
            "profiles": [],
            "tracked_profiles": [profile("starting", INTENDED, 60_000)]
        }))
        .await
        .assert_status(StatusCode::CONFLICT);
        assert_eq!(test.scheduler.worker_count().await, 0);
        test.stop().await;
    }
}

#[tokio::test]
async fn unconnected_tracking_uses_native_predictions_and_preserves_request_order() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let db = grants(db, &[project_id()]).append_query_results([vec![
        team_registration(OTHER_INTENDED),
        team_registration(INTENDED),
    ]]);
    let test = CapacityTest::start(db, auth).await;
    test.enqueue(build(project_id(), 0, BuildEstimate::default()))
        .await;
    test.enqueue(build(
        project_id(),
        1,
        BuildEstimate::fixed(Some(100_000), Some(60_000), 5),
    ))
    .await;
    let before = test.scheduler.metrics_snapshot().await;
    let res = test
        .post(&json!({
            "profiles": [],
            "tracked_profiles": [
                profile("small", INTENDED, 60_000),
                profile("large", OTHER_INTENDED, 128_000)
            ]
        }))
        .await;
    res.assert_status_ok();
    for hidden in [
        "private-worker-token-hash",
        "private-worker-url",
        "aaaa-private-build.drv",
    ] {
        assert!(!res.text().contains(hidden), "response disclosed {hidden}");
    }
    let body: Value = res.json();
    let message = &body["message"];
    assert_eq!(message["workers"], json!([]));
    assert_eq!(message["profiles"], json!([]));
    for (index, name, identity) in [(0, "small", INTENDED), (1, "large", OTHER_INTENDED)] {
        let tracked = &message["tracked_workers"][index];
        assert_eq!(tracked["profile"]["name"], name);
        assert_eq!(tracked["profile"]["worker_id"], identity);
        assert_eq!(tracked["connected"], false);
        assert_eq!(tracked.get("worker_index"), Some(&Value::Null));
    }
    let unknown = &message["builds"][0]["tracked_workers"][0];
    assert_eq!(unknown.get("prediction_fit"), Some(&Value::Null));
    assert_eq!(
        unknown["resources"].get("predicted_ram_mb"),
        Some(&Value::Null)
    );
    assert_eq!(
        unknown["resources"].get("reserved_ram_mb"),
        Some(&Value::Null)
    );
    assert_eq!(unknown["resources"].get("ram_fit"), Some(&Value::Null));
    assert_eq!(unknown["resources"]["memory_assumption"], "unmeasured");
    assert_eq!(unknown["resources"]["own_samples"], 0);
    assert_eq!(unknown["resources"]["build_time_known"], false);
    let small = &message["builds"][1]["tracked_workers"][0];
    assert_eq!(small["prediction_fit"], "does_not_fit");
    assert_eq!(small["resources"]["predicted_ram_mb"], 100_000);
    assert_eq!(small["resources"]["reserved_ram_mb"], 45_313);
    assert_eq!(small["resources"]["ram_fit"], "fits");
    assert_eq!(small["resources"]["memory_assumption"], "proportional");
    let large = &message["builds"][1]["tracked_workers"][1];
    assert_eq!(large["prediction_fit"], "fits");
    assert_eq!(large["resources"]["reserved_ram_mb"], 100_000);
    assert_eq!(large["resources"]["memory_assumption"], "predicted");
    assert_eq!(test.scheduler.metrics_snapshot().await, before);
    for identity in [INTENDED, OTHER_INTENDED] {
        assert!(!test.scheduler.is_worker_connected(identity).await);
    }
    test.stop().await;
}

#[tokio::test]
async fn tracked_fixed_output_fits_intersect_all_three_network_permissions() {
    for (registered, advertised, planned) in [
        (false, true, true),
        (true, false, true),
        (true, true, false),
        (true, true, true),
    ] {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let db = grants(db, &[project_id()]).append_query_results([vec![team_worker::Model {
            enable_network: registered,
            ..team_registration(INTENDED)
        }]]);
        let test = CapacityTest::start(db, auth).await;
        test.enqueue(build(
            project_id(),
            0,
            BuildEstimate::fixed(Some(1_000), None, 1),
        ))
        .await;
        let mut fixed = build(project_id(), 1, BuildEstimate::fixed(Some(1_000), None, 1));
        fixed.is_fixed_output = true;
        fixed.job.build.is_fixed_output = true;
        test.enqueue(fixed).await;
        let mut tracked = profile("starting", INTENDED, 60_000);
        tracked["profile"]["capabilities"]["network"] = json!(advertised);
        tracked["enable_network"] = json!(planned);
        let res = test
            .post(&json!({ "profiles": [], "tracked_profiles": [tracked] }))
            .await;
        res.assert_status_ok();
        let body: Value = res.json();
        let message = &body["message"];
        let enabled = registered && advertised && planned;
        assert_eq!(
            message["tracked_workers"][0]["profile"]["capabilities"]["network"],
            enabled
        );
        assert!(message["builds"][0]["tracked_workers"][0].is_object());
        assert_eq!(
            message["builds"][1]["tracked_workers"][0].is_object(),
            enabled
        );
        test.stop().await;
    }
}

#[tokio::test]
async fn tracked_policy_uses_canonical_ids_and_connected_ineligible_workers_get_no_credit() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let db = grants(db, &[project_id()]).append_query_results([vec![
        team_registration(INTENDED),
        team_registration(OTHER_INTENDED),
    ]]);
    let test = CapacityTest::with_state(
        TestState::new().worker_job_policy(Arc::new(OnlyIntendedWorker)),
        db,
        auth,
    )
    .await;
    test.enqueue(build(
        project_id(),
        0,
        BuildEstimate::fixed(Some(1_000), None, 1),
    ))
    .await;
    let _signals = test
        .worker(OTHER_INTENDED, HashSet::from([project_id()]))
        .await;
    let res = test
        .post(&json!({
            "profiles": [],
            "tracked_profiles": [
                profile("arbitrary-sizing-name", &INTENDED.to_uppercase(), 60_000),
                profile(INTENDED, OTHER_INTENDED, 60_000)
            ]
        }))
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    let message = &body["message"];
    assert_eq!(message["workers"], json!([]));
    assert_eq!(
        message["tracked_workers"][0]["profile"]["worker_id"],
        INTENDED
    );
    assert_eq!(message["tracked_workers"][0]["connected"], false);
    assert!(message["builds"][0]["tracked_workers"][0].is_object());
    assert_eq!(message["tracked_workers"][1]["connected"], true);
    assert_eq!(
        message["tracked_workers"][1].get("worker_index"),
        Some(&Value::Null)
    );
    assert_eq!(message["builds"][0]["tracked_workers"][1], Value::Null);
    test.stop().await;
}

#[tokio::test]
async fn duplicate_canonical_tracked_ids_are_refused_before_owned_registration_lookup() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(grants(db, &[project_id()]), auth).await;
    test.post(&json!({
        "profiles": [],
        "tracked_profiles": [
            profile("first", INTENDED, 60_000),
            profile("second", &INTENDED.to_uppercase(), 128_000)
        ]
    }))
    .await
    .assert_status(StatusCode::BAD_REQUEST);
    let statements = all_statements(test.web_db.clone().into_transaction_log());
    assert!(
        statements
            .iter()
            .all(|statement| !statement.sql.contains(r#"FROM "team_worker""#))
    );
    test.stop().await;
}

#[tokio::test]
async fn connected_tracking_correlates_live_resources_without_starting_credit_or_foreign_ids() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let db = grants(db, &[project_id()]).append_query_results([vec![team_registration(LIVE)]]);
    let test = CapacityTest::start(db, auth).await;
    test.enqueue(build(
        project_id(),
        0,
        BuildEstimate::fixed(Some(1_000), None, 1),
    ))
    .await;
    let mut signals = test.worker(LIVE, HashSet::from([project_id()])).await;
    let _foreign_signals = test
        .worker(OTHER_INTENDED, HashSet::from([ProjectId::now_v7()]))
        .await;
    while signals.try_recv().is_ok() {}
    let before = test.scheduler.metrics_snapshot().await;
    let res = test
        .post(&json!({
            "profiles": [],
            "tracked_profiles": [profile("planned-large", LIVE, 128_000)]
        }))
        .await;
    res.assert_status_ok();
    assert!(!res.text().contains(OTHER_INTENDED));
    let body: Value = res.json();
    let message = &body["message"];
    assert_eq!(
        message["workers"],
        json!([{ "free_slots": 2, "available_ram_mb": 60_000 }])
    );
    assert_eq!(message["tracked_workers"][0]["connected"], true);
    assert_eq!(message["tracked_workers"][0]["worker_index"], 0);
    assert_eq!(
        message["tracked_workers"][0]["profile"]["resources"]["ram_total_mb"],
        128_000
    );
    assert!(message["builds"][0]["workers"][0].is_object());
    assert_eq!(message["builds"][0]["tracked_workers"], json!([null]));
    assert_eq!(test.scheduler.metrics_snapshot().await, before);
    assert!(signals.try_recv().is_err());
    test.stop().await;
}

#[tokio::test]
async fn connected_out_of_scope_tracking_never_becomes_starting_supply_even_for_empty_scope() {
    for explicit_empty in [false, true] {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let db = grants(db, &[project_id()]).append_query_results([vec![team_registration(LIVE)]]);
        let test = CapacityTest::start(db, auth).await;
        test.enqueue(build(
            project_id(),
            0,
            BuildEstimate::fixed(Some(1_000), None, 1),
        ))
        .await;
        let _signals = test
            .worker(LIVE, HashSet::from([ProjectId::now_v7()]))
            .await;
        let mut request = json!({
            "profiles": [],
            "tracked_profiles": [profile("starting-if-disconnected", LIVE, 60_000)]
        });
        if explicit_empty {
            request["projects"] = json!([]);
        }
        let res = test.post(&request).await;
        res.assert_status_ok();
        let body: Value = res.json();
        let message = &body["message"];
        assert_eq!(message["workers"], json!([]));
        assert_eq!(message["tracked_workers"][0]["connected"], true);
        assert_eq!(
            message["tracked_workers"][0].get("worker_index"),
            Some(&Value::Null)
        );
        if explicit_empty {
            assert_eq!(message["builds"], json!([]));
            assert_eq!(message["total_ready_builds"], 0);
        } else {
            assert_eq!(message["builds"][0]["workers"], json!([]));
            assert_eq!(message["builds"][0]["tracked_workers"], json!([null]));
        }
        test.stop().await;
    }
}

#[tokio::test]
async fn checked_workers_only_supply_live_capacity_when_ready_and_never_starting_credit() {
    for (results, expires, ready) in [
        (vec![], false, false),
        (vec![true], false, true),
        (vec![false], false, false),
        (vec![false, true], false, true),
        (vec![true], true, false),
    ] {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let db = unregistered(
            grants(db, &[project_id()]).append_query_results([vec![team_registration(LIVE)]]),
            1,
        );
        let test = CapacityTest::start(db, auth).await;
        test.enqueue(build(
            project_id(),
            0,
            BuildEstimate::fixed(Some(1_000), None, 1),
        ))
        .await;
        let mut signals = test
            .worker_with_active_and_readiness(LIVE, HashSet::from([project_id()]), vec![], true)
            .await;
        let session = test.scheduler.worker_port(LIVE).await.unwrap();
        for healthy in results {
            test.scheduler
                .update_worker_health(
                    LIVE,
                    Arc::clone(&session),
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
                        valid_for_secs: if expires { 1 } else { 86_400 },
                    },
                )
                .await
                .unwrap();
        }
        if expires {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while test.scheduler.board_workers().await[0].health_ready() {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("health report expired");
        }
        while signals.try_recv().is_ok() {}
        let before = test.scheduler.metrics_snapshot().await;
        let res = test
            .post(&json!({
                "profiles": [profile("replacement", INTENDED, 60_000)],
                "tracked_profiles": [profile("already-connected", LIVE, 128_000)]
            }))
            .await;
        res.assert_status_ok();
        let body: Value = res.json();
        let message = &body["message"];
        assert_eq!(message["total_ready_builds"], 1);
        assert_eq!(
            message["workers"].as_array().unwrap().len(),
            usize::from(ready)
        );
        assert_eq!(
            message["builds"][0]["workers"].as_array().unwrap().len(),
            usize::from(ready)
        );
        assert!(message["builds"][0]["profiles"][0].is_object());
        assert_eq!(message["tracked_workers"][0]["connected"], true);
        assert_eq!(
            message["tracked_workers"][0]["worker_index"],
            if ready { json!(0) } else { Value::Null }
        );
        assert_eq!(message["builds"][0]["tracked_workers"], json!([null]));
        if ready {
            assert_eq!(message["workers"][0]["free_slots"], 2);
            assert!(message["builds"][0]["workers"][0].is_object());
        }
        assert!(test.scheduler.is_worker_connected(LIVE).await);
        assert_eq!(test.scheduler.metrics_snapshot().await, before);
        while let Ok(signal) = signals.try_recv() {
            assert!(matches!(signal, SessionSignal::Offers(_)), "{signal:?}");
        }
        assert_no_registration_or_dispatch(&test.web_db);
        assert_no_registration_or_dispatch(&test.worker_db);
        test.stop().await;
    }
}

#[tokio::test]
async fn fleet_limits_accept_complete_supply_and_reject_oversized_scans() {
    for (workers, readiness_required, accepted) in [
        (256, false, true),
        (257, false, false),
        (1_024, true, false),
    ] {
        let (db, auth) = scoped_db(TeamRole::Admin, false);
        let test = CapacityTest::start(grants(db, &[project_id()]), auth).await;
        test.enqueue(build(project_id(), 0, BuildEstimate::default()))
            .await;
        for index in 0..workers {
            test.worker_with_active_and_readiness(
                &format!("fleet-{index}"),
                HashSet::from([project_id()]),
                vec![],
                readiness_required,
            )
            .await;
        }
        let before = test.scheduler.metrics_snapshot().await;
        let res = test.post(&json!({ "profiles": [] })).await;
        res.assert_status(if accepted {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        });
        let body: Value = res.json();
        if accepted {
            assert_eq!(body["message"]["total_ready_builds"], 1);
            assert_eq!(
                body["message"]["workers"].as_array().unwrap().len(),
                workers
            );
            assert_eq!(
                body["message"]["builds"][0]["workers"]
                    .as_array()
                    .unwrap()
                    .len(),
                workers
            );
        } else {
            assert_eq!(body["error"], true);
            assert_eq!(body["code"], "service_unavailable");
        }
        assert_eq!(test.scheduler.metrics_snapshot().await, before);
        assert_eq!(test.scheduler.board_workers().await.len(), workers);
        assert_no_registration_or_dispatch(&test.web_db);
        assert_no_registration_or_dispatch(&test.worker_db);
        test.stop().await;
    }
}

#[tokio::test]
async fn tracked_profile_requests_share_the_http_body_bound() {
    let (db, auth) = scoped_db(TeamRole::Admin, false);
    let test = CapacityTest::start(db, auth).await;
    test.post(&json!({
        "profiles": [],
        "tracked_profiles": [profile(&"x".repeat(64 * 1024), INTENDED, 60_000)]
    }))
    .await
    .assert_status(StatusCode::PAYLOAD_TOO_LARGE);
    let statements = all_statements(test.web_db.clone().into_transaction_log());
    assert!(
        statements
            .iter()
            .all(|statement| !statement.sql.contains(r#"FROM "team_worker""#))
    );
    test.stop().await;
}

#[tokio::test]
async fn native_waiting_credit_is_opt_in_capped_and_omitted_for_unselected_owners() {
    for (include_waiting, scoped_away, truncated) in [
        (None, false, false),
        (Some(false), false, false),
        (Some(true), false, false),
        (Some(true), true, false),
        (Some(true), false, true),
    ] {
        let other_project = ProjectId::now_v7();
        let projects = HashSet::from([project_id(), other_project]);
        let (_key, state) = page_state();
        let (db, auth) = if truncated {
            page_db(
                &[
                    &[project_id(), other_project],
                    &[project_id(), other_project],
                ],
                0,
            )
        } else {
            let (db, auth) = scoped_db(TeamRole::Admin, false);
            (grants(db, &[project_id(), other_project]), auth)
        };
        let test = CapacityTest::with_state(state, db, auth).await;
        let mut active = build(
            project_id(),
            -1,
            BuildEstimate::fixed(Some(40_000), None, 1),
        );
        active.estimate.max_cores = Some(1);
        let active_id = active.derivation_build.to_string();
        let assignment_id = DispatchedJobId::now_v7();
        let _signals = test
            .worker_with_active(
                LIVE,
                projects.clone(),
                vec![Reattached::single(
                    build_job_key(active.derivation_build),
                    assignment_id,
                    PendingJob::Build(active),
                )],
            )
            .await;
        let initial = test
            .scheduler
            .build_capacity_snapshot(projects)
            .await
            .expect("restored active build captured");
        assert_eq!(
            initial.workers[0].caps.metrics.unwrap().ram_available_mb(),
            Some(20_000)
        );
        if scoped_away || truncated {
            for index in 0..if truncated { 128 } else { 1 } {
                let mut unsupported = build(
                    if scoped_away {
                        other_project
                    } else {
                        project_id()
                    },
                    index,
                    BuildEstimate::fixed(Some(1_000), None, 1),
                );
                unsupported.job.requirement.required_features = vec!["not-on-worker".into()];
                test.enqueue(unsupported).await;
            }
        }
        let mut waiting = build(
            project_id(),
            130,
            BuildEstimate::fixed(Some(30_000), None, 1),
        );
        waiting.estimate.max_cores = Some(1);
        waiting.rescore_count = gradient_pool::score::weights::RESCORE_MAX_ROUNDS;
        waiting.prioritized = true;
        let waiting_id = waiting.derivation_build.to_string();
        test.enqueue(waiting).await;
        assert!(
            test.scheduler
                .request_job(LIVE, JobKind::Build)
                .await
                .is_none(),
            "the only eligible pending build waits for the active build's RAM"
        );
        let before = test.scheduler.metrics_snapshot().await;
        let mut request = json!({ "profiles": [] });
        if let Some(include) = include_waiting {
            request["include_waiting_builds"] = json!(include);
        }
        if scoped_away {
            request["projects"] = json!([other_project]);
        }
        if truncated {
            request["build_page"] = json!({});
        }
        let res = test.post(&request).await;
        res.assert_status_ok();
        for hidden in [
            active_id,
            assignment_id.to_string(),
            waiting_id,
            LIVE.into(),
            "aaaa-private-build.drv".into(),
        ] {
            assert!(!res.text().contains(&hidden), "response disclosed {hidden}");
        }
        let body: Value = res.json();
        let message = &body["message"];
        assert!(message.get("tracked_workers").is_none());
        if include_waiting == Some(true) && !scoped_away && !truncated {
            assert_eq!(
                message["workers"],
                json!([{
                    "free_slots": 1,
                    "available_ram_mb": 0,
                    "waiting_build": { "build_index": 0, "ram_credit_mb": 20_000 }
                }])
            );
            assert_eq!(
                message["builds"][0]["workers"][0]["reserved_ram_mb"],
                30_000
            );
        } else {
            assert_eq!(
                message["workers"],
                json!([{ "free_slots": 1, "available_ram_mb": 0 }])
            );
        }
        assert_eq!(message["builds_truncated"], truncated);
        assert_eq!(
            message["total_ready_builds"],
            if truncated { 129 } else { 1 }
        );
        assert_eq!(
            message["builds"].as_array().unwrap().len(),
            if truncated { 128 } else { 1 }
        );
        if scoped_away || truncated {
            for build in message["builds"].as_array().unwrap() {
                assert_eq!(build["required_features"], json!(["not-on-worker"]));
                assert_eq!(build["workers"], json!([null]));
            }
        }
        assert_eq!(test.scheduler.metrics_snapshot().await, before);
        if truncated {
            let cursor = message["build_page"]["next"].as_str().unwrap();
            let res = test
                .post(&json!({
                    "profiles": [],
                    "include_waiting_builds": true,
                    "build_page": { "after": cursor }
                }))
                .await;
            res.assert_status_ok();
            let body: Value = res.json();
            assert_eq!(
                body["message"]["workers"][0]["waiting_build"],
                json!({ "build_index": 0, "ram_credit_mb": 20_000 })
            );
            assert_eq!(body["message"]["builds"].as_array().unwrap().len(), 1);
            assert_eq!(body["message"]["build_page"]["next"], Value::Null);
            assert_eq!(test.scheduler.metrics_snapshot().await, before);
        }
        assert_eq!(test.scheduler.workers_info().await[0].assigned_job_count, 1);
        assert_no_registration_or_dispatch(&test.worker_db);
        assert_no_registration_or_dispatch(&test.web_db);
        test.stop().await;
    }
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

#[tokio::test]
async fn capacity_pages_reach_fit_work_behind_an_unfit_prefix_without_disclosing_ids() {
    for prefix in ["architecture", "features", "memory"] {
        let (key, state) = page_state();
        let (db, auth) = page_db(&[&[project_id()], &[project_id()]], 1);
        let test = CapacityTest::with_state(state, db, auth).await;
        let mut anchor = None;
        for seconds in 0..128 {
            let mut job = build(
                project_id(),
                seconds,
                BuildEstimate::fixed(
                    Some(if prefix == "memory" { 120_000 } else { 1_000 }),
                    None,
                    1,
                ),
            );
            job.estimate.max_cores = Some(1);
            match prefix {
                "architecture" => job.job.requirement.architecture = "aarch64-linux".into(),
                "features" => job.job.requirement.required_features = vec!["missing".into()],
                _ => {}
            }
            if seconds == 127 {
                anchor = Some(job.derivation_build);
            }
            test.enqueue(job).await;
        }
        let ready = build(
            project_id(),
            128,
            BuildEstimate::fixed(Some(1_000), None, 1),
        );
        let hidden = [
            ready.derivation_build.to_string(),
            ready.derivation.to_string(),
            ready.evaluation_id.to_string(),
            project_id().to_string(),
            anchor.unwrap().to_string(),
        ];
        test.enqueue(ready).await;
        let before = test.scheduler.metrics_snapshot().await;
        let first = test
            .post(&json!({
                "profiles": [profile("builder", INTENDED, 60_000)],
                "build_page": {}
            }))
            .await;
        first.assert_status_ok();
        let body: Value = first.json();
        let message = &body["message"];
        assert_eq!(message["total_ready_builds"], 129);
        assert_eq!(message["build_page"]["remaining_ready_builds"], 1);
        assert_eq!(message["builds"].as_array().unwrap().len(), 128);
        for build in message["builds"].as_array().unwrap() {
            if prefix == "memory" {
                assert_eq!(build["profiles"][0]["prediction_fit"], "does_not_fit");
            } else {
                assert_eq!(build["profiles"], json!([null]));
            }
        }
        let cursor = message["build_page"]["next"].as_str().unwrap();
        assert!(!cursor.is_empty());
        let plaintext = gradient_ci::actions::decrypt_action_secret(
            cursor,
            &std::fs::read(key.path()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&plaintext).unwrap(),
            json!({
                "kind": "build_capacity_page_v1",
                "team": team_id(),
                "projects": [project_id()],
                "after": {
                    "ready_at": test_date() + Duration::seconds(127),
                    "build": anchor.unwrap()
                }
            })
        );
        for id in &hidden {
            assert!(!first.text().contains(id), "response disclosed {id}");
        }
        let second = test
            .post(&json!({
                "profiles": [profile("builder", INTENDED, 60_000)],
                "build_page": { "after": cursor }
            }))
            .await;
        second.assert_status_ok();
        let body: Value = second.json();
        let message = &body["message"];
        assert_eq!(message["total_ready_builds"], 129);
        assert_eq!(message["builds_truncated"], true);
        assert_eq!(message["build_page"]["remaining_ready_builds"], 0);
        assert_eq!(message["build_page"]["next"], Value::Null);
        assert_eq!(message["builds"].as_array().unwrap().len(), 1);
        assert_eq!(
            message["builds"][0]["ready_at"],
            json!(test_date() + Duration::seconds(128))
        );
        assert_eq!(
            message["builds"][0]["profiles"][0]["prediction_fit"],
            "fits"
        );
        for id in &hidden {
            assert!(!second.text().contains(id), "response disclosed {id}");
        }
        assert_eq!(test.scheduler.metrics_snapshot().await, before);
        assert_no_registration_or_dispatch(&test.worker_db);
        assert_no_registration_or_dispatch(&test.web_db);
        test.stop().await;
    }
}

#[tokio::test]
async fn capacity_cursors_bind_resolved_scope_and_reject_invalid_ciphertext() {
    let (key, state) = page_state();
    let other = ProjectId::now_v7();
    let scope = [project_id(), other];
    let (db, auth) = page_db(
        &[
            &scope,
            &scope,
            &[project_id()],
            &scope,
            &scope,
            &scope,
            &scope,
        ],
        0,
    );
    let test = CapacityTest::with_state(state, db, auth).await;
    for seconds in 0..129 {
        test.enqueue(build(project_id(), seconds, BuildEstimate::default()))
            .await;
    }
    let first = test
        .post(&json!({ "profiles": [], "build_page": {} }))
        .await;
    first.assert_status_ok();
    let body: Value = first.json();
    let cursor = body["message"]["build_page"]["next"].as_str().unwrap();
    let next = test
        .post(&json!({
            "projects": [other, project_id()],
            "profiles": [],
            "build_page": { "after": cursor }
        }))
        .await;
    next.assert_status_ok();
    let body: Value = next.json();
    assert_eq!(body["message"]["build_page"]["next"], Value::Null);
    let changed = test
        .post(&json!({ "profiles": [], "build_page": { "after": cursor } }))
        .await;
    changed.assert_status(StatusCode::BAD_REQUEST);
    let malformed = test
        .post(&json!({ "profiles": [], "build_page": { "after": "not base64" } }))
        .await;
    malformed.assert_status(StatusCode::BAD_REQUEST);
    let tampered = format!(
        "{}{}",
        if cursor.starts_with('A') { 'B' } else { 'A' },
        &cursor[1..]
    );
    test.post(&json!({ "profiles": [], "build_page": { "after": tampered } }))
        .await
        .assert_status(StatusCode::BAD_REQUEST);
    for (kind, team) in [
        ("other_feature", team_id()),
        ("build_capacity_page_v1", TeamId::now_v7()),
    ] {
        let plaintext = json!({
            "kind": kind,
            "team": team,
            "projects": scope,
            "after": { "ready_at": test_date(), "build": DerivationBuildId::now_v7() }
        });
        let encrypted = gradient_ci::actions::encrypt_action_secret(
            &plaintext.to_string(),
            &std::fs::read(key.path()).unwrap(),
        )
        .unwrap();
        test.post(&json!({ "profiles": [], "build_page": { "after": encrypted } }))
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
    assert_eq!(test.scheduler.pending_job_count().await, 129);
    assert_no_registration_or_dispatch(&test.worker_db);
    assert_no_registration_or_dispatch(&test.web_db);
    test.stop().await;
}

#[tokio::test]
async fn a_missing_cursor_key_refuses_only_paged_assessments() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join("missing-key")
        .to_string_lossy()
        .into_owned();
    let state = TestState::new().config(move |config| config.secrets.crypt_file = path);
    let (db, auth) = page_db(&[&[project_id()], &[project_id()]], 0);
    let test = CapacityTest::with_state(state, db, auth).await;
    test.enqueue(build(project_id(), 0, BuildEstimate::default()))
        .await;
    let legacy = test.post(&json!({ "profiles": [] })).await;
    legacy.assert_status_ok();
    let body: Value = legacy.json();
    assert_eq!(body["message"]["total_ready_builds"], 1);
    assert!(body["message"].get("build_page").is_none());
    let paged = test
        .post(&json!({ "profiles": [], "build_page": {} }))
        .await;
    paged.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = paged.json();
    assert!(body["message"].get("total_ready_builds").is_none());
    assert_eq!(test.scheduler.pending_job_count().await, 1);
    test.stop().await;
}
