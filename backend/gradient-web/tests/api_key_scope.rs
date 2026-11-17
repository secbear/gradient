/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

#![expect(
    clippy::unwrap_used,
    reason = "Test scaffolding: Fixture helpers that cannot construct their own value fail the test loudly."
)]

use axum::http::StatusCode;
use base64::Engine;
use gradient_db::api_keys::team_key_verifier;
use gradient_entity::team_user::TeamRole;
use gradient_entity::{api, cache, ids::*, team_project, team_user, team_worker};
use gradient_fixtures::database::TestDatabase;
use gradient_fixtures::ids::{cache_id, project_id, user_id};
use gradient_fixtures::mock::{all_statements, exec_ok, mock_db};
use gradient_fixtures::rows::{
    cache_member, cache_view_role, live_session, team, team_member, user,
};
use gradient_test_support::web::{
    bearer, make_test_server, make_token, signed_in, signed_in_as, with_auth,
};
use gradient_types::consts::BASE_CACHE_ROLE_VIEW_ID;
use gradient_types::permissions::{CachePermission, Permission, cache_mask_from, mask_from};
use sea_orm::{EntityTrait, IntoActiveModel, MockDatabase};
use serde_json::json;
use sha2::{Digest, Sha256};

fn hash_api_key(raw: &str) -> String {
    let mut h = Sha256::new();
    h.update(raw.as_bytes());
    let mut out = String::with_capacity(64);
    for b in h.finalize() {
        use std::fmt::Write as _;
        write!(&mut out, "{:02x}", b).unwrap();
    }
    out
}

fn key_row(raw: &str, permission: i64, cache: Option<CacheId>) -> api::Model {
    let now = chrono::Utc::now().naive_utc();
    api::Model {
        id: ApiId::now_v7(),
        owned_by: user_id(),
        name: "key".into(),
        key: hash_api_key(raw),
        last_used_at: now,
        created_at: now,
        permission,
        cache,
        ..Default::default()
    }
}

fn view_project_key(raw: &str) -> api::Model {
    key_row(raw, mask_from(&[Permission::ViewProject]), None)
}

fn with_key(db: MockDatabase, key: &api::Model) -> MockDatabase {
    with_key_as(db, key, user())
}

fn with_key_as(
    db: MockDatabase,
    key: &api::Model,
    actor: gradient_entity::user::Model,
) -> MockDatabase {
    db.append_query_results([vec![key.clone()]])
        .append_exec_results([exec_ok()])
        .append_query_results([vec![key.clone()]])
        .append_query_results([vec![actor]])
}

fn private_cache() -> cache::Model {
    gradient_fixtures::rows::cache()
}

fn bearer_key(raw: &str) -> String {
    format!("Bearer GRAD{raw}")
}

fn bearer_team_key(raw: &str) -> String {
    format!("Bearer GTEAM{raw}")
}

fn team_key_row(raw: &str, permission: i64) -> api::Model {
    api::Model {
        key: team_key_verifier(&hash_api_key(raw)),
        team: Some(gradient_fixtures::ids::team_id()),
        ..key_row(raw, permission, None)
    }
}

fn basic(password: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("nix:{password}"));
    format!("Basic {encoded}")
}

async fn refused_with_key(
    db: MockDatabase,
    authorization: String,
    request: impl FnOnce(&axum_test::TestServer) -> axum_test::TestRequest,
) -> StatusCode {
    let server = make_test_server(db.into_connection());
    request(&server)
        .add_header("authorization", authorization)
        .await
        .status_code()
}

#[tokio::test]
async fn any_project_key_cannot_create_a_cache() {
    let raw = "a".repeat(64);
    let db = with_key(mock_db(), &view_project_key(&raw));
    let status = refused_with_key(db, bearer_key(&raw), |s| {
        s.put("/api/v1/caches").json(&json!({
            "name": "new-cache",
            "display_name": "New",
            "description": "",
            "priority": 30,
        }))
    })
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn any_project_key_cannot_change_or_delete_a_member_cache() {
    for method in ["PATCH", "DELETE"] {
        let raw = "b".repeat(64);
        let db = with_key(mock_db(), &key_row(&raw, i64::MAX, None))
            .append_query_results([vec![private_cache()]]);
        let status = refused_with_key(db, bearer_key(&raw), |s| match method {
            "PATCH" => s
                .patch("/api/v1/caches/test-cache")
                .json(&json!({ "display_name": "Renamed" })),
            _ => s.delete("/api/v1/caches/test-cache"),
        })
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method}");
    }
}

#[tokio::test]
async fn any_project_key_cannot_read_a_private_cache_without_a_grant() {
    let raw = "c".repeat(64);
    let db = with_key(mock_db(), &view_project_key(&raw))
        .append_query_results([vec![private_cache()]])
        .append_query_results([Vec::<gradient_entity::project_cache::Model>::new()]);
    let status =
        refused_with_key(db, bearer_key(&raw), |s| s.get("/api/v1/caches/test-cache")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn api_keys_cannot_create_list_change_or_delete_teams() {
    let any_project = "d".repeat(64);
    let cache_scope = "e".repeat(64);
    let keys = [
        (any_project.clone(), key_row(&any_project, i64::MAX, None)),
        (
            cache_scope.clone(),
            key_row(
                &cache_scope,
                cache_mask_from(CachePermission::ALL),
                Some(cache_id()),
            ),
        ),
    ];
    for (raw, key) in keys {
        for method in ["PUT", "GET", "PATCH", "DELETE"] {
            let db = with_key(mock_db(), &key);
            let status = refused_with_key(db, bearer_key(&raw), |s| match method {
                "PUT" => s
                    .put("/api/v1/teams")
                    .json(&json!({ "name": "platform", "display_name": "Platform" })),
                "GET" => s.get("/api/v1/teams"),
                "PATCH" => s
                    .patch("/api/v1/teams/platform")
                    .json(&json!({ "display_name": "Renamed" })),
                _ => s.delete("/api/v1/teams/platform"),
            })
            .await;
            assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "{method} with {:?}",
                key.cache
            );
        }
    }
}

const TEAM_WORKER_URL: &str = "/api/v1/teams/platform/workers";
const WORKER_ID: &str = "7e000000-0000-0000-0000-0000000000f1";

fn worker_request(server: &axum_test::TestServer, operation: &str) -> axum_test::TestRequest {
    match operation {
        "identity" => server.get("/api/v1/teams/platform"),
        "list" => server.get(TEAM_WORKER_URL),
        "register" => server
            .post(TEAM_WORKER_URL)
            .json(&json!({ "worker_id": WORKER_ID, "display_name": "Builder" })),
        "update" => server
            .patch(&format!("{TEAM_WORKER_URL}/{WORKER_ID}"))
            .json(&json!({ "active": false })),
        "delete" => server.delete(&format!("{TEAM_WORKER_URL}/{WORKER_ID}")),
        "demand" => server.get(&format!("{TEAM_WORKER_URL}/demand")),
        "capacity" => server
            .post(&format!("{TEAM_WORKER_URL}/capacity-assessment"))
            .json(&json!({ "projects": [], "profiles": [] })),
        "webhooks" => server.get("/api/v1/teams/platform/webhooks"),
        _ => panic!("unknown operation {operation}"),
    }
}

#[tokio::test]
async fn api_key_transport_must_match_the_stored_team_scope() {
    for (team_scope, team_transport, expected) in [
        (false, false, StatusCode::OK),
        (true, true, StatusCode::OK),
        (false, true, StatusCode::UNAUTHORIZED),
        (true, false, StatusCode::UNAUTHORIZED),
    ] {
        let raw = "TEAMcustom-token";
        let key = if team_scope {
            team_key_row(raw, Permission::ManageWorkers.bit())
        } else {
            key_row(raw, Permission::ManageWorkers.bit(), None)
        };
        let authorization = if team_transport {
            bearer_team_key(raw)
        } else {
            bearer_key(raw)
        };
        let status = refused_with_key(with_key(mock_db(), &key), authorization, |s| {
            s.get("/api/v1/user/keys/permissions")
        })
        .await;
        assert_eq!(
            status, expected,
            "team_scope={team_scope}, team_transport={team_transport}"
        );
    }
}

#[tokio::test]
async fn team_worker_api_keys_require_team_scope_and_explicit_permission() {
    for operation in [
        "identity", "list", "register", "update", "delete", "demand", "capacity", "webhooks",
    ] {
        for (permission, project, cache, team, superuser) in [
            (
                Permission::ViewProject.bit(),
                None,
                None,
                Some(gradient_fixtures::ids::team_id()),
                false,
            ),
            (0, None, None, Some(gradient_fixtures::ids::team_id()), true),
            (i64::MAX, None, None, None, false),
            (i64::MAX, Some(project_id()), None, None, true),
            (i64::MAX, None, Some(cache_id()), None, true),
        ] {
            let raw = "s".repeat(64);
            let mut key = api::Model {
                project,
                team,
                ..key_row(&raw, permission, cache)
            };
            let authorization = if team.is_some() {
                key.key = team_key_verifier(&key.key);
                bearer_team_key(&raw)
            } else {
                bearer_key(&raw)
            };
            let db = with_key_as(
                mock_db(),
                &key,
                gradient_entity::user::Model {
                    superuser,
                    ..user()
                },
            );
            let status =
                refused_with_key(db, authorization, |s| worker_request(s, operation)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{operation} with {key:?}");
        }
    }
}

#[tokio::test]
async fn team_worker_api_keys_remain_bounded_by_the_owners_team_role() {
    for operation in [
        "identity", "list", "register", "update", "delete", "demand", "capacity", "webhooks",
    ] {
        for role in [None, Some(TeamRole::Member)] {
            let raw = "t".repeat(64);
            let key = team_key_row(&raw, i64::MAX);
            let mut db = with_key(mock_db(), &key).append_query_results([vec![team()]]);
            db = db.append_query_results([role
                .map(team_member)
                .into_iter()
                .collect::<Vec<team_user::Model>>()]);
            let expected = match role {
                None => StatusCode::NOT_FOUND,
                Some(_) if matches!(operation, "identity" | "list") => {
                    if operation == "list" {
                        db = db.append_query_results([Vec::<team_worker::Model>::new()]);
                    }
                    StatusCode::OK
                }
                Some(_) => StatusCode::FORBIDDEN,
            };
            let status =
                refused_with_key(db, bearer_team_key(&raw), |s| worker_request(s, operation)).await;
            assert_eq!(status, expected, "{operation} with {role:?}");
        }
    }
}

#[tokio::test]
async fn worker_management_and_team_webhook_key_permissions_are_independent() {
    for (permission, operation, expected) in [
        (Permission::ManageWorkers, "identity", StatusCode::OK),
        (Permission::ManageWorkers, "list", StatusCode::OK),
        (Permission::ManageWorkers, "demand", StatusCode::OK),
        (Permission::ManageWorkers, "capacity", StatusCode::OK),
        (Permission::ManageWorkers, "webhooks", StatusCode::FORBIDDEN),
        (Permission::ManageWebhooks, "webhooks", StatusCode::OK),
        (Permission::ManageWebhooks, "list", StatusCode::FORBIDDEN),
    ] {
        let raw = "u".repeat(64);
        let key = team_key_row(&raw, permission.bit());
        let mut db = with_key(mock_db(), &key)
            .append_query_results([vec![team()]])
            .append_query_results([vec![team_member(TeamRole::Admin)]]);
        db = match operation {
            "list" => db.append_query_results([Vec::<team_worker::Model>::new()]),
            "demand" | "capacity" => db.append_query_results([Vec::<team_project::Model>::new()]),
            "webhooks" => db.append_query_results([Vec::<gradient_entity::webhook::Model>::new()]),
            _ => db,
        };
        let status =
            refused_with_key(db, bearer_team_key(&raw), |s| worker_request(s, operation)).await;
        assert_eq!(status, expected, "{operation} with {permission:?}");
    }
}

#[tokio::test]
async fn worker_management_key_does_not_open_other_team_administration() {
    let raw = "v".repeat(64);
    let key = team_key_row(&raw, i64::MAX);
    for operation in ["members", "grants", "requests", "invitations"] {
        let db = with_key(mock_db(), &key);
        let status = refused_with_key(db, bearer_team_key(&raw), |s| {
            s.get(&format!("/api/v1/teams/platform/{operation}"))
        })
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{operation}");
    }
}

#[tokio::test]
async fn team_admin_worker_key_registers_updates_and_deletes_workers() {
    for operation in ["register", "update", "delete"] {
        let raw = "w".repeat(64);
        let key = team_key_row(&raw, Permission::ManageWorkers.bit());
        let worker = team_worker::Model {
            id: TeamWorkerId::now_v7(),
            team: gradient_fixtures::ids::team_id(),
            worker_id: WORKER_ID.into(),
            active: true,
            ..Default::default()
        };
        let mut db = with_key(mock_db(), &key)
            .append_query_results([vec![team()]])
            .append_query_results([vec![team_member(TeamRole::Admin)]]);
        if operation == "register" {
            db = db
                .append_query_results([Vec::<gradient_entity::worker_registration::Model>::new()])
                .append_query_results([Vec::<team_worker::Model>::new()]);
        }
        db = db.append_query_results([vec![worker.clone()]]);
        if operation == "update" {
            db = db.append_query_results([vec![team_worker::Model {
                active: false,
                ..worker
            }]]);
        }
        db = db
            .append_query_results([Vec::<team_project::Model>::new()])
            .append_exec_results([exec_ok()]);
        let connection = db.into_connection();
        let server = make_test_server(connection.clone());
        let res = worker_request(&server, operation)
            .add_header("authorization", bearer_team_key(&raw))
            .await;
        res.assert_status_ok();
        if operation == "register" {
            let body: serde_json::Value = res.json();
            assert_eq!(body["message"]["token"].as_str().map(str::len), Some(64));
        }
        if operation == "update" {
            let statements = all_statements(connection.into_transaction_log());
            let update = statements
                .iter()
                .find(|statement| statement.sql.starts_with("UPDATE \"team_worker\""))
                .unwrap();
            assert!(update.to_string().contains("\"active\" = FALSE"));
        }
    }
}

#[tokio::test]
async fn team_api_key_cannot_access_another_team_or_project_and_cache_routes() {
    let raw = "x".repeat(64);
    let key = team_key_row(&raw, i64::MAX);
    let another = gradient_entity::team::Model {
        id: TeamId::now_v7(),
        name: "another".into(),
        ..team()
    };
    for superuser in [false, true] {
        let db = with_key_as(
            mock_db(),
            &key,
            gradient_entity::user::Model {
                superuser,
                ..user()
            },
        )
        .append_query_results([vec![another.clone()]]);
        let status = refused_with_key(db, bearer_team_key(&raw), |s| {
            s.get("/api/v1/teams/another/workers")
        })
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "superuser={superuser}");
    }

    for path in [
        "/api/v1/projects",
        "/api/v1/projects/test-project",
        "/api/v1/caches/test-cache",
        "/api/v1/board/jobs/pending",
        "/api/v1/admin/webhooks",
    ] {
        let db = with_key(mock_db(), &key);
        let status = refused_with_key(db, bearer_team_key(&raw), |s| s.get(path)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
    }

    let db = with_key(
        mock_db().append_query_results([vec![private_cache()]]),
        &key,
    );
    assert_eq!(
        pull_status(db, &format!("GTEAM{raw}")).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn only_a_team_admin_can_issue_a_team_pinned_key() {
    for (role, superuser) in [
        (Some(TeamRole::Member), false),
        (Some(TeamRole::Admin), false),
        (None, true),
    ] {
        let (db, auth) = signed_in_as(gradient_entity::user::Model {
            superuser,
            ..user()
        });
        let key = team_key_row("issued-key", Permission::ManageWorkers.bit());
        let db = db
            .append_query_results([Vec::<api::Model>::new()])
            .append_query_results([vec![team()]])
            .append_query_results([role.map(team_member).into_iter().collect::<Vec<_>>()])
            .append_query_results([vec![key]]);
        let connection = db.into_connection();
        let server = make_test_server(connection.clone());
        let res = server
            .post("/api/v1/user/keys")
            .add_header("authorization", auth)
            .json(&json!({
                "name": "worker-controller",
                "permissions": ["manageWorkers"],
                "team": "platform",
                "expires_in_days": 1,
            }))
            .await;
        if superuser || role == Some(TeamRole::Admin) {
            res.assert_status_ok();
            let body: serde_json::Value = res.json();
            let raw = body["message"]
                .as_str()
                .unwrap()
                .strip_prefix("GTEAM")
                .unwrap();
            let statements = all_statements(connection.into_transaction_log());
            let inserted = statements
                .iter()
                .find(|s| s.sql.starts_with("INSERT INTO \"api\""))
                .unwrap();
            let values = &inserted.values.as_ref().unwrap().0;
            assert!(values.contains(&sea_orm::Value::from(format!("team:{}", hash_api_key(raw)))));
            assert!(values.contains(&sea_orm::Value::from(gradient_fixtures::ids::team_id())));
        } else {
            res.assert_status_forbidden();
        }
    }
}

#[tokio::test]
async fn team_key_creation_rejects_combined_scope_pins() {
    for other_pin in ["project", "cache"] {
        let (db, auth) = signed_in();
        let server = make_test_server(db.into_connection());
        let mut body = json!({
            "name": "worker-controller", "permissions": ["manageWorkers"], "team": "platform",
        });
        body[other_pin] = json!("other");
        let res = server
            .post("/api/v1/user/keys")
            .add_header("authorization", auth)
            .json(&body)
            .await;
        res.assert_status_bad_request();
    }
}

#[tokio::test]
async fn api_key_team_scope_cannot_be_changed_by_patch() {
    for key in [
        key_row("edited-key", Permission::ManageWorkers.bit(), None),
        api::Model {
            project: Some(project_id()),
            ..key_row("edited-key", Permission::ManageWorkers.bit(), None)
        },
        key_row(
            "edited-key",
            cache_mask_from(CachePermission::ALL),
            Some(cache_id()),
        ),
        team_key_row("edited-key", Permission::ManageWorkers.bit()),
    ] {
        let mut patches = vec![
            json!({"team": null}),
            json!({"team": "platform"}),
            json!({"team": "another"}),
        ];
        if key.team.is_some() {
            patches.extend([json!({"project": null}), json!({"project": "test-project"})]);
        }
        for patch in patches {
            let (db, auth) = signed_in();
            let connection = db
                .append_query_results([vec![key.clone()]])
                .into_connection();
            let server = make_test_server(connection.clone());
            let res = server
                .patch(&format!("/api/v1/user/keys/{}", key.id))
                .add_header("authorization", auth)
                .json(&patch)
                .await;
            res.assert_status_bad_request();
            let statements = all_statements(connection.into_transaction_log());
            assert!(
                statements
                    .iter()
                    .all(|s| !s.sql.starts_with("UPDATE \"api\"")),
                "{patch}"
            );
        }
    }
}

#[tokio::test]
async fn team_key_issuance_and_allowed_edits_preserve_the_stored_scope_and_verifier() {
    let db = TestDatabase::create().await;
    let connection = db.connection();
    gradient_entity::user::Entity::insert(user().into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    gradient_entity::team::Entity::insert(team().into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    team_user::Entity::insert(team_member(TeamRole::Admin).into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    let session_id = SessionId::now_v7();
    gradient_entity::session::Entity::insert(live_session(session_id).into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    let server = make_test_server(connection.clone());
    let issued = server
        .post("/api/v1/user/keys")
        .add_header("authorization", bearer(session_id))
        .json(&json!({
            "name": "worker-controller",
            "permissions": ["manageWorkers"],
            "team": "platform",
        }))
        .await;
    issued.assert_status_ok();
    let body: serde_json::Value = issued.json();
    let raw = body["message"]
        .as_str()
        .unwrap()
        .strip_prefix("GTEAM")
        .unwrap();
    let mut expected =
        gradient_db::api_keys::by_owner_and_name(&connection, user_id(), "worker-controller")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(expected.key, format!("team:{}", hash_api_key(raw)));
    assert_eq!(expected.team, Some(gradient_fixtures::ids::team_id()));
    assert_eq!(expected.project, None);
    assert_eq!(expected.cache, None);
    assert_eq!(expected.permission, Permission::ManageWorkers.bit());

    for patch in [
        json!({"name": "renamed-controller"}),
        json!({"permissions": ["manageWebhooks"]}),
        json!({"allowed_ips": ["192.0.2.0/24"]}),
    ] {
        if let Some(name) = patch["name"].as_str() {
            expected.name = name.into();
        }
        if patch.get("permissions").is_some() {
            expected.permission = Permission::ManageWebhooks.bit();
        }
        if patch.get("allowed_ips").is_some() {
            expected.allowed_ips = Some(vec!["192.0.2.0/24".into()]);
        }
        let edited = server
            .patch(&format!("/api/v1/user/keys/{}", expected.id))
            .add_header("authorization", bearer(session_id))
            .json(&patch)
            .await;
        edited.assert_status_ok();
        assert_eq!(db.find::<api::Entity>(expected.id).await.unwrap(), expected);
        let body: serde_json::Value = edited.json();
        assert_eq!(body["message"]["team"], "platform");
    }
    drop(server);
    db.finish().await;
}

#[tokio::test]
async fn team_key_migration_preserves_old_keys_and_prevents_automatic_scope_expansion() {
    use gradient_migration::{Migrator, MigratorTrait};

    let db = TestDatabase::create().await;
    let connection = db.connection();
    gradient_entity::user::Entity::insert(user().into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    gradient_entity::team::Entity::insert(team().into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    gradient_entity::project::Entity::insert(
        gradient_fixtures::rows::project().into_active_model(),
    )
    .exec_without_returning(&connection)
    .await
    .unwrap();
    let old_raw = "o".repeat(64);
    let old = api::Model {
        project: Some(project_id()),
        ..key_row(&old_raw, i64::MAX, None)
    };
    api::Entity::insert(old.clone().into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    let old = db.find::<api::Entity>(old.id).await.unwrap();
    let error = Migrator::down(&connection, Some(1)).await.unwrap_err();
    assert!(error.to_string().contains("irreversible"));
    assert_eq!(db.find::<api::Entity>(old.id).await.unwrap(), old);
    // Build the pre-upgrade schema only in this disposable database.
    db.execute(
        "ALTER TABLE public.api DROP COLUMN team; \
         DELETE FROM seaql_migrations WHERE version = 'm20261130_000000_team_api_keys';",
    )
    .await;
    Migrator::up(&connection, None).await.unwrap();
    assert_eq!(db.find::<api::Entity>(old.id).await.unwrap(), old);
    let key = api::Model {
        name: "team-key".into(),
        ..team_key_row("new-team-key", Permission::ManageWorkers.bit())
    };
    api::Entity::insert(key.clone().into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    let duplicate = api::Model {
        id: ApiId::now_v7(),
        name: "duplicate-team-verifier".into(),
        ..key.clone()
    };
    let error = api::Entity::insert(duplicate.into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("api_team_verifier_unique"));
    let overlapping = api::Model {
        id: ApiId::now_v7(),
        name: "overlap".into(),
        key: team_key_verifier(&hash_api_key("overlapping-key")),
        project: Some(project_id()),
        ..key.clone()
    };
    let error = api::Entity::insert(overlapping.into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("api_team_scope_exclusive"));
    for (name, verifier, pin) in [
        ("untagged-team", hash_api_key("untagged-team"), key.team),
        (
            "tagged-without-team",
            team_key_verifier(&hash_api_key("tagged-without-team")),
            None,
        ),
    ] {
        let invalid = api::Model {
            id: ApiId::now_v7(),
            name: name.into(),
            key: verifier,
            team: pin,
            ..key.clone()
        };
        let error = api::Entity::insert(invalid.into_active_model())
            .exec_without_returning(&connection)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("api_team_verifier_scope"));
    }
    assert_eq!(
        gradient_db::api_keys::by_key_hash(&connection, &key.key)
            .await
            .unwrap()
            .unwrap()
            .id,
        key.id
    );
    // Legacy GRAD decoding always hashes to hex, including re-prefixed bearer suffixes.
    for raw in ["new-team-key", "TEAMnew-team-key", "team:new-team-key"] {
        let plain = hash_api_key(raw);
        assert!(plain.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(plain, key.key);
        assert!(
            gradient_db::api_keys::by_key_hash(&connection, &plain)
                .await
                .unwrap()
                .is_none()
        );
    }
    gradient_entity::project::Entity::delete_by_id(project_id())
        .exec(&connection)
        .await
        .unwrap();
    let retained = db.find::<api::Entity>(old.id).await.unwrap();
    assert!(retained.project.is_none());
    assert!(retained.team.is_none());
    let server = make_test_server(connection.clone());
    server
        .get(TEAM_WORKER_URL)
        .add_header("authorization", bearer_key(&old_raw))
        .await
        .assert_status_forbidden();
    server
        .get("/api/v1/user/keys/permissions")
        .add_header("authorization", bearer_team_key("new-team-key"))
        .await
        .assert_status_ok();
    for raw in ["new-team-key", "TEAMnew-team-key", "team:new-team-key"] {
        server
            .get("/api/v1/user/keys/permissions")
            .add_header("authorization", bearer_key(raw))
            .await
            .assert_status_unauthorized();
    }
    drop(server);

    gradient_entity::team::Entity::delete_by_id(gradient_fixtures::ids::team_id())
        .exec(&connection)
        .await
        .unwrap();
    assert!(db.find::<api::Entity>(key.id).await.is_none());
    gradient_entity::team::Entity::insert(team().into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    api::Entity::insert(key.clone().into_active_model())
        .exec_without_returning(&connection)
        .await
        .unwrap();
    let error = Migrator::down(&connection, Some(1)).await.unwrap_err();
    assert!(error.to_string().contains("irreversible"));
    assert!(db.find::<api::Entity>(key.id).await.is_some());
    api::Entity::delete_by_id(key.id)
        .exec(&connection)
        .await
        .unwrap();
    let error = Migrator::down(&connection, Some(1)).await.unwrap_err();
    assert!(error.to_string().contains("irreversible"));
    assert!(db.find::<api::Entity>(key.id).await.is_none());
    assert!(db.find::<api::Entity>(old.id).await.is_some());
    db.finish().await;
}

fn cache_scope_key(raw: &str) -> api::Model {
    key_row(raw, cache_mask_from(CachePermission::ALL), Some(cache_id()))
}

#[tokio::test]
async fn api_keys_cannot_create_a_project() {
    let any_project = "h".repeat(64);
    let cache_scope = "i".repeat(64);
    for (raw, key) in [
        (any_project.clone(), key_row(&any_project, i64::MAX, None)),
        (cache_scope.clone(), cache_scope_key(&cache_scope)),
    ] {
        let db = with_key(mock_db(), &key);
        let status = refused_with_key(db, bearer_key(&raw), |s| {
            s.put("/api/v1/projects").json(&json!({
                "name": "new-project",
                "display_name": "New",
                "description": "",
            }))
        })
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{:?}", key.cache);
    }
}

#[tokio::test]
async fn cache_scope_key_is_refused_outside_cache_routes() {
    let raw = "j".repeat(64);
    let key = cache_scope_key(&raw);
    for path in [
        "/api/v1/projects",
        "/api/v1/dashboard/tasks",
        "/api/v1/projects/test-project",
        "/api/v1/user/stars",
    ] {
        let db = with_key(mock_db(), &key);
        let status = refused_with_key(db, bearer_key(&raw), |s| s.get(path)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
    }
}

#[tokio::test]
async fn project_scope_key_is_refused_on_routes_reading_all_projects() {
    let raw = "q".repeat(64);
    let key = api::Model {
        project: Some(project_id()),
        ..key_row(&raw, mask_from(Permission::ALL), None)
    };
    for path in [
        "/api/v1/dashboard/tasks",
        "/api/v1/search?q=nightly",
        "/api/v1/board/jobs/pending",
        "/api/v1/metrics/catalog",
    ] {
        let db = with_key(mock_db(), &key);
        let status = refused_with_key(db, bearer_key(&raw), |s| s.get(path)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
    }
}

async fn pull_status(db: MockDatabase, password: &str) -> StatusCode {
    let server = make_test_server(db.into_connection());
    server
        .get("/cache/test-cache/nix-cache-info")
        .add_header("authorization", basic(password))
        .await
        .status_code()
}

#[tokio::test]
async fn any_project_key_cannot_pull_from_a_member_cache() {
    let raw = "f".repeat(64);
    let db = with_key(
        mock_db().append_query_results([vec![private_cache()]]),
        &view_project_key(&raw),
    )
    .append_query_results([Vec::<gradient_entity::project_cache::Model>::new()])
    .append_query_results([Vec::<gradient_entity::cache_user::Model>::new()])
    .append_query_results([Vec::<gradient_entity::project_cache::Model>::new()]);
    assert_eq!(
        pull_status(db, &format!("GRAD{raw}")).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn any_project_key_of_a_cache_member_is_told_to_use_a_cache_key() {
    let raw = "r".repeat(64);
    let db = with_key(
        mock_db().append_query_results([vec![private_cache()]]),
        &view_project_key(&raw),
    )
    .append_query_results([Vec::<gradient_entity::project_cache::Model>::new()])
    .append_query_results([vec![cache_member(BASE_CACHE_ROLE_VIEW_ID)]])
    .append_query_results([vec![cache_view_role()]]);
    let server = make_test_server(db.into_connection());
    let res = server
        .get("/cache/test-cache/nix-cache-info")
        .add_header("authorization", basic(&format!("GRAD{raw}")))
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
    assert!(res.text().contains("pin to this cache"), "{}", res.text());
}

#[tokio::test]
async fn cache_scope_key_pulls_from_its_cache() {
    let raw = "g".repeat(64);
    let key = key_row(
        &raw,
        cache_mask_from(&[CachePermission::ViewCache, CachePermission::ReadStore]),
        Some(cache_id()),
    );
    let db = with_key(
        mock_db().append_query_results([vec![private_cache()]]),
        &key,
    )
    .append_query_results([vec![cache_member(BASE_CACHE_ROLE_VIEW_ID)]])
    .append_query_results([vec![cache_view_role()]]);
    assert_eq!(pull_status(db, &format!("GRAD{raw}")).await, StatusCode::OK);
}

#[tokio::test]
async fn session_token_pulls_from_a_member_cache() {
    let session_id = SessionId::now_v7();
    let db = with_auth(
        mock_db().append_query_results([vec![private_cache()]]),
        session_id,
    )
    .append_query_results([vec![cache_member(BASE_CACHE_ROLE_VIEW_ID)]])
    .append_query_results([vec![cache_view_role()]]);
    assert_eq!(
        pull_status(db, &make_token(session_id)).await,
        StatusCode::OK
    );
}
