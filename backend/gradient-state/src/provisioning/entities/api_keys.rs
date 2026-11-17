/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use super::super::DynError;
use super::super::StateApplicator;
use super::super::{lookup_id, parse_api_key_hash, read_credential};
use crate::config::*;
use anyhow::Result;
use gradient_entity::*;
use gradient_types::*;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, Set};
use std::collections::HashMap;

impl<'a> StateApplicator<'a> {
    pub(crate) async fn apply_api_keys(
        &self,
        state_api_keys: &HashMap<String, StateApiKey>,
        team_ids: &HashMap<String, TeamId>,
    ) -> Result<(), DynError> {
        let user_lookup = self.user_lookup().await?;
        let project_lookup = self.project_lookup().await?;
        let now = now();

        for state_api_key in state_api_keys.values() {
            if state_api_key.project.is_some() && state_api_key.team.is_some() {
                return Err(format!(
                    "API key '{}' is pinned to a team or to a project, not both",
                    state_api_key.name
                )
                .into());
            }
            let pinned_team = state_api_key
                .team
                .as_ref()
                .map(|name| lookup_id(team_ids, name, "Team"))
                .transpose()?;
            let owned_by_id = user_lookup
                .get(&state_api_key.owned_by)
                .copied()
                .ok_or_else(|| {
                    format!(
                        "User '{}' not found for API key '{}'",
                        state_api_key.owned_by, state_api_key.name
                    )
                })?;

            let mut perms = Vec::with_capacity(state_api_key.permissions.len());
            for wire in &state_api_key.permissions {
                let p = gradient_types::permissions::Permission::from_wire_name(wire).ok_or_else(
                    || {
                        format!(
                            "API key '{}' references unknown permission '{}'",
                            state_api_key.name, wire
                        )
                    },
                )?;
                perms.push(p);
            }
            if perms.is_empty() {
                return Err(format!(
                    "API key '{}' must declare at least one permission",
                    state_api_key.name
                )
                .into());
            }
            let mask = gradient_types::permissions::mask_from(&perms);

            let pinned_project = match &state_api_key.project {
                None => None,
                Some(name) => Some(project_lookup.get(name).copied().ok_or_else(|| {
                    format!(
                        "Project '{}' referenced by API key '{}' not found",
                        name, state_api_key.name
                    )
                })?),
            };

            let (key_value, key_path) = read_credential(
                self.credentials_dir,
                "api",
                &state_api_key.name,
                "key",
                "API key file",
            )?;
            let key_hash = parse_api_key_hash(&key_value, &key_path)?;
            let key_verifier = if pinned_team.is_some() {
                gradient_db::api_keys::team_key_verifier(&key_hash)
            } else {
                key_hash
            };

            let existing_api_key = api::Entity::find()
                .filter(api::Column::Name.eq(&state_api_key.name))
                .filter(api::Column::OwnedBy.eq(owned_by_id))
                .one(self.db)
                .await?;

            if let Some(api_key_model) = existing_api_key {
                let mut api_key: api::ActiveModel = api_key_model.into();
                api_key.key = Set(key_verifier);
                api_key.managed = Set(true);
                api_key.permission = Set(mask);
                api_key.project = Set(pinned_project);
                api_key.team = Set(pinned_team);
                if pinned_team.is_some() {
                    api_key.cache = Set(None);
                }
                api_key.update(self.db).await?;
                tracing::info!(name = %state_api_key.name, "Updated managed API key");
            } else {
                let api_key_model = api::Model {
                    id: ApiId::now_v7(),
                    owned_by: owned_by_id,
                    name: state_api_key.name.clone(),
                    key: key_verifier,
                    last_used_at: now,
                    created_at: now,
                    managed: true,
                    permission: mask,
                    project: pinned_project,
                    team: pinned_team,
                    ..Default::default()
                }
                .into_active_model();

                api_key_model.insert(self.db).await?;
                tracing::info!(name = %state_api_key.name, "Created managed API key");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gradient_fixtures::mock::{all_statements, mock_db};
    use gradient_fixtures::rows::{project, user};
    use std::path::Path;

    async fn apply(
        db: &sea_orm::DatabaseConnection,
        credentials: &Path,
        project: Option<&str>,
        team: Option<&str>,
        team_ids: &HashMap<String, TeamId>,
    ) -> Result<(), DynError> {
        let app = StateApplicator {
            db,
            crypt_secret_file: "",
            credentials_dir: credentials,
            email_enabled: false,
        };
        let key = StateApiKey {
            name: "automation".into(),
            key_file: String::new(),
            owned_by: user().username,
            permissions: vec!["manageWorkers".into()],
            project: project.map(str::to_owned),
            team: team.map(str::to_owned),
        };
        app.apply_api_keys(&HashMap::from([(key.name.clone(), key)]), team_ids)
            .await
    }

    #[tokio::test]
    async fn api_key_apply_sets_team_scope_and_preserves_unrelated_fields() {
        let dir = std::env::temp_dir().join(format!("gradient-state-api-{}", ApiId::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let hash = "a".repeat(64);
        std::fs::write(dir.join("gradient_api_automation_key"), &hash).unwrap();
        let team = TeamId::now_v7();
        let cache = CacheId::now_v7();
        let team_ids = HashMap::from([("platform".to_string(), team)]);
        for (existing, pinned_project, pinned_team) in [
            (None, None, Some("platform")),
            (None, None, None),
            (None, Some("test-project"), None),
            (
                Some(api::Model {
                    project: Some(project().id),
                    ..Default::default()
                }),
                None,
                Some("platform"),
            ),
            (
                Some(api::Model {
                    cache: Some(cache),
                    ..Default::default()
                }),
                None,
                Some("platform"),
            ),
            (
                Some(api::Model {
                    team: Some(team),
                    ..Default::default()
                }),
                Some("test-project"),
                None,
            ),
            (
                Some(api::Model {
                    team: Some(team),
                    ..Default::default()
                }),
                None,
                None,
            ),
            (
                Some(api::Model {
                    cache: Some(cache),
                    ..Default::default()
                }),
                None,
                None,
            ),
            (
                Some(api::Model {
                    project: Some(project().id),
                    ..Default::default()
                }),
                Some("test-project"),
                None,
            ),
        ] {
            let inserting = existing.is_none();
            let existing = existing.map(|row| api::Model {
                id: ApiId::now_v7(),
                owned_by: user().id,
                name: "automation".into(),
                key: if row.team.is_some() {
                    format!("team:{hash}")
                } else {
                    hash.clone()
                },
                expires_at: Some(now()),
                revoked_at: Some(now()),
                allowed_ips: Some(vec!["192.0.2.0/24".into()]),
                ..row
            });
            let db = mock_db()
                .append_query_results([vec![user()]])
                .append_query_results([vec![project()]])
                .append_query_results([existing.into_iter().collect::<Vec<_>>()])
                .append_query_results([vec![api::Model::default()]])
                .into_connection();
            apply(&db, &dir, pinned_project, pinned_team, &team_ids)
                .await
                .unwrap();

            let statements = all_statements(db.into_transaction_log());
            let writes: Vec<_> = statements
                .iter()
                .filter(|s| {
                    s.sql.starts_with("INSERT INTO \"api\"") || s.sql.starts_with("UPDATE \"api\"")
                })
                .collect();
            assert_eq!(
                writes.len(),
                1,
                "verifier and scope must change in one write"
            );
            let write = writes[0];
            let sql = write.to_string();
            let expected_verifier = if pinned_team.is_some() {
                format!("team:{hash}")
            } else {
                hash.clone()
            };
            if inserting {
                assert!(write.sql.contains("\"team\""), "{sql}");
                assert_eq!(
                    sql.contains(&team.to_string()),
                    pinned_team.is_some(),
                    "{sql}"
                );
                assert!(sql.contains(&format!("'{expected_verifier}'")), "{sql}");
                continue;
            }
            assert!(
                sql.contains(&format!("\"key\" = '{expected_verifier}'")),
                "{sql}"
            );
            let expected_team = pinned_team
                .map(|_| format!("'{team}'"))
                .unwrap_or_else(|| "NULL".into());
            let expected_project = pinned_project
                .map(|_| format!("'{}'", project().id))
                .unwrap_or_else(|| "NULL".into());
            assert!(
                sql.contains(&format!("\"team\" = {expected_team}")),
                "{sql}"
            );
            assert!(
                sql.contains(&format!("\"project\" = {expected_project}")),
                "{sql}"
            );
            assert_eq!(
                sql.contains("\"cache\" = NULL"),
                pinned_team.is_some(),
                "{sql}"
            );
            assert!(!sql.contains("\"cache\" = '"), "{sql}");
            for column in [
                "owned_by",
                "created_at",
                "last_used_at",
                "expires_at",
                "revoked_at",
                "allowed_ips",
            ] {
                assert!(!sql.contains(&format!("\"{column}\" = ")), "{sql}");
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn api_key_apply_rejects_invalid_team_scope_before_reading_credentials() {
        let team_ids = HashMap::from([("platform".to_string(), TeamId::now_v7())]);
        for (project_name, team_name, message) in [
            (Some("test-project"), Some("platform"), "not both"),
            (None, Some("ghost"), "Team 'ghost' not found"),
        ] {
            let db = mock_db()
                .append_query_results([vec![user()]])
                .append_query_results([vec![project()]])
                .into_connection();
            let error = apply(&db, Path::new(""), project_name, team_name, &team_ids)
                .await
                .unwrap_err();
            assert!(error.to_string().contains(message), "{error}");
        }
    }
}
