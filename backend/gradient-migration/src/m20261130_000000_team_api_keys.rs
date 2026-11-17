/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_orm::ConnectionTrait;

const UP: &str = "ALTER TABLE public.api \
    ADD COLUMN team uuid REFERENCES public.team(id) ON UPDATE CASCADE ON DELETE CASCADE, \
    ADD CONSTRAINT api_team_scope_exclusive CHECK \
        (team IS NULL OR (project IS NULL AND cache IS NULL)), \
    ADD CONSTRAINT api_team_verifier_scope CHECK \
        ((team IS NULL AND key NOT LIKE 'team:%') OR \
         (team IS NOT NULL AND key ~ '^team:[0-9a-f]{64}$')); \
    CREATE INDEX api_team_idx ON public.api (team) WHERE team IS NOT NULL; \
    CREATE UNIQUE INDEX api_team_verifier_unique ON public.api (key) WHERE team IS NOT NULL;";

const DOWN: &str = "DELETE FROM public.api WHERE team IS NOT NULL; \
    ALTER TABLE public.api DROP CONSTRAINT api_team_scope_exclusive, \
        DROP CONSTRAINT api_team_verifier_scope, DROP COLUMN team;";

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.get_connection().execute_unprepared(UP).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.get_connection().execute_unprepared(DOWN).await?;
        Ok(())
    }
}
