/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use gradient_types::*;
use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, QuerySelect};
use std::collections::BTreeSet;

pub async fn of_derivation<C: ConnectionTrait>(
    db: &C,
    derivation: DerivationId,
) -> Result<Option<MDerivationBuild>, DbErr> {
    EDerivationBuild::find()
        .filter(CDerivationBuild::Derivation.eq(derivation))
        .one(db)
        .await
}

pub async fn of_derivations<C: ConnectionTrait>(
    db: &C,
    derivations: &[DerivationId],
) -> Result<Vec<MDerivationBuild>, DbErr> {
    let unique: Vec<_> = derivations
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    crate::fetch_in_chunks(&unique, |chunk| async move {
        EDerivationBuild::find()
            .filter(CDerivationBuild::Derivation.is_in(chunk))
            .all(db)
            .await
    })
    .await
}

pub async fn by_derivation<C: ConnectionTrait>(
    db: &C,
    derivation: DerivationId,
) -> Result<Option<MDerivationBuild>, DbErr> {
    EDerivationBuild::find()
        .filter(CDerivationBuild::Derivation.eq(derivation))
        .one(db)
        .await
}

pub async fn derivations_of<C: ConnectionTrait>(
    db: &C,
    derivation_builds: &[DerivationBuildId],
) -> Result<Vec<(DerivationId, DerivationBuildId)>, DbErr> {
    crate::fetch_in_chunks(derivation_builds, |chunk| async move {
        EDerivationBuild::find()
            .select_only()
            .column(CDerivationBuild::Derivation)
            .column(CDerivationBuild::Id)
            .filter(CDerivationBuild::Id.is_in(chunk))
            .into_tuple()
            .all(db)
            .await
    })
    .await
}

pub async fn first_with_status_among<C: ConnectionTrait>(
    db: &C,
    derivations: Vec<DerivationId>,
    statuses: Vec<gradient_entity::build::BuildStatus>,
) -> Result<Option<MDerivationBuild>, DbErr> {
    EDerivationBuild::find()
        .filter(CDerivationBuild::Derivation.is_in(derivations))
        .filter(CDerivationBuild::Status.is_in(statuses))
        .one(db)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IN_CHUNK_SIZE;
    use gradient_fixtures::mock::{mock_db, raw_statements};

    #[tokio::test]
    async fn a_large_derivation_list_is_chunked_and_duplicate_ids_are_queried_once() {
        let ids: Vec<_> = (1..=IN_CHUNK_SIZE + 1)
            .map(|id| DerivationId::new(uuid::Uuid::from_u128(id as u128)))
            .collect();
        let first = MDerivationBuild {
            derivation: ids[0],
            ..Default::default()
        };
        let last = MDerivationBuild {
            derivation: ids[IN_CHUNK_SIZE],
            ..Default::default()
        };
        let db = mock_db()
            .append_query_results([vec![first.clone()], vec![last.clone()]])
            .into_connection();
        let mut requested = ids.clone();
        requested.extend_from_slice(&ids);

        let found = of_derivations(&db, &requested)
            .await
            .expect("shared builds");

        assert_eq!(found, vec![first, last]);
        let statements = raw_statements(db.into_transaction_log());
        assert_eq!(statements.len(), 2);
        assert_eq!(
            statements[0].values.as_ref().expect("bound IDs").0.len(),
            IN_CHUNK_SIZE
        );
        assert_eq!(statements[1].values.as_ref().expect("bound IDs").0.len(), 1);
    }

    #[tokio::test]
    async fn no_derivations_runs_no_query() {
        let db = mock_db().into_connection();
        assert!(
            of_derivations(&db, &[])
                .await
                .expect("empty list")
                .is_empty()
        );
        assert!(raw_statements(db.into_transaction_log()).is_empty());
    }
}
