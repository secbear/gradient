/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use crate::graph::walks::eval_closure_cte;
use crate::sql::StatusWrite;
use crate::status::TransitionChange;

use super::transitions::returned_transitions;
use gradient_entity::build::BuildStatus;
use gradient_types::DerivationId;
use sea_orm::{ConnectionTrait, DatabaseTransaction, DbErr, TransactionTrait, Value};

pub async fn substitute_created_shared_builds<C: ConnectionTrait>(
    db: &C,
    derivations: &[DerivationId],
) -> Result<Vec<TransitionChange>, DbErr> {
    let ids: Vec<uuid::Uuid> = derivations.iter().map(|d| d.into_inner()).collect();
    let rows = db
        .query_all_raw(SUBSTITUTE_CREATED_SHARED_BUILDS.bind([ids.into()]))
        .await?;

    Ok(returned_transitions(rows))
}

fn substitute_created_shared_builds_sql() -> String {
    format!(
        r#"
        UPDATE derivation_build AS db
        SET status = {substituted}, substituted = true,
            updated_at = (now() AT TIME ZONE 'UTC')
        WHERE db.status = {created} AND db.derivation = ANY($1::uuid[])
        RETURNING db.derivation, old.status AS from_status, db.status AS to_status
        "#,
        substituted = crate::sql::status::to(BuildStatus::SUBSTITUTE_CREATED),
        created = crate::sql::status::from(BuildStatus::SUBSTITUTE_CREATED),
    )
}

crate::sql_fn! {
    SUBSTITUTE_CREATED_SHARED_BUILDS = substitute_created_shared_builds_sql,
        params = [DerivationIds(64)],
        status_write = StatusWrite::build(BuildStatus::SUBSTITUTE_CREATED);
}

pub async fn repair_cached_shared_builds_for_eval<C>(
    db: &C,
    evaluation: gradient_types::EvaluationId,
) -> Result<Vec<TransitionChange>, DbErr>
where
    C: ConnectionTrait + TransactionTrait<Transaction = DatabaseTransaction>,
{
    let walk = crate::graph::walks::begin_walk(db, &REPAIR_CACHED_SHARED_BUILDS_FOR_EVAL).await?;
    let rows = walk
        .query_all_raw(
            REPAIR_CACHED_SHARED_BUILDS_FOR_EVAL.bind([Value::Uuid(Some(evaluation.into_inner()))]),
        )
        .await?;
    walk.commit().await?;

    Ok(returned_transitions(rows))
}

fn repair_cached_shared_builds_for_eval_sql() -> String {
    let cte = eval_closure_cte();
    let not_in_flight = crate::scheduling::assignment_record::no_open_assignment_predicate(
        &crate::scheduling::assignment_record::build_job_key_sql("db.id"),
    );
    format!(
        r#"
    {cte},
    complete_cached_builds AS MATERIALIZED (
        SELECT o.derivation
        FROM derivation_output o
        LEFT JOIN cached_path cp ON cp.hash = o.hash
        WHERE o.derivation IN (SELECT derivation FROM closure)
        GROUP BY o.derivation
        HAVING bool_and(cp.file_hash IS NOT NULL)
    )
    UPDATE derivation_build db
    SET status = CASE WHEN db.status IN ({terminal_success}) THEN db.status ELSE {completed} END,
        updated_at = (now() AT TIME ZONE 'UTC')
    WHERE db.derivation IN (SELECT derivation FROM closure)
      AND db.status NOT IN ({terminal_success})
      AND {not_in_flight}
      AND db.derivation IN (SELECT derivation FROM complete_cached_builds)
    RETURNING db.derivation, old.status AS from_status, db.status AS to_status
    "#,
        terminal_success = crate::sql::status::build_in(&BuildStatus::TERMINAL_SUCCESS),
        completed = crate::sql::status::to(BuildStatus::COMPLETE_CACHED),
    )
}

crate::sql_fn! {
    REPAIR_CACHED_SHARED_BUILDS_FOR_EVAL = repair_cached_shared_builds_for_eval_sql,
        params = [EvaluationId],
        tier = Walk,
        budget = crate::sql::Budget::walk().buffers(750_000)
            .because("the same whole-closure walk PROMOTE_CLOSURE_QUERY pays for, plus \
                      one aggregate scan of the evaluation's outputs and cached paths"),
        flags = [Walk],
        status_write = StatusWrite::build(BuildStatus::COMPLETE_CACHED);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitute_created_shared_builds_moves_only_created_rows() {
        let sql = substitute_created_shared_builds_sql()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(sql.contains(&format!(
            "SET status = {}",
            crate::sql::status::build(BuildStatus::Substituted)
        )));
        assert!(sql.contains(&format!(
            "db.status = {}",
            crate::sql::status::build(BuildStatus::Created)
        )));
        assert!(sql.contains(
            "RETURNING db.derivation, old.status AS from_status, db.status AS to_status"
        ));
    }

    #[test]
    fn the_cached_repair_leaves_a_shared_build_whose_assignment_row_is_open() {
        let norm = |s: String| s.split_whitespace().collect::<Vec<_>>().join(" ");
        let gate = norm(
            crate::scheduling::assignment_record::no_open_assignment_predicate(
                &crate::scheduling::assignment_record::build_job_key_sql("db.id"),
            ),
        );

        assert!(norm(repair_cached_shared_builds_for_eval_sql()).contains(&gate));
    }

    #[tokio::test]
    async fn cached_repair_requires_every_output_and_preserves_scope_and_assignments() {
        use gradient_fixtures::shape::shape;
        use gradient_types::EvaluationId;

        let (evaluation, other) = (EvaluationId::now_v7(), EvaluationId::now_v7());
        let complete = "00000000000000000000000000000000-complete.drv";
        let partial = "11111111111111111111111111111111-partial.drv";
        let empty = "22222222222222222222222222222222-empty.drv";
        let remote = "33333333333333333333333333333333-remote.drv";
        let assigned = "44444444444444444444444444444444-assigned.drv";
        let terminal = "55555555555555555555555555555555-terminal.drv";
        let outside = "66666666666666666666666666666666-outside.drv";
        let db = shape()
            .evaluation(evaluation)
            .derivation(complete)
            .job()
            .output("00000000000000000000000000000001")
            .cached_output(complete)
            .named_output("00000000000000000000000000000002", "dev")
            .cached_path("00000000000000000000000000000002", "complete-dev")
            .derivation(partial)
            .job()
            .output("11111111111111111111111111111112")
            .cached_output(partial)
            .named_output("11111111111111111111111111111113", "dev")
            .derivation(empty)
            .job()
            .derivation(remote)
            .job()
            .output("33333333333333333333333333333334")
            .cache_available()
            .derivation(assigned)
            .job()
            .output("44444444444444444444444444444445")
            .cached_output(assigned)
            .open_assignment()
            .derivation(terminal)
            .job()
            .output("55555555555555555555555555555556")
            .cached_output(terminal)
            .build_status(BuildStatus::Substituted)
            .evaluation(other)
            .derivation(outside)
            .job()
            .output("66666666666666666666666666666667")
            .cached_output(outside)
            .into_database()
            .await;

        let changed = repair_cached_shared_builds_for_eval(&db.connection(), evaluation)
            .await
            .expect("cached outputs are repaired");
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].derivation, db.id_of(complete).await);
        for (path, expected) in [
            (complete, BuildStatus::Completed),
            (partial, BuildStatus::Created),
            (empty, BuildStatus::Created),
            (remote, BuildStatus::Created),
            (assigned, BuildStatus::Created),
            (terminal, BuildStatus::Substituted),
            (outside, BuildStatus::Created),
        ] {
            assert_eq!(db.shared_build(path).await.status, expected, "{path}");
        }
        db.finish().await;
    }
}
