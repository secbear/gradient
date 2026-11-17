/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

#[cfg(test)]
mod tests {
    use gradient_entity::ids::EvaluationId;
    use gradient_fixtures::{shape::shape, test_date};

    use crate::extract::{create_table, export_tables, redact_row, write_rows};
    use crate::logs::{create_log_table, insert_log};
    use crate::redact::Redactor;
    use crate::schema::{ReportOptions, open_report};
    use crate::tables::{Row, TableSpec, eval_scope_tables};

    const REPO: &str = "git@git.supersandro.de:sandro/nixos-config.git";
    const EMAIL: &str = "sandro@example.invalid";
    const PKG: &str = "clap_complete-4.6.9";
    const STORE_PATH: &str = "/nix/store/2s7ijz3qblblfb903r4spy3pvd7ag35f-clap_complete-4.6.9";

    fn spec(name: &str) -> &'static TableSpec {
        eval_scope_tables()
            .iter()
            .find(|s| s.name == name)
            .expect("spec exists")
    }

    fn row_with(spec: &TableSpec, values: &[(&str, &str)]) -> Row {
        let mut row = vec![None; spec.columns.len()];
        for (column, value) in values {
            let index = spec
                .columns
                .iter()
                .position(|c| c == column)
                .unwrap_or_else(|| panic!("{} has no column {column}", spec.name));
            row[index] = Some((*value).to_owned());
        }
        row
    }

    fn write_seeded_report(conn: &rusqlite::Connection, redactor: &Redactor) {
        let seeds: [(&str, Vec<(&str, &str)>); 5] = [
            (
                "evaluation",
                vec![
                    ("id", "01a05a38-3276-7252-bc05-c139d9c8a015"),
                    ("repository", REPO),
                    ("flake_source", REPO),
                    ("started_by", EMAIL),
                    ("status", "7"),
                ],
            ),
            (
                "derivation",
                vec![("name", PKG), ("pname", "clap_complete")],
            ),
            (
                "derivation_output",
                vec![
                    ("package", PKG),
                    ("deriver", STORE_PATH),
                    ("references_list", STORE_PATH),
                ],
            ),
            (
                "cached_path",
                vec![("package", PKG), ("deriver", STORE_PATH)],
            ),
            (
                "derivation_metric",
                vec![("input_pnames", "clap_complete,libgit2")],
            ),
        ];

        for (name, values) in seeds {
            let spec = spec(name);
            create_table(conn, spec).expect("ddl");
            let row = row_with(spec, &values);
            write_rows(conn, spec, &[redact_row(spec, redactor, &row)]).expect("write");
        }

        create_log_table(conn).expect("log ddl");
        insert_log(
            conn,
            redactor,
            "0199-attempt",
            &format!("error: while building {STORE_PATH} for {REPO}"),
        )
        .expect("log");
    }

    #[test]
    fn resource_measurements_survive_export_and_missing_values_stay_null() {
        #[derive(Debug, Default, PartialEq)]
        struct Measurements {
            cpu_time_ms: Option<i64>,
            avg_cpu_pct: Option<f64>,
            disk_read_bytes: Option<i64>,
            disk_write_bytes: Option<i64>,
        }

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async {
                let evaluation = EvaluationId::now_v7();
                let db = shape()
                    .evaluation(evaluation)
                    .evaluation_option(|e| {
                        e.created_at = test_date();
                        e.finished_at = Some(test_date());
                    })
                    .derivation("/nix/store/dddddddddddddddddddddddddddddddd-hello.drv")
                    .job()
                    .derivation_metric(|m| {
                        m.cpu_time_ms = Some(1234);
                        m.avg_cpu_pct = Some(42.5);
                        m.disk_read_bytes = Some(1048576);
                        m.disk_write_bytes = Some(2097152);
                    })
                    .derivation_metric(|_| {})
                    .into_database()
                    .await;
                let conn = rusqlite::Connection::open_in_memory().expect("report");
                export_tables(
                    &db.connection(),
                    &conn,
                    std::slice::from_ref(spec("derivation_metric")),
                    evaluation.into_inner(),
                    &Redactor::new(opts(false, false)),
                )
                .await
                .expect("export measurements");
                let rows: Vec<Measurements> = conn
                    .prepare("SELECT cpu_time_ms, avg_cpu_pct, disk_read_bytes, disk_write_bytes FROM derivation_metric ORDER BY cpu_time_ms DESC")
                    .expect("resource query")
                    .query_map([], |row| Ok(Measurements {
                        cpu_time_ms: row.get(0)?,
                        avg_cpu_pct: row.get(1)?,
                        disk_read_bytes: row.get(2)?,
                        disk_write_bytes: row.get(3)?,
                    }))
                    .expect("rows")
                    .collect::<Result<_, _>>()
                    .expect("resource values");
                assert_eq!(
                    rows,
                    vec![
                        Measurements {
                            cpu_time_ms: Some(1234),
                            avg_cpu_pct: Some(42.5),
                            disk_read_bytes: Some(1048576),
                            disk_write_bytes: Some(2097152),
                        },
                        Measurements::default(),
                    ]
                );
                db.finish().await;
            });
    }

    /// `SELECT *` is correct only here. The test must read whatever the file contains, the opposite
    /// of the extractor's rule.
    fn count_occurrences(conn: &rusqlite::Connection, needle: &str) -> usize {
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .and_then(|mut s| s.query_map([], |r| r.get(0)).and_then(|m| m.collect()))
            .expect("tables");

        let mut hits = 0;
        for table in tables {
            let mut stmt = conn
                .prepare(&format!("SELECT * FROM \"{table}\""))
                .expect("select");
            let columns = stmt.column_count();
            let mut rows = stmt.query([]).expect("query");
            while let Some(row) = rows.next().expect("row") {
                for i in 0..columns {
                    if let Ok(text) = row.get::<_, String>(i)
                        && text.contains(needle)
                    {
                        hits += 1;
                    }
                }
            }
        }

        hits
    }

    fn opts(identities: bool, packages: bool) -> ReportOptions {
        ReportOptions {
            anonymize_identities: identities,
            anonymize_packages: packages,
            include_logs: true,
            include_instance: true,
            include_exec: true,
        }
    }

    #[test]
    fn an_anonymized_report_contains_no_original_identifier() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("r.db");
        let conn = open_report(&path).expect("open");
        write_seeded_report(&conn, &Redactor::new(opts(true, true)));
        drop(conn);

        let conn = rusqlite::Connection::open(&path).expect("reopen");
        for needle in [
            REPO,
            EMAIL,
            PKG,
            "supersandro",
            "nixos-config",
            "clap_complete",
            "libgit2",
        ] {
            assert_eq!(
                count_occurrences(&conn, needle),
                0,
                "anonymized report still contains {needle}"
            );
        }
    }

    /// The hash half is one-way. It is enabling an upstream cache check, and full anonymisation
    /// must keep it.
    #[test]
    fn store_path_hashes_survive_full_anonymization() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("r.db");
        let conn = open_report(&path).expect("open");
        write_seeded_report(&conn, &Redactor::new(opts(true, true)));
        drop(conn);

        let conn = rusqlite::Connection::open(&path).expect("reopen");
        assert!(
            count_occurrences(&conn, "2s7ijz3qblblfb903r4spy3pvd7ag35f") > 0,
            "the store hash must survive: it is what a cache check needs"
        );
    }

    #[test]
    fn an_unanonymized_report_keeps_every_identifier() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("r.db");
        let conn = open_report(&path).expect("open");
        write_seeded_report(&conn, &Redactor::new(opts(false, false)));
        drop(conn);

        let conn = rusqlite::Connection::open(&path).expect("reopen");
        assert!(count_occurrences(&conn, REPO) > 0);
        assert!(count_occurrences(&conn, PKG) > 0);
        assert!(count_occurrences(&conn, EMAIL) > 0);
    }

    #[test]
    fn packages_can_be_kept_while_identities_are_hidden() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("r.db");
        let conn = open_report(&path).expect("open");
        write_seeded_report(&conn, &Redactor::new(opts(true, false)));
        drop(conn);

        let conn = rusqlite::Connection::open(&path).expect("reopen");
        assert_eq!(count_occurrences(&conn, REPO), 0);
        assert_eq!(count_occurrences(&conn, EMAIL), 0);
        assert!(
            count_occurrences(&conn, PKG) > 0,
            "knowing which package broke is the point of the separate toggle"
        );
    }
}
