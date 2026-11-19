/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use gradient_wire::codec::agreement::version_frame;
use gradient_wire::codec::{from_bytes, to_bytes};
use gradient_wire::messages::ClientMessage;
use gradient_wire::types::{
    GradientCapabilities, QueryMode, WorkerCheckStatus, WorkerHealth, WorkerHealthCheck,
};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn assert_pinned(message: ClientMessage, expected: &str) {
    let bytes = to_bytes(&message, 30).expect("encodes");
    assert_eq!(hex(&bytes), expected);
    assert_eq!(from_bytes::<ClientMessage>(bytes, 30), Ok(message));
}

#[test]
fn a_cache_query_keeps_its_baseline_bytes() {
    assert_pinned(
        ClientMessage::CacheQuery {
            job_id: "j".into(),
            query_id: "q".into(),
            paths: vec!["/a".into(), "/b".into()],
            mode: QueryMode::Pull,
            nar_sizes: vec![None, Some(300)],
            external: true,
        },
        "15016a017102022f61022f6201020001ac0201",
    );
}

#[test]
fn worker_metrics_keep_their_baseline_bytes() {
    assert_pinned(
        ClientMessage::WorkerMetrics {
            cpu_usage_pct: 1.5,
            ram_free_mb: 128,
            disk_speed_mbps: Some(-2.0),
            upload_speed_mbps: None,
            download_speed_mbps: None,
            build_peak_ram_mb: None,
            lifetime_left_secs: None,
            exec: None,
        },
        "050000c03f800101000000c00000",
    );
}

#[test]
fn the_version_frame_keeps_its_bytes() {
    assert_eq!(hex(&version_frame(&(27..=27))), "475241441b001b00");
}

#[test]
fn readiness_defaults_to_false_on_every_legacy_version() {
    let init = |readiness_required| ClientMessage::InitConnection {
        capabilities: GradientCapabilities::default(),
        id: "w".into(),
        readiness_required,
    };
    for version in 41..=60 {
        let bytes = to_bytes(&init(true), version).unwrap();
        assert_eq!(hex(&bytes), "0000000000000000000177");
        assert_eq!(
            from_bytes::<ClientMessage>(bytes, version).unwrap(),
            init(false)
        );
    }
    for required in [false, true] {
        let sent = init(required);
        let bytes = to_bytes(&sent, 61).unwrap();
        assert_eq!(
            hex(&bytes),
            if required {
                "000000000000000000017701"
            } else {
                "000000000000000000017700"
            }
        );
        assert_eq!(from_bytes::<ClientMessage>(bytes, 61).unwrap(), sent);
    }
}

#[test]
fn health_requires_protocol_61_and_keeps_its_bytes() {
    let message = ClientMessage::WorkerHealth {
        health: WorkerHealth {
            checks: vec![WorkerHealthCheck {
                name: "hook".into(),
                status: WorkerCheckStatus::TimedOut,
                healthy: false,
            }],
            valid_for_secs: 60,
        },
    };
    assert!(to_bytes(&message, 60).is_err());
    let bytes = to_bytes(&message, 61).unwrap();
    assert_eq!(hex(&bytes), "280104686f6f6b03003c");
    assert!(from_bytes::<ClientMessage>(bytes.clone(), 60).is_err());
    assert_eq!(from_bytes::<ClientMessage>(bytes, 61).unwrap(), message);
    assert_eq!(
        hex(&version_frame(&gradient_wire::PROTO_VERSIONS)),
        "4752414429003d00"
    );
}
