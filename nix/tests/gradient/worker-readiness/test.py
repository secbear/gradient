# SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
# SPDX-License-Identifier: AGPL-3.0-only

import shlex
import time
from uuid import UUID

API = "http://127.0.0.1:3000/api/v1"
TOKEN = None


def api(method, path, body=None):
    args = ["curl", "-fsS", "--max-time", "10", "-X", method, f"{API}/{path}"]
    if TOKEN:
        args += ["-H", f"Authorization: Bearer {TOKEN}"]
    if body is not None:
        args += ["-H", "Content-Type: application/json", "--data", json.dumps(body)]
    response = json.loads(server.succeed(shlex.join(args)))
    assert response["error"] is False, response
    return response["message"]


def login():
    global TOKEN
    TOKEN = api("POST", "auth/basic/login", {
        "loginname": "admin", "password": "admin_password",
    })


def sql(query):
    return server.succeed(shlex.join([
        "runuser", "-u", "postgres", "--", "psql", "-X", "-At",
        "-v", "ON_ERROR_STOP=1", "-d", "gradient", "-c", query,
    ])).strip()


def poll(predicate, message, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    print(server.succeed("journalctl -u gradient-server --no-pager -n 100"))
    for node in (checked, unchecked):
        print(node.succeed("journalctl -u gradient-worker --no-pager -n 80"))
    raise AssertionError(message)


def workers():
    return {entry["worker_id"]: entry for entry in api("GET", "teams/readiness/workers")}


def checked_entry():
    return workers()[WORKER_IDS["checked"]]


def ready(want):
    entry = checked_entry()
    health = entry.get("live", {}).get("health")
    return entry["connected"] and health is not None and health["ready"] is want


def failed_report_since(observed_at):
    health = checked_entry().get("live", {}).get("health", {})
    return (
        ready(False)
        and health.get("observed_at", "") > observed_at
        and any(
            check["name"] == "fixture-resource"
            and check["status"] == "failed"
            and check["healthy"] is False
            for check in health.get("checks", [])
        )
    )


def connection_id():
    return sql(
        "SELECT id FROM worker_connection "
        f"WHERE worker_id = '{WORKER_IDS['checked']}' AND disconnected_at IS NULL"
    )


def evaluate(task):
    evaluation = str(UUID(api("POST", f"tasks/project/{task}/evaluate", {})))
    return evaluation


def complete(evaluation):
    def completed():
        state = api("GET", f"evals/{evaluation}")["status"]
        assert state not in ("Failed", "Aborted"), (evaluation, state)
        return state == "Completed"
    poll(completed, f"evaluation {evaluation} did not complete")


def dispatch_workers(evaluation):
    return sql(
        "SELECT worker_id FROM dispatched_job "
        f"WHERE evaluation_id = '{evaluation}' AND kind = 1 ORDER BY created_at"
    ).splitlines()


def assessment():
    return api("POST", "teams/readiness/workers/capacity-assessment", {
        "profiles": [],
        "tracked_profiles": [{
            "enable_network": True,
            "enable_build": True,
            "profile": {
                "name": "checked",
                "worker_id": WORKER_IDS["checked"],
                "capabilities": {
                    "core": False, "federate": False, "cache": False,
                    "build": True, "eval": True, "network": True,
                    "secrets": False, "exec": False,
                },
                "resources": {
                    "architectures": [SYSTEM],
                    "system_features": ["readiness-fixture"],
                    "max_concurrent_builds": 2,
                    "cpu_count": 2,
                    "cpu_core_score": 111,
                    "ram_total_mb": 1024,
                    "zone": None, "endpoint": None, "build_cores": 1, "exec": None,
                },
            },
        }],
    })


def unusable():
    snapshot = assessment()
    tracked = snapshot["tracked_workers"][0]
    return (
        tracked["connected"]
        and tracked["worker_index"] is None
        and snapshot["total_ready_builds"] > 0
        and all(build["tracked_workers"][0] is None for build in snapshot["builds"])
    )


def usable():
    tracked = assessment()["tracked_workers"][0]
    return tracked["connected"] and tracked["worker_index"] is not None


start_all()
server.wait_for_unit("gradient-server.service")
server.wait_for_open_port(3000)
server.wait_for_unit("git-daemon.service")
for node in (checked, unchecked):
    node.wait_for_unit("gradient-worker.service")

with subtest("Publish a self-contained flake"):
    server.succeed(f"install -m 0644 {shlex.quote(FLAKE)} /srv/git/worker-readiness/flake.nix")
    server.succeed("git -C /srv/git/worker-readiness init -b main")
    server.succeed("git -C /srv/git/worker-readiness add flake.nix")
    server.succeed("nix flake lock /srv/git/worker-readiness")
    server.succeed(
        "git -C /srv/git/worker-readiness add flake.nix flake.lock "
        "&& git -C /srv/git/worker-readiness -c user.name=test "
        "-c user.email=test@example.com commit -m fixture"
    )
    server.succeed("chown -R git:git /srv/git/worker-readiness")
    login()
    poll(lambda: len(workers()) == 2 and all(
        entry["connected"] and entry.get("live") is not None for entry in workers().values()
    ), "both real workers did not connect")
    assert "health" not in workers()[WORKER_IDS["unchecked"]]["live"]
    poll(lambda: ready(False), "required check did not block startup")

with subtest("Startup failure blocks only eligible checked work"):
    long_eval = evaluate("long")
    poll(unusable, "unready worker supplied usable capacity or starting credit")
    assert dispatch_workers(long_eval) == []
    plain_eval = evaluate("plain")
    complete(plain_eval)
    assert dispatch_workers(plain_eval) == [WORKER_IDS["unchecked"]]
    assert dispatch_workers(long_eval) == []
    checked.succeed("test ! -e /run/gradient-readiness-build/long-running")

with subtest("Recovery starts the queued build"):
    checked.succeed("touch /run/gradient-readiness/healthy")
    poll(lambda: ready(True) and usable(), "checked worker did not recover usable capacity")
    checked.wait_until_succeeds("test -f /run/gradient-readiness-build/long-running")
    assert dispatch_workers(long_eval) == [WORKER_IDS["checked"]]
    snapshot = assessment()
    index = snapshot["tracked_workers"][0]["worker_index"]
    assert index is not None and snapshot["workers"][index]["free_slots"] == 1
    control_eval = evaluate("control")
    complete(control_eval)
    assert dispatch_workers(control_eval) == [WORKER_IDS["checked"]]
    checked.succeed("test ! -e /run/gradient-readiness-build/release-long")

with subtest("Check failure preserves running work and blocks new work"):
    observed_at = checked_entry()["live"]["health"]["observed_at"]
    checked.succeed("rm /run/gradient-readiness/healthy")
    poll(lambda: failed_report_since(observed_at), "periodic failure did not produce a new failed report")
    next_eval = evaluate("next")
    poll(unusable, "unhealthy worker still supplied new-build capacity")
    assert dispatch_workers(next_eval) == []
    assert checked_entry()["live"]["assigned_job_count"] == 1
    checked.succeed("touch /run/gradient-readiness-build/release-long")
    complete(long_eval)
    assert dispatch_workers(next_eval) == []
    checked.succeed("test $(wc -l < /run/gradient-readiness-build/long-starts) -eq 1")
    checked.succeed("touch /run/gradient-readiness/healthy")
    poll(lambda: ready(True), "worker did not recover after periodic failure")
    complete(next_eval)
    assert dispatch_workers(next_eval) == [WORKER_IDS["checked"]]

with subtest("A new worker session cannot retain previous readiness"):
    old_connection = str(UUID(connection_id()))
    checked.systemctl("stop gradient-worker.service")
    checked.succeed("touch /run/gradient-readiness/hold-startup")
    checked.systemctl("start gradient-worker.service")
    checked.wait_until_succeeds("test -f /var/lib/gradient-worker/startup-held")

    def new_session_pending():
        entry = checked_entry()
        health = entry.get("live", {}).get("health", {})
        return (
            connection_id() not in ("", old_connection)
            and entry["connected"]
            and health.get("ready") is False
            and any(
                check["name"] == "fixture-startup"
                and check["status"] == "pending"
                and check["healthy"] is False
                for check in health.get("checks", [])
            )
        )

    poll(new_session_pending, "a new connection did not report its pending startup check", timeout=20)
    checked.succeed("test -e /run/gradient-readiness/healthy")
    checked.succeed("rm /run/gradient-readiness/hold-startup")
    poll(lambda: ready(True), "reconnected worker did not recover")

with subtest("Health expires before connection liveness"):
    checked.succeed("systemctl kill --signal=STOP gradient-worker.service")
    try:
        poll(lambda: ready(False), "server did not expire an unrefreshed health report", timeout=20)
        assert checked_entry()["connected"]
        snapshot = assessment()
        assert snapshot["tracked_workers"][0]["connected"]
        assert snapshot["tracked_workers"][0]["worker_index"] is None
    finally:
        checked.succeed("systemctl kill --signal=CONT gradient-worker.service")
    poll(lambda: ready(True), "fresh reports did not restore readiness")

with subtest("An unhealthy worker reclaims active work after server restart"):
    restart_eval = evaluate("restart")
    checked.wait_until_succeeds("test -f /run/gradient-readiness-build/restart-running")
    observed_at = checked_entry()["live"]["health"]["observed_at"]
    checked.succeed("rm /run/gradient-readiness/healthy")
    poll(lambda: failed_report_since(observed_at), "restart fixture did not publish a failed check")
    after_restart_eval = evaluate("after-restart")
    poll(unusable, "restart fixture had no blocked runnable build")
    server.systemctl("restart gradient-server.service")
    server.wait_for_unit("gradient-server.service")
    server.wait_for_open_port(3000)
    TOKEN = None
    login()
    poll(lambda: ready(False) and checked_entry()["live"]["assigned_job_count"] == 1,
         "server restart did not reclaim the unhealthy worker's active build")
    assert dispatch_workers(after_restart_eval) == []
    checked.succeed("test $(wc -l < /run/gradient-readiness-build/restart-starts) -eq 1")
    checked.succeed("touch /run/gradient-readiness-build/release-restart")
    complete(restart_eval)
    assert dispatch_workers(restart_eval) == [WORKER_IDS["checked"]]
    checked.succeed("test $(wc -l < /run/gradient-readiness-build/restart-starts) -eq 1")
    assert dispatch_workers(after_restart_eval) == []
    checked.succeed("touch /run/gradient-readiness/healthy")
    poll(lambda: ready(True), "unhealthy reclaimed worker did not recover")
    complete(after_restart_eval)
    assert dispatch_workers(after_restart_eval) == [WORKER_IDS["checked"]]
