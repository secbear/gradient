/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

{ pkgs }: let
  configure = worker: (pkgs.nixos {
    imports = [ ../modules/gradient-worker ];
    system.stateVersion = "26.05";
    services.gradient.worker = {
      enable = true;
      serverUrl = "ws://127.0.0.1:3000/proto";
    } // worker;
  }).config;
  legacy = configure { };
  checked = configure { healthChecksFile = "/run/worker-checks.json"; };
  overridden = configure {
    healthChecksFile = "/run/worker-checks.json";
    environmentVariables.GRADIENT_WORKER_HEALTH_CHECKS_FILE = "/run/other-checks.json";
  };
  option = builtins.head (builtins.filter (spec: spec.field == "health_checks_file")
    (builtins.fromJSON (builtins.readFile ../../backend/gradient-worker/options.json)));
in
assert option.env == "GRADIENT_WORKER_HEALTH_CHECKS_FILE";
assert option.default == null;
assert legacy.services.gradient.worker.healthChecksFile == null;
assert !(legacy.systemd.services.gradient-worker.environment ? GRADIENT_WORKER_HEALTH_CHECKS_FILE);
assert checked.systemd.services.gradient-worker.environment.GRADIENT_WORKER_HEALTH_CHECKS_FILE
  == "%d/gradient_worker_health_checks";
assert builtins.elem "gradient_worker_health_checks:/run/worker-checks.json"
  checked.systemd.services.gradient-worker.serviceConfig.LoadCredential;
assert overridden.systemd.services.gradient-worker.environment.GRADIENT_WORKER_HEALTH_CHECKS_FILE
  == "/run/other-checks.json";
pkgs.runCommand "gradient-worker-readiness-options" { } ''
  touch $out
''
