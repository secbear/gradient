/*
  SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>

  SPDX-License-Identifier: AGPL-3.0-only
*/

{ pkgs, ... }:
let
  inherit (pkgs) lib;
  system = pkgs.stdenv.hostPlatform.system;
  workerIds = {
    checked = "a0000000-0000-0000-0000-000000000001";
    unchecked = "a0000000-0000-0000-0000-000000000002";
  };
  workerToken = "C9ve6tvVONhtbRzFks56HQlYQotlRmXel/5NFLk/HjbSFGc+IZjCGfxegW2NKpY5";
  tool = pkgs.pkgsStatic.busybox;
  sshKey = pkgs.runCommand "worker-readiness-ssh-key" { nativeBuildInputs = [ pkgs.openssh ]; } ''
    mkdir -p "$out"
    ssh-keygen -q -t ed25519 -N "" -f "$out/key"
  '';
  flake = pkgs.replaceVars ./flake.nix {
    inherit system;
    busybox = toString tool;
  };
  startupCheck = pkgs.writeShellScript "worker-readiness-startup-check" ''
    if test -e /run/gradient-readiness/hold-startup; then
      printf held > /var/lib/gradient-worker/startup-held
      while test -e /run/gradient-readiness/hold-startup; do
        ${pkgs.coreutils}/bin/sleep 0.1
      done
    fi
  '';
  checks = pkgs.writeText "worker-readiness-checks.json" (
    builtins.toJSON [
      {
        name = "fixture-resource";
        command = [
          "${pkgs.coreutils}/bin/test"
          "-e"
          "/run/gradient-readiness/healthy"
        ];
        intervalSecs = 1;
        timeoutSecs = 1;
        failureThreshold = 1;
        successThreshold = 1;
      }
      {
        name = "fixture-startup";
        command = [ (toString startupCheck) ];
        intervalSecs = null;
        timeoutSecs = 30;
        failureThreshold = 1;
        successThreshold = 1;
      }
    ]
  );
  workerNode = name: { ... }: {
    imports = [ ../../../modules/gradient-worker ];
    services.gradient.worker = {
      enable = true;
      id = workerIds.${name};
      serverUrl = "ws://server:3000/proto";
      peersFile = "/etc/gradient/worker-peers";
      healthChecksFile = if name == "checked" then checks else null;
      system = {
        architectures = [ system ];
        features = if name == "checked" then [ "readiness-fixture" ] else [ ];
        cpuCoreScore = 111;
      };
      build = {
        maxConcurrent = 2;
        maxCores = 1;
      };
      eval.maxConcurrent = 1;
    };
    environment.etc."gradient/worker-peers" = {
      text = "*:${workerToken}";
      mode = "0600";
      user = "gradient-worker";
    };
    systemd.tmpfiles.rules = [
      "d /run/gradient-readiness 0755 root root -"
      "d /run/gradient-readiness-build 0777 root root -"
    ];
    nix.settings = {
      sandbox = true;
      extra-sandbox-paths = [ "/run/gradient-readiness-build" ];
      system-features = lib.optionals (name == "checked") [ "readiness-fixture" ];
      max-jobs = 2;
      cores = 1;
    };
  };
in
{
  value = pkgs.testers.runNixOSTest {
    name = "gradient-worker-readiness";
    globalTimeout = 1200;

    defaults = {
      documentation.enable = false;
      networking.firewall.enable = false;
      virtualisation = {
        cores = 2;
        memorySize = 2560;
        diskSize = 8192;
        writableStore = true;
        additionalPaths = [ tool ];
      };
      nix.settings = {
        experimental-features = [
          "nix-command"
          "flakes"
        ];
        substituters = lib.mkForce [ ];
      };
    };

    nodes = {
      server = { ... }: {
        imports = [ ../../../modules/gradient.nix ];
        environment.systemPackages = [
          pkgs.curl
          pkgs.git
        ];
        services.gradient = {
          enable = true;
          listenAddr = "0.0.0.0";
          frontend.enable = false;
          useTls = false;
          domain = "gradient.local";
          postgres.enable = true;
          proto = {
            public = true;
            workerHeartbeatTimeoutSecs = 120;
            assignmentReclaimGraceSecs = 120;
          };
          secrets = {
            jwtFile = toString (
              pkgs.writeText "jwt-secret" "b68a8eaa8ebcff23ebaba1bd74ecb8a2eb7ba959570ff8842f148207524c7b8d731d7a1998584105e951599221f9dcd20e41223be17275ca70ab6f7e6ecafa8d4"
            );
            cryptFile = toString (pkgs.writeText "crypt-secret" "aW52YWxpZC1pbnZhbGlkLWludmFsaWQK");
          };
          state = {
            users.admin = {
              email = "admin@example.com";
              password_file = toString (
                pkgs.writeText "admin-password" "$argon2id$v=19$m=4096,t=3,p=1$c29tZXNhbHQxMjM0NQ$hIKBEy9SOWlnAlcwUv2PLPBdsMkKhVlCyjTxaWIK+v4"
              );
              email_verified = true;
              superuser = true;
            };
            teams.readiness.members = [
              {
                user = "admin";
                role = "Admin";
              }
            ];
            projects.project = {
              created_by = "admin";
              private_key_file = "${sshKey}/key";
              teams = [
                {
                  team = "readiness";
                  users = false;
                  workers = true;
                }
              ];
            };
            workers = lib.mapAttrs (name: id: {
              worker_id = id;
              display_name = name;
              team = "readiness";
              token_file = toString (pkgs.writeText "worker-token" workerToken);
              created_by = "admin";
            }) workerIds;
            tasks = lib.genAttrs [ "long" "next" "plain" "control" "restart" "after-restart" ] (name: {
              project = "project";
              repository = "git://server/worker-readiness";
              wildcard = "packages.${system}.${name}";
              wait_for_workers = true;
              created_by = "admin";
              triggers = [
                {
                  type = "polling";
                  active = false;
                  config.interval_secs = 3600;
                }
              ];
            });
            caches.main = {
              projects = [ "project" ];
              upstream_caches = [ ];
              signing_key_file = toString (
                pkgs.writeText "cache-key" "22yRW7p/hxuPRWJh9pcfGH0oXPk2MFUuG0wIA1rfq1BvDbvMqzMZS+er/BE8ucbxNSG5KZ8B0ELO4TJal8mZlw=="
              );
              created_by = "admin";
            };
          };
        };
        services.gitDaemon = {
          enable = true;
          basePath = "/srv/git";
          exportAll = true;
        };
        systemd.tmpfiles.rules = [
          "d /srv/git 0755 root root -"
          "d /srv/git/worker-readiness 0755 root root -"
        ];
      };
      checked = workerNode "checked";
      unchecked = workerNode "unchecked";
    };

    testScript = ''
      import json
      SYSTEM = ${builtins.toJSON system}
      FLAKE = ${builtins.toJSON (toString flake)}
      WORKER_IDS = json.loads(${builtins.toJSON (builtins.toJSON workerIds)})
      ${builtins.readFile ./test.py}
    '';
  };
}
