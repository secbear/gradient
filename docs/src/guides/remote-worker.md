<!--
SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
SPDX-License-Identifier: AGPL-3.0-only
-->

# Add a Remote Worker

A remote worker is an extra machine that can evaluate and build for a project. It can run alongside the local worker or take over from it.

**Requirements:**

- A Mac with [nix-darwin](https://github.com/nix-darwin/nix-darwin) installed, or a NixOS machine with network access to the Gradient server
- A project with a cache subscription, see [First Project](../get-started/first-project.md)

## 1. Pick a Worker ID

A worker ID is a UUID. Generate one on the worker machine.

=== "NixOS"

    ```sh
    cat /proc/sys/kernel/random/uuid
    ```

=== "macOS"

    ```sh
    uuidgen | tr A-F a-f
    ```

## 2. Register the Worker

=== "UI"

    In the project, go to **Settings -> Workers -> Register Worker**. Provide a name and the worker ID from step 1.

    The dialog then shows a **Peers file entry** once. Save that entry as a secret on the worker machine, for example at `/run/secrets/gradient-worker-peers`.

=== "Declarative"

    Configure the server with:

    ```nix
    services.gradient.state = {
      projects.acme.id = "<project uuid>"; # (1)!
      workers.builder-1 = {
        worker_id = "<worker id from step 1>";
        projects = [ "acme" ];
        token_file = "/run/secrets/builder-1-token"; # (2)!
      };
    };
    ```

    1.  The peers file for the worker references this ID. Add the project to the peers file before the server's first start.
    2.  Generate the token with `openssl rand -base64 48`. Put the same token in the worker's peers file.

    On the worker machine, write the peers file entry as `<project uuid>:<token>`.

## 3. Open the Server to Workers

Remote workers use a WebSocket at `/proto` to reach the server. The bundled nginx forwards `/proto` only when `proto.public` is enabled.

```nix
# configuration.nix on the server
services.gradient.proto.public = true;
```

Without this setting, the local worker can still connect on the loopback address.

## 4. Configure the Machine

=== "NixOS"

    ```nix
    # configuration.nix on the worker machine
    {
      imports = [ inputs.gradient.nixosModules.default ];

      services.gradient.worker = {
        enable = true;
        id = "<worker id from step 1>";
        serverUrl = "wss://gradient.example.com/proto";
        peersFile = "/run/secrets/gradient-worker-peers"; # (1)!
        build.metrics = true; # (2)!
      };
    }
    ```

    1.  This file holds one line per project on this worker. See [Peers File](#peers-file).
    2.  Optional. With this enabled, the worker records memory and CPU usage for each build. The scheduler can use those numbers to place heavy builds on machines with matching capacity.

=== "macOS"

    ```nix
    # darwin-configuration.nix on the worker machine
    {
      imports = [ inputs.gradient.darwinModules.default ];

      services.gradient.worker = {
        enable = true;
        id = "<worker id from step 1>";
        serverUrl = "wss://gradient.example.com/proto";
        peersFile = "/var/lib/gradient-worker/peers"; # (1)!
      };
    }
    ```

    1.  Give the `_gradient-worker` user read access to the file. The `owner = "_gradient-worker"` setting in sops-nix or agenix can assign the owner. The worker can open the file directly. No systemd credentials exist on macOS.

## Required Readiness Checks

Define checks for resources that your worker needs.
Set `services.gradient.worker.healthChecksFile` to a JSON file.
Outside the Nix module, use `--health-checks-file` or `GRADIENT_WORKER_HEALTH_CHECKS_FILE`.
Configured checks require server protocol 61 or newer.

```json
[
  {
    "name": "device-access",
    "command": ["/run/current-system/sw/bin/test", "-r", "/dev/kvm"],
    "timeoutSecs": 5,
    "intervalSecs": 30,
    "failureThreshold": 1,
    "successThreshold": 1
  }
]
```

All checks must pass before the worker accepts new jobs.
A failed check blocks new jobs but does not abort running jobs. New jobs resume after recovery.
Commands run as the worker user, without an implicit shell. The worker discards their output.
The four numeric fields above show their defaults.
Set `intervalSecs` to `null` to retry a startup check until it passes, then stop repeating it.
Timeouts range from 1 to 300 seconds, intervals from 1 to 3600 seconds, and thresholds from 1 to 100 consecutive results.
The file accepts up to 32 checks with unique names of 1 to 80 printable ASCII characters. Empty commands and unknown fields are rejected.
On NixOS, the worker reads the file through a systemd credential. On macOS, give `_gradient-worker` read access to the file.

## Verify Deployment

- Check **Settings -> Workers**. The worker appears as connected, together with the detected systems and features.
- The next project evaluation places builds on the new worker.
- The connection to the server is visible in `/var/log/gradient-worker.log` on macOS.

!!! tip "One Machine, Many Projects"
    Register the same worker ID in each project. Give each project its own line in the peers file. To share a worker across a whole team, use a [team worker](../concepts/teams.md#team-workers) instead.

## macOS Workers

The ``darwinModules.default`` module can run the worker as a launchd daemon ``org.nixos.gradient-worker``. The macOS module and the NixOS module differ.

- User: ``_gradient-worker``, with UID and GID 536. Set ``users.users._gradient-worker.uid`` and ``users.groups._gradient-worker.gid`` to different values if another account can use 536.
- Logs: ``/var/log/gradient-worker.log``.
- Restart: ``sudo launchctl kickstart -k system/org.nixos.gradient-worker``.
- System: ``aarch64-darwin`` only. Support for Intel Mac (``x86_64-darwin``) is absent. nixpkgs deprecated this platform.
- Linux only: exec jobs (``capabilities.exec``), ``build.metrics`` and the reverse proxy options.
- Nix settings: the module can add ``_gradient-worker`` to ``trusted-users`` and enable the ``nix-command``, ``flakes`` and ``ca-derivations`` features. Add both to the Nix settings manually for ``nix.enable = false``, for example Determinate Nix.

## Peers File

The worker authenticates with one line per project.

```text
# /run/secrets/gradient-worker-peers
<project uuid>:<token>
<other project uuid>:<other token>
```

A single `*:<token>` line can cover every project with the same token instead.

| | One line per project | `*:<token>` |
|---|---|---|
| Setup | One new line per project | One line. Every project must register the worker with the same pre-generated token |
| Leaked token | One project exposed | Every project of the worker exposed |
| Revoking | Per project | Everywhere at once |
| Fit | Workers shared between teams | A worker owned by one team, serving all of that team's projects |

Prefer one line per project. Reserve `*` for workers where a single owner holds every project.

A [team worker](../concepts/teams.md#team-workers) needs one line for its team, `<team id>:<token>`, or `*:<token>`.

## Next Steps

- [Workers](../concepts/workers.md): capabilities, matching and access
- [Worker options](../reference/configuration.md#worker): every worker setting
