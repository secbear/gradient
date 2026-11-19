/*
  SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>

  SPDX-License-Identifier: AGPL-3.0-only
*/

{
  inputs.tool = {
    url = "path:@busybox@";
    flake = false;
  };
  outputs =
    { tool, ... }:
    let
      mk =
        name: required: gate:
        builtins.derivation {
          name = "readiness-${name}";
          system = "@system@";
          builder = "${tool}/bin/sh";
          PATH = "${tool}/bin";
          requiredSystemFeatures = required;
          args = [
            "-c"
            (
              (
                if gate == null then
                  ""
                else
                  ''
                    umask 000
                    echo started >> /run/gradient-readiness-build/${gate}-starts
                    echo running > /run/gradient-readiness-build/${gate}-running
                    while ! test -e /run/gradient-readiness-build/release-${gate}; do
                      sleep 0.1
                    done
                  ''
              )
              + ''
                umask 022
                echo ${name} > "$out"
              ''
            )
          ];
        };
    in
    {
      packages."@system@" = {
        plain = mk "plain" [ ] null;
        long = mk "long" [ "readiness-fixture" ] "long";
        next = mk "next" [ "readiness-fixture" ] null;
        control = mk "control" [ "readiness-fixture" ] null;
        restart = mk "restart" [ "readiness-fixture" ] "restart";
        after-restart = mk "after-restart" [ "readiness-fixture" ] null;
      };
    };
}
