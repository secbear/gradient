/*
  SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>

  SPDX-License-Identifier: AGPL-3.0-only
*/

{ self, pkgs, ... }:
{
  value = self.packages.${pkgs.stdenv.hostPlatform.system}.gradient.capacityTests;
}
