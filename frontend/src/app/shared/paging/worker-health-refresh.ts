/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import type { WorkerHealthInfo } from '@core/models';
import { serverTime } from '@shared/text';
import { DEFAULT_VISIBLE_RELOAD_DELAY_MS, MIN_ADAPTIVE_RELOAD_DELAY_MS } from './refresh';

export function workerHealthRefreshDelay(reports: Iterable<WorkerHealthInfo | null | undefined>, now = Date.now()): number {
  let delay = DEFAULT_VISIBLE_RELOAD_DELAY_MS;
  for (const report of reports) {
    if (!report?.expires_at) continue;
    const expiry = serverTime(report.expires_at).getTime();
    if (Number.isFinite(expiry) && expiry > now) delay = Math.min(delay, (expiry - now) / 2);
  }
  return Math.max(MIN_ADAPTIVE_RELOAD_DELAY_MS, delay);
}
