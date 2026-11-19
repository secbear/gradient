/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { TestBed } from '@angular/core/testing';
import { signal } from '@angular/core';
import { Subject, finalize, of, tap, throwError } from 'rxjs';
import type { WorkerHealthInfo } from '@core/models';
import { runReloadIntervalWhileTabVisible } from './refresh';
import { workerHealthRefreshDelay } from './worker-health-refresh';

describe('runReloadIntervalWhileTabVisible', () => {
  let visibility: DocumentVisibilityState;

  beforeEach(() => {
    vi.useFakeTimers();
    visibility = 'visible';
    vi.spyOn(document, 'visibilityState', 'get').mockImplementation(() => visibility);
  });

  afterEach(() => {
    TestBed.resetTestingModule();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('reloads on every tick while the tab is visible and keeps going after a failure', () => {
    const fetch = vi.fn().mockReturnValueOnce(throwError(() => new Error('down'))).mockReturnValue(of(1));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, 1000));
    vi.advanceTimersByTime(3000);
    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it('skips ticks in a hidden tab and catches up when the tab shows again', () => {
    const fetch = vi.fn(() => of(1));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, 1000));
    visibility = 'hidden';
    vi.advanceTimersByTime(3000);
    expect(fetch).not.toHaveBeenCalled();

    visibility = 'visible';
    document.dispatchEvent(new Event('visibilitychange'));
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('keeps the default interval at 15 seconds', () => {
    const fetch = vi.fn(() => of(1));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch));
    vi.advanceTimersByTime(14_999);
    expect(fetch).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('keeps numeric interval cancellation and destruction behavior', () => {
    const finish = vi.fn();
    const pending = new Subject<number>();
    const fetch = vi.fn(() => pending.pipe(finalize(finish)));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, 1000));
    vi.advanceTimersByTime(2000);
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(finish).toHaveBeenCalledTimes(1);
    TestBed.resetTestingModule();
    expect(finish).toHaveBeenCalledTimes(2);
    vi.advanceTimersByTime(2000);
    document.dispatchEvent(new Event('visibilitychange'));
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('retimes the first adaptive tick when initially loaded data changes the delay', () => {
    const delay = signal(15_000);
    const fetch = vi.fn(() => of(1));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, undefined, delay));
    TestBed.tick();
    vi.advanceTimersByTime(2000);
    delay.set(1500);
    TestBed.tick();
    vi.advanceTimersByTime(1499);
    expect(fetch).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('waits for a long adaptive response and schedules the next tick from its new delay', () => {
    const delay = signal(1000);
    const pending = new Subject<number>();
    const finish = vi.fn();
    const fetch = vi.fn()
      .mockReturnValueOnce(pending.pipe(tap(() => delay.set(500)), finalize(finish)))
      .mockReturnValue(of(1));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, undefined, delay));
    TestBed.tick();
    vi.advanceTimersByTime(11_000);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(finish).not.toHaveBeenCalled();
    pending.next(1);
    TestBed.tick();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(finish).not.toHaveBeenCalled();
    pending.complete();
    TestBed.tick();
    vi.advanceTimersByTime(499);
    expect(fetch).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it.each([
    { delay: 0, expected: 500 },
    { delay: -1, expected: 500 },
    { delay: Number.NaN, expected: 15_000 },
    { delay: Number.POSITIVE_INFINITY, expected: 15_000 },
  ])('bounds an adaptive delay of $delay and continues after failure', ({ delay, expected }) => {
    const fetch = vi.fn().mockReturnValueOnce(throwError(() => new Error('down'))).mockReturnValue(of(1));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, undefined, () => delay));
    TestBed.tick();
    vi.advanceTimersByTime(expected * 2);
    expect(fetch).toHaveBeenCalledTimes(2);
    TestBed.resetTestingModule();
    vi.advanceTimersByTime(expected * 2);
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('cancels adaptive requests on visibility changes and destruction', () => {
    const pending = new Subject<number>();
    const finish = vi.fn();
    const fetch = vi.fn().mockReturnValueOnce(pending.pipe(finalize(finish))).mockReturnValue(of(1));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, undefined, () => 1000));
    TestBed.tick();
    vi.advanceTimersByTime(1000);
    visibility = 'hidden';
    document.dispatchEvent(new Event('visibilitychange'));
    expect(finish).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(30_000);
    expect(fetch).toHaveBeenCalledTimes(1);
    visibility = 'visible';
    document.dispatchEvent(new Event('visibilitychange'));
    expect(fetch).toHaveBeenCalledTimes(2);
    TestBed.resetTestingModule();
    vi.advanceTimersByTime(30_000);
    document.dispatchEvent(new Event('visibilitychange'));
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it.each([false, true])('keeps sustained expired reports at 15 seconds with failure=%s', (failure) => {
    vi.setSystemTime(new Date('2026-11-18T18:00:00Z'));
    const reports = signal<WorkerHealthInfo[]>([{
      ready: false,
      checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
      expires_at: '2026-11-18T17:59:59',
    }]);
    const fetch = vi.fn(() => failure ? throwError(() => new Error('down')) :
      of(1).pipe(tap(() => reports.update((current) => [...current]))));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, undefined, () => workerHealthRefreshDelay(reports())));
    TestBed.tick();
    for (let attempt = 1; attempt <= 3; attempt++) {
      vi.advanceTimersByTime(14_999);
      expect(fetch).toHaveBeenCalledTimes(attempt - 1);
      vi.advanceTimersByTime(1);
      TestBed.tick();
      expect(fetch).toHaveBeenCalledTimes(attempt);
    }
  });

  it.each([false, true])('backs off after an unchanged future report expires with failure=%s', (failure) => {
    vi.setSystemTime(new Date('2026-11-18T18:00:00Z'));
    const reports = signal<WorkerHealthInfo[]>([{
      ready: true,
      checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
      expires_at: '2026-11-18T18:00:03',
    }]);
    const fetch = vi.fn(() => failure ? throwError(() => new Error('down')) :
      of(1).pipe(tap(() => reports.update((current) => [...current]))));
    TestBed.runInInjectionContext(() => runReloadIntervalWhileTabVisible(fetch, undefined, () => workerHealthRefreshDelay(reports())));
    TestBed.tick();
    for (let elapsed = 0; elapsed < 3500; elapsed += 250) {
      vi.advanceTimersByTime(250);
      TestBed.tick();
    }
    expect(fetch).toHaveBeenCalledTimes(4);
    vi.advanceTimersByTime(14_000);
    expect(fetch).toHaveBeenCalledTimes(4);
    vi.advanceTimersByTime(1000);
    TestBed.tick();
    expect(fetch).toHaveBeenCalledTimes(5);
  });

  it.each<{ expiries: (string | null | undefined)[]; expected: number }>([
    { expiries: [], expected: 15_000 },
    { expiries: [undefined, null], expected: 15_000 },
    { expiries: ['invalid'], expected: 15_000 },
    { expiries: ['2026-11-18T18:00:03'], expected: 1500 },
    { expiries: ['2026-11-18T18:00:00.250'], expected: 500 },
    { expiries: ['2026-11-18T18:00:00'], expected: 15_000 },
    { expiries: ['2026-11-18T17:59:59'], expected: 15_000 },
    { expiries: ['2026-11-18T18:02:00'], expected: 15_000 },
    { expiries: ['2026-11-18T18:00:10', '2026-11-18T18:00:03'], expected: 1500 },
    { expiries: ['2026-11-18T17:59:59', '2026-11-18T18:00:03'], expected: 1500 },
    { expiries: ['2026-11-18T12:00:03-06:00'], expected: 1500 },
  ])('uses the earliest worker expiry $expiries with bounded delay $expected', ({ expiries, expected }) => {
    const reports: WorkerHealthInfo[] = expiries.map((expires_at) => ({
      ready: true,
      checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
      expires_at,
    }));
    expect(workerHealthRefreshDelay(reports, Date.parse('2026-11-18T18:00:00Z'))).toBe(expected);
  });
});
