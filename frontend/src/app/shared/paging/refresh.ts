/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { DOCUMENT, DestroyRef, computed, inject } from '@angular/core';
import { takeUntilDestroyed, toObservable } from '@angular/core/rxjs-interop';
import { EMPTY, Observable, catchError, concatWith, defer, filter, finalize, fromEvent, interval, map, merge, repeat, switchMap, timer } from 'rxjs';

export const DEFAULT_VISIBLE_RELOAD_DELAY_MS = 15_000;
export const MIN_ADAPTIVE_RELOAD_DELAY_MS = 500;

/// A timer and the return of the browser tab to view both start `fetch` again. A list in a background tab can catch up without a reload. A failed run does not clear the content on screen. Later ticks retry the run.
export function runReloadIntervalWhileTabVisible(
  fetch: () => Observable<unknown>,
  millisecondsBetweenVisibleReloads = DEFAULT_VISIBLE_RELOAD_DELAY_MS,
  nextDelay?: () => number,
): void {
  const document = inject(DOCUMENT);
  const destroyRef = inject(DestroyRef);
  if (nextDelay) {
    const delay = () => {
      const requested = nextDelay();
      return Math.max(MIN_ADAPTIVE_RELOAD_DELAY_MS, Number.isFinite(requested) ? requested : millisecondsBetweenVisibleReloads);
    };
    let reloading = false;
    const reload = () => defer(() => {
      reloading = true;
      return fetch();
    }).pipe(catchError(() => EMPTY), finalize(() => reloading = false));
    const ticks = () => defer(() => timer(delay()).pipe(
      filter(() => document.visibilityState === 'visible'),
      switchMap(reload),
    )).pipe(repeat());

    // Signal changes retime a pending tick, including data from the initial load.
    merge(
      toObservable(computed(nextDelay)).pipe(filter(() => !reloading), map(() => false)),
      fromEvent(document, 'visibilitychange').pipe(map(() => true)),
    ).pipe(
      switchMap((refreshNow) => document.visibilityState !== 'visible' ? EMPTY :
        refreshNow ? reload().pipe(concatWith(ticks())) : ticks()),
      takeUntilDestroyed(destroyRef),
    ).subscribe();
    return;
  }
  merge(interval(millisecondsBetweenVisibleReloads), fromEvent(document, 'visibilitychange'))
    .pipe(
      filter(() => document.visibilityState === 'visible'),
      switchMap(() => fetch().pipe(catchError(() => EMPTY))),
      takeUntilDestroyed(destroyRef),
    )
    .subscribe();
}
