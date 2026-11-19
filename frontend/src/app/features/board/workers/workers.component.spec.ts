/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { Observable, Subject, defer, finalize, of, throwError } from 'rxjs';
import { BoardService } from '@core/services/board.service';
import type { BoardWorker, WorkerLoad, WorkerLoadAverage } from '@core/models';
import { BoardWorkersComponent } from './workers.component';

const bucket = (key: string, in_flight: number, capacity: number) => ({ key, in_flight, capacity, workers: 1 });

const NOW: WorkerLoad = {
  by_capability: [bucket('eval', 0, 4), bucket('network', 0, 4), bucket('build', 4, 4), bucket('exec', 1, 2)],
  by_architecture: [bucket('x86_64-linux', 4, 4)],
  by_feature: [],
};

const AVERAGE: WorkerLoadAverage = {
  by_capability: [bucket('eval', 1, 4), bucket('network', 0, 4), bucket('build', 2, 4)],
  by_architecture: [bucket('x86_64-linux', 1, 4), bucket('aarch64-linux', 0.5, 2)],
};

function setup(
  average: () => Observable<WorkerLoadAverage> = () => of(AVERAGE),
  workers: Observable<BoardWorker[]> = of([]),
) {
  const getWorkers = vi.fn(() => workers);
  const getFleet = vi.fn(() => of([]));
  const getWorkerLoad = vi.fn(() => of(NOW));
  const getWorkerLoadAverage = vi.fn((_hours: number) => average());
  TestBed.configureTestingModule({
    imports: [BoardWorkersComponent],
    providers: [
      provideRouter([]),
      {
        provide: BoardService,
        useValue: {
          getWorkers,
          getFleet,
          getWorkerLoad,
          getWorkerLoadAverage,
        },
      },
    ],
  });
  const fixture = TestBed.createComponent(BoardWorkersComponent);
  fixture.detectChanges();
  return { cmp: fixture.componentInstance, getWorkers, getFleet, getWorkerLoad, getWorkerLoadAverage, fixture };
}

describe('BoardWorkersComponent - load range', () => {
  it('shows the live load until a range is picked', () => {
    const { cmp, getWorkerLoadAverage } = setup();
    expect(getWorkerLoadAverage).not.toHaveBeenCalled();
    expect(cmp.capLoad().cats).toEqual(['eval', 'network', 'build', 'exec']);
    expect(cmp.capLoad().series[0].data).toEqual([0, 0, 100, 50]);
  });

  it('keeps a tenth of a percent so a light load does not read as zero', () => {
    const { cmp } = setup();
    cmp.load.set({ ...NOW, by_capability: [bucket('build', 1, 300)] });
    expect(cmp.capLoad().series[0].data).toEqual([0.3]);
  });

  it('shows a feature no worker offers as no data instead of an idle 0 %', () => {
    const { cmp, fixture } = setup();
    cmp.load.set({ ...NOW, by_feature: [bucket('kvm', 0, 0), bucket('benchmark', 0, 0)] });
    fixture.detectChanges();
    expect(cmp.featLoad().series[0].data).toEqual([null, null]);
    const chart = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('gr-metric-chart')).at(-1)!;
    expect(chart.textContent).toContain('No data');
  });

  it('says the average failed instead of drawing empty radars like an idle fleet', () => {
    const { cmp, fixture } = setup(() => throwError(() => new Error('Internal server error')));
    cmp.setLoadRange('24h');
    fixture.detectChanges();
    const root = fixture.nativeElement as HTMLElement;
    expect(root.textContent).toContain('Failed to load worker load');
    expect(root.textContent).toContain('Internal server error');

    cmp.setLoadRange('now');
    fixture.detectChanges();
    expect(root.textContent).not.toContain('Failed to load worker load');
  });

  it('averages both radar charts over the picked range', () => {
    const { cmp, getWorkerLoadAverage } = setup();
    cmp.setLoadRange('7d');
    expect(getWorkerLoadAverage).toHaveBeenCalledWith(168);
    expect(cmp.capLoad().series[0].data).toEqual([25, 0, 50]);
    expect(cmp.archLoad()).toEqual({ cats: ['x86_64-linux', 'aarch64-linux'], series: [{ name: 'busy %', data: [25, 25] }] });
  });
});

describe('BoardWorkersComponent - failed workers request', () => {
  it('says the worker list failed instead of claiming no workers are connected', () => {
    const { fixture } = setup(undefined, throwError(() => new Error('Internal server error')));
    const table = (fixture.nativeElement as HTMLElement).querySelector('.workers tbody')?.textContent ?? '';
    expect(table).toContain("Couldn't load");
    expect(table).not.toContain('No connected workers.');
  });

  it('says no workers are connected once the empty list loads', () => {
    const { fixture } = setup();
    expect((fixture.nativeElement as HTMLElement).querySelector('.workers tbody')?.textContent).toContain('No connected workers.');
  });
});

describe('BoardWorkersComponent - worker rows', () => {
  const WORKER: BoardWorker = {
    id: 'w1', name: 'Dev Worker', projects: [], draining: false, assigned_jobs: 0, max_concurrent_builds: 4,
    eval: true, network: true, build: true, architectures: ['x86_64-linux'],
    exec: { labels: ['w1'], devices: [{ name: 'gpu', total: 0, free: 0 }, { name: 'fpga', total: 2, free: 1 }] },
  };

  describe('refresh', () => {
    let visibility: DocumentVisibilityState;

    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-11-18T18:00:00Z'));
      visibility = 'visible';
      vi.spyOn(document, 'visibilityState', 'get').mockImplementation(() => visibility);
    });

    afterEach(() => {
      TestBed.resetTestingModule();
      vi.useRealTimers();
      vi.restoreAllMocks();
    });

    it('recovers a pending worker and updates its latest check without reloading telemetry', () => {
      const pending: BoardWorker = { ...WORKER, health: { ready: false, checks: [] } };
      const ready: BoardWorker = {
        ...WORKER,
        health: {
          ready: true,
          checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
          observed_at: '2026-11-18T18:00:15',
          expires_at: '2026-11-18T18:01:00',
        },
      };
      const { fixture, getWorkers, getFleet, getWorkerLoad, getWorkerLoadAverage } = setup(undefined, of([pending]));
      const root = fixture.nativeElement as HTMLElement;
      expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Not ready');
      root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
      fixture.detectChanges();
      expect(document.querySelector('.health-report')?.textContent).toContain('No health report received.');

      getWorkers.mockReturnValue(of([ready]));
      vi.advanceTimersByTime(15_000);
      fixture.detectChanges();
      expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Ready');
      expect(document.querySelector('.health-report')?.textContent).toContain('Nix store');
      expect(document.querySelector('.health-report')?.textContent).toContain('Passed');
      expect(document.querySelector('.health-report')?.textContent).toContain('Report received just now');
      expect(getWorkers).toHaveBeenCalledTimes(2);
      expect(getFleet).toHaveBeenCalledTimes(1);
      expect(getWorkerLoad).toHaveBeenCalledTimes(1);
      expect(getWorkerLoadAverage).not.toHaveBeenCalled();
    });

    it.each([3000, 20_000])('keeps a healthy report with %i ms validity renewed', (validity) => {
      const initial: BoardWorker = {
        ...WORKER,
        health: {
          ready: true,
          checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
          observed_at: '2026-11-18T18:00:00',
          expires_at: new Date(Date.now() + validity).toISOString(),
        },
      };
      const { fixture, getWorkers, getFleet, getWorkerLoad } = setup(undefined, of([initial]));
      getWorkers.mockImplementation(() => of([{
        ...initial,
        health: { ...initial.health!, observed_at: new Date(Date.now()).toISOString(), expires_at: new Date(Date.now() + validity).toISOString() },
      }]));
      for (let elapsed = 0; elapsed < validity * 3; elapsed += 500) {
        vi.advanceTimersByTime(500);
        fixture.detectChanges();
        expect((fixture.nativeElement as HTMLElement).querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Ready');
      }
      expect(getWorkers.mock.calls.length).toBeGreaterThan(3);
      expect(getFleet).toHaveBeenCalledTimes(1);
      expect(getWorkerLoad).toHaveBeenCalledTimes(1);
    });

    it('reschedules after a long initial response supplies a three-second expiry', () => {
      const pending = new Subject<BoardWorker[]>();
      const { fixture, getWorkers } = setup(undefined, pending);
      vi.advanceTimersByTime(16_000);
      expect(getWorkers).toHaveBeenCalledTimes(1);
      const worker: BoardWorker = {
        ...WORKER,
        health: {
          ready: true,
          checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
          observed_at: new Date(Date.now()).toISOString(),
          expires_at: new Date(Date.now() + 3000).toISOString(),
        },
      };
      pending.next([worker]);
      pending.complete();
      fixture.detectChanges();
      getWorkers.mockImplementation(() => of([{
        ...worker,
        health: { ...worker.health!, observed_at: new Date(Date.now()).toISOString(), expires_at: new Date(Date.now() + 3000).toISOString() },
      }]));
      vi.advanceTimersByTime(1499);
      expect(getWorkers).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(1);
      fixture.detectChanges();
      expect(getWorkers).toHaveBeenCalledTimes(2);
      for (let elapsed = 0; elapsed < 4500; elapsed += 500) {
        vi.advanceTimersByTime(500);
        fixture.detectChanges();
        expect((fixture.nativeElement as HTMLElement).querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Ready');
      }
    });

    it('skips hidden tabs and catches up when the tab becomes visible', () => {
      const { fixture, getWorkers } = setup(undefined, of([WORKER]));
      getWorkers.mockReturnValue(of([{ ...WORKER, health: { ready: false, checks: [] } }]));
      visibility = 'hidden';
      vi.advanceTimersByTime(30_000);
      expect(getWorkers).toHaveBeenCalledTimes(1);

      visibility = 'visible';
      document.dispatchEvent(new Event('visibilitychange'));
      fixture.detectChanges();
      expect(getWorkers).toHaveBeenCalledTimes(2);
      expect((fixture.nativeElement as HTMLElement).querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Not ready');
    });

    it('keeps the last report after a refresh failure, lets it expire, and retries successfully', () => {
      const initial: BoardWorker = {
        ...WORKER,
        health: {
          ready: true,
          checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
          observed_at: '2026-11-18T18:00:00',
          expires_at: '2026-11-18T18:00:20',
        },
      };
      const { fixture, getWorkers } = setup(undefined, of([initial]));
      const root = fixture.nativeElement as HTMLElement;
      getWorkers.mockReturnValue(throwError(() => new Error('Internal server error')));
      vi.advanceTimersByTime(15_000);
      fixture.detectChanges();
      expect(fixture.componentInstance.workers()).toEqual([initial]);
      expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Ready');
      expect(root.textContent).not.toContain('No connected workers.');
      expect(root.textContent).not.toContain("Couldn't load");

      vi.advanceTimersByTime(5_000);
      fixture.detectChanges();
      expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Health expired');
      getWorkers.mockReturnValue(of([{
        ...initial,
        health: { ...initial.health!, observed_at: '2026-11-18T18:00:30', expires_at: '2026-11-18T18:01:00' },
      }]));
      vi.advanceTimersByTime(10_000);
      fixture.detectChanges();
      expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Ready');
      expect(fixture.componentInstance.workers()?.[0].health?.expires_at).toBe('2026-11-18T18:01:00');
    });

    it('cancels superseded refreshes and stops requests on destruction', () => {
      const { fixture, getWorkers } = setup(undefined, of([WORKER]));
      const pending = new Subject<BoardWorker[]>();
      let activeRequests = 0;
      const finish = vi.fn(() => activeRequests--);
      getWorkers.mockReturnValue(defer(() => {
        activeRequests++;
        return pending.pipe(finalize(finish));
      }));
      vi.advanceTimersByTime(15_000);
      expect(activeRequests).toBe(1);
      document.dispatchEvent(new Event('visibilitychange'));
      expect(activeRequests).toBe(1);
      expect(finish).toHaveBeenCalledTimes(1);
      expect(getWorkers).toHaveBeenCalledTimes(3);

      fixture.destroy();
      expect(activeRequests).toBe(0);
      expect(finish).toHaveBeenCalledTimes(2);
      pending.next([]);
      vi.advanceTimersByTime(15_000);
      document.dispatchEvent(new Event('visibilitychange'));
      expect(getWorkers).toHaveBeenCalledTimes(3);
      expect(fixture.componentInstance.workers()).toEqual([WORKER]);
    });

    it('does not overlap the initial worker request and cancels it on destruction', () => {
      const pending = new Subject<BoardWorker[]>();
      const finish = vi.fn();
      const { fixture, getWorkers } = setup(undefined, pending.pipe(finalize(finish)));
      vi.advanceTimersByTime(30_000);
      document.dispatchEvent(new Event('visibilitychange'));
      expect(getWorkers).toHaveBeenCalledTimes(1);

      fixture.destroy();
      expect(finish).toHaveBeenCalledTimes(1);
      pending.next([WORKER]);
      vi.advanceTimersByTime(15_000);
      document.dispatchEvent(new Event('visibilitychange'));
      expect(getWorkers).toHaveBeenCalledTimes(1);
      expect(fixture.componentInstance.workers()).toBeNull();
    });
  });

  it('shows readiness and latest named checks in the existing state cell', () => {
    const worker: BoardWorker = {
      ...WORKER,
      health: {
        ready: false,
        checks: [
          { name: 'Nix store', status: 'pending', healthy: false },
          { name: 'Cache upload', status: 'failed', healthy: true },
        ],
        observed_at: '2026-11-18T12:00:00',
      },
    };
    const { fixture } = setup(undefined, of([worker]));
    const root = fixture.nativeElement as HTMLElement;
    const state = root.querySelector('tbody tr td:nth-child(3)')!;
    expect(state.textContent).toContain('active');
    expect(state.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Not ready');
    state.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    fixture.detectChanges();
    const checks = Array.from(document.querySelectorAll('.health-report li'));
    expect(checks.map((check) => check.querySelector('.check-name')?.textContent)).toEqual(['Nix store', 'Cache upload']);
    expect(checks.map((check) => check.querySelector('gr-badge')?.textContent?.trim())).toEqual(['Pending', 'Failed']);
    expect(root.querySelectorAll('thead th')).toHaveLength(8);
    expect(root.querySelector('tbody tr td:nth-child(4)')?.textContent?.trim()).toBe('0/4');
  });

  it.each([
    { draining: false, label: 'active' },
    { draining: true, label: 'draining' },
  ])('preserves the legacy $label worker state without a readiness badge', ({ draining, label }) => {
    const { fixture } = setup(undefined, of([{ ...WORKER, draining }]));
    const state = (fixture.nativeElement as HTMLElement).querySelector('tbody tr td:nth-child(3)')!;
    expect(state.textContent?.trim()).toBe(label);
    expect(state.querySelector('app-worker-health')).toBeNull();
  });

  it('leaves out the label every worker carries for its own id', () => {
    const { fixture } = setup(undefined, of([{ ...WORKER, exec: { ...WORKER.exec!, labels: ['w1', 'lab-a'] } }]));
    const labels = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('td.exec gr-badge')).map((b) => b.textContent?.trim());
    expect(labels).toEqual(['lab-a']);
  });

  it('draws a usage bar only for a device class the worker has', () => {
    const { fixture } = setup(undefined, of([WORKER]));
    const devices = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('td.exec .device'));
    expect(devices.map((d) => !!d.querySelector('gr-segmented-bar'))).toEqual([false, true]);
  });

  it('hides the column when no worker has a label beyond its own id or a device', () => {
    const { fixture } = setup(undefined, of([{ ...WORKER, exec: { labels: ['w1'], devices: [] } }]));
    const el = fixture.nativeElement as HTMLElement;
    expect(Array.from(el.querySelectorAll('thead th')).map((th) => th.textContent?.trim())).not.toContain('Exec');
    expect(el.querySelector('td.exec')).toBeNull();
  });
});
