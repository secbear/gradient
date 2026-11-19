/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { signal } from '@angular/core';
import { EMPTY, Observable, Subject, of, throwError } from 'rxjs';
import { ConfirmRequest, ConfirmService } from '@shared/confirm';
import { ApiError } from '@core/services/api.service';
import { TeamWorkersComponent } from './team-workers.component';
import { TeamsService } from '@core/services/teams.service';
import { AuthService } from '@core/services/auth.service';
import { ConfigService } from '@core/services/config.service';
import { NotifyService } from '@core/services/notify.service';
import { TeamResponse, TeamWorkerEntry, WorkerHealthInfo, WorkerLiveInfo } from '@core/models';

const team = { id: 't1', name: 'platform', display_name: 'Platform', managed: false, role: 'admin' } as TeamResponse;

const connection: TeamWorkerEntry = {
  worker_id: 'g1',
  display_name: 'Gradient.CI Servers',
  registered_at: '2026-10-05T12:00:00',
  active: true,
  managed: false,
  gradient_ci: true,
  enable_network: true,
  enable_secrets: false,
  enable_exec: false,
  enable_eval: true,
  enable_build: true,
  connected: true,
};

const WORKER_ID = '3f2b8c1e-9a4d-4e6f-b7c2-1d5e8f9a0b3c';

const offline = { ...connection, worker_id: 'w2', display_name: 'Sleeper', gradient_ci: false, connected: false };

const live: WorkerLiveInfo = {
  capabilities: { core: true, cache: false, federate: false, network: true, secrets: false, exec: false, eval: true, build: true },
  architectures: [],
  system_features: [],
  max_concurrent_builds: 1,
  assigned_job_count: 0,
  draining: false,
};

function setup(
  role: 'admin' | 'member' = 'admin',
  workers: TeamWorkerEntry[] = [connection],
  list: () => Observable<TeamWorkerEntry[]> = () => of(workers),
) {
  const updateWorker = vi.fn().mockReturnValue(of('updated'));
  const registerWorker = vi.fn().mockReturnValue(of({ token: 'secret' }));
  const removeWorker = vi.fn().mockReturnValue(of('removed'));
  const notify = { success: vi.fn(), failure: vi.fn() };
  const confirm = vi.fn((request: ConfirmRequest): Observable<unknown> => request.action());
  TestBed.configureTestingModule({
    imports: [TeamWorkersComponent],
    providers: [
      provideRouter([]),
      provideHttpClient(),
      provideHttpClientTesting(),
      { provide: AuthService, useValue: { user: signal({ superuser: false }) } },
      { provide: ConfigService, useValue: { gradientCiEnabled: true, gradientCiUrl: 'https://servers.gradient.ci' } },
      { provide: TeamsService, useValue: { workers: list, updateWorker, registerWorker, removeWorker } },
      { provide: NotifyService, useValue: notify },
      { provide: ConfirmService, useValue: { confirm } },
    ],
  });
  const fixture = TestBed.createComponent(TeamWorkersComponent);
  fixture.componentRef.setInput('team', { ...team, role });
  fixture.detectChanges();
  return { fixture, updateWorker, registerWorker, removeWorker, notify, confirm, root: fixture.nativeElement as HTMLElement };
}

function button(root: Element, label: string): HTMLButtonElement | undefined {
  return (Array.from(root.querySelectorAll('button')) as HTMLButtonElement[]).find(
    (b) => b.querySelector('.gr-button__label')?.textContent?.trim() === label,
  );
}

describe('TeamWorkersComponent', () => {
  describe('refresh', () => {
    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-11-18T18:00:00Z'));
      vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
    });

    afterEach(() => {
      TestBed.resetTestingModule();
      vi.useRealTimers();
      vi.restoreAllMocks();
    });

    it.each([true, false])('renews a three-second report with initial ready=%s', (ready) => {
      const initial: TeamWorkerEntry = {
        ...connection,
        live: {
          ...live,
          health: {
            ready,
            checks: [{ name: 'Nix store', status: ready ? 'passed' : 'pending', healthy: ready }],
            observed_at: '2026-11-18T18:00:00',
            expires_at: '2026-11-18T18:00:03',
          },
        },
      };
      const list = vi.fn(() => of([initial]));
      const { fixture, root } = setup('member', [initial], list);
      list.mockImplementation(() => of([{
        ...initial,
        live: {
          ...live,
          health: {
            ready: true,
            checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
            observed_at: new Date(Date.now()).toISOString(),
            expires_at: new Date(Date.now() + 3000).toISOString(),
          },
        },
      }]));
      for (let elapsed = 500; elapsed <= 6000; elapsed += 500) {
        vi.advanceTimersByTime(500);
        fixture.detectChanges();
        expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim())
          .toBe(ready || elapsed >= 1500 ? 'Ready' : 'Not ready');
      }
      expect(list).toHaveBeenCalledTimes(5);
    });

    it('ignores disconnected deadlines and keeps legacy refresh at 15 seconds', () => {
      const disconnected: TeamWorkerEntry = {
        ...offline,
        live: {
          ...live,
          health: {
            ready: false,
            checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
            expires_at: '2026-11-18T17:59:59',
          },
        },
      };
      const list = vi.fn(() => of([connection, disconnected]));
      setup('member', [connection, disconnected], list);
      vi.advanceTimersByTime(14_999);
      expect(list).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(1);
      expect(list).toHaveBeenCalledTimes(2);
    });
  });

  it.each<{ health: WorkerHealthInfo; label: string; state?: string }>([
    { health: { ready: true, checks: [{ name: 'Nix store', status: 'passed', healthy: true }], observed_at: '2026-11-18T17:55:00' }, label: 'Ready', state: 'Passed' },
    { health: { ready: false, checks: [{ name: 'Nix store', status: 'pending', healthy: false }], observed_at: '2026-11-18T17:55:00' }, label: 'Not ready', state: 'Pending' },
    { health: { ready: false, checks: [] }, label: 'Not ready' },
  ])('shows connection separately from $label health', ({ health, label, state }) => {
    const { fixture, root } = setup('member', [{ ...connection, live: { ...live, health } }]);
    expect(Array.from(root.querySelectorAll('gr-badge')).map((badge) => badge.textContent?.trim())).toContain('Connected');
    expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe(label);
    root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    fixture.detectChanges();
    const report = document.querySelector('.health-report')!;
    if (state) {
      expect(report.textContent).toContain('Nix store');
      expect(report.textContent).toContain(state);
    } else {
      expect(report.textContent).toContain('No health report received.');
      expect(report.querySelector('li')).toBeNull();
    }
  });

  it('does not claim legacy connected workers have verified health', () => {
    const { root } = setup('member', [{ ...connection, live }]);
    expect(root.querySelector('app-worker-health')?.textContent).toContain('Unverified health');
    expect(root.querySelector('[aria-label="Worker health checks"]')).toBeNull();
    expect(root.textContent).toContain('Connected');
  });

  it('escapes check names and reports the latest state separately from health', () => {
    const name = '<img src=x onerror=alert(1)>';
    const { fixture, root } = setup('member', [{
      ...connection,
      live: { ...live, health: { ready: true, checks: [{ name, status: 'failed', healthy: true }], observed_at: '2026-11-18T12:00:00' } },
    }]);
    expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Ready');
    root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    fixture.detectChanges();
    const report = document.querySelector('.health-report')!;
    expect(report.querySelector('.check-name')?.textContent).toBe(name);
    expect(report.querySelector('img')).toBeNull();
    expect(report.textContent).toContain('Failed');
    expect(report.textContent).toContain('Healthy');
  });

  it('labels health on an offline worker as a last report, never current readiness', () => {
    const { fixture, root } = setup('admin', [{
      ...offline,
      live: { ...live, health: { ready: true, checks: [{ name: 'Nix store', status: 'passed', healthy: true }], observed_at: '2026-11-18T12:00:00' } },
    }]);
    button(root, 'Show Offline (1)')!.click();
    fixture.detectChanges();
    expect(root.textContent).toContain('Offline');
    expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Health unavailable');
    root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    fixture.detectChanges();
    expect(document.querySelector('.health-report')?.textContent).toContain('Last health report (worker offline)');
  });

  it('shows members what a worker is allowed without letting them change it', () => {
    const { root } = setup('member');
    expect(root.textContent).toContain('Allowed:');
    expect(button(root, 'Edit')).toBeUndefined();
  });

  it('hides the Workers section from members while the team has no workers, but not from admins', () => {
    expect(setup('member', []).root.textContent).not.toContain('Workers');
    TestBed.resetTestingModule();
    expect(setup('admin', []).root.textContent).toContain('Workers');
  });

  it('saves what a Gradient.CI connection is allowed from its Edit dialog', () => {
    const { fixture, root, updateWorker } = setup();

    button(root, 'Edit')!.click();
    fixture.detectChanges();
    const dialog = document.querySelector('.gr-dialog')!;
    Array.from(dialog.querySelectorAll('gr-allowed-capabilities button'))
      .find((b) => b.textContent?.includes('secrets'))!
      .dispatchEvent(new Event('click'));
    button(dialog, 'Save')!.click();

    expect(updateWorker).toHaveBeenCalledWith('platform', 'g1', { enable_secrets: true });
  });

  it('keeps a refused registration inside the Register dialog', () => {
    const { fixture, root, registerWorker } = setup();
    registerWorker.mockReturnValue(throwError(() => new ApiError('Worker is already registered', 409)));
    button(root, 'Register Worker')!.click();
    fixture.componentInstance.form = { worker_id: WORKER_ID, display_name: 'Builder' };
    fixture.componentInstance.register();
    fixture.detectChanges();
    expect(fixture.componentInstance.showRegister()).toBe(true);
    expect(document.querySelector('.gr-dialog')?.textContent).toContain('Worker is already registered');
    expect(root.textContent).not.toContain('Worker is already registered');
  });

  it('names the missing fields under them and sends no registration', () => {
    const { fixture, root, registerWorker } = setup();
    button(root, 'Register Worker')!.click();
    fixture.detectChanges();
    const dialog = document.querySelector('.gr-dialog')!;
    button(dialog, 'Register')!.click();
    fixture.detectChanges();
    expect(dialog.textContent).toContain('Enter a worker ID.');
    expect(dialog.textContent).toContain('Enter a display name.');
    expect(registerWorker).not.toHaveBeenCalled();
  });

  it('refuses a worker ID that is no UUID before asking the server', () => {
    const { fixture, root, registerWorker } = setup();
    button(root, 'Register Worker')!.click();
    fixture.componentInstance.form = { worker_id: 'builder-1', display_name: 'Builder' };
    fixture.componentInstance.register();
    fixture.detectChanges();
    expect(document.querySelector('.gr-dialog')?.textContent).toContain('Enter a UUID');
    expect(registerWorker).not.toHaveBeenCalled();
  });

  it('closes the Edit dialog without a request when nothing changed', () => {
    const { fixture, root, updateWorker } = setup();
    button(root, 'Edit')!.click();
    fixture.detectChanges();
    button(document.querySelector('.gr-dialog')!, 'Save')!.click();
    expect(updateWorker).not.toHaveBeenCalled();
    expect(fixture.componentInstance.editing()).toBeNull();
  });

  it('keeps the Register dialog open and sends one request while registration runs', () => {
    const { fixture, root, registerWorker } = setup();
    registerWorker.mockReturnValue(new Subject());
    button(root, 'Register Worker')!.click();
    fixture.componentInstance.form = { worker_id: WORKER_ID, display_name: 'Builder' };
    fixture.detectChanges();
    const dialog = document.querySelector('.gr-dialog')!;
    button(dialog, 'Register')!.click();
    fixture.detectChanges();
    button(dialog, 'Register')!.click();
    dialog.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27, bubbles: true }));
    (document.querySelector('.gr-dialog-backdrop') as HTMLElement).click();
    fixture.detectChanges();
    expect(registerWorker).toHaveBeenCalledTimes(1);
    expect(fixture.componentInstance.showRegister()).toBe(true);
    expect(button(dialog, 'Cancel')!.disabled).toBe(true);
  });

  it('keeps a failed edit inside the Edit dialog', () => {
    const { fixture, root, updateWorker } = setup();
    updateWorker.mockReturnValue(throwError(() => new ApiError('Worker is managed', 409)));
    button(root, 'Edit')!.click();
    fixture.componentInstance.editAllowed = { ...connection, enable_secrets: true };
    fixture.detectChanges();
    button(document.querySelector('.gr-dialog')!, 'Save')!.click();
    fixture.detectChanges();
    expect(fixture.componentInstance.editing()).not.toBeNull();
    expect(document.querySelector('.gr-dialog')?.textContent).toContain('Worker is managed');
    expect(root.textContent).not.toContain('Worker is managed');
  });

  it('reports a failed deactivation as a toast', () => {
    const { fixture, root, updateWorker, notify } = setup();
    const failure = new ApiError('database is down', 500);
    updateWorker.mockReturnValue(throwError(() => failure));
    button(root, 'Deactivate')!.click();
    fixture.detectChanges();
    expect(notify.failure).toHaveBeenCalledWith('Failed to deactivate worker', failure);
    expect(fixture.componentInstance.busy()).toBeNull();
  });

  it('asks before deleting a worker, naming it, then confirms the deletion', () => {
    const { root, removeWorker, notify, confirm } = setup();
    root.querySelector<HTMLButtonElement>('gr-row button.gr-button--danger')!.click();
    expect(confirm).toHaveBeenCalledWith(expect.objectContaining({ title: 'Delete worker', target: 'Gradient.CI Servers' }));
    expect(removeWorker).toHaveBeenCalledWith('platform', 'g1');
    expect(notify.success).toHaveBeenCalledWith('Worker deleted');
  });

  it('sends no delete when the confirmation is cancelled', () => {
    const { fixture, removeWorker, confirm } = setup();
    confirm.mockImplementation(() => EMPTY);
    fixture.componentInstance.remove(connection);
    expect(removeWorker).not.toHaveBeenCalled();
  });

  it('hides offline workers until asked to show them', () => {
    const { fixture, root } = setup('admin', [connection, offline]);
    expect(root.textContent).toContain('Gradient.CI Servers');
    expect(root.textContent).not.toContain('Sleeper');

    button(root, 'Show Offline (1)')!.click();
    fixture.detectChanges();
    expect(root.textContent).toContain('Sleeper');
  });

  it.each([
    ['only offline workers', [offline]],
    ['no workers', []],
  ])('says no worker is online with %s', (_, workers) => {
    const { root } = setup('admin', workers);
    expect(root.textContent).toContain('No online workers');
  });

  it('links a Gradient.CI connection to its site', () => {
    const { root } = setup('member');
    expect(root.querySelector('a[href="https://servers.gradient.ci"]')).not.toBeNull();
  });

  it('shows a failed worker load with Try Again, never as no online workers', () => {
    let fail = true;
    const { fixture, root } = setup('member', [connection], () =>
      fail ? throwError(() => new ApiError('database is down', 500)) : of([connection]),
    );
    expect(root.textContent).toContain('Failed to load workers');
    expect(root.textContent).not.toContain('No online workers');

    fail = false;
    root.querySelector<HTMLButtonElement>('.gr-load-state__error button')!.click();
    fixture.detectChanges();
    expect(root.textContent).toContain('Gradient.CI Servers');
  });
});
