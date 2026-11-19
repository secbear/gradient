/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { EMPTY, Subject, of, throwError } from 'rxjs';
import { ConfirmRequest, ConfirmService } from '@shared/confirm';
import { ActivatedRoute } from '@angular/router';
import { WorkersComponent } from './workers.component';
import { WorkersService } from '@core/services/workers.service';
import { ProjectsService } from '@core/services/projects.service';
import { projectRouteStub } from '@core/resolvers/access-route.testing';
import { ConfigService } from '@core/services/config.service';
import { AccessState } from '@core/models/access.model';
import type { CacheSubscriptionItem, Permission } from '@core/models';
import { TeamsService } from '@core/services/teams.service';
import { AuthService } from '@core/services/auth.service';
import { signal } from '@angular/core';
import { MenuItem } from '@gradient/ui/ui';
import { MANAGED_ITEM_DISABLED_REASON } from '@shared/access';
import { ProjectWorkerEntry, WorkerHealthCheck, WorkerLiveInfo } from '@core/models';
import { ApiError } from '@core/services/api.service';
import { NotifyService } from '@core/services/notify.service';

const notify = { success: vi.fn(), failure: vi.fn() };
const confirm = vi.fn();

type MockedProjects = {
  getProject: ReturnType<typeof vi.fn>;
  getSubscribedCaches: ReturnType<typeof vi.fn>;
};


const workerUnmanaged: ProjectWorkerEntry = {
  worker_id: 'w1',
  display_name: 'Builder',
  registered_at: '2026-01-01T00:00:00',
  active: true,
  gradient_ci: false,
  connected: false,
  enable_network: true,
  enable_secrets: true,
  enable_exec: false,
  enable_eval: true,
  enable_build: true,
};

const workerOfTeam: ProjectWorkerEntry = {
  worker_id: 'w3',
  display_name: 'Team worker',
  registered_at: '2026-01-01T00:00:00',
  active: true,
  team: 'platform',
  team_display_name: 'Platform Team',
  gradient_ci: false,
  connected: false,
  enable_network: true,
  enable_secrets: true,
  enable_exec: false,
  enable_eval: true,
  enable_build: true,
};

function setup(opts: {
  access: AccessState;
  workers: ProjectWorkerEntry[];
  caches: Partial<CacheSubscriptionItem>[];
  testWorker?: ReturnType<typeof vi.fn>;
  gradientCi?: boolean;
  memberOf?: string[];
  permissions?: Permission[];
  role?: string | null;
}) {
  const workersService = {
    getWorkers: vi.fn(() => of(opts.workers)),
    testWorker: opts.testWorker ?? vi.fn(() => of({ ok: true, connected: true, authorized_for_project: true, message: 'ok' })),
    patchWorker: vi.fn(() => of('updated')),
    setWorkerActive: vi.fn(() => of('updated')),
    deleteWorker: vi.fn(() => of('deleted')),
  };
  notify.success.mockClear();
  notify.failure.mockClear();
  confirm.mockReset().mockImplementation((request: ConfirmRequest) => request.action());
  const projects: MockedProjects = {
    getProject: vi.fn(() => of({ id: 'project-uuid', display_name: 'Project' } as never)),
    getSubscribedCaches: vi.fn(() => of(opts.caches.map((cache) => ({ mode: 'ReadWrite', status: 'active', ...cache })))),
  };
  TestBed.configureTestingModule({
    imports: [WorkersComponent],
    providers: [
      provideRouter([]),
      provideHttpClient(),
      provideHttpClientTesting(),
      { provide: WorkersService, useValue: workersService },
      { provide: ProjectsService, useValue: projects },
      {
        provide: TeamsService,
        useValue: {
          list: () => of((opts.memberOf ?? ['platform']).map((name) => ({ name, display_name: name }))),
          projectGrants: () => of([]),
        },
      },
      { provide: AuthService, useValue: { isAuthenticated: () => true, user: signal({ superuser: false }) } },
      {
        provide: ActivatedRoute,
        useFactory: () =>
          projectRouteStub('demo', opts.access, {
            ...(opts.permissions && { permissions: opts.permissions }),
            ...(opts.role !== undefined && { role: opts.role }),
          }),
      },
      { provide: NotifyService, useValue: notify },
      { provide: ConfirmService, useValue: { confirm } },
      {
        provide: ConfigService,
        useValue: { gradientCiEnabled: opts.gradientCi ?? true, gradientCiUrl: 'https://servers.gradient.ci', createTeam: 'everyone', canCreate: () => true },
      },
    ],
  });
  return TestBed.createComponent(WorkersComponent);
}

async function settled(fixture: ComponentFixture<WorkersComponent>) {
  fixture.detectChanges();
  await fixture.whenStable();
  fixture.detectChanges();
}

function findByText(root: HTMLElement, text: string): HTMLElement | null {
  const target = text.toLowerCase();
  return (Array.from(root.querySelectorAll('button')) as HTMLElement[]).find(
    (el) => (el.textContent ?? '').trim().toLowerCase().includes(target),
  ) ?? null;
}

function menuOf(fixture: ComponentFixture<WorkersComponent>, worker: ProjectWorkerEntry): MenuItem[] {
  return fixture.componentInstance.workerMenus().get(worker.worker_id) ?? [];
}

function itemOf(menu: MenuItem[], label: string): MenuItem | undefined {
  return menu.find((i) => i.label === label);
}

describe('WorkersComponent - live state', () => {
  afterEach(() => vi.useRealTimers());

  it('reloads the workers while the page is open', () => {
    vi.useFakeTimers();
    const fixture = setup({ access: { managed: false, canEdit: true, canTrigger: true }, workers: [workerUnmanaged], caches: [] });
    fixture.detectChanges();
    const getWorkers = TestBed.inject(WorkersService).getWorkers as ReturnType<typeof vi.fn>;
    getWorkers.mockReturnValue(of([{ ...workerUnmanaged, display_name: 'Renamed' }]));
    vi.advanceTimersByTime(15_000);
    fixture.detectChanges();
    const text = (fixture.nativeElement as HTMLElement).textContent ?? '';
    expect(text).toContain('Renamed');
  });
});

describe('WorkersComponent - no-cache banner (existing)', () => {
  it('shows the banner when the project has no subscribed caches', async () => {
    const fixture = setup({ access: { managed: false, canEdit: true, canTrigger: true }, workers: [], caches: [] });
    await settled(fixture);
    const banner = fixture.nativeElement.querySelector('[data-testid="no-cache-banner"]');
    expect(banner, 'banner element').toBeTruthy();
  });

  it('hides the banner when the project has at least one subscribed cache', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [],
      caches: [{ id: 'c', name: 'cache-1' }],
    });
    await settled(fixture);
    const banner = fixture.nativeElement.querySelector('[data-testid="no-cache-banner"]');
    expect(banner, 'banner element').toBeNull();
  });

  it('shows the banner when every subscription is read-only or pending', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [],
      caches: [{ id: 'r', name: 'read-only', mode: 'ReadOnly' }, { id: 'p', name: 'pending', status: 'pending' }],
    });
    await settled(fixture);
    expect(fixture.nativeElement.querySelector('[data-testid="no-cache-banner"]')?.textContent).toContain('Subscription request pending');
  });
});

describe('WorkersComponent - access gating', () => {
  it('hides worker writes from a role without the manage workers permission', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      permissions: ['viewProject', 'createTask', 'editTask', 'manageProjectSettings'],
      workers: [workerUnmanaged],
      caches: [{ id: 'c', name: 'c' }],
    });
    await settled(fixture);
    expect(findByText(fixture.nativeElement, 'register worker')).toBeNull();
    expect(menuOf(fixture, workerUnmanaged)).toEqual([]);
  });

  it('hides Register Worker button under read-only project access', async () => {
    const fixture = setup({ access: { managed: false, canEdit: false, canTrigger: false }, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    expect(findByText(fixture.nativeElement, 'register worker')).toBeNull();
  });

  it('offers no row menu under read-only project access', async () => {
    const fixture = setup({ access: { managed: false, canEdit: false, canTrigger: false }, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    expect(menuOf(fixture, workerUnmanaged)).toEqual([]);
    expect(fixture.nativeElement.querySelector('[aria-label="Worker actions"]')).toBeNull();
  });

  it('shows but disables Register Worker under state-managed project', async () => {
    const fixture = setup({ access: { managed: true, canEdit: true, canTrigger: true }, workers: [], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const btn = findByText(fixture.nativeElement, 'register worker') as HTMLButtonElement | null;
    expect(btn).not.toBeNull();
    expect(btn!.getAttribute('aria-disabled')).toBe('true');
  });

  it('keeps Edit, Test worker and Delete in the row menu, each running its action', async () => {
    const fixture = setup({ access: { managed: false, canEdit: true, canTrigger: true }, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const cmp = fixture.componentInstance;
    const rename = vi.spyOn(cmp, 'openRenameDialog').mockImplementation(() => undefined);
    const test = vi.spyOn(cmp, 'fireTest').mockImplementation(() => undefined);
    const remove = vi.spyOn(cmp, 'deleteWorker').mockImplementation(() => undefined);
    const menu = menuOf(fixture, workerUnmanaged);

    itemOf(menu, 'Edit')?.command?.();
    itemOf(menu, 'Test worker')?.command?.();
    itemOf(menu, 'Delete')?.command?.();

    expect(rename).toHaveBeenCalledWith(workerUnmanaged);
    expect(test).toHaveBeenCalledWith(workerUnmanaged);
    expect(remove).toHaveBeenCalledWith(workerUnmanaged);
    expect(itemOf(menu, 'Delete')?.danger).toBe(true);
    expect(findByText(fixture.nativeElement, 'fire test')).toBeNull();
  });

  it('disables Test worker with a reason until the worker is connected', async () => {
    const online = { ...workerUnmanaged, worker_id: 'online', connected: true };
    const fixture = setup({ access: { managed: false, canEdit: true, canTrigger: true }, workers: [workerUnmanaged, online], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    expect(itemOf(menuOf(fixture, workerUnmanaged), 'Test worker')?.disabledReason).toBe('Worker is offline');
    expect(itemOf(menuOf(fixture, online), 'Test worker')?.disabledReason).toBeUndefined();
    expect(itemOf(menuOf(fixture, online), 'Test worker')?.disabled).toBe(false);
  });
});

describe('WorkersComponent - team workers', () => {
  it('renders a team worker read-only with a link to its team', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [workerOfTeam],
      caches: [{ id: 'c', name: 'c' }],
    });
    await settled(fixture);

    const badge = (Array.from(fixture.nativeElement.querySelectorAll('gr-badge')) as HTMLElement[])
      .find((el) => (el.textContent ?? '').trim() === 'Team Platform Team');
    expect(badge, 'Team badge').toBeTruthy();
    const menu = menuOf(fixture, workerOfTeam);
    expect(String(itemOf(menu, 'Manage on team')?.routerLink)).toBe('/team/platform');
    expect(itemOf(menu, 'Edit')).toBeUndefined();
    expect(itemOf(menu, 'Deactivate')).toBeUndefined();
    expect(itemOf(menu, 'Delete')).toBeUndefined();
  });

  it('offers no team link to a viewer outside the team, whose team page would not load', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [workerOfTeam],
      caches: [{ id: 'c', name: 'c' }],
      memberOf: [],
    });
    await settled(fixture);

    expect(itemOf(menuOf(fixture, workerOfTeam), 'Manage on team')).toBeUndefined();
  });

  it('keeps Deactivate usable on a worker in a managed project, since state restores it on restart', async () => {
    const fixture = setup({
      access: { managed: true, canEdit: true, canTrigger: true },
      workers: [workerUnmanaged],
      caches: [{ id: 'c', name: 'c' }],
    });
    await settled(fixture);
    const menu = menuOf(fixture, workerUnmanaged);
    expect(itemOf(menu, 'Deactivate')?.disabled).toBe(false);
    expect(itemOf(menu, 'Edit')?.disabledReason).toBe(MANAGED_ITEM_DISABLED_REASON);
    expect(itemOf(menu, 'Delete')?.disabledReason).toBe(MANAGED_ITEM_DISABLED_REASON);
  });

  it('fireTest calls the service and surfaces the result via a toast', async () => {
    const testWorker = vi.fn(() => of({ ok: true, connected: true, authorized_for_project: true, message: 'reachable' }));
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [workerOfTeam],
      caches: [{ id: 'c', name: 'c' }],
      testWorker,
    });
    await settled(fixture);
    const cmp = fixture.componentInstance;
    const addSpy = vi.spyOn(cmp['messageService'], 'add');

    cmp.fireTest(workerOfTeam);

    expect(testWorker).toHaveBeenCalledWith('demo', workerOfTeam.worker_id);
    expect(cmp.testingId()).toBeNull();
    expect(addSpy).toHaveBeenCalledWith(
      expect.objectContaining({ severity: 'success', detail: 'reachable' }),
    );
  });
});

describe('WorkersComponent - system features', () => {
  it('lists the architectures and keeps the system features behind an info button', async () => {
    const live = {
      capabilities: { network: true, secrets: true, exec: false, eval: true, build: true, federate: false },
      architectures: ['x86_64-linux'], system_features: ['kvm', 'nixos-test'], max_concurrent_builds: 1, assigned_job_count: 0, draining: false,
    } as unknown as ProjectWorkerEntry['live'];
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [{ ...workerUnmanaged, live }],
      caches: [{ id: 'c', name: 'c' }],
    });
    await settled(fixture);
    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector('.worker-archs')?.textContent).toContain('x86_64-linux');
    expect(root.textContent).not.toContain('nixos-test');

    (root.querySelector('[aria-label="System features"]') as HTMLButtonElement).click();
    fixture.detectChanges();
    expect(document.body.textContent).toContain('kvm, nixos-test');
  });
});

describe('WorkersComponent - connection status', () => {
  const live = {
    capabilities: { network: true, secrets: true, exec: false, eval: true, build: true, federate: false },
    architectures: [], system_features: [], max_concurrent_builds: 32, assigned_job_count: 3, draining: false,
  } as unknown as ProjectWorkerEntry['live'];
  const status = (fixture: ComponentFixture<WorkersComponent>) =>
    (fixture.nativeElement as HTMLElement).querySelector('.worker-stats')?.textContent?.trim();
  const badges = (fixture: ComponentFixture<WorkersComponent>) =>
    Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('gr-badge')).map((b) => b.textContent?.trim());

  it('shows a deactivated worker as inactive, also while it is still connected', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [{ ...workerUnmanaged, active: false, connected: true, live }],
      caches: [{ id: 'c', name: 'cache-1' }],
    });
    await settled(fixture);
    expect(status(fixture)).toBe('Inactive');
  });

  it('shows an active connected worker with its jobs and capacity, without a status badge', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [{ ...workerUnmanaged, connected: true, live }],
      caches: [{ id: 'c', name: 'cache-1' }],
    });
    await settled(fixture);
    expect(status(fixture)).toBe('Connected · 3/32 jobs');
    expect(badges(fixture)).not.toContain('Connected');
  });

  it('says a connected worker without live data takes no jobs of this project', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [{ ...workerUnmanaged, connected: true, live: null }],
      caches: [{ id: 'c', name: 'cache-1' }],
    });
    await settled(fixture);
    expect(status(fixture)).toBe('Connected, no jobs for this project (reconnect with the project token)');
    expect(badges(fixture)).toContain('Unverified health');
  });

  it('shows a worker without a connection as offline', async () => {
    const fixture = setup({
      access: { managed: false, canEdit: true, canTrigger: true },
      workers: [{ ...workerUnmanaged, live: null }],
      caches: [{ id: 'c', name: 'cache-1' }],
    });
    await settled(fixture);
    expect(status(fixture)).toBe('Offline');
    expect(badges(fixture)).toContain('Unverified health');
    expect(badges(fixture)).not.toContain('Ready');
  });
});

function buttonsLabelled(root: Element, label: string): HTMLButtonElement[] {
  return (Array.from(root.querySelectorAll('button')) as HTMLButtonElement[]).filter(
    (b) => b.querySelector('.gr-button__label')?.textContent?.trim() === label,
  );
}

function entry(fixture: ComponentFixture<WorkersComponent>): HTMLElement | null {
  return (fixture.nativeElement as HTMLElement).querySelector('[data-testid="gradient-ci-entry"]');
}

const gciRegistration: ProjectWorkerEntry = {
  worker_id: 'g1',
  display_name: 'Gradient.CI Servers',
  registered_at: '2026-01-01T00:00:00',
  active: true,
  gradient_ci: true,
  connected: false,
  last_error: {
    reason: '401 unknown worker id or wrong token',
    at: '2026-10-02T12:00:00',
    direction: 'outbound',
    before_auth: false,
  },
  enable_network: true,
  enable_secrets: false,
  enable_exec: false,
  enable_eval: true,
  enable_build: true,
};

const gciTeam: ProjectWorkerEntry = { ...gciRegistration, worker_id: 'g2', team: 'platform', team_display_name: 'Platform Team', last_error: undefined };
const writable: AccessState = { managed: false, canEdit: true, canTrigger: true };
const caches = [{ id: 'c', name: 'c' }];

describe('WorkersComponent - worker health', () => {
  const live: WorkerLiveInfo = {
    capabilities: { core: true, cache: false, federate: false, network: true, secrets: false, exec: false, eval: true, build: true },
    architectures: [],
    system_features: [],
    max_concurrent_builds: 32,
    assigned_job_count: 3,
    draining: false,
  };

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
      const initial: ProjectWorkerEntry = {
        ...workerUnmanaged,
        connected: true,
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
      const fixture = setup({ access: writable, workers: [initial], caches });
      fixture.detectChanges();
      const getWorkers = TestBed.inject(WorkersService).getWorkers as ReturnType<typeof vi.fn>;
      getWorkers.mockImplementation(() => of([{
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
        expect((fixture.nativeElement as HTMLElement).querySelector('app-worker-health gr-badge')?.textContent?.trim())
          .toBe(ready || elapsed >= 1500 ? 'Ready' : 'Not ready');
      }
      expect(getWorkers).toHaveBeenCalledTimes(5);
      expect((TestBed.inject(ProjectsService) as unknown as MockedProjects).getSubscribedCaches).toHaveBeenCalledTimes(1);
    });

    it('ignores disconnected deadlines and keeps legacy refresh at 15 seconds', () => {
      const fixture = setup({
        access: writable,
        caches,
        workers: [
          { ...workerUnmanaged, connected: true, live },
          {
            ...workerOfTeam,
            live: {
              ...live,
              health: {
                ready: false,
                checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
                expires_at: '2026-11-18T17:59:59',
              },
            },
          },
        ],
      });
      fixture.detectChanges();
      const getWorkers = TestBed.inject(WorkersService).getWorkers as ReturnType<typeof vi.fn>;
      vi.advanceTimersByTime(14_999);
      expect(getWorkers).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(1);
      expect(getWorkers).toHaveBeenCalledTimes(2);
    });
  });

  it.each([
    ['project worker', workerUnmanaged],
    ['team worker', workerOfTeam],
    ['Gradient.CI worker', gciRegistration],
  ])('shows readiness and named checks on a connected %s', async (_, worker) => {
    const fixture = setup({
      access: writable,
      workers: [{
        ...worker,
        connected: true,
        live: { ...live, health: { ready: true, checks: [{ name: 'Nix store', status: 'passed', healthy: true }], observed_at: '2026-11-18T12:00:00' } },
      }],
      caches,
    });
    await settled(fixture);
    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector('.worker-stats')?.textContent).toContain('Connected');
    expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Ready');
    root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    await settled(fixture);
    expect(document.querySelector('.health-report')?.textContent).toContain('Nix store');
    expect(document.querySelector('.health-report')?.textContent).toContain('Passed');
  });

  it.each<{ state: WorkerHealthCheck['status']; label: string }>([
    { state: 'pending', label: 'Pending' },
    { state: 'failed', label: 'Failed' },
    { state: 'timed_out', label: 'Timed out' },
    { state: 'error', label: 'Error' },
  ])('shows a connected worker as not ready with a $state check', async ({ state, label }) => {
    const fixture = setup({
      access: writable,
      workers: [{
        ...workerUnmanaged,
        connected: true,
        live: { ...live, health: { ready: false, checks: [{ name: 'Nix store', status: state, healthy: false }], observed_at: '2026-11-18T12:00:00' } },
      }],
      caches,
    });
    await settled(fixture);
    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector('.worker-stats')?.textContent).toContain('Connected');
    expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Not ready');
    root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    await settled(fixture);
    expect(document.querySelector('.health-report')?.textContent).toContain(label);
    expect(document.querySelector('.health-report')?.textContent).toContain('Unhealthy');
  });

  it('shows an initial missing report as not ready without inventing an observation time', async () => {
    const fixture = setup({
      access: writable,
      workers: [{ ...workerUnmanaged, connected: true, live: { ...live, health: { ready: false, checks: [] } } }],
      caches,
    });
    await settled(fixture);
    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Not ready');
    root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    await settled(fixture);
    const report = document.querySelector('.health-report')!;
    expect(report.textContent).toContain('No health report received.');
    expect(report.textContent).not.toContain('Report received');
    expect(report.querySelector('li')).toBeNull();
  });

  it('shows names as text and keeps health unavailable after disconnecting', async () => {
    const name = '<img src=x onerror=alert(1)>';
    const fixture = setup({
      access: writable,
      workers: [{
        ...workerUnmanaged,
        connected: true,
        live: { ...live, health: { ready: true, checks: [{ name, status: 'passed', healthy: true }], observed_at: '2026-11-18T12:00:00' } },
      }],
      caches,
    });
    await settled(fixture);
    const root = fixture.nativeElement as HTMLElement;
    root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    await settled(fixture);
    const report = document.querySelector('.health-report')!;
    expect(report.querySelector('.check-name')?.textContent).toBe(name);
    expect(report.querySelector('img')).toBeNull();

    fixture.componentInstance.workers.update((workers) => workers.map((worker) => ({ ...worker, connected: false })));
    await settled(fixture);
    expect(root.querySelector('.worker-stats')?.textContent).toContain('Offline');
    expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Health unavailable');
    expect(report.textContent).toContain('Last health report (worker offline)');

    fixture.componentInstance.workers.update((workers) => workers.map((worker) => ({ ...worker, live: null })));
    await settled(fixture);
    expect(root.querySelector('.worker-stats')?.textContent).toContain('Offline');
    expect(root.querySelector('app-worker-health gr-badge')?.textContent?.trim()).toBe('Unverified health');
    expect(root.querySelector('[aria-label="Worker health checks"]')).toBeNull();
    expect(document.querySelector('.health-report')).toBeNull();
  });
});

describe('WorkersComponent - Gradient.CI Servers entry', () => {
  it('offers Connect when nothing is connected', async () => {
    const fixture = setup({ access: writable, workers: [], caches });
    await settled(fixture);
    expect(buttonsLabelled(entry(fixture)!, 'Connect').length).toBe(1);
  });

  it('shows but disables Connect on a state-managed project, like Register Worker', async () => {
    const fixture = setup({ access: { ...writable, managed: true }, workers: [], caches });
    await settled(fixture);
    expect(buttonsLabelled(entry(fixture)!, 'Connect')[0].getAttribute('aria-disabled')).toBe('true');
  });

  it('shows a project connection with its offline reason and Disconnect', async () => {
    const fixture = setup({ access: writable, workers: [gciRegistration], caches });
    await settled(fixture);
    expect(entry(fixture)!.textContent).toContain('401 unknown worker id or wrong token');
    expect(buttonsLabelled(entry(fixture)!, 'Disconnect').length).toBe(1);
  });

  it('shows a connection of a granted team as coming from that team', async () => {
    const fixture = setup({ access: writable, workers: [gciTeam], caches });
    await settled(fixture);
    expect(entry(fixture)!.textContent).toContain('Via team Platform Team');
    expect(entry(fixture)!.textContent).toContain('Allowed:');
    expect(buttonsLabelled(entry(fixture)!, 'Disconnect').length).toBe(0);
    expect(buttonsLabelled(entry(fixture)!, 'Edit').length).toBe(0);
    expect(buttonsLabelled(entry(fixture)!, 'Deactivate').length).toBe(0);
  });

  it('switches a project connection off and on like a registered worker', async () => {
    const fixture = setup({ access: writable, workers: [{ ...gciRegistration, active: false }], caches });
    await settled(fixture);
    const workers = TestBed.inject(WorkersService) as unknown as { setWorkerActive: ReturnType<typeof vi.fn> };

    buttonsLabelled(entry(fixture)!, 'Activate')[0].click();

    expect(workers.setWorkerActive).toHaveBeenCalledWith('demo', 'g1', true);
  });

  it('saves what a project connection is allowed from its Edit dialog', async () => {
    const fixture = setup({ access: writable, workers: [gciRegistration], caches });
    await settled(fixture);
    const workers = TestBed.inject(WorkersService) as unknown as { patchWorker: ReturnType<typeof vi.fn> };
    expect(entry(fixture)!.textContent).toContain('Allowed:');

    buttonsLabelled(entry(fixture)!, 'Edit')[0].click();
    fixture.detectChanges();
    const dialog = document.querySelector('.gr-dialog')!;
    expect(dialog.querySelector('#rename-name')).toBeNull();
    Array.from(dialog.querySelectorAll('gr-allowed-capabilities button'))
      .find((b) => b.textContent?.includes('secrets'))!
      .dispatchEvent(new Event('click'));
    buttonsLabelled(dialog, 'Save')[0].click();

    expect(workers.patchWorker).toHaveBeenCalledWith('demo', 'g1', { enable_secrets: true });
  });

  it('links to the Gradient.CI site only while connected', async () => {
    const link = (fixture: ComponentFixture<WorkersComponent>) =>
      entry(fixture)!.querySelector('a[href="https://servers.gradient.ci"]');
    const connected = setup({ access: writable, workers: [gciRegistration], caches });
    await settled(connected);
    expect(link(connected)?.getAttribute('target')).toBe('_blank');

    TestBed.resetTestingModule();
    const offered = setup({ access: writable, workers: [], caches });
    await settled(offered);
    expect(link(offered)).toBeNull();
  });

  it('hides the entry when the option is off and nothing is connected', async () => {
    const fixture = setup({ access: writable, workers: [], caches, gradientCi: false });
    await settled(fixture);
    expect(entry(fixture)).toBeNull();
  });

  it('keeps an existing connection listed when the option is off', async () => {
    const fixture = setup({ access: writable, workers: [gciRegistration], caches, gradientCi: false });
    await settled(fixture);
    expect(buttonsLabelled(entry(fixture)!, 'Disconnect').length).toBe(1);
    expect(buttonsLabelled(entry(fixture)!, 'Connect').length).toBe(0);
  });

  it('shows the state but no Connect to read-only members', async () => {
    const fixture = setup({ access: { managed: false, canEdit: false, canTrigger: false }, workers: [], caches });
    await settled(fixture);
    expect(entry(fixture)).not.toBeNull();
    expect(buttonsLabelled(entry(fixture)!, 'Connect').length).toBe(0);
  });

  it('shows the last failure under an offline registered worker', async () => {
    const offline: ProjectWorkerEntry = {
      ...workerUnmanaged,
      last_error: {
        reason: 'dial timed out after 10 s',
        at: '2026-10-02T12:00:00',
        direction: 'outbound',
        before_auth: false,
      },
    };
    const fixture = setup({ access: writable, workers: [offline], caches });
    await settled(fixture);
    expect((fixture.nativeElement as HTMLElement).textContent).toContain('dial timed out after 10 s');
  });
});

describe('WorkersComponent - load errors', () => {
  it('shows a failed load as an error, never as the empty state and loads again on Try Again', async () => {
    const fixture = setup({ access: { managed: false, canEdit: true, canTrigger: true }, workers: [], caches: [], gradientCi: false });
    const service = TestBed.inject(WorkersService) as unknown as { getWorkers: () => unknown };
    service.getWorkers = () => throwError(() => new Error('database is down'));
    await settled(fixture);
    const root = fixture.nativeElement as HTMLElement;
    expect(root.textContent).toContain('Failed to load workers');
    expect(root.textContent).toContain('database is down');
    expect(root.textContent).not.toContain('No workers registered');

    service.getWorkers = () => of([]);
    root.querySelector<HTMLButtonElement>('.gr-load-state__error button')!.click();
    await settled(fixture);
    expect(root.textContent).not.toContain('Failed to load workers');
  });

  it('shows a visitor outside the project no access and asks the server nothing', async () => {
    const fixture = setup({ access: { managed: false, canEdit: false, canTrigger: false }, workers: [], caches: [], gradientCi: false, permissions: [], role: null });
    const service = TestBed.inject(WorkersService) as unknown as { getWorkers: ReturnType<typeof vi.fn> };
    const projects = TestBed.inject(ProjectsService) as unknown as MockedProjects;
    await settled(fixture);
    const root = fixture.nativeElement as HTMLElement;
    expect(root.textContent).toContain('No access');
    expect(root.textContent).toContain('Only the members of this project can view this.');
    expect(root.textContent).not.toContain('Try Again');
    expect(root.querySelector('app-team-grants')).toBeNull();
    expect(service.getWorkers).not.toHaveBeenCalled();
    expect(projects.getSubscribedCaches).not.toHaveBeenCalled();
  });
});

describe('WorkersComponent - write failures', () => {
  const writable: AccessState = { managed: false, canEdit: true, canTrigger: true };

  it('asks before deleting a worker, naming it, then confirms the deletion', async () => {
    const fixture = setup({ access: writable, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const workers = TestBed.inject(WorkersService) as unknown as { deleteWorker: ReturnType<typeof vi.fn> };
    fixture.componentInstance.deleteWorker(workerUnmanaged);
    expect(confirm).toHaveBeenCalledWith(expect.objectContaining({ title: 'Delete worker', target: 'Builder' }));
    expect(workers.deleteWorker).toHaveBeenCalledWith('demo', 'w1');
    expect(notify.success).toHaveBeenCalledWith('Worker deleted');
  });

  it('sends no delete when the confirmation is cancelled', async () => {
    const fixture = setup({ access: writable, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    confirm.mockImplementation(() => EMPTY);
    const workers = TestBed.inject(WorkersService) as unknown as { deleteWorker: ReturnType<typeof vi.fn> };
    fixture.componentInstance.deleteWorker(workerUnmanaged);
    expect(workers.deleteWorker).not.toHaveBeenCalled();
  });

  it('reports a failed deactivate instead of logging it', async () => {
    const fixture = setup({ access: writable, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const failure = new ApiError('The server failed to process the request. Try again later.', 500);
    const workers = TestBed.inject(WorkersService) as unknown as { setWorkerActive: ReturnType<typeof vi.fn> };
    workers.setWorkerActive.mockReturnValue(throwError(() => failure));
    const cmp = fixture.componentInstance;

    cmp.requestToggleWorker(workerUnmanaged);
    expect(notify.failure).toHaveBeenCalledWith('Worker deactivation failed', failure);
    expect(cmp.togglingId()).toBeNull();
  });

  it('keeps a failed edit inside the open Edit Worker dialog', async () => {
    const fixture = setup({ access: writable, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const workers = TestBed.inject(WorkersService) as unknown as { patchWorker: ReturnType<typeof vi.fn> };
    workers.patchWorker.mockReturnValue(throwError(() => new ApiError('Name is already used by another worker', 409)));
    const cmp = fixture.componentInstance;

    cmp.openRenameDialog(workerUnmanaged);
    fixture.detectChanges();
    cmp.newName = 'Renamed';
    cmp.confirmRename();
    fixture.detectChanges();

    const dialog = document.querySelector('.gr-dialog')!;
    expect(dialog.querySelector('gr-message-banner')?.textContent).toContain('Name is already used by another worker');
    expect(cmp.showRenameDialog()).toBe(true);

    cmp.cancelRename();
    cmp.openRenameDialog(workerUnmanaged);
    fixture.detectChanges();
    expect(document.querySelector('.gr-dialog gr-message-banner')).toBeNull();
  });

  it('keeps the deactivate dialog open while it runs and shows a failure inside it', async () => {
    const live = {
      capabilities: { network: true, secrets: true, exec: false, eval: true, build: true, federate: false },
      architectures: [], system_features: [], max_concurrent_builds: 1, assigned_job_count: 0, draining: false,
    } as unknown as ProjectWorkerEntry['live'];
    const connected = { ...workerUnmanaged, live };
    const fixture = setup({ access: writable, workers: [connected], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const pending = new Subject<string>();
    const workers = TestBed.inject(WorkersService) as unknown as { setWorkerActive: ReturnType<typeof vi.fn> };
    workers.setWorkerActive.mockReturnValue(pending);
    const cmp = fixture.componentInstance;

    cmp.requestToggleWorker(connected);
    fixture.detectChanges();
    const dialog = document.querySelector('.gr-dialog')!;
    buttonsLabelled(dialog, 'Deactivate')[0].click();
    fixture.detectChanges();
    buttonsLabelled(dialog, 'Deactivate')[0].click();
    dialog.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27, bubbles: true }));
    fixture.detectChanges();
    expect(workers.setWorkerActive).toHaveBeenCalledTimes(1);
    expect(cmp.showToggleWarningDialog()).toBe(true);

    pending.error(new ApiError('Worker has running jobs', 409));
    fixture.detectChanges();
    expect(dialog.querySelector('gr-message-banner')?.textContent).toContain('Worker has running jobs');
    expect(cmp.showToggleWarningDialog()).toBe(true);
    expect(notify.failure).not.toHaveBeenCalled();
  });

  it('keeps the Gradient.CI disconnect dialog open while it runs and shows a failure inside it', async () => {
    const fixture = setup({ access: writable, workers: [gciRegistration], caches });
    await settled(fixture);
    const pending = new Subject<string>();
    const workers = TestBed.inject(WorkersService) as unknown as { deleteWorker: ReturnType<typeof vi.fn> };
    workers.deleteWorker.mockReturnValue(pending);
    const cmp = fixture.componentInstance;

    buttonsLabelled(entry(fixture)!, 'Disconnect')[0].click();
    fixture.detectChanges();
    const dialog = document.querySelector('.gr-dialog')!;
    buttonsLabelled(dialog, 'Disconnect')[0].click();
    fixture.detectChanges();
    buttonsLabelled(dialog, 'Disconnect')[0].click();
    (document.querySelector('.gr-dialog-backdrop') as HTMLElement).click();
    fixture.detectChanges();
    expect(workers.deleteWorker).toHaveBeenCalledTimes(1);
    expect(cmp.showGradientCiDisconnect()).toBe(true);

    pending.error(new ApiError('Worker is still building', 409));
    fixture.detectChanges();
    expect(dialog.querySelector('gr-message-banner')?.textContent).toContain('Worker is still building');
    expect(cmp.showGradientCiDisconnect()).toBe(true);
  });
});

describe('WorkersComponent - register and edit validation', () => {
  const writable: AccessState = { managed: false, canEdit: true, canTrigger: true };

  async function openRegister() {
    const fixture = setup({ access: writable, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const registerWorker = vi.fn(() => of({ peer_id: 'project-uuid', token: 'generated' }));
    Object.assign(TestBed.inject(WorkersService), { registerWorker });
    fixture.componentInstance.openRegisterDialog();
    await settled(fixture);
    return { fixture, registerWorker, dialog: document.querySelector('.gr-dialog')! };
  }

  const errors = (dialog: Element) =>
    Array.from(dialog.querySelectorAll('.field-error')).map((e) => e.textContent?.trim() ?? '');

  it('names the missing fields on submit and sends nothing', async () => {
    const { fixture, registerWorker, dialog } = await openRegister();
    buttonsLabelled(dialog, 'Register')[0].click();
    fixture.detectChanges();
    expect(errors(dialog)).toEqual(['Enter a worker name.', 'Enter a worker ID.']);
    expect(registerWorker).not.toHaveBeenCalled();
  });

  it('refuses a worker ID that is not a UUID and a token that is not base64', async () => {
    const { fixture, registerWorker, dialog } = await openRegister();
    const cmp = fixture.componentInstance;
    cmp.newWorkerName = 'Builder';
    cmp.newWorkerId = 'builder-1';
    cmp.newWorkerToken = 'not base64!';
    cmp.registerWorker();
    await settled(fixture);
    expect(errors(dialog)).toEqual([
      expect.stringContaining('UUID'),
      expect.stringContaining('base64'),
    ]);
    expect(registerWorker).not.toHaveBeenCalled();
  });

  it('generates a UUID worker ID that registers', async () => {
    const { fixture, registerWorker, dialog } = await openRegister();
    const cmp = fixture.componentInstance;
    cmp.newWorkerName = 'Builder';
    buttonsLabelled(dialog, 'Generate')[0].click();
    expect(cmp.workerIdError).toBeNull();
    cmp.registerWorker();
    expect(registerWorker).toHaveBeenCalledWith('demo', cmp.newWorkerId, 'Builder', undefined, undefined, expect.anything());
  });

  it('closes the Edit Worker dialog without a request when nothing changed', async () => {
    const fixture = setup({ access: writable, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const workers = TestBed.inject(WorkersService) as unknown as { patchWorker: ReturnType<typeof vi.fn> };
    const cmp = fixture.componentInstance;
    cmp.openRenameDialog(workerUnmanaged);
    cmp.confirmRename();
    expect(workers.patchWorker).not.toHaveBeenCalled();
    expect(cmp.showRenameDialog()).toBe(false);
  });

  it('refuses an empty worker name in the Edit Worker dialog', async () => {
    const fixture = setup({ access: writable, workers: [workerUnmanaged], caches: [{ id: 'c', name: 'c' }] });
    await settled(fixture);
    const workers = TestBed.inject(WorkersService) as unknown as { patchWorker: ReturnType<typeof vi.fn> };
    const cmp = fixture.componentInstance;
    cmp.openRenameDialog(workerUnmanaged);
    await settled(fixture);
    cmp.newName = ' ';
    cmp.confirmRename();
    await settled(fixture);
    expect(errors(document.querySelector('.gr-dialog')!)).toEqual(['Enter a worker name.']);
    expect(workers.patchWorker).not.toHaveBeenCalled();
  });
});
