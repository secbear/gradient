/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { TestBed } from '@angular/core/testing';
import { Router, provideRouter } from '@angular/router';
import type { WaitingReason } from '@core/models';
import type { WaitingAccess } from '@shared/evaluation';
import { WaitingNoticeComponent } from './waiting-notice.component';

function render(reason: WaitingReason, access: WaitingAccess, banner = false) {
  TestBed.configureTestingModule({ imports: [WaitingNoticeComponent], providers: [provideRouter([])] });
  const fixture = TestBed.createComponent(WaitingNoticeComponent);
  fixture.componentRef.setInput('reason', reason);
  fixture.componentRef.setInput('project', 'acme');
  fixture.componentRef.setInput('access', access);
  fixture.componentRef.setInput('banner', banner);
  fixture.detectChanges();
  return { fixture, root: fixture.nativeElement as HTMLElement };
}

describe('WaitingNoticeComponent', () => {
  it.each([false, true])('renders repeated transfer labels with banner=%s', (banner) => {
    const warnings = vi.spyOn(console, 'warn');
    const requirement = { architecture: 'x86_64-linux', required_features: ['kvm'], build_count: 1 };
    const reason: WaitingReason = {
      kind: 'workers',
      unmet: [
        requirement,
        { ...requirement, build: true, network: true },
        { ...requirement, build: false, network: true },
        { ...requirement, architecture: 'aarch64-linux', build: false, network: true },
      ],
      connected_workers: 0,
      available_architectures: [],
    };
    const { fixture, root } = render(reason, {
      manageWorkers: true, manageSubscriptions: false, trigger: false, superuser: false,
    }, banner);
    const rows = () => Array.from(root.querySelectorAll('.waiting-requirements li'));
    const expected = [
      'x86_64-linux with feature kvm - 1 build',
      'x86_64-linux with feature kvm with network - 1 build',
      'network transfer - 1 transfer',
      'network transfer - 1 transfer',
    ];
    expect(rows().map(row => row.textContent?.trim())).toEqual(expected);

    fixture.componentRef.setInput('reason', { ...reason, unmet: [...reason.unmet].reverse() });
    fixture.detectChanges();
    expect(rows().map(row => row.textContent?.trim())).toEqual([...expected].reverse());
    expect(warnings).not.toHaveBeenCalled();
    warnings.mockRestore();
  });

  it('links a project admin straight to the page that fixes the wait', () => {
    const { root } = render({ kind: 'no_cache' }, { manageWorkers: true, manageSubscriptions: true, trigger: true, superuser: false });
    expect(root.textContent).toContain('No writable cache');
    expect(root.querySelector('a[grButton]')?.getAttribute('href')).toBe('/project/acme/caches');
  });

  it('shows a viewer whom to ask, with no link', () => {
    const { root } = render({ kind: 'no_cache' }, { manageWorkers: false, manageSubscriptions: false, trigger: false, superuser: false }, true);
    expect(root.querySelector('gr-message-banner')).not.toBeNull();
    expect(root.querySelector('a[grButton]')).toBeNull();
    expect(root.textContent).toContain('Ask a project admin');
  });

  it('offers the pending job next to the fix, or alone when there is nothing to fix', () => {
    const viewer = { manageWorkers: false, manageSubscriptions: false, trigger: false, superuser: false };
    const { fixture, root } = render({ kind: 'no_cache' }, viewer);
    fixture.componentRef.setInput('pendingJobLink', TestBed.inject(Router).createUrlTree(['/board', 'pending', 'e1']));
    fixture.detectChanges();
    expect(Array.from(root.querySelectorAll('a[grButton]')).map((link) => link.getAttribute('href'))).toEqual(['/board/pending/e1']);
    expect(root.textContent).toContain('Ask a project admin');
  });

  it('starts a manual exec job from its button', () => {
    const { fixture, root } = render({ kind: 'manual_start' }, { manageWorkers: false, manageSubscriptions: false, trigger: true, superuser: false });
    const started = vi.fn();
    fixture.componentInstance.start.subscribe(started);
    root.querySelector<HTMLButtonElement>('button[grButton]')?.click();
    expect(started).toHaveBeenCalledOnce();
  });
});
