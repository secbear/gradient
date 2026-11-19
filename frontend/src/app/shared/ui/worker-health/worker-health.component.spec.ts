/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { TestBed } from '@angular/core/testing';
import { WorkerHealthComponent } from './worker-health.component';

describe('WorkerHealthComponent', () => {
  afterEach(() => vi.useRealTimers());

  it('shows server report receipt age and stops claiming readiness when the report expires', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-11-18T18:00:00Z'));
    TestBed.configureTestingModule({ imports: [WorkerHealthComponent] });
    const fixture = TestBed.createComponent(WorkerHealthComponent);
    fixture.componentRef.setInput('connected', true);
    fixture.componentRef.setInput('health', {
      ready: true,
      checks: [{ name: 'Nix store', status: 'passed', healthy: true }],
      observed_at: '2026-11-18T17:55:00',
      expires_at: '2026-11-18T18:00:01',
    });
    fixture.detectChanges();
    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector('gr-badge')?.textContent?.trim()).toBe('Ready');
    root.querySelector<HTMLButtonElement>('[aria-label="Worker health checks"]')!.click();
    fixture.detectChanges();
    expect(document.querySelector('.health-report')?.textContent).toContain('Report received 5m ago');
    vi.advanceTimersByTime(1_000);
    fixture.detectChanges();
    expect(root.querySelector('gr-badge')?.textContent?.trim()).toBe('Health expired');
    expect(document.querySelector('.health-report')?.textContent).toContain('Report expired.');
  });
});
