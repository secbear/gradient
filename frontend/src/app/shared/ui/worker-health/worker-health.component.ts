/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { ChangeDetectionStrategy, Component, computed, inject, input } from '@angular/core';
import { WorkerHealthCheck, WorkerHealthInfo } from '@core/models';
import { BadgeComponent, BadgeSeverity, ButtonComponent, PopoverComponent } from '@gradient/ui/ui';
import { CurrentTimeSignal, RelativeTimePipe, serverTime } from '@shared/text';

const CHECK_LABELS: Record<WorkerHealthCheck['status'], string> = {
  pending: 'Pending',
  passed: 'Passed',
  failed: 'Failed',
  timed_out: 'Timed out',
  error: 'Error',
};

const CHECK_SEVERITIES: Record<WorkerHealthCheck['status'], BadgeSeverity> = {
  pending: 'neutral',
  passed: 'success',
  failed: 'danger',
  timed_out: 'warning',
  error: 'danger',
};

@Component({
  selector: 'app-worker-health',
  standalone: true,
  imports: [BadgeComponent, ButtonComponent, PopoverComponent, RelativeTimePipe],
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrl: './worker-health.component.scss',
  template: `
    <gr-badge [severity]="severity()">{{ label() }}</gr-badge>
    @if (health(); as report) {
      <button grButton type="button" icon="info" severity="secondary" size="small"
        aria-label="Worker health checks" (click)="details.toggle($event)"></button>
      <gr-popover #details>
        <div class="health-report">
          <strong>{{ connected() ? 'Latest health report' : 'Last health report (worker offline)' }}</strong>
          @if (report.observed_at) {
            <p class="text-secondary">Report received {{ report.observed_at | relativeTime }}</p>
          } @else {
            <p class="text-secondary">No health report received.</p>
          }
          @if (expired()) {
            <p class="text-secondary">Report expired. Waiting for a new health report.</p>
          }
          <ul>
            @for (check of report.checks; track check.name) {
              <li>
                <span class="check-name">{{ check.name }}</span>
                <gr-badge [severity]="checkSeverities[check.status]">{{ checkLabels[check.status] }}</gr-badge>
                <span class="text-secondary">{{ check.healthy ? 'Healthy' : 'Unhealthy' }}</span>
              </li>
            }
          </ul>
        </div>
      </gr-popover>
    }
  `,
})
export class WorkerHealthComponent {
  health = input<WorkerHealthInfo | null | undefined>(null);
  connected = input.required<boolean>();
  protected readonly checkLabels = CHECK_LABELS;
  protected readonly checkSeverities = CHECK_SEVERITIES;
  private readonly clock = inject(CurrentTimeSignal);

  protected readonly expired = computed(() => {
    const expiresAt = this.health()?.expires_at;
    return expiresAt ? serverTime(expiresAt).getTime() <= this.clock.currentTimeMilliseconds() : false;
  });

  protected readonly label = computed(() => {
    const report = this.health();
    if (!report) return 'Unverified health';
    if (!this.connected()) return 'Health unavailable';
    if (this.expired()) return 'Health expired';
    return report.ready ? 'Ready' : 'Not ready';
  });

  protected readonly severity = computed<BadgeSeverity>(() => {
    const report = this.health();
    if (!report || !this.connected()) return 'neutral';
    return report.ready && !this.expired() ? 'success' : 'warning';
  });
}
