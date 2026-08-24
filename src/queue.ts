import type { Adapter } from "./adapter";
import { JobNotFoundError } from "./errors";
import { Job, createRecord } from "./job";
import type {
  JobFilter,
  JobOptions,
  JobRecord,
  JobStatus,
  QueueMeta,
  RepeatableRecord,
} from "./types";
import { clock, id, nextCron } from "./util";

export type QueueOptions = {
  adapter: Adapter;
  tenant?: string;
  defaultJobOptions?: JobOptions;
  concurrency?: number | null;
};

export type BulkJob<T = unknown> = {
  name: string;
  data: T;
  opts?: JobOptions;
};

export class Queue<T = unknown> {
  readonly name: string;
  readonly tenant: string;
  readonly adapter: Adapter;
  private defaults: JobOptions;

  constructor(name: string, opts: QueueOptions) {
    this.name = name;
    this.tenant = opts.tenant ?? "default";
    this.adapter = opts.adapter;
    this.defaults = opts.defaultJobOptions ?? {};
    void this.adapter.setQueueMeta({
      tenant: this.tenant,
      name: this.name,
      paused: false,
      concurrency: opts.concurrency ?? null,
    });
  }

  async add(name: string, data: T, opts: JobOptions = {}): Promise<Job<T>> {
    const merged: JobOptions = { ...this.defaults, ...opts };
    if (merged.repeat) {
      await this.addRepeatable(name, data, merged);
    }
    const record = createRecord({
      tenant: merged.tenant ?? this.tenant,
      queue: this.name,
      name,
      data,
      opts: merged,
    });
    const stored = (await this.adapter.addJob(record as JobRecord)) as JobRecord<T>;
    await this.adapter.appendEvent({
      id: id("evt"),
      tenant: stored.tenant,
      queue: stored.queue,
      jobId: stored.id,
      type: stored.status === "delayed" ? "delayed" : "added",
      payload: { name: stored.name },
      timestamp: clock.now(),
    });
    return new Job(stored, this.adapter);
  }

  async addBulk(jobs: BulkJob<T>[]): Promise<Job<T>[]> {
    const records = jobs.map((j) => {
      const merged: JobOptions = { ...this.defaults, ...j.opts };
      return createRecord({
        tenant: merged.tenant ?? this.tenant,
        queue: this.name,
        name: j.name,
        data: j.data,
        opts: merged,
      }) as JobRecord;
    });
    const stored = await this.adapter.addJobs(records);
    const ts = clock.now();
    await Promise.all(
      stored.map((s) =>
        this.adapter.appendEvent({
          id: id("evt"),
          tenant: s.tenant,
          queue: s.queue,
          jobId: s.id,
          type: "added",
          payload: { name: s.name },
          timestamp: ts,
        }),
      ),
    );
    return stored.map((s) => new Job(s as JobRecord<T>, this.adapter));
  }

  private async addRepeatable(name: string, data: T, opts: JobOptions) {
    const repeat = opts.repeat!;
    const key =
      repeat.key ?? `${this.tenant}:${this.name}:${name}:${repeat.cron ?? repeat.every ?? ""}`;
    const now = clock.now();
    const next =
      repeat.cron != null ? nextCron(repeat.cron, now) : now + (repeat.every ?? 0);
    const rec: RepeatableRecord = {
      id: id("rep"),
      tenant: this.tenant,
      queue: this.name,
      name,
      data,
      opts,
      every: repeat.every ?? null,
      cron: repeat.cron ?? null,
      tz: repeat.tz ?? null,
      limit: repeat.limit ?? null,
      count: 0,
      next: repeat.immediately ? now : next,
      key,
    };
    const existing = (await this.adapter.listRepeatable(this.tenant, this.name)).find(
      (r) => r.key === key,
    );
    if (existing) rec.id = existing.id;
    await this.adapter.upsertRepeatable(rec);
  }

  async getJob(jobId: string): Promise<Job<T> | null> {
    if (!jobId) return null;
    const row = await this.adapter.getJob(this.tenant, this.name, jobId);
    return row ? new Job(row as JobRecord<T>, this.adapter) : null;
  }

  async getJobs(
    status?: JobStatus | JobStatus[],
    start = 0,
    limit = 50,
  ): Promise<Job<T>[]> {
    const filter: JobFilter = {
      tenant: this.tenant,
      queue: this.name,
      status,
      start,
      limit,
    };
    const rows = await this.adapter.listJobs(filter);
    return rows.map((r) => new Job(r as JobRecord<T>, this.adapter));
  }

  async getJobCounts() {
    return this.adapter.countJobs({ tenant: this.tenant, queue: this.name });
  }

  async pause(): Promise<void> {
    const meta = await this.adapter.getQueueMeta(this.tenant, this.name);
    meta.paused = true;
    await this.adapter.setQueueMeta(meta);
    await this.adapter.appendEvent({
      id: id("evt"),
      tenant: this.tenant,
      queue: this.name,
      jobId: null,
      type: "paused",
      payload: null,
      timestamp: clock.now(),
    });
  }

  async resume(): Promise<void> {
    const meta = await this.adapter.getQueueMeta(this.tenant, this.name);
    meta.paused = false;
    await this.adapter.setQueueMeta(meta);
    await this.adapter.appendEvent({
      id: id("evt"),
      tenant: this.tenant,
      queue: this.name,
      jobId: null,
      type: "resumed",
      payload: null,
      timestamp: clock.now(),
    });
  }

  async isPaused(): Promise<boolean> {
    const meta = await this.adapter.getQueueMeta(this.tenant, this.name);
    return meta.paused;
  }

  async setConcurrency(n: number | null): Promise<void> {
    const meta = await this.adapter.getQueueMeta(this.tenant, this.name);
    meta.concurrency = n;
    await this.adapter.setQueueMeta(meta);
  }

  async retry(jobId: string): Promise<Job<T>> {
    const row = await this.adapter.getJob(this.tenant, this.name, jobId);
    if (!row) throw new JobNotFoundError(jobId);
    row.status = "waiting";
    row.failedReason = null;
    row.stacktrace = [];
    row.finishedOn = null;
    row.processAt = clock.now();
    row.lockUntil = null;
    row.lockToken = null;
    await this.adapter.updateJob(row);
    await this.adapter.appendEvent({
      id: id("evt"),
      tenant: this.tenant,
      queue: this.name,
      jobId,
      type: "waiting",
      payload: { retry: true },
      timestamp: clock.now(),
    });
    return new Job(row as JobRecord<T>, this.adapter);
  }

  async remove(jobId: string): Promise<void> {
    await this.adapter.removeJob(this.tenant, this.name, jobId);
    await this.adapter.appendEvent({
      id: id("evt"),
      tenant: this.tenant,
      queue: this.name,
      jobId,
      type: "removed",
      payload: null,
      timestamp: clock.now(),
    });
  }

  async clean(status: JobStatus, graceMs = 0, limit = 1000): Promise<number> {
    return this.adapter.clean(this.tenant, this.name, status, clock.now() - graceMs, limit);
  }

  async obliterate(): Promise<void> {
    for (const status of [
      "waiting",
      "delayed",
      "active",
      "completed",
      "failed",
      "waiting-children",
    ] as JobStatus[]) {
      await this.adapter.clean(this.tenant, this.name, status, clock.now() + 1, 100_000);
    }
  }

  async getRepeatable(): Promise<RepeatableRecord[]> {
    return this.adapter.listRepeatable(this.tenant, this.name);
  }

  async removeRepeatable(repeatableId: string): Promise<void> {
    await this.adapter.removeRepeatable(repeatableId);
  }

  async meta(): Promise<QueueMeta> {
    return this.adapter.getQueueMeta(this.tenant, this.name);
  }
}
