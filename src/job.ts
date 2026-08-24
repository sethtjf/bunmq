import type { Adapter } from "./adapter";
import { JobNotFoundError, LockError } from "./errors";
import type { JobOptions, JobRecord, JobStatus } from "./types";
import { clock, clone, id } from "./util";

export class Job<T = unknown> {
  constructor(
    public record: JobRecord<T>,
    private adapter: Adapter,
  ) {}

  get id() {
    return this.record.id;
  }
  get name() {
    return this.record.name;
  }
  get queue() {
    return this.record.queue;
  }
  get namespace() {
    return this.record.namespace;
  }
  get data(): T {
    return this.record.data;
  }
  get status(): JobStatus {
    return this.record.status;
  }
  get attempts() {
    return this.record.attempts;
  }
  get progress() {
    return this.record.progress;
  }
  get opts(): JobOptions {
    return this.record.opts;
  }
  get returnvalue() {
    return this.record.returnvalue;
  }
  get failedReason() {
    return this.record.failedReason;
  }

  async updateProgress(progress: number | Record<string, unknown>): Promise<void> {
    this.record.progress = progress;
    await this.adapter.updateJob(this.record as JobRecord);
    await this.adapter.appendEvent({
      id: id("evt"),
      namespace: this.record.namespace,
      queue: this.record.queue,
      jobId: this.record.id,
      type: "progress",
      payload: progress,
      timestamp: clock.now(),
    });
  }

  async log(message: string): Promise<void> {
    this.record.logs.push({ ts: clock.now(), message });
    if (this.record.logs.length > 100) this.record.logs.splice(0, this.record.logs.length - 100);
    await this.adapter.updateJob(this.record as JobRecord);
    await this.adapter.appendEvent({
      id: id("evt"),
      namespace: this.record.namespace,
      queue: this.record.queue,
      jobId: this.record.id,
      type: "log",
      payload: message,
      timestamp: clock.now(),
    });
  }

  async extendLock(ms: number, token: string): Promise<void> {
    const ok = await this.adapter.renewLock(
      this.record.namespace,
      this.record.queue,
      this.record.id,
      token,
      clock.now() + ms,
    );
    if (!ok) throw new LockError(this.record.id);
  }

  async refresh(): Promise<this> {
    const next = await this.adapter.getJob(this.record.namespace, this.record.queue, this.record.id);
    if (!next) throw new JobNotFoundError(this.record.id);
    this.record = next as JobRecord<T>;
    return this;
  }

  toJSON(): JobRecord<T> {
    return clone(this.record);
  }
}

export function createRecord<T>(input: {
  namespace: string;
  queue: string;
  name: string;
  data: T;
  opts: JobOptions;
  parent?: { id: string; queue: string; pending?: number };
  workflowId?: string;
  stepKey?: string;
}): JobRecord<T> {
  const now = clock.now();
  const delay = input.opts.delay ?? 0;
  const status = delay > 0 ? "delayed" : "waiting";
  return {
    id: input.opts.jobId ?? id("job"),
    namespace: input.namespace,
    queue: input.queue,
    name: input.name,
    data: input.data,
    status,
    priority: input.opts.priority ?? 0,
    attempts: 0,
    maxAttempts: input.opts.attempts ?? 1,
    progress: 0,
    delay,
    timestamp: now,
    processAt: now + delay,
    processedOn: null,
    finishedOn: null,
    failedReason: null,
    stacktrace: [],
    returnvalue: null,
    parentId: input.parent?.id ?? input.opts.parent?.id ?? null,
    parentQueue: input.parent?.queue ?? input.opts.parent?.queue ?? null,
    workflowId: input.workflowId ?? null,
    stepKey: input.stepKey ?? null,
    pendingChildren: 0,
    groupId: input.opts.group?.id ?? null,
    groupMax: input.opts.group?.max ?? 1,
    idempotencyKey: input.opts.idempotencyKey ?? null,
    lockUntil: null,
    lockToken: null,
    opts: { ...input.opts },
    logs: [],
  };
}
