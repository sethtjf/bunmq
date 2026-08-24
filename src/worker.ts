import type { Adapter } from "./adapter";
import { Emitter } from "./emitter";
import { TimeoutError, UnrecoverableError } from "./errors";
import { Job } from "./job";
import { createRecord } from "./job";
import type { JobRecord, NamespaceScope } from "./types";
import {
  backoffDelay,
  clock,
  errMessage,
  id,
  Limiter,
  nextCron,
  sleep,
  stackLines,
} from "./util";

export type Processor<T = unknown> = (
  job: Job<T>,
  ctx: { signal: AbortSignal },
) => Promise<unknown> | unknown;

export type WorkerOptions = {
  adapter: Adapter;
  namespace?: NamespaceScope;
  concurrency?: number;
  limiter?: { max: number; duration: number };
  lockDuration?: number;
  stallInterval?: number;
  pollInterval?: number;
  autorun?: boolean;
};

type Middleware<T> = (job: Job<T>, next: () => Promise<unknown>) => Promise<unknown>;

export class Worker<T = unknown> extends Emitter {
  readonly name: string;
  readonly namespace: NamespaceScope;
  readonly adapter: Adapter;
  concurrency: number;
  private processor: Processor<T>;
  private lockDuration: number;
  private stallInterval: number;
  private pollInterval: number;
  private limiter: Limiter | null;
  private running = false;
  private paused = false;
  private closing: AbortController | null = null;
  private active = new Set<Promise<void>>();
  private middleware: Middleware<T>[] = [];
  processed = 0;
  failed = 0;
  private workerId = id("w");
  private stallTimer: ReturnType<typeof setInterval> | null = null;
  private repeatTimer: ReturnType<typeof setInterval> | null = null;

  constructor(name: string, processor: Processor<T>, opts: WorkerOptions) {
    super();
    this.name = name;
    this.processor = processor;
    this.adapter = opts.adapter;
    this.namespace = opts.namespace ?? "default";
    this.concurrency = opts.concurrency ?? 1;
    this.lockDuration = opts.lockDuration ?? 30_000;
    this.stallInterval = opts.stallInterval ?? 5_000;
    this.pollInterval = opts.pollInterval ?? 50;
    this.limiter = opts.limiter ? new Limiter(opts.limiter.max, opts.limiter.duration) : null;
    if (opts.autorun !== false) this.run();
  }

  use(fn: Middleware<T>): this {
    this.middleware.push(fn);
    return this;
  }

  run(): void {
    if (this.running) return;
    this.running = true;
    this.closing = new AbortController();
    void this.loop();
    this.stallTimer = setInterval(() => void this.checkStalled(), this.stallInterval);
    this.repeatTimer = setInterval(() => void this.tickRepeatable(), this.pollInterval * 4);
  }

  pause(): void {
    this.paused = true;
  }

  resume(): void {
    this.paused = false;
  }

  get isPaused() {
    return this.paused;
  }

  async close(): Promise<void> {
    this.running = false;
    this.closing?.abort();
    if (this.stallTimer) clearInterval(this.stallTimer);
    if (this.repeatTimer) clearInterval(this.repeatTimer);
    this.stallTimer = null;
    this.repeatTimer = null;
    await Promise.allSettled([...this.active]);
  }

  get activeCount() {
    return this.active.size;
  }

  private async loop() {
    const signal = this.closing!.signal;
    while (this.running) {
      try {
        if (this.paused) {
          await sleep(this.pollInterval, signal);
          continue;
        }
        if (this.active.size >= this.concurrency) {
          await Promise.race([...this.active, sleep(this.pollInterval, signal)]);
          continue;
        }
        if (this.limiter) {
          const wait = this.limiter.take();
          if (wait > 0) {
            await sleep(Math.min(wait, this.pollInterval), signal);
            continue;
          }
        }
        const now = clock.now();
        const claimed = await this.adapter.claimNext(
          this.namespace,
          this.name,
          this.workerId,
          now,
          now + this.lockDuration,
        );
        if (!claimed) {
          await sleep(this.pollInterval, signal);
          continue;
        }
        const work = this.processClaimed(claimed as JobRecord<T>);
        this.active.add(work);
        void work.finally(() => this.active.delete(work));
      } catch (err) {
        if (!this.running) break;
        this.emit("error", err);
        await sleep(this.pollInterval);
      }
    }
  }

  private async processClaimed(record: JobRecord<T>) {
    const job = new Job(record, this.adapter);
    const timeout = record.opts.timeout;
    const ac = new AbortController();
    const onClose = () => ac.abort();
    this.closing?.signal.addEventListener("abort", onClose);
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (timeout && timeout > 0) {
      timer = setTimeout(() => ac.abort(new TimeoutError(job.id, timeout)), timeout);
    }
    const hb = setInterval(() => {
      void job.extendLock(this.lockDuration, this.workerId).catch(() => ac.abort());
    }, Math.max(250, this.lockDuration / 2));

    await this.adapter.appendEvent({
      id: id("evt"),
      namespace: record.namespace,
      queue: record.queue,
      jobId: record.id,
      type: "active",
      payload: { attempts: record.attempts },
      timestamp: clock.now(),
    });
    this.emit("active", job);

    try {
      const result = await this.runProcessor(job, ac.signal);
      await this.complete(job, result);
    } catch (err) {
      await this.fail(job, err);
    } finally {
      clearInterval(hb);
      if (timer) clearTimeout(timer);
      this.closing?.signal.removeEventListener("abort", onClose);
    }
  }

  private async runProcessor(job: Job<T>, signal: AbortSignal): Promise<unknown> {
    const chain = this.middleware.reduceRight<() => Promise<unknown>>(
      (next, mw) => () => mw(job, next),
      () => Promise.resolve(this.processor(job, { signal })),
    );
    if (signal.aborted) throw signal.reason ?? new Error("aborted");
    return chain();
  }

  private async complete(job: Job<T>, result: unknown) {
    const record = job.record;
    record.status = "completed";
    record.returnvalue = result;
    record.finishedOn = clock.now();
    record.lockUntil = null;
    record.lockToken = null;
    record.progress = 100;
    await this.adapter.updateJob(record as JobRecord);
    await this.trim(record as JobRecord, "completed");
    await this.adapter.appendEvent({
      id: id("evt"),
      namespace: record.namespace,
      queue: record.queue,
      jobId: record.id,
      type: "completed",
      payload: { returnvalue: result },
      timestamp: clock.now(),
    });
    this.processed += 1;
    this.emit("completed", job, result);
    await this.onFinished(record as JobRecord, true);
  }

  private async fail(job: Job<T>, err: unknown) {
    const record = job.record;
    const unrecoverable = err instanceof UnrecoverableError;
    const attemptsLeft = !unrecoverable && record.attempts < record.maxAttempts;
    record.failedReason = errMessage(err);
    record.stacktrace = stackLines(err);
    record.lockUntil = null;
    record.lockToken = null;

    if (attemptsLeft) {
      const delay = backoffDelay(record.opts.backoff, record.attempts);
      record.status = delay > 0 ? "delayed" : "waiting";
      record.processAt = clock.now() + delay;
      record.finishedOn = null;
      await this.adapter.updateJob(record as JobRecord);
      await this.adapter.appendEvent({
        id: id("evt"),
        namespace: record.namespace,
        queue: record.queue,
        jobId: record.id,
        type: "delayed",
        payload: { attempt: record.attempts, delay },
        timestamp: clock.now(),
      });
    } else {
      record.status = "failed";
      record.finishedOn = clock.now();
      await this.adapter.updateJob(record as JobRecord);
      await this.trim(record as JobRecord, "failed");
      await this.adapter.appendEvent({
        id: id("evt"),
        namespace: record.namespace,
        queue: record.queue,
        jobId: record.id,
        type: "failed",
        payload: { failedReason: record.failedReason },
        timestamp: clock.now(),
      });
      this.failed += 1;
    }
    this.emit("failed", job, err);
    if (!attemptsLeft) await this.onFinished(record as JobRecord, false);
  }

  private async trim(record: JobRecord, status: "completed" | "failed") {
    const opt = status === "completed" ? record.opts.removeOnComplete : record.opts.removeOnFail;
    if (opt === true) {
      await this.adapter.removeJob(record.namespace, record.queue, record.id);
      return;
    }
    if (typeof opt === "number") {
      const counts = await this.adapter.countJobs({
        namespace: record.namespace,
        queue: record.queue,
      });
      const extra = counts[status] - opt;
      if (extra > 0) {
        await this.adapter.clean(record.namespace, record.queue, status, clock.now() + 1, extra);
      }
    }
  }

  private async onFinished(record: JobRecord, ok: boolean) {
    if (record.parentId && record.parentQueue) {
      const parent = await this.adapter.getJob(record.namespace, record.parentQueue, record.parentId);
      if (parent) {
        if (!ok && record.opts.failParentOnFailure !== false) {
          parent.pendingChildren = Math.max(0, parent.pendingChildren - 1);
          parent.status = "failed";
          parent.failedReason = `child ${record.id} failed: ${record.failedReason}`;
          parent.finishedOn = clock.now();
          await this.adapter.updateJob(parent);
          await this.adapter.appendEvent({
            id: id("evt"),
            namespace: parent.namespace,
            queue: parent.queue,
            jobId: parent.id,
            type: "failed",
            payload: { failedReason: parent.failedReason },
            timestamp: clock.now(),
          });
        } else {
          parent.pendingChildren = Math.max(0, parent.pendingChildren - 1);
          if (parent.pendingChildren === 0 && parent.status === "waiting-children") {
            parent.status = "waiting";
            parent.processAt = clock.now();
            await this.adapter.updateJob(parent);
            await this.adapter.appendEvent({
              id: id("evt"),
              namespace: parent.namespace,
              queue: parent.queue,
              jobId: parent.id,
              type: "waiting",
              payload: { from: "waiting-children" },
              timestamp: clock.now(),
            });
          } else {
            await this.adapter.updateJob(parent);
          }
        }
      }
    }
    this.emit("finished", record, ok);
  }

  private async checkStalled() {
    try {
      const stalled = await this.adapter.releaseStalled(clock.now());
      for (const job of stalled) {
        await this.adapter.appendEvent({
          id: id("evt"),
          namespace: job.namespace,
          queue: job.queue,
          jobId: job.id,
          type: "stalled",
          payload: { attempts: job.attempts },
          timestamp: clock.now(),
        });
        this.emit("stalled", job);
        if (job.attempts >= job.maxAttempts) {
          job.status = "failed";
          job.failedReason = "stalled";
          job.finishedOn = clock.now();
          await this.adapter.updateJob(job);
        }
      }
    } catch (err) {
      this.emit("error", err);
    }
  }

  private async tickRepeatable() {
    try {
      const due = await this.adapter.dueRepeatable(clock.now());
      for (const r of due) {
        if (this.namespace !== "*" && r.namespace !== this.namespace) continue;
        if (this.name !== "*" && r.queue !== this.name) continue;
        const record = createRecord({
          namespace: r.namespace,
          queue: r.queue,
          name: r.name,
          data: r.data,
          opts: { ...r.opts, repeat: undefined, jobId: undefined, delay: 0 },
        });
        await this.adapter.addJob(record);
        r.count += 1;
        if (r.limit != null && r.count >= r.limit) {
          await this.adapter.removeRepeatable(r.id);
        } else {
          r.next = r.cron ? nextCron(r.cron, clock.now()) : clock.now() + (r.every ?? 1000);
          await this.adapter.upsertRepeatable(r);
        }
        await this.adapter.appendEvent({
          id: id("evt"),
          namespace: r.namespace,
          queue: r.queue,
          jobId: record.id,
          type: "added",
          payload: { repeat: r.key },
          timestamp: clock.now(),
        });
      }
    } catch (err) {
      this.emit("error", err);
    }
  }
}
