import { MemoryAdapter } from "./memory";
import type { JobRecord, QueueEvent, QueueMeta, RepeatableRecord, WorkflowRecord } from "./types";

export type FileSnapshot = {
  jobs: JobRecord[];
  queues: QueueMeta[];
  workflows: WorkflowRecord[];
  events: QueueEvent[];
  repeatable: RepeatableRecord[];
};

export type FileIO = {
  load: () => Promise<FileSnapshot | null> | FileSnapshot | null;
  save: (snap: FileSnapshot) => Promise<void> | void;
};

/**
 * Durable local adapter. Wraps memory and flushes a JSON snapshot.
 *
 * Bun:
 *   new FileAdapter({
 *     load: () => Bun.file("bunmq.json").json().catch(() => null),
 *     save: (snap) => Bun.write("bunmq.json", JSON.stringify(snap)),
 *   })
 *
 * Browser:
 *   new FileAdapter(localStorageIO("bunmq"))
 */
export class FileAdapter extends MemoryAdapter {
  override readonly kind: string = "file";
  private io: FileIO;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private debounceMs: number;
  readonly ready: Promise<void>;

  constructor(opts: FileIO & { debounceMs?: number }) {
    super();
    this.io = opts;
    this.debounceMs = opts.debounceMs ?? 40;
    this.ready = this.boot();
  }

  private async boot() {
    const snap = await this.io.load();
    if (snap) this.hydrate(snap);
  }

  private schedule() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      void this.flush();
    }, this.debounceMs);
  }

  async flush() {
    await this.ready;
    await this.io.save(this.snapshot());
  }

  private persist<T>(fn: () => Promise<T>): Promise<T> {
    return this.ready.then(fn).then((v) => {
      this.schedule();
      return v;
    });
  }

  private after<T>(fn: () => Promise<T>): Promise<T> {
    return this.ready.then(fn);
  }

  override addJob(job: JobRecord) {
    return this.persist(() => super.addJob(job));
  }
  override addJobs(jobs: JobRecord[]) {
    return this.persist(() => super.addJobs(jobs));
  }
  override updateJob(job: JobRecord) {
    return this.persist(() => super.updateJob(job));
  }
  override removeJob(namespace: string, queue: string, id: string) {
    return this.persist(() => super.removeJob(namespace, queue, id));
  }
  override claimNext(
    namespace: string | "*",
    queue: string,
    workerId: string,
    now: number,
    lockUntil: number,
  ) {
    return this.persist(() => super.claimNext(namespace, queue, workerId, now, lockUntil));
  }
  override setQueueMeta(meta: QueueMeta) {
    return this.persist(() => super.setQueueMeta(meta));
  }
  override saveWorkflow(wf: WorkflowRecord) {
    return this.persist(() => super.saveWorkflow(wf));
  }
  override appendEvent(event: QueueEvent) {
    return this.persist(() => super.appendEvent(event));
  }
  override upsertRepeatable(job: RepeatableRecord) {
    return this.persist(() => super.upsertRepeatable(job));
  }
  override removeRepeatable(id: string) {
    return this.persist(() => super.removeRepeatable(id));
  }
  override releaseStalled(now: number) {
    return this.persist(() => super.releaseStalled(now));
  }
  override clean(namespace: string, queue: string, status: Parameters<MemoryAdapter["clean"]>[2], olderThan: number, limit: number) {
    return this.persist(() => super.clean(namespace, queue, status, olderThan, limit));
  }
  override reset() {
    return this.persist(() => super.reset?.() ?? Promise.resolve());
  }
  override getJob(namespace: string, queue: string, id: string) {
    return this.after(() => super.getJob(namespace, queue, id));
  }
  override listJobs(filter: Parameters<MemoryAdapter["listJobs"]>[0]) {
    return this.after(() => super.listJobs(filter));
  }
}

export function localStorageIO(key = "bunmq"): FileIO {
  return {
    load: () => {
      if (typeof localStorage === "undefined") return null;
      const raw = localStorage.getItem(key);
      if (!raw) return null;
      try {
        return JSON.parse(raw) as FileSnapshot;
      } catch {
        return null;
      }
    },
    save: (snap) => {
      if (typeof localStorage === "undefined") return;
      localStorage.setItem(key, JSON.stringify(snap));
    },
  };
}

export class LocalStorageAdapter extends FileAdapter {
  override readonly kind: string = "localStorage";
  constructor(key = "bunmq", debounceMs = 80) {
    super({ ...localStorageIO(key), debounceMs });
  }
}
