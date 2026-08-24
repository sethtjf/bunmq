import type { Adapter } from "./adapter";
import { emptyCounts } from "./adapter";
import { Emitter } from "./emitter";
import { LockError } from "./errors";
import type {
  CountFilter,
  EventFilter,
  JobFilter,
  JobRecord,
  JobStatus,
  QueueEvent,
  QueueMeta,
  RepeatableRecord,
  WorkflowRecord,
} from "./types";
import { asArray, clone, Mutex, qk } from "./util";

type GroupKey = string;

function gk(namespace: string, queue: string, groupId: string): GroupKey {
  return `${namespace}::${queue}::${groupId}`;
}

export class MemoryAdapter extends Emitter implements Adapter {
  readonly kind: string = "memory";
  private jobs = new Map<string, JobRecord>();
  private queues = new Map<string, QueueMeta>();
  private repeatable = new Map<string, RepeatableRecord>();
  private events: QueueEvent[] = [];
  private workflows = new Map<string, WorkflowRecord>();
  private groupActive = new Map<GroupKey, number>();
  private rate = new Map<string, number[]>();
  private mutex = new Mutex();
  private eventCap = 800;
  private idIndex = new Map<string, string>(); // job.id -> store key

  private key(namespace: string, queue: string, id: string) {
    return `${namespace}::${queue}::${id}`;
  }

  private store(job: JobRecord) {
    const k = this.key(job.namespace, job.queue, job.id);
    this.jobs.set(k, job);
    this.idIndex.set(job.id, k);
  }

  async addJob(job: JobRecord): Promise<JobRecord> {
    return this.mutex.run(() => this.addJobSync(job));
  }

  async addJobs(jobs: JobRecord[]): Promise<JobRecord[]> {
    return this.mutex.run(() => jobs.map((j) => this.addJobSync(j)));
  }

  private addJobSync(job: JobRecord): JobRecord {
    if (job.idempotencyKey) {
      for (const existing of this.jobs.values()) {
        if (
          existing.namespace === job.namespace &&
          existing.queue === job.queue &&
          existing.idempotencyKey === job.idempotencyKey &&
          (existing.status === "waiting" ||
            existing.status === "delayed" ||
            existing.status === "active" ||
            existing.status === "waiting-children")
        ) {
          return clone(existing);
        }
      }
    }
    const copy = clone(job);
    this.store(copy);
    const q = this.ensureQueue(copy.namespace, copy.queue);
    this.queues.set(qk(copy.namespace, copy.queue), q);
    return clone(copy);
  }

  async getJob(namespace: string, queue: string, id: string): Promise<JobRecord | null> {
    const row = this.jobs.get(this.key(namespace, queue, id));
    return row ? clone(row) : null;
  }

  async updateJob(job: JobRecord): Promise<void> {
    return this.mutex.run(() => {
      const k = this.key(job.namespace, job.queue, job.id);
      const prev = this.jobs.get(k);
      if (!prev) return;
      this.adjustGroup(prev, job);
      this.store(clone(job));
    });
  }

  async removeJob(namespace: string, queue: string, id: string): Promise<void> {
    return this.mutex.run(() => {
      const k = this.key(namespace, queue, id);
      const prev = this.jobs.get(k);
      if (prev) {
        this.adjustGroup(prev, null);
        this.jobs.delete(k);
        this.idIndex.delete(id);
      }
    });
  }

  async claimNext(
    namespace: string | "*",
    queue: string,
    workerId: string,
    now: number,
    lockUntil: number,
  ): Promise<JobRecord | null> {
    return this.mutex.run(() => {
      const candidates: JobRecord[] = [];
      for (const job of this.jobs.values()) {
        if (queue !== "*" && job.queue !== queue) continue;
        if (namespace !== "*" && job.namespace !== namespace) continue;
        const meta = this.ensureQueue(job.namespace, job.queue);
        if (meta.paused) continue;
        if (job.status === "delayed" && job.processAt <= now) {
          job.status = "waiting";
        }
        if (job.status !== "waiting") continue;
        if (job.processAt > now) continue;
        candidates.push(job);
      }
      candidates.sort((a, b) => b.priority - a.priority || a.timestamp - b.timestamp);

      const activeByQueue = new Map<string, number>();
      for (const job of this.jobs.values()) {
        if (job.status === "active") {
          const k = qk(job.namespace, job.queue);
          activeByQueue.set(k, (activeByQueue.get(k) ?? 0) + 1);
        }
      }

      for (const job of candidates) {
        const meta = this.ensureQueue(job.namespace, job.queue);
        if (meta.concurrency != null) {
          const active = activeByQueue.get(qk(job.namespace, job.queue)) ?? 0;
          if (active >= meta.concurrency) continue;
        }
        if (job.groupId) {
          const g = this.groupActive.get(gk(job.namespace, job.queue, job.groupId)) ?? 0;
          if (g >= job.groupMax) continue;
        }
        job.status = "active";
        job.attempts += 1;
        job.processedOn = now;
        job.lockUntil = lockUntil;
        job.lockToken = workerId;
        if (job.groupId) {
          const key = gk(job.namespace, job.queue, job.groupId);
          this.groupActive.set(key, (this.groupActive.get(key) ?? 0) + 1);
        }
        return clone(job);
      }
      return null;
    });
  }

  async renewLock(
    namespace: string,
    queue: string,
    id: string,
    token: string,
    lockUntil: number,
  ): Promise<boolean> {
    return this.mutex.run(() => {
      const job = this.jobs.get(this.key(namespace, queue, id));
      if (!job || job.lockToken !== token || job.status !== "active") return false;
      job.lockUntil = lockUntil;
      return true;
    });
  }

  async listJobs(filter: JobFilter): Promise<JobRecord[]> {
    const statuses = asArray(filter.status);
    const rows: JobRecord[] = [];
    for (const job of this.jobs.values()) {
      if (filter.namespace && job.namespace !== filter.namespace) continue;
      if (filter.queue && job.queue !== filter.queue) continue;
      if (statuses && !statuses.includes(job.status)) continue;
      if (filter.ids && !filter.ids.includes(job.id)) continue;
      if (filter.parentId && job.parentId !== filter.parentId) continue;
      if (filter.workflowId && job.workflowId !== filter.workflowId) continue;
      rows.push(job);
    }
    rows.sort((a, b) => b.timestamp - a.timestamp);
    const start = filter.start ?? 0;
    const limit = filter.limit ?? rows.length;
    return rows.slice(start, start + limit).map(clone);
  }

  async countJobs(filter: CountFilter): Promise<Record<JobStatus, number>> {
    const counts = emptyCounts();
    for (const job of this.jobs.values()) {
      if (filter.namespace && job.namespace !== filter.namespace) continue;
      if (filter.queue && job.queue !== filter.queue) continue;
      counts[job.status] += 1;
    }
    return counts;
  }

  async getQueueMeta(namespace: string, queue: string): Promise<QueueMeta> {
    return clone(this.ensureQueue(namespace, queue));
  }

  async setQueueMeta(meta: QueueMeta): Promise<void> {
    this.queues.set(qk(meta.namespace, meta.name), { ...meta });
  }

  async listQueues(namespace?: string): Promise<QueueMeta[]> {
    const out: QueueMeta[] = [];
    for (const q of this.queues.values()) {
      if (namespace && q.namespace !== namespace) continue;
      out.push(clone(q));
    }
    out.sort((a, b) => a.name.localeCompare(b.name) || a.namespace.localeCompare(b.namespace));
    return out;
  }

  async upsertRepeatable(job: RepeatableRecord): Promise<void> {
    this.repeatable.set(job.id, clone(job));
  }

  async listRepeatable(namespace: string, queue?: string): Promise<RepeatableRecord[]> {
    const out: RepeatableRecord[] = [];
    for (const r of this.repeatable.values()) {
      if (r.namespace !== namespace) continue;
      if (queue && r.queue !== queue) continue;
      out.push(clone(r));
    }
    return out;
  }

  async removeRepeatable(id: string): Promise<void> {
    this.repeatable.delete(id);
  }

  async dueRepeatable(now: number): Promise<RepeatableRecord[]> {
    return [...this.repeatable.values()].filter((r) => r.next <= now).map(clone);
  }

  async appendEvent(event: QueueEvent): Promise<void> {
    this.events.push(clone(event));
    if (this.events.length > this.eventCap) {
      this.events.splice(0, this.events.length - this.eventCap);
    }
    this.emit(event.type, event);
    this.emit("event", event);
  }

  async listEvents(filter: EventFilter): Promise<QueueEvent[]> {
    const types = asArray(filter.type);
    const rows = this.events.filter((e) => {
      if (filter.namespace && e.namespace !== filter.namespace) return false;
      if (filter.queue && e.queue !== filter.queue) return false;
      if (types && !types.includes(e.type)) return false;
      if (filter.jobId && e.jobId !== filter.jobId) return false;
      if (filter.since && e.timestamp < filter.since) return false;
      return true;
    });
    const limit = filter.limit ?? 200;
    return rows.slice(-limit).map(clone);
  }

  async releaseStalled(now: number): Promise<JobRecord[]> {
    return this.mutex.run(() => {
      const stalled: JobRecord[] = [];
      for (const job of this.jobs.values()) {
        if (job.status !== "active") continue;
        if (job.lockUntil != null && job.lockUntil > now) continue;
        this.adjustGroup(job, { ...job, status: "waiting" });
        job.status = "waiting";
        job.lockUntil = null;
        job.lockToken = null;
        job.processAt = now;
        stalled.push(clone(job));
      }
      return stalled;
    });
  }

  async clean(
    namespace: string,
    queue: string,
    status: JobStatus,
    olderThan: number,
    limit: number,
  ): Promise<number> {
    return this.mutex.run(() => {
      const victims: string[] = [];
      for (const [k, job] of this.jobs) {
        if (job.namespace !== namespace || job.queue !== queue) continue;
        if (job.status !== status) continue;
        if ((job.finishedOn ?? job.timestamp) > olderThan) continue;
        victims.push(k);
        if (victims.length >= limit) break;
      }
      for (const k of victims) {
        const job = this.jobs.get(k);
        if (job) {
          this.adjustGroup(job, null);
          this.idIndex.delete(job.id);
        }
        this.jobs.delete(k);
      }
      return victims.length;
    });
  }

  async saveWorkflow(wf: WorkflowRecord): Promise<void> {
    this.workflows.set(wf.id, clone(wf));
  }

  async getWorkflow(id: string): Promise<WorkflowRecord | null> {
    const row = this.workflows.get(id);
    return row ? clone(row) : null;
  }

  async listWorkflows(namespace: string, limit = 50): Promise<WorkflowRecord[]> {
    const rows = [...this.workflows.values()]
      .filter((w) => w.namespace === namespace)
      .sort((a, b) => b.createdAt - a.createdAt)
      .slice(0, limit);
    return rows.map(clone);
  }

  async takeRateLimit(key: string, max: number, duration: number, now: number): Promise<number> {
    const cut = now - duration;
    const stamps = (this.rate.get(key) ?? []).filter((t) => t > cut);
    if (stamps.length >= max) return stamps[0]! + duration - now;
    stamps.push(now);
    this.rate.set(key, stamps);
    return 0;
  }

  async reset(): Promise<void> {
    this.jobs.clear();
    this.queues.clear();
    this.repeatable.clear();
    this.events = [];
    this.workflows.clear();
    this.groupActive.clear();
    this.rate.clear();
    this.idIndex.clear();
  }

  snapshot(): {
    jobs: JobRecord[];
    queues: QueueMeta[];
    workflows: WorkflowRecord[];
    events: QueueEvent[];
    repeatable: RepeatableRecord[];
  } {
    return {
      jobs: [...this.jobs.values()].map(clone),
      queues: [...this.queues.values()].map(clone),
      workflows: [...this.workflows.values()].map(clone),
      events: this.events.slice(-200).map(clone),
      repeatable: [...this.repeatable.values()].map(clone),
    };
  }

  private ensureQueue(namespace: string, name: string): QueueMeta {
    const k = qk(namespace, name);
    let q = this.queues.get(k);
    if (!q) {
      q = { namespace, name, paused: false, concurrency: null };
      this.queues.set(k, q);
    }
    return q;
  }

  private adjustGroup(prev: JobRecord, next: JobRecord | null) {
    const was = prev.status === "active" && prev.groupId;
    const now = next && next.status === "active" && next.groupId;
    if (was && !now) {
      const key = gk(prev.namespace, prev.queue, prev.groupId!);
      const n = (this.groupActive.get(key) ?? 1) - 1;
      if (n <= 0) this.groupActive.delete(key);
      else this.groupActive.set(key, n);
    }
  }

  /** Used by FileAdapter. */
  hydrate(data: {
    jobs?: JobRecord[];
    queues?: QueueMeta[];
    workflows?: WorkflowRecord[];
    events?: QueueEvent[];
    repeatable?: RepeatableRecord[];
  }) {
    this.jobs.clear();
    this.queues.clear();
    this.workflows.clear();
    this.repeatable.clear();
    this.events = [];
    this.groupActive.clear();
    for (const j of data.jobs ?? []) this.store(clone(j));
    for (const q of data.queues ?? []) this.queues.set(qk(q.namespace, q.name), clone(q));
    for (const w of data.workflows ?? []) this.workflows.set(w.id, clone(w));
    for (const r of data.repeatable ?? []) this.repeatable.set(r.id, clone(r));
    this.events = (data.events ?? []).map(clone);
    for (const j of this.jobs.values()) {
      if (j.status === "active" && j.groupId) {
        const key = gk(j.namespace, j.queue, j.groupId);
        this.groupActive.set(key, (this.groupActive.get(key) ?? 0) + 1);
      }
    }
  }
}

export function assertLock(job: JobRecord, token: string) {
  if (job.lockToken !== token) throw new LockError(job.id);
}
