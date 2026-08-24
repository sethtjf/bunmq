import type { Adapter } from "./adapter";
import { emptyCounts } from "./adapter";
import { Emitter } from "./emitter";
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
import { asArray, clock, qk } from "./util";

/**
 * Minimal Redis surface. Works with Bun's `RedisClient` or any ioredis-like
 * wrapper — you inject the client, bunmq never imports a Redis package.
 */
export type RedisLike = {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<unknown>;
  del(...keys: string[]): Promise<unknown>;
  sadd(key: string, ...members: string[]): Promise<unknown>;
  srem(key: string, ...members: string[]): Promise<unknown>;
  smembers(key: string): Promise<string[]>;
  zadd(key: string, score: number, member: string): Promise<unknown>;
  zrem(key: string, ...members: string[]): Promise<unknown>;
  zrange(key: string, min: number, max: number): Promise<string[]>;
  zrangebyscore?(key: string, min: number | string, max: number | string): Promise<string[]>;
};

const PREFIX = "bunmq";

export class RedisAdapter extends Emitter implements Adapter {
  readonly kind = "redis";

  constructor(private redis: RedisLike) {
    super();
  }

  private jobKey(id: string) {
    return `${PREFIX}:job:${id}`;
  }
  private setKey(namespace: string, queue: string, status: string) {
    return `${PREFIX}:q:${namespace}:${queue}:${status}`;
  }
  private metaKey(namespace: string, queue: string) {
    return `${PREFIX}:meta:${qk(namespace, queue)}`;
  }

  private async saveJob(job: JobRecord) {
    await this.redis.set(this.jobKey(job.id), JSON.stringify(job));
    await this.redis.sadd(`${PREFIX}:ids`, job.id);
    await this.redis.sadd(`${PREFIX}:queues`, qk(job.namespace, job.queue));
  }

  private async readJob(id: string): Promise<JobRecord | null> {
    const raw = await this.redis.get(this.jobKey(id));
    return raw ? (JSON.parse(raw) as JobRecord) : null;
  }

  private async index(job: JobRecord, prev?: JobRecord | null) {
    if (prev) {
      await this.redis.srem(this.setKey(prev.namespace, prev.queue, prev.status), job.id);
    }
    await this.redis.sadd(this.setKey(job.namespace, job.queue, job.status), job.id);
  }

  async addJob(job: JobRecord): Promise<JobRecord> {
    if (job.idempotencyKey) {
      const ids = await this.redis.smembers(`${PREFIX}:ids`);
      for (const id of ids) {
        const existing = await this.readJob(id);
        if (
          existing &&
          existing.namespace === job.namespace &&
          existing.queue === job.queue &&
          existing.idempotencyKey === job.idempotencyKey &&
          ["waiting", "delayed", "active", "waiting-children"].includes(existing.status)
        ) {
          return existing;
        }
      }
    }
    await this.saveJob(job);
    await this.index(job);
    return job;
  }

  async addJobs(jobs: JobRecord[]): Promise<JobRecord[]> {
    const out: JobRecord[] = [];
    for (const j of jobs) out.push(await this.addJob(j));
    return out;
  }

  async getJob(_namespace: string, _queue: string, id: string): Promise<JobRecord | null> {
    return this.readJob(id);
  }

  async updateJob(job: JobRecord): Promise<void> {
    const prev = await this.readJob(job.id);
    await this.saveJob(job);
    await this.index(job, prev);
  }

  async removeJob(_namespace: string, _queue: string, id: string): Promise<void> {
    const prev = await this.readJob(id);
    if (prev) await this.redis.srem(this.setKey(prev.namespace, prev.queue, prev.status), id);
    await this.redis.del(this.jobKey(id));
    await this.redis.srem(`${PREFIX}:ids`, id);
  }

  async claimNext(
    namespace: string | "*",
    queue: string,
    workerId: string,
    now: number,
    lockUntil: number,
  ): Promise<JobRecord | null> {
    const ids = await this.redis.smembers(`${PREFIX}:ids`);
    const jobs: JobRecord[] = [];
    for (const id of ids) {
      const job = await this.readJob(id);
      if (!job) continue;
      if (namespace !== "*" && job.namespace !== namespace) continue;
      if (queue !== "*" && job.queue !== queue) continue;
      const meta = await this.getQueueMeta(job.namespace, job.queue);
      if (meta.paused) continue;
      if (job.status === "delayed" && job.processAt <= now) {
        job.status = "waiting";
        await this.updateJob(job);
      }
      if (job.status === "waiting" && job.processAt <= now) jobs.push(job);
    }
    jobs.sort((a, b) => b.priority - a.priority || a.timestamp - b.timestamp);
    const activeCounts = new Map<string, number>();
    const groupCounts = new Map<string, number>();
    for (const id of ids) {
      const j = await this.readJob(id);
      if (j?.status === "active") {
        const k = qk(j.namespace, j.queue);
        activeCounts.set(k, (activeCounts.get(k) ?? 0) + 1);
        if (j.groupId) {
          const g = `${k}::${j.groupId}`;
          groupCounts.set(g, (groupCounts.get(g) ?? 0) + 1);
        }
      }
    }
    for (const job of jobs) {
      const meta = await this.getQueueMeta(job.namespace, job.queue);
      if (meta.concurrency != null && (activeCounts.get(qk(job.namespace, job.queue)) ?? 0) >= meta.concurrency) {
        continue;
      }
      if (job.groupId) {
        const g = `${qk(job.namespace, job.queue)}::${job.groupId}`;
        if ((groupCounts.get(g) ?? 0) >= job.groupMax) continue;
      }
      job.status = "active";
      job.attempts += 1;
      job.processedOn = now;
      job.lockUntil = lockUntil;
      job.lockToken = workerId;
      await this.updateJob(job);
      return job;
    }
    return null;
  }

  async renewLock(
    _namespace: string,
    _queue: string,
    id: string,
    token: string,
    lockUntil: number,
  ): Promise<boolean> {
    const job = await this.readJob(id);
    if (!job || job.lockToken !== token || job.status !== "active") return false;
    job.lockUntil = lockUntil;
    await this.saveJob(job);
    return true;
  }

  async listJobs(filter: JobFilter): Promise<JobRecord[]> {
    const ids = await this.redis.smembers(`${PREFIX}:ids`);
    const statuses = asArray(filter.status);
    const rows: JobRecord[] = [];
    for (const id of ids) {
      const job = await this.readJob(id);
      if (!job) continue;
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
    return rows.slice(start, start + limit);
  }

  async countJobs(filter: CountFilter): Promise<Record<JobStatus, number>> {
    const counts = emptyCounts();
    const jobs = await this.listJobs({ namespace: filter.namespace, queue: filter.queue, limit: 100_000 });
    for (const j of jobs) counts[j.status] += 1;
    return counts;
  }

  async getQueueMeta(namespace: string, queue: string): Promise<QueueMeta> {
    const raw = await this.redis.get(this.metaKey(namespace, queue));
    if (!raw) return { namespace, name: queue, paused: false, concurrency: null };
    return JSON.parse(raw) as QueueMeta;
  }

  async setQueueMeta(meta: QueueMeta): Promise<void> {
    await this.redis.set(this.metaKey(meta.namespace, meta.name), JSON.stringify(meta));
    await this.redis.sadd(`${PREFIX}:queues`, qk(meta.namespace, meta.name));
  }

  async listQueues(namespace?: string): Promise<QueueMeta[]> {
    const keys = await this.redis.smembers(`${PREFIX}:queues`);
    const out: QueueMeta[] = [];
    for (const k of keys) {
      const [t, name] = k.split("::");
      if (!t || !name) continue;
      if (namespace && t !== namespace) continue;
      out.push(await this.getQueueMeta(t, name));
    }
    return out;
  }

  async upsertRepeatable(job: RepeatableRecord): Promise<void> {
    await this.redis.set(`${PREFIX}:rep:${job.id}`, JSON.stringify(job));
    await this.redis.sadd(`${PREFIX}:reps`, job.id);
  }

  async listRepeatable(namespace: string, queue?: string): Promise<RepeatableRecord[]> {
    const ids = await this.redis.smembers(`${PREFIX}:reps`);
    const out: RepeatableRecord[] = [];
    for (const id of ids) {
      const raw = await this.redis.get(`${PREFIX}:rep:${id}`);
      if (!raw) continue;
      const r = JSON.parse(raw) as RepeatableRecord;
      if (r.namespace !== namespace) continue;
      if (queue && r.queue !== queue) continue;
      out.push(r);
    }
    return out;
  }

  async removeRepeatable(id: string): Promise<void> {
    await this.redis.del(`${PREFIX}:rep:${id}`);
    await this.redis.srem(`${PREFIX}:reps`, id);
  }

  async dueRepeatable(now: number): Promise<RepeatableRecord[]> {
    const ids = await this.redis.smembers(`${PREFIX}:reps`);
    const out: RepeatableRecord[] = [];
    for (const id of ids) {
      const raw = await this.redis.get(`${PREFIX}:rep:${id}`);
      if (!raw) continue;
      const r = JSON.parse(raw) as RepeatableRecord;
      if (r.next <= now) out.push(r);
    }
    return out;
  }

  async appendEvent(event: QueueEvent): Promise<void> {
    await this.redis.set(`${PREFIX}:evt:${event.id}`, JSON.stringify(event));
    await this.redis.zadd(`${PREFIX}:evts`, event.timestamp, event.id);
    this.emit(event.type, event);
    this.emit("event", event);
  }

  async listEvents(filter: EventFilter): Promise<QueueEvent[]> {
    const ids = this.redis.zrangebyscore
      ? await this.redis.zrangebyscore(`${PREFIX}:evts`, filter.since ?? 0, clock.now() + 1)
      : await this.redis.zrange(`${PREFIX}:evts`, 0, -1);
    const types = asArray(filter.type);
    const out: QueueEvent[] = [];
    for (const id of ids) {
      const raw = await this.redis.get(`${PREFIX}:evt:${id}`);
      if (!raw) continue;
      const e = JSON.parse(raw) as QueueEvent;
      if (filter.namespace && e.namespace !== filter.namespace) continue;
      if (filter.queue && e.queue !== filter.queue) continue;
      if (types && !types.includes(e.type)) continue;
      if (filter.jobId && e.jobId !== filter.jobId) continue;
      if (filter.since && e.timestamp < filter.since) continue;
      out.push(e);
    }
    return out.slice(-(filter.limit ?? 200));
  }

  async releaseStalled(now: number): Promise<JobRecord[]> {
    const ids = await this.redis.smembers(`${PREFIX}:ids`);
    const stalled: JobRecord[] = [];
    for (const id of ids) {
      const job = await this.readJob(id);
      if (!job || job.status !== "active") continue;
      if (job.lockUntil != null && job.lockUntil > now) continue;
      const prev = { ...job };
      job.status = "waiting";
      job.lockUntil = null;
      job.lockToken = null;
      job.processAt = now;
      await this.saveJob(job);
      await this.index(job, prev);
      stalled.push(job);
    }
    return stalled;
  }

  async clean(
    namespace: string,
    queue: string,
    status: JobStatus,
    olderThan: number,
    limit: number,
  ): Promise<number> {
    const jobs = await this.listJobs({ namespace, queue, status, limit: 10_000 });
    let n = 0;
    for (const job of jobs) {
      if ((job.finishedOn ?? job.timestamp) > olderThan) continue;
      await this.removeJob(namespace, queue, job.id);
      n += 1;
      if (n >= limit) break;
    }
    return n;
  }

  async saveWorkflow(wf: WorkflowRecord): Promise<void> {
    await this.redis.set(`${PREFIX}:wf:${wf.id}`, JSON.stringify(wf));
    await this.redis.sadd(`${PREFIX}:wfs`, wf.id);
  }

  async getWorkflow(id: string): Promise<WorkflowRecord | null> {
    const raw = await this.redis.get(`${PREFIX}:wf:${id}`);
    return raw ? (JSON.parse(raw) as WorkflowRecord) : null;
  }

  async listWorkflows(namespace: string, limit = 50): Promise<WorkflowRecord[]> {
    const ids = await this.redis.smembers(`${PREFIX}:wfs`);
    const out: WorkflowRecord[] = [];
    for (const id of ids) {
      const wf = await this.getWorkflow(id);
      if (wf && wf.namespace === namespace) out.push(wf);
    }
    out.sort((a, b) => b.createdAt - a.createdAt);
    return out.slice(0, limit);
  }

  async takeRateLimit(key: string, max: number, duration: number, now: number): Promise<number> {
    const k = `${PREFIX}:rl:${key}`;
    const raw = await this.redis.get(k);
    let stamps: number[] = raw ? (JSON.parse(raw) as number[]) : [];
    stamps = stamps.filter((t) => t > now - duration);
    if (stamps.length >= max) return stamps[0]! + duration - now;
    stamps.push(now);
    await this.redis.set(k, JSON.stringify(stamps));
    return 0;
  }
}
