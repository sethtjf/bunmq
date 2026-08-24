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

export type SqlDialect = "sqlite" | "postgres";

export type SqlDriver = {
  dialect: SqlDialect;
  exec(sql: string): Promise<void>;
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]>;
  transaction<T>(fn: () => Promise<T>): Promise<T>;
};

function ph(dialect: SqlDialect, n: number) {
  return dialect === "postgres" ? `$${n}` : "?";
}

function placeholders(dialect: SqlDialect, count: number, start = 1) {
  return Array.from({ length: count }, (_, i) => ph(dialect, start + i)).join(", ");
}

const SCHEMA_SQLITE = `
CREATE TABLE IF NOT EXISTS bunmq_jobs (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  status TEXT NOT NULL,
  priority INTEGER NOT NULL,
  process_at INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  group_id TEXT,
  lock_until INTEGER,
  workflow_id TEXT,
  parent_id TEXT,
  body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bunmq_jobs_claim ON bunmq_jobs (namespace, queue, status, process_at, priority, timestamp);
CREATE INDEX IF NOT EXISTS bunmq_jobs_wf ON bunmq_jobs (workflow_id);
CREATE TABLE IF NOT EXISTS bunmq_queues (
  k TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  paused INTEGER NOT NULL DEFAULT 0,
  concurrency INTEGER
);
CREATE TABLE IF NOT EXISTS bunmq_repeatable (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  next INTEGER NOT NULL,
  body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bunmq_events (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  type TEXT NOT NULL,
  job_id TEXT,
  timestamp INTEGER NOT NULL,
  body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bunmq_events_ts ON bunmq_events (timestamp);
CREATE TABLE IF NOT EXISTS bunmq_workflows (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bunmq_rate (
  k TEXT PRIMARY KEY,
  stamps TEXT NOT NULL
);
`;

const SCHEMA_POSTGRES = `
CREATE TABLE IF NOT EXISTS bunmq_jobs (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  status TEXT NOT NULL,
  priority BIGINT NOT NULL,
  process_at BIGINT NOT NULL,
  timestamp BIGINT NOT NULL,
  group_id TEXT,
  lock_until BIGINT,
  workflow_id TEXT,
  parent_id TEXT,
  body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bunmq_jobs_claim ON bunmq_jobs (namespace, queue, status, process_at, priority, timestamp);
CREATE INDEX IF NOT EXISTS bunmq_jobs_wf ON bunmq_jobs (workflow_id);
CREATE TABLE IF NOT EXISTS bunmq_queues (
  k TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  concurrency INTEGER
);
CREATE TABLE IF NOT EXISTS bunmq_repeatable (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  next BIGINT NOT NULL,
  body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bunmq_events (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  queue TEXT NOT NULL,
  type TEXT NOT NULL,
  job_id TEXT,
  timestamp BIGINT NOT NULL,
  body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bunmq_events_ts ON bunmq_events (timestamp);
CREATE TABLE IF NOT EXISTS bunmq_workflows (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bunmq_rate (
  k TEXT PRIMARY KEY,
  stamps TEXT NOT NULL
);
`;

export class SqlAdapter extends Emitter implements Adapter {
  readonly kind: string;
  private ready: Promise<void>;

  constructor(private driver: SqlDriver) {
    super();
    this.kind = driver.dialect;
    this.ready = this.migrate();
  }

  private p(n: number) {
    return ph(this.driver.dialect, n);
  }

  private async migrate() {
    const schema = this.driver.dialect === "postgres" ? SCHEMA_POSTGRES : SCHEMA_SQLITE;
    await this.driver.exec(schema);
  }

  private async all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    await this.ready;
    return this.driver.query<T>(sql, params);
  }

  private async run(sql: string, params: unknown[] = []) {
    await this.ready;
    await this.driver.query(sql, params);
  }

  private rowFrom(job: JobRecord) {
    return {
      id: job.id,
      namespace: job.namespace,
      queue: job.queue,
      status: job.status,
      priority: job.priority,
      process_at: job.processAt,
      timestamp: job.timestamp,
      group_id: job.groupId,
      lock_until: job.lockUntil,
      workflow_id: job.workflowId,
      parent_id: job.parentId,
      body: JSON.stringify(job),
    };
  }

  private parseJob(row: Record<string, unknown>): JobRecord {
    return JSON.parse(String(row.body)) as JobRecord;
  }

  async addJob(job: JobRecord): Promise<JobRecord> {
    await this.ready;
    if (job.idempotencyKey) {
      const existing = await this.all<Record<string, unknown>>(
        `SELECT body FROM bunmq_jobs WHERE namespace = ${this.p(1)} AND queue = ${this.p(2)} AND body LIKE ${this.p(3)} AND status IN ('waiting','delayed','active','waiting-children') LIMIT 1`,
        [job.namespace, job.queue, `%"idempotencyKey":"${job.idempotencyKey}"%`],
      );
      if (existing[0]) return this.parseJob(existing[0]);
    }
    const r = this.rowFrom(job);
    await this.run(
      `INSERT INTO bunmq_jobs (id, namespace, queue, status, priority, process_at, timestamp, group_id, lock_until, workflow_id, parent_id, body)
       VALUES (${placeholders(this.driver.dialect, 12)})`,
      [
        r.id,
        r.namespace,
        r.queue,
        r.status,
        r.priority,
        r.process_at,
        r.timestamp,
        r.group_id,
        r.lock_until,
        r.workflow_id,
        r.parent_id,
        r.body,
      ],
    );
    return job;
  }

  async addJobs(jobs: JobRecord[]): Promise<JobRecord[]> {
    const out: JobRecord[] = [];
    for (const j of jobs) out.push(await this.addJob(j));
    return out;
  }

  async getJob(namespace: string, queue: string, id: string): Promise<JobRecord | null> {
    const rows = await this.all<Record<string, unknown>>(
      `SELECT body FROM bunmq_jobs WHERE id = ${this.p(1)} AND namespace = ${this.p(2)} AND queue = ${this.p(3)}`,
      [id, namespace, queue],
    );
    return rows[0] ? this.parseJob(rows[0]) : null;
  }

  async updateJob(job: JobRecord): Promise<void> {
    const r = this.rowFrom(job);
    await this.run(
      `UPDATE bunmq_jobs SET status=${this.p(1)}, priority=${this.p(2)}, process_at=${this.p(3)}, timestamp=${this.p(4)},
        group_id=${this.p(5)}, lock_until=${this.p(6)}, workflow_id=${this.p(7)}, parent_id=${this.p(8)}, body=${this.p(9)}
        WHERE id=${this.p(10)}`,
      [
        r.status,
        r.priority,
        r.process_at,
        r.timestamp,
        r.group_id,
        r.lock_until,
        r.workflow_id,
        r.parent_id,
        r.body,
        r.id,
      ],
    );
  }

  async removeJob(_namespace: string, _queue: string, id: string): Promise<void> {
    await this.run(`DELETE FROM bunmq_jobs WHERE id = ${this.p(1)}`, [id]);
  }

  async claimNext(
    namespace: string | "*",
    queue: string,
    workerId: string,
    now: number,
    lockUntil: number,
  ): Promise<JobRecord | null> {
    return this.driver.transaction(async () => {
      await this.run(
        `UPDATE bunmq_jobs SET status='waiting' WHERE status='delayed' AND process_at <= ${this.p(1)}`,
        [now],
      );
      const params: unknown[] = [now];
      let sql = `SELECT j.body, j.namespace, j.queue, j.id FROM bunmq_jobs j
        LEFT JOIN bunmq_queues q ON q.k = ${this.concatNamespaceQueue("j")}
        WHERE j.status='waiting' AND j.process_at <= ${this.p(1)}`;
      let n = 2;
      if (namespace !== "*") {
        sql += ` AND j.namespace = ${this.p(n++)}`;
        params.push(namespace);
      }
      if (queue !== "*") {
        sql += ` AND j.queue = ${this.p(n++)}`;
        params.push(queue);
      }
      sql += ` AND (q.paused IS NULL OR q.paused = ${this.driver.dialect === "postgres" ? "FALSE" : "0"})
        ORDER BY j.priority DESC, j.timestamp ASC LIMIT 24`;
      const rows = await this.all<Record<string, unknown>>(sql, params);
      for (const row of rows) {
        const job = this.parseJob(row);
        const meta = await this.getQueueMeta(job.namespace, job.queue);
        if (meta.paused) continue;
        if (meta.concurrency != null) {
          const [{ c }] = await this.all<{ c: number }>(
            `SELECT COUNT(*) as c FROM bunmq_jobs WHERE namespace=${this.p(1)} AND queue=${this.p(2)} AND status='active'`,
            [job.namespace, job.queue],
          );
          if (Number(c) >= meta.concurrency) continue;
        }
        if (job.groupId) {
          const [{ c }] = await this.all<{ c: number }>(
            `SELECT COUNT(*) as c FROM bunmq_jobs WHERE namespace=${this.p(1)} AND queue=${this.p(2)} AND group_id=${this.p(3)} AND status='active'`,
            [job.namespace, job.queue, job.groupId],
          );
          if (Number(c) >= job.groupMax) continue;
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
    });
  }

  private concatNamespaceQueue(alias: string) {
    if (this.driver.dialect === "postgres") {
      return `(${alias}.namespace || '::' || ${alias}.queue)`;
    }
    return `(${alias}.namespace || '::' || ${alias}.queue)`;
  }

  async renewLock(
    namespace: string,
    queue: string,
    id: string,
    token: string,
    lockUntil: number,
  ): Promise<boolean> {
    const job = await this.getJob(namespace, queue, id);
    if (!job || job.lockToken !== token || job.status !== "active") return false;
    job.lockUntil = lockUntil;
    await this.updateJob(job);
    return true;
  }

  async listJobs(filter: JobFilter): Promise<JobRecord[]> {
    const clauses: string[] = ["1=1"];
    const params: unknown[] = [];
    let n = 1;
    if (filter.namespace) {
      clauses.push(`namespace = ${this.p(n++)}`);
      params.push(filter.namespace);
    }
    if (filter.queue) {
      clauses.push(`queue = ${this.p(n++)}`);
      params.push(filter.queue);
    }
    const statuses = asArray(filter.status);
    if (statuses?.length) {
      clauses.push(`status IN (${statuses.map(() => this.p(n++)).join(",")})`);
      params.push(...statuses);
    }
    if (filter.parentId) {
      clauses.push(`parent_id = ${this.p(n++)}`);
      params.push(filter.parentId);
    }
    if (filter.workflowId) {
      clauses.push(`workflow_id = ${this.p(n++)}`);
      params.push(filter.workflowId);
    }
    const start = filter.start ?? 0;
    const limit = filter.limit ?? 100;
    params.push(limit, start);
    const rows = await this.all<Record<string, unknown>>(
      `SELECT body FROM bunmq_jobs WHERE ${clauses.join(" AND ")} ORDER BY timestamp DESC LIMIT ${this.p(n++)} OFFSET ${this.p(n++)}`,
      params,
    );
    let jobs = rows.map((r) => this.parseJob(r));
    if (filter.ids) jobs = jobs.filter((j) => filter.ids!.includes(j.id));
    return jobs;
  }

  async countJobs(filter: CountFilter): Promise<Record<JobStatus, number>> {
    const counts = emptyCounts();
    const clauses: string[] = [];
    const params: unknown[] = [];
    let n = 1;
    if (filter.namespace) {
      clauses.push(`namespace = ${this.p(n++)}`);
      params.push(filter.namespace);
    }
    if (filter.queue) {
      clauses.push(`queue = ${this.p(n++)}`);
      params.push(filter.queue);
    }
    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const rows = await this.all<{ status: JobStatus; c: number }>(
      `SELECT status, COUNT(*) as c FROM bunmq_jobs ${where} GROUP BY status`,
      params,
    );
    for (const r of rows) counts[r.status] = Number(r.c);
    return counts;
  }

  async getQueueMeta(namespace: string, queue: string): Promise<QueueMeta> {
    const rows = await this.all<Record<string, unknown>>(
      `SELECT * FROM bunmq_queues WHERE k = ${this.p(1)}`,
      [qk(namespace, queue)],
    );
    if (!rows[0]) return { namespace, name: queue, paused: false, concurrency: null };
    const r = rows[0];
    return {
      namespace: String(r.namespace),
      name: String(r.name),
      paused: Boolean(r.paused),
      concurrency: r.concurrency == null ? null : Number(r.concurrency),
    };
  }

  async setQueueMeta(meta: QueueMeta): Promise<void> {
    const paused = this.driver.dialect === "postgres" ? meta.paused : meta.paused ? 1 : 0;
    await this.run(
      `INSERT INTO bunmq_queues (k, namespace, name, paused, concurrency) VALUES (${placeholders(this.driver.dialect, 5)})
       ON CONFLICT (k) DO UPDATE SET paused=${this.p(6)}, concurrency=${this.p(7)}`,
      [qk(meta.namespace, meta.name), meta.namespace, meta.name, paused, meta.concurrency, paused, meta.concurrency],
    );
  }

  async listQueues(namespace?: string): Promise<QueueMeta[]> {
    const rows = namespace
      ? await this.all<Record<string, unknown>>(`SELECT * FROM bunmq_queues WHERE namespace = ${this.p(1)}`, [
          namespace,
        ])
      : await this.all<Record<string, unknown>>(`SELECT * FROM bunmq_queues`);
    return rows.map((r) => ({
      namespace: String(r.namespace),
      name: String(r.name),
      paused: Boolean(r.paused),
      concurrency: r.concurrency == null ? null : Number(r.concurrency),
    }));
  }

  async upsertRepeatable(job: RepeatableRecord): Promise<void> {
    await this.run(`DELETE FROM bunmq_repeatable WHERE id = ${this.p(1)}`, [job.id]);
    await this.run(
      `INSERT INTO bunmq_repeatable (id, namespace, queue, next, body) VALUES (${placeholders(this.driver.dialect, 5)})`,
      [job.id, job.namespace, job.queue, job.next, JSON.stringify(job)],
    );
  }

  async listRepeatable(namespace: string, queue?: string): Promise<RepeatableRecord[]> {
    const rows = queue
      ? await this.all<Record<string, unknown>>(
          `SELECT body FROM bunmq_repeatable WHERE namespace=${this.p(1)} AND queue=${this.p(2)}`,
          [namespace, queue],
        )
      : await this.all<Record<string, unknown>>(`SELECT body FROM bunmq_repeatable WHERE namespace=${this.p(1)}`, [
          namespace,
        ]);
    return rows.map((r) => JSON.parse(String(r.body)) as RepeatableRecord);
  }

  async removeRepeatable(id: string): Promise<void> {
    await this.run(`DELETE FROM bunmq_repeatable WHERE id = ${this.p(1)}`, [id]);
  }

  async dueRepeatable(now: number): Promise<RepeatableRecord[]> {
    const rows = await this.all<Record<string, unknown>>(
      `SELECT body FROM bunmq_repeatable WHERE next <= ${this.p(1)}`,
      [now],
    );
    return rows.map((r) => JSON.parse(String(r.body)) as RepeatableRecord);
  }

  async appendEvent(event: QueueEvent): Promise<void> {
    await this.run(
      `INSERT INTO bunmq_events (id, namespace, queue, type, job_id, timestamp, body) VALUES (${placeholders(this.driver.dialect, 7)})`,
      [event.id, event.namespace, event.queue, event.type, event.jobId, event.timestamp, JSON.stringify(event)],
    );
    await this.run(
      `DELETE FROM bunmq_events WHERE timestamp < ${this.p(1)} AND id NOT IN (SELECT id FROM bunmq_events ORDER BY timestamp DESC LIMIT 800)`,
      [clock.now() - 3_600_000],
    ).catch(() => undefined);
    this.emit(event.type, event);
    this.emit("event", event);
  }

  async listEvents(filter: EventFilter): Promise<QueueEvent[]> {
    const clauses: string[] = ["1=1"];
    const params: unknown[] = [];
    let n = 1;
    if (filter.namespace) {
      clauses.push(`namespace = ${this.p(n++)}`);
      params.push(filter.namespace);
    }
    if (filter.queue) {
      clauses.push(`queue = ${this.p(n++)}`);
      params.push(filter.queue);
    }
    if (filter.jobId) {
      clauses.push(`job_id = ${this.p(n++)}`);
      params.push(filter.jobId);
    }
    if (filter.since) {
      clauses.push(`timestamp >= ${this.p(n++)}`);
      params.push(filter.since);
    }
    const limit = filter.limit ?? 200;
    params.push(limit);
    const rows = await this.all<Record<string, unknown>>(
      `SELECT body FROM bunmq_events WHERE ${clauses.join(" AND ")} ORDER BY timestamp ASC LIMIT ${this.p(n++)}`,
      params,
    );
    let events = rows.map((r) => JSON.parse(String(r.body)) as QueueEvent);
    const types = asArray(filter.type);
    if (types) events = events.filter((e) => types.includes(e.type));
    return events;
  }

  async releaseStalled(now: number): Promise<JobRecord[]> {
    const rows = await this.all<Record<string, unknown>>(
      `SELECT body FROM bunmq_jobs WHERE status='active' AND (lock_until IS NULL OR lock_until <= ${this.p(1)})`,
      [now],
    );
    const stalled: JobRecord[] = [];
    for (const row of rows) {
      const job = this.parseJob(row);
      job.status = "waiting";
      job.lockUntil = null;
      job.lockToken = null;
      job.processAt = now;
      await this.updateJob(job);
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
    const rows = await this.all<Record<string, unknown>>(
      `SELECT id, body FROM bunmq_jobs WHERE namespace=${this.p(1)} AND queue=${this.p(2)} AND status=${this.p(3)} LIMIT ${this.p(4)}`,
      [namespace, queue, status, limit],
    );
    let n = 0;
    for (const row of rows) {
      const job = this.parseJob(row);
      if ((job.finishedOn ?? job.timestamp) > olderThan) continue;
      await this.removeJob(namespace, queue, job.id);
      n += 1;
    }
    return n;
  }

  async saveWorkflow(wf: WorkflowRecord): Promise<void> {
    await this.run(`DELETE FROM bunmq_workflows WHERE id = ${this.p(1)}`, [wf.id]);
    await this.run(
      `INSERT INTO bunmq_workflows (id, namespace, created_at, body) VALUES (${placeholders(this.driver.dialect, 4)})`,
      [wf.id, wf.namespace, wf.createdAt, JSON.stringify(wf)],
    );
  }

  async getWorkflow(id: string): Promise<WorkflowRecord | null> {
    const rows = await this.all<Record<string, unknown>>(`SELECT body FROM bunmq_workflows WHERE id = ${this.p(1)}`, [
      id,
    ]);
    return rows[0] ? (JSON.parse(String(rows[0].body)) as WorkflowRecord) : null;
  }

  async listWorkflows(namespace: string, limit = 50): Promise<WorkflowRecord[]> {
    const rows = await this.all<Record<string, unknown>>(
      `SELECT body FROM bunmq_workflows WHERE namespace = ${this.p(1)} ORDER BY created_at DESC LIMIT ${this.p(2)}`,
      [namespace, limit],
    );
    return rows.map((r) => JSON.parse(String(r.body)) as WorkflowRecord);
  }

  async takeRateLimit(key: string, max: number, duration: number, now: number): Promise<number> {
    const rows = await this.all<Record<string, unknown>>(`SELECT stamps FROM bunmq_rate WHERE k = ${this.p(1)}`, [key]);
    let stamps: number[] = rows[0] ? (JSON.parse(String(rows[0].stamps)) as number[]) : [];
    stamps = stamps.filter((t) => t > now - duration);
    if (stamps.length >= max) return stamps[0]! + duration - now;
    stamps.push(now);
    await this.run(`DELETE FROM bunmq_rate WHERE k = ${this.p(1)}`, [key]);
    await this.run(`INSERT INTO bunmq_rate (k, stamps) VALUES (${this.p(1)}, ${this.p(2)})`, [
      key,
      JSON.stringify(stamps),
    ]);
    return 0;
  }

  async reset(): Promise<void> {
    await this.driver.exec(
      `DELETE FROM bunmq_jobs; DELETE FROM bunmq_queues; DELETE FROM bunmq_repeatable; DELETE FROM bunmq_events; DELETE FROM bunmq_workflows; DELETE FROM bunmq_rate;`,
    );
  }
}
