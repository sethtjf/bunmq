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
import { asArray, clock, Mutex, qk } from "./util";

/**
 * Injected Azure surface. Table Storage holds job records; Service Bus
 * carries dispatch (including scheduled delayed jobs). bunmq never imports
 * `@azure/*` — pass the official SDK through `azureFromSdk`, or use
 * `createMemoryAzureBus` in tests.
 */
export type AzureTableEntity = {
  partition: string;
  row: string;
  value: string;
  etag?: string;
};

export type AzureBusMessage = {
  id: string;
  tenant: string;
  queue: string;
  lockToken: string;
};

export type AzureStorageBusClient = {
  upsertEntity(entity: AzureTableEntity): Promise<{ etag: string }>;
  getEntity(partition: string, row: string): Promise<AzureTableEntity | null>;
  deleteEntity(partition: string, row: string): Promise<void>;
  listEntities(partition: string): Promise<AzureTableEntity[]>;
  /** Conditional replace. `false` means another writer won. */
  replaceEntity(entity: AzureTableEntity, etag: string): Promise<boolean>;
  send(message: { id: string; tenant: string; queue: string; scheduledAt?: number }): Promise<void>;
  receive(opts: {
    tenant?: string;
    queue?: string;
    maxWaitMs?: number;
  }): Promise<AzureBusMessage | null>;
  complete(lockToken: string): Promise<void>;
  abandon(lockToken: string): Promise<void>;
  renew?(lockToken: string): Promise<void>;
  reset?(): Promise<void>;
};

const LIVE: JobStatus[] = ["waiting", "delayed", "active", "waiting-children"];

function jobPart(tenant: string, queue: string) {
  return `j:${tenant}:${queue}`;
}

export class AzureStorageBusAdapter extends Emitter implements Adapter {
  readonly kind = "azure";
  private mutex = new Mutex();

  constructor(private client: AzureStorageBusClient) {
    super();
  }

  private async put(partition: string, row: string, value: unknown): Promise<string> {
    const { etag } = await this.client.upsertEntity({
      partition,
      row,
      value: JSON.stringify(value),
    });
    return etag;
  }

  private async read<T>(partition: string, row: string): Promise<{ value: T; etag: string } | null> {
    const hit = await this.client.getEntity(partition, row);
    if (!hit) return null;
    return { value: JSON.parse(hit.value) as T, etag: hit.etag ?? "" };
  }

  private async readJob(tenant: string, queue: string, id: string): Promise<JobRecord | null> {
    const hit = await this.read<JobRecord>(jobPart(tenant, queue), id);
    return hit?.value ?? null;
  }

  private async readJobById(id: string): Promise<JobRecord | null> {
    const ptr = await this.read<{ tenant: string; queue: string }>("ptr", id);
    if (!ptr) return null;
    return this.readJob(ptr.value.tenant, ptr.value.queue, id);
  }

  private async persistJob(job: JobRecord): Promise<void> {
    await this.put(jobPart(job.tenant, job.queue), job.id, job);
    await this.put("ptr", job.id, { tenant: job.tenant, queue: job.queue });
    await this.put("ids", job.id, qk(job.tenant, job.queue));
    await this.put("qs", qk(job.tenant, job.queue), {
      tenant: job.tenant,
      name: job.queue,
    });
  }

  private async dispatch(job: JobRecord): Promise<void> {
    if (job.status !== "waiting" && job.status !== "delayed") return;
    await this.client.send({
      id: job.id,
      tenant: job.tenant,
      queue: job.queue,
      scheduledAt: job.processAt,
    });
  }

  async addJob(job: JobRecord): Promise<JobRecord> {
    if (job.idempotencyKey) {
      const hit = await this.read<string>("idem", `${job.tenant}:${job.queue}:${job.idempotencyKey}`);
      if (hit) {
        const existing = await this.readJob(job.tenant, job.queue, hit.value);
        if (existing && LIVE.includes(existing.status)) return existing;
      }
    }
    await this.persistJob(job);
    if (job.idempotencyKey) {
      await this.put("idem", `${job.tenant}:${job.queue}:${job.idempotencyKey}`, job.id);
    }
    await this.dispatch(job);
    return job;
  }

  async addJobs(jobs: JobRecord[]): Promise<JobRecord[]> {
    const out: JobRecord[] = [];
    for (const j of jobs) out.push(await this.addJob(j));
    return out;
  }

  async getJob(tenant: string, queue: string, id: string): Promise<JobRecord | null> {
    return this.readJob(tenant, queue, id);
  }

  async updateJob(job: JobRecord): Promise<void> {
    await this.persistJob(job);
    if (job.status === "waiting" || job.status === "delayed") await this.dispatch(job);
  }

  async removeJob(tenant: string, queue: string, id: string): Promise<void> {
    const job = await this.readJob(tenant, queue, id);
    await this.client.deleteEntity(jobPart(tenant, queue), id);
    await this.client.deleteEntity("ptr", id);
    await this.client.deleteEntity("ids", id);
    if (job?.idempotencyKey) {
      await this.client.deleteEntity("idem", `${tenant}:${queue}:${job.idempotencyKey}`);
    }
  }

  async claimNext(
    tenant: string | "*",
    queue: string,
    workerId: string,
    now: number,
    lockUntil: number,
  ): Promise<JobRecord | null> {
    return this.mutex.run(async () => {
      await this.drainHints(tenant, queue, now);
      const jobs = await this.collectRunnable(tenant, queue, now);
      jobs.sort((a, b) => b.priority - a.priority || a.timestamp - b.timestamp);

      const activeCounts = new Map<string, number>();
      const groupCounts = new Map<string, number>();
      for (const j of await this.allJobs()) {
        if (j.status !== "active") continue;
        const k = qk(j.tenant, j.queue);
        activeCounts.set(k, (activeCounts.get(k) ?? 0) + 1);
        if (j.groupId) {
          const g = `${k}::${j.groupId}`;
          groupCounts.set(g, (groupCounts.get(g) ?? 0) + 1);
        }
      }

      for (const job of jobs) {
        const meta = await this.getQueueMeta(job.tenant, job.queue);
        if (meta.paused) continue;
        if (meta.concurrency != null && (activeCounts.get(qk(job.tenant, job.queue)) ?? 0) >= meta.concurrency) {
          continue;
        }
        if (job.groupId) {
          const g = `${qk(job.tenant, job.queue)}::${job.groupId}`;
          if ((groupCounts.get(g) ?? 0) >= job.groupMax) continue;
        }
        const claimed: JobRecord = {
          ...job,
          status: "active",
          attempts: job.attempts + 1,
          processedOn: now,
          lockUntil,
          lockToken: workerId,
        };
        const row = await this.client.getEntity(jobPart(job.tenant, job.queue), job.id);
        if (!row?.etag) continue;
        const ok = await this.client.replaceEntity(
          { partition: jobPart(job.tenant, job.queue), row: job.id, value: JSON.stringify(claimed) },
          row.etag,
        );
        if (!ok) continue;
        await this.put("ptr", claimed.id, { tenant: claimed.tenant, queue: claimed.queue });
        return claimed;
      }
      return null;
    });
  }

  private async drainHints(tenant: string | "*", queue: string, now: number): Promise<void> {
    for (let i = 0; i < 32; i++) {
      const msg = await this.client.receive({
        tenant: tenant === "*" ? undefined : tenant,
        queue: queue === "*" ? undefined : queue,
        maxWaitMs: 0,
      });
      if (!msg) break;
      const job = await this.readJob(msg.tenant, msg.queue, msg.id);
      if (!job) {
        await this.client.complete(msg.lockToken);
        continue;
      }
      if (job.status === "delayed" && job.processAt <= now) {
        job.status = "waiting";
        await this.persistJob(job);
      }
      await this.client.complete(msg.lockToken);
    }
  }

  private async collectRunnable(
    tenant: string | "*",
    queue: string,
    now: number,
  ): Promise<JobRecord[]> {
    const jobs = await this.scopedJobs(tenant, queue);
    const out: JobRecord[] = [];
    for (const job of jobs) {
      const meta = await this.getQueueMeta(job.tenant, job.queue);
      if (meta.paused) continue;
      if (job.status === "delayed" && job.processAt <= now) {
        job.status = "waiting";
        await this.persistJob(job);
      }
      if (job.status === "waiting" && job.processAt <= now) out.push(job);
    }
    return out;
  }

  private async scopedJobs(tenant: string | "*", queue: string): Promise<JobRecord[]> {
    if (tenant !== "*" && queue !== "*") {
      const rows = await this.client.listEntities(jobPart(tenant, queue));
      return rows.map((r) => JSON.parse(r.value) as JobRecord);
    }
    const jobs = await this.allJobs();
    return jobs.filter((j) => {
      if (tenant !== "*" && j.tenant !== tenant) return false;
      if (queue !== "*" && j.queue !== queue) return false;
      return true;
    });
  }

  private async allJobs(): Promise<JobRecord[]> {
    const ptrs = await this.client.listEntities("ids");
    const out: JobRecord[] = [];
    for (const p of ptrs) {
      let key = p.value;
      try {
        key = JSON.parse(p.value) as string;
      } catch {
        /* stored as raw qk */
      }
      const [t, q] = splitQK(key);
      if (!t || !q) continue;
      const job = await this.readJob(t, q, p.row);
      if (job) out.push(job);
    }
    return out;
  }

  async renewLock(
    tenant: string,
    queue: string,
    id: string,
    token: string,
    lockUntil: number,
  ): Promise<boolean> {
    const job = await this.readJob(tenant, queue, id);
    if (!job || job.lockToken !== token || job.status !== "active") return false;
    job.lockUntil = lockUntil;
    await this.persistJob(job);
    return true;
  }

  async listJobs(filter: JobFilter): Promise<JobRecord[]> {
    const statuses = asArray(filter.status);
    const rows = (await this.allJobs()).filter((job) => {
      if (filter.tenant && job.tenant !== filter.tenant) return false;
      if (filter.queue && job.queue !== filter.queue) return false;
      if (statuses && !statuses.includes(job.status)) return false;
      if (filter.ids && !filter.ids.includes(job.id)) return false;
      if (filter.parentId && job.parentId !== filter.parentId) return false;
      if (filter.workflowId && job.workflowId !== filter.workflowId) return false;
      return true;
    });
    rows.sort((a, b) => b.timestamp - a.timestamp);
    const start = filter.start ?? 0;
    const limit = filter.limit ?? rows.length;
    return rows.slice(start, start + limit);
  }

  async countJobs(filter: CountFilter): Promise<Record<JobStatus, number>> {
    const counts = emptyCounts();
    for (const j of await this.listJobs({ tenant: filter.tenant, queue: filter.queue, limit: 100_000 })) {
      counts[j.status] += 1;
    }
    return counts;
  }

  async getQueueMeta(tenant: string, queue: string): Promise<QueueMeta> {
    const hit = await this.read<QueueMeta>("qm", qk(tenant, queue));
    if (hit) return hit.value;
    return { tenant, name: queue, paused: false, concurrency: null };
  }

  async setQueueMeta(meta: QueueMeta): Promise<void> {
    await this.put("qm", qk(meta.tenant, meta.name), meta);
    await this.put("qs", qk(meta.tenant, meta.name), { tenant: meta.tenant, name: meta.name });
  }

  async listQueues(tenant?: string): Promise<QueueMeta[]> {
    const rows = await this.client.listEntities("qs");
    const out: QueueMeta[] = [];
    for (const r of rows) {
      const [t, name] = r.row.split("::");
      if (!t || !name) continue;
      if (tenant && t !== tenant) continue;
      out.push(await this.getQueueMeta(t, name));
    }
    return out;
  }

  async upsertRepeatable(job: RepeatableRecord): Promise<void> {
    await this.put("r", job.id, job);
    await this.put("reps", job.id, "1");
  }

  async listRepeatable(tenant: string, queue?: string): Promise<RepeatableRecord[]> {
    const ids = await this.client.listEntities("reps");
    const out: RepeatableRecord[] = [];
    for (const row of ids) {
      const hit = await this.read<RepeatableRecord>("r", row.row);
      if (!hit) continue;
      if (hit.value.tenant !== tenant) continue;
      if (queue && hit.value.queue !== queue) continue;
      out.push(hit.value);
    }
    return out;
  }

  async removeRepeatable(id: string): Promise<void> {
    await this.client.deleteEntity("r", id);
    await this.client.deleteEntity("reps", id);
  }

  async dueRepeatable(now: number): Promise<RepeatableRecord[]> {
    const ids = await this.client.listEntities("reps");
    const out: RepeatableRecord[] = [];
    for (const row of ids) {
      const hit = await this.read<RepeatableRecord>("r", row.row);
      if (hit && hit.value.next <= now) out.push(hit.value);
    }
    return out;
  }

  async appendEvent(event: QueueEvent): Promise<void> {
    await this.put("e", event.id, event);
    await this.put("evts", event.id, String(event.timestamp));
    this.emit(event.type, event);
    this.emit("event", event);
  }

  async listEvents(filter: EventFilter): Promise<QueueEvent[]> {
    const types = asArray(filter.type);
    const ids = await this.client.listEntities("evts");
    const out: QueueEvent[] = [];
    for (const row of ids) {
      const hit = await this.read<QueueEvent>("e", row.row);
      if (!hit) continue;
      const e = hit.value;
      if (filter.tenant && e.tenant !== filter.tenant) continue;
      if (filter.queue && e.queue !== filter.queue) continue;
      if (types && !types.includes(e.type)) continue;
      if (filter.jobId && e.jobId !== filter.jobId) continue;
      if (filter.since && e.timestamp < filter.since) continue;
      out.push(e);
    }
    out.sort((a, b) => a.timestamp - b.timestamp);
    return out.slice(-(filter.limit ?? 200));
  }

  async releaseStalled(now: number): Promise<JobRecord[]> {
    const stalled: JobRecord[] = [];
    for (const job of await this.allJobs()) {
      if (job.status !== "active") continue;
      if (job.lockUntil != null && job.lockUntil > now) continue;
      job.status = "waiting";
      job.lockUntil = null;
      job.lockToken = null;
      job.processAt = now;
      await this.persistJob(job);
      await this.dispatch(job);
      stalled.push(job);
    }
    return stalled;
  }

  async clean(
    tenant: string,
    queue: string,
    status: JobStatus,
    olderThan: number,
    limit: number,
  ): Promise<number> {
    const jobs = await this.listJobs({ tenant, queue, status, limit: 10_000 });
    let n = 0;
    for (const job of jobs) {
      if ((job.finishedOn ?? job.timestamp) > olderThan) continue;
      await this.removeJob(tenant, queue, job.id);
      n += 1;
      if (n >= limit) break;
    }
    return n;
  }

  async saveWorkflow(wf: WorkflowRecord): Promise<void> {
    await this.put("w", wf.id, wf);
    await this.put("wfs", wf.id, wf.tenant);
  }

  async getWorkflow(id: string): Promise<WorkflowRecord | null> {
    const hit = await this.read<WorkflowRecord>("w", id);
    return hit?.value ?? null;
  }

  async listWorkflows(tenant: string, limit = 50): Promise<WorkflowRecord[]> {
    const ids = await this.client.listEntities("wfs");
    const out: WorkflowRecord[] = [];
    for (const row of ids) {
      const wf = await this.getWorkflow(row.row);
      if (wf && wf.tenant === tenant) out.push(wf);
    }
    out.sort((a, b) => b.createdAt - a.createdAt);
    return out.slice(0, limit);
  }

  async takeRateLimit(key: string, max: number, duration: number, now: number): Promise<number> {
    const hit = await this.read<number[]>("rl", key);
    let stamps = (hit?.value ?? []).filter((t) => t > now - duration);
    if (stamps.length >= max) return stamps[0]! + duration - now;
    stamps = [...stamps, now];
    await this.put("rl", key, stamps);
    return 0;
  }

  async reset(): Promise<void> {
    if (this.client.reset) {
      await this.client.reset();
      return;
    }
    const partitions = ["ids", "ptr", "qs", "qm", "idem", "r", "reps", "e", "evts", "w", "wfs", "rl"];
    const extras = new Set<string>();
    for (const p of await this.client.listEntities("ids")) {
      let key = p.value;
      try {
        key = JSON.parse(p.value) as string;
      } catch {
        /* stored as raw qk */
      }
      extras.add(jobPart(...splitQK(key)));
    }
    for (const part of [...partitions, ...extras]) {
      for (const row of await this.client.listEntities(part)) {
        await this.client.deleteEntity(row.partition, row.row);
      }
    }
  }
}

function splitQK(value: string): [string, string] {
  const i = value.indexOf("::");
  if (i < 0) return [value, ""];
  return [value.slice(0, i), value.slice(i + 2)];
}

/**
 * In-process Table Storage + Service Bus. Same contract as Azure, no network.
 */
export function createMemoryAzureBus(): AzureStorageBusClient {
  const tables = new Map<string, { value: string; etag: string }>();
  let n = 1;
  type Msg = {
    id: string;
    tenant: string;
    queue: string;
    scheduledAt: number;
    lockToken: string | null;
    lockUntil: number;
    dead: boolean;
  };
  const bus: Msg[] = [];
  const locked = new Map<string, Msg>();

  const tk = (p: string, r: string) => `${p}\t${r}`;

  return {
    async upsertEntity(entity) {
      const etag = `W/"${n++}"`;
      tables.set(tk(entity.partition, entity.row), { value: entity.value, etag });
      return { etag };
    },
    async getEntity(partition, row) {
      const hit = tables.get(tk(partition, row));
      if (!hit) return null;
      return { partition, row, value: hit.value, etag: hit.etag };
    },
    async deleteEntity(partition, row) {
      tables.delete(tk(partition, row));
    },
    async listEntities(partition) {
      const prefix = `${partition}\t`;
      const out: AzureTableEntity[] = [];
      for (const [k, v] of tables) {
        if (!k.startsWith(prefix)) continue;
        out.push({ partition, row: k.slice(prefix.length), value: v.value, etag: v.etag });
      }
      return out;
    },
    async replaceEntity(entity, etag) {
      const hit = tables.get(tk(entity.partition, entity.row));
      if (!hit || hit.etag !== etag) return false;
      const next = `W/"${n++}"`;
      tables.set(tk(entity.partition, entity.row), { value: entity.value, etag: next });
      return true;
    },
    async send(message) {
      bus.push({
        id: message.id,
        tenant: message.tenant,
        queue: message.queue,
        scheduledAt: message.scheduledAt ?? 0,
        lockToken: null,
        lockUntil: 0,
        dead: false,
      });
    },
    async receive(opts) {
      const now = clock.now();
      for (const m of bus) {
        if (m.dead) continue;
        if (m.lockToken && m.lockUntil > now) continue;
        if (m.scheduledAt > now) continue;
        if (opts.queue && opts.queue !== "*" && m.queue !== opts.queue) continue;
        if (opts.tenant && opts.tenant !== "*" && m.tenant !== opts.tenant) continue;
        m.lockToken = `lock_${n++}`;
        m.lockUntil = now + 30_000;
        locked.set(m.lockToken, m);
        return { id: m.id, tenant: m.tenant, queue: m.queue, lockToken: m.lockToken };
      }
      return null;
    },
    async complete(lockToken) {
      const m = locked.get(lockToken);
      if (!m) return;
      m.dead = true;
      m.lockToken = null;
      locked.delete(lockToken);
    },
    async abandon(lockToken) {
      const m = locked.get(lockToken);
      if (!m) return;
      m.lockToken = null;
      m.lockUntil = 0;
      locked.delete(lockToken);
    },
    async renew(lockToken) {
      const m = locked.get(lockToken);
      if (m) m.lockUntil = clock.now() + 30_000;
    },
    async reset() {
      tables.clear();
      bus.length = 0;
      locked.clear();
    },
  };
}

/** Loose shape of `@azure/data-tables` TableClient + `@azure/service-bus`. */
export type AzureSdkHandles = {
  table: {
    createTable?: () => Promise<unknown>;
    upsertEntity: (entity: Record<string, unknown>, mode?: string) => Promise<unknown>;
    getEntity: (partitionKey: string, rowKey: string) => Promise<Record<string, unknown>>;
    deleteEntity: (partitionKey: string, rowKey: string) => Promise<unknown>;
    listEntities: (opts?: {
      queryOptions?: { filter?: string };
    }) => AsyncIterable<Record<string, unknown>>;
    updateEntity: (
      entity: Record<string, unknown>,
      mode: string,
      options?: { etag?: string },
    ) => Promise<unknown>;
  };
  sender: {
    sendMessages: (messages: unknown) => Promise<unknown>;
  };
  receiver: {
    receiveMessages: (
      maxMessageCount: number,
      options?: { maxWaitTimeInMs?: number },
    ) => Promise<Array<{
      body?: unknown;
      lockToken?: string;
      messageId?: string;
      applicationProperties?: Record<string, unknown>;
    }>>;
    completeMessage: (message: { lockToken?: string }) => Promise<unknown>;
    abandonMessage: (message: { lockToken?: string }) => Promise<unknown>;
    renewMessageLock?: (message: { lockToken?: string }) => Promise<unknown>;
  };
};

/**
 * Wrap the official Azure SDKs without bunmq taking a dependency on them.
 *
 *   import { TableClient } from "@azure/data-tables"
 *   import { ServiceBusClient } from "@azure/service-bus"
 *   const bus = ServiceBusClient.fromConnectionString(process.env.AZURE_SERVICEBUS!)
 *   const adapter = new AzureStorageBusAdapter(azureFromSdk({
 *     table: TableClient.fromConnectionString(process.env.AZURE_STORAGE!, "bunmq"),
 *     sender: bus.createSender("bunmq"),
 *     receiver: bus.createReceiver("bunmq"),
 *   }))
 */
export function azureFromSdk(sdk: AzureSdkHandles): AzureStorageBusClient {
  const inflight = new Map<string, { lockToken?: string }>();
  void sdk.table.createTable?.().catch(() => undefined);

  const asEntity = (e: Record<string, unknown>): AzureTableEntity => ({
    partition: String(e.partitionKey ?? e.PartitionKey ?? ""),
    row: String(e.rowKey ?? e.RowKey ?? ""),
    value: String(e.body ?? e.Body ?? ""),
    etag: String(e.etag ?? e.odataEtag ?? ""),
  });

  return {
    async upsertEntity(entity) {
      const result = (await sdk.table.upsertEntity(
        { partitionKey: entity.partition, rowKey: entity.row, body: entity.value },
        "Replace",
      )) as { etag?: string } | undefined;
      return { etag: result?.etag ?? `W/"${clock.now()}"` };
    },
    async getEntity(partition, row) {
      try {
        const e = await sdk.table.getEntity(partition, row);
        return asEntity(e);
      } catch {
        return null;
      }
    },
    async deleteEntity(partition, row) {
      try {
        await sdk.table.deleteEntity(partition, row);
      } catch {
        /* missing is fine */
      }
    },
    async listEntities(partition) {
      const out: AzureTableEntity[] = [];
      const filter = `PartitionKey eq '${partition.replace(/'/g, "''")}'`;
      for await (const e of sdk.table.listEntities({ queryOptions: { filter } })) {
        out.push(asEntity(e as Record<string, unknown>));
      }
      return out;
    },
    async replaceEntity(entity, etag) {
      try {
        await sdk.table.updateEntity(
          { partitionKey: entity.partition, rowKey: entity.row, body: entity.value },
          "Replace",
          { etag },
        );
        return true;
      } catch {
        return false;
      }
    },
    async send(message) {
      await sdk.sender.sendMessages({
        body: { id: message.id, tenant: message.tenant, queue: message.queue },
        messageId: message.id,
        scheduledEnqueueTimeUtc: message.scheduledAt ? new Date(message.scheduledAt) : undefined,
        applicationProperties: { tenant: message.tenant, queue: message.queue },
      });
    },
    async receive(opts) {
      const batch = await sdk.receiver.receiveMessages(1, { maxWaitTimeInMs: opts.maxWaitMs ?? 0 });
      const raw = batch[0];
      if (!raw) return null;
      const body = (raw.body ?? {}) as { id?: string; tenant?: string; queue?: string };
      const tenant = String(raw.applicationProperties?.tenant ?? body.tenant ?? "");
      const queue = String(raw.applicationProperties?.queue ?? body.queue ?? "");
      if (opts.tenant && opts.tenant !== "*" && tenant !== opts.tenant) {
        await sdk.receiver.abandonMessage(raw);
        return null;
      }
      if (opts.queue && opts.queue !== "*" && queue !== opts.queue) {
        await sdk.receiver.abandonMessage(raw);
        return null;
      }
      const lockToken = raw.lockToken ?? "";
      inflight.set(lockToken, raw);
      return { id: String(body.id ?? raw.messageId ?? ""), tenant, queue, lockToken };
    },
    async complete(lockToken) {
      const msg = inflight.get(lockToken);
      if (msg) await sdk.receiver.completeMessage(msg);
      inflight.delete(lockToken);
    },
    async abandon(lockToken) {
      const msg = inflight.get(lockToken);
      if (msg) await sdk.receiver.abandonMessage(msg);
      inflight.delete(lockToken);
    },
    async renew(lockToken) {
      const msg = inflight.get(lockToken);
      if (msg && sdk.receiver.renewMessageLock) await sdk.receiver.renewMessageLock(msg);
    },
  };
}
