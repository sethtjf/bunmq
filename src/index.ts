export { Queue, type QueueOptions, type BulkJob } from "./queue";
export { Worker, type WorkerOptions, type Processor } from "./worker";
export { Job, createRecord } from "./job";
export { FlowProducer, type FlowNode, type FlowProducerOptions } from "./flow";
export {
  Orchestrator,
  defineWorkflow,
  type WorkflowDef,
  type StepDef,
  type StepContext,
  type OrchestratorOptions,
} from "./workflow";
export { MemoryAdapter } from "./memory";
export {
  FileAdapter,
  LocalStorageAdapter,
  localStorageIO,
  type FileIO,
  type FileSnapshot,
} from "./file";
export { SqliteAdapter, type SqliteDatabase } from "./sqlite";
export { PostgresAdapter, type PostgresClient } from "./postgres";
export { RedisAdapter, type RedisLike } from "./redis";
export {
  AzureStorageBusAdapter,
  azureFromSdk,
  createMemoryAzureBus,
  type AzureStorageBusClient,
  type AzureSdkHandles,
  type AzureBusMessage,
  type AzureTableEntity,
} from "./azure";
export { SqlAdapter, type SqlDriver, type SqlDialect } from "./sql";
export type { Adapter } from "./adapter";
export { Emitter } from "./emitter";
export {
  BunMQError,
  JobNotFoundError,
  QueuePausedError,
  LockError,
  UnrecoverableError,
  RateLimitError,
  TimeoutError,
  WorkflowError,
} from "./errors";
export { clock, id, nextCron, parseCron, backoffDelay } from "./util";
export type {
  JobStatus,
  JobRecord,
  JobOptions,
  JobFilter,
  QueueMeta,
  QueueEvent,
  QueueEventType,
  RepeatableRecord,
  RepeatOptions,
  Backoff,
  GroupOptions,
  WorkflowRecord,
  WorkflowStatus,
  TenantScope,
  CountFilter,
  EventFilter,
} from "./types";
export { JOB_STATUSES } from "./types";
export { runSelfTest, assertAllPass, type TestResult } from "./selftest";

import { Queue } from "./queue";
import { Worker, type Processor, type WorkerOptions } from "./worker";
import { FlowProducer } from "./flow";
import { Orchestrator } from "./workflow";
import type { Adapter } from "./adapter";
import type { JobOptions } from "./types";

export type BunMQOptions = {
  adapter: Adapter;
  tenant?: string;
  defaultJobOptions?: JobOptions;
};

/** One handle: queues, workers, flows, orchestrator — same adapter, same tenant. */
export function createBunMQ(opts: BunMQOptions) {
  const tenant = opts.tenant ?? "default";
  const { adapter } = opts;
  return {
    adapter,
    tenant,
    queue: <T = unknown>(
      name: string,
      extra?: { concurrency?: number | null; defaultJobOptions?: JobOptions },
    ) =>
      new Queue<T>(name, {
        adapter,
        tenant,
        defaultJobOptions: extra?.defaultJobOptions ?? opts.defaultJobOptions,
        concurrency: extra?.concurrency,
      }),
    worker: <T = unknown>(
      name: string,
      processor: Processor<T>,
      extra?: Omit<WorkerOptions, "adapter">,
    ) => new Worker<T>(name, processor, { adapter, tenant, ...extra }),
    flow: () => new FlowProducer({ adapter, tenant }),
    orchestrator: (extra?: { concurrency?: number; autorun?: boolean }) =>
      new Orchestrator({ adapter, tenant, ...extra }),
  };
}
