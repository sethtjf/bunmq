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
  NamespaceScope,
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
  namespace?: string;
  defaultJobOptions?: JobOptions;
};

/** One handle: queues, workers, flows, orchestrator — same adapter, same namespace. */
export function createBunMQ(opts: BunMQOptions) {
  const namespace = opts.namespace ?? "default";
  const { adapter } = opts;
  return {
    adapter,
    namespace,
    queue: <T = unknown>(
      name: string,
      extra?: { concurrency?: number | null; defaultJobOptions?: JobOptions },
    ) =>
      new Queue<T>(name, {
        adapter,
        namespace,
        defaultJobOptions: extra?.defaultJobOptions ?? opts.defaultJobOptions,
        concurrency: extra?.concurrency,
      }),
    worker: <T = unknown>(
      name: string,
      processor: Processor<T>,
      extra?: Omit<WorkerOptions, "adapter">,
    ) => new Worker<T>(name, processor, { adapter, namespace, ...extra }),
    flow: () => new FlowProducer({ adapter, namespace }),
    orchestrator: (extra?: { concurrency?: number; autorun?: boolean }) =>
      new Orchestrator({ adapter, namespace, ...extra }),
  };
}
