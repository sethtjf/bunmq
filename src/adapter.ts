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

export interface Adapter extends Emitter {
  readonly kind: string;

  addJob(job: JobRecord): Promise<JobRecord>;
  addJobs(jobs: JobRecord[]): Promise<JobRecord[]>;
  getJob(tenant: string, queue: string, id: string): Promise<JobRecord | null>;
  updateJob(job: JobRecord): Promise<void>;
  removeJob(tenant: string, queue: string, id: string): Promise<void>;

  /**
   * Atomically claim the next runnable job. Must be safe under concurrent
   * workers. Returns null if nothing is ready.
   */
  claimNext(
    tenant: string | "*",
    queue: string,
    workerId: string,
    now: number,
    lockUntil: number,
  ): Promise<JobRecord | null>;

  renewLock(
    tenant: string,
    queue: string,
    id: string,
    token: string,
    lockUntil: number,
  ): Promise<boolean>;

  listJobs(filter: JobFilter): Promise<JobRecord[]>;
  countJobs(filter: CountFilter): Promise<Record<JobStatus, number>>;

  getQueueMeta(tenant: string, queue: string): Promise<QueueMeta>;
  setQueueMeta(meta: QueueMeta): Promise<void>;
  listQueues(tenant?: string): Promise<QueueMeta[]>;

  upsertRepeatable(job: RepeatableRecord): Promise<void>;
  listRepeatable(tenant: string, queue?: string): Promise<RepeatableRecord[]>;
  removeRepeatable(id: string): Promise<void>;
  dueRepeatable(now: number): Promise<RepeatableRecord[]>;

  appendEvent(event: QueueEvent): Promise<void>;
  listEvents(filter: EventFilter): Promise<QueueEvent[]>;

  releaseStalled(now: number): Promise<JobRecord[]>;
  clean(
    tenant: string,
    queue: string,
    status: JobStatus,
    olderThan: number,
    limit: number,
  ): Promise<number>;

  saveWorkflow(wf: WorkflowRecord): Promise<void>;
  getWorkflow(id: string): Promise<WorkflowRecord | null>;
  listWorkflows(tenant: string, limit?: number): Promise<WorkflowRecord[]>;

  takeRateLimit(key: string, max: number, duration: number, now: number): Promise<number>;

  /** Optional: wipe everything. Used in tests. */
  reset?(): Promise<void>;
}

export function emptyCounts(): Record<JobStatus, number> {
  return {
    waiting: 0,
    delayed: 0,
    active: 0,
    completed: 0,
    failed: 0,
    "waiting-children": 0,
  };
}
