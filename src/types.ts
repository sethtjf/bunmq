export type JobStatus =
  | "waiting"
  | "delayed"
  | "active"
  | "completed"
  | "failed"
  | "waiting-children";

export type Backoff =
  | number
  | { type: "fixed" | "exponential"; delay: number }
  | { type: "custom"; delay: (attemptsMade: number) => number };

export type RepeatOptions = {
  every?: number;
  cron?: string;
  tz?: string;
  limit?: number;
  immediately?: boolean;
  key?: string;
};

export type GroupOptions = {
  id: string;
  max?: number;
};

export type JobOptions = {
  attempts?: number;
  backoff?: Backoff;
  delay?: number;
  priority?: number;
  timeout?: number;
  removeOnComplete?: boolean | number;
  removeOnFail?: boolean | number;
  repeat?: RepeatOptions;
  jobId?: string;
  parent?: { id: string; queue: string };
  namespace?: string;
  group?: GroupOptions;
  idempotencyKey?: string;
  failParentOnFailure?: boolean;
};

export type JobLog = { ts: number; message: string };

export type JobRecord<T = unknown> = {
  id: string;
  namespace: string;
  queue: string;
  name: string;
  data: T;
  status: JobStatus;
  priority: number;
  attempts: number;
  maxAttempts: number;
  progress: number | Record<string, unknown>;
  delay: number;
  timestamp: number;
  processAt: number;
  processedOn: number | null;
  finishedOn: number | null;
  failedReason: string | null;
  stacktrace: string[];
  returnvalue: unknown;
  parentId: string | null;
  parentQueue: string | null;
  workflowId: string | null;
  stepKey: string | null;
  pendingChildren: number;
  groupId: string | null;
  groupMax: number;
  idempotencyKey: string | null;
  lockUntil: number | null;
  lockToken: string | null;
  opts: JobOptions;
  logs: JobLog[];
};

export type RepeatableRecord = {
  id: string;
  namespace: string;
  queue: string;
  name: string;
  data: unknown;
  opts: JobOptions;
  every: number | null;
  cron: string | null;
  tz: string | null;
  limit: number | null;
  count: number;
  next: number;
  key: string;
};

export type QueueMeta = {
  namespace: string;
  name: string;
  paused: boolean;
  concurrency: number | null;
};

export type QueueEventType =
  | "added"
  | "waiting"
  | "active"
  | "progress"
  | "completed"
  | "failed"
  | "stalled"
  | "removed"
  | "delayed"
  | "paused"
  | "resumed"
  | "retries-exhausted"
  | "workflow-completed"
  | "workflow-failed"
  | "log";

export type QueueEvent = {
  id: string;
  namespace: string;
  queue: string;
  jobId: string | null;
  type: QueueEventType;
  payload: unknown;
  timestamp: number;
};

export type JobFilter = {
  namespace?: string;
  queue?: string;
  status?: JobStatus | JobStatus[];
  ids?: string[];
  parentId?: string;
  workflowId?: string;
  start?: number;
  limit?: number;
};

export type CountFilter = {
  namespace?: string;
  queue?: string;
};

export type EventFilter = {
  namespace?: string;
  queue?: string;
  type?: QueueEventType | QueueEventType[];
  jobId?: string;
  since?: number;
  limit?: number;
};

export type WorkflowStatus =
  | "running"
  | "completed"
  | "failed"
  | "compensating";

export type WorkflowRecord = {
  id: string;
  namespace: string;
  name: string;
  status: WorkflowStatus;
  input: unknown;
  output: unknown;
  stepResults: Record<string, unknown>;
  completedSteps: string[];
  failedStep: string | null;
  failedReason: string | null;
  createdAt: number;
  finishedAt: number | null;
};

export type NamespaceScope = string | "*";

export const JOB_STATUSES: JobStatus[] = [
  "waiting",
  "delayed",
  "active",
  "completed",
  "failed",
  "waiting-children",
];
