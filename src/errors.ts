export class BunMQError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BunMQError";
  }
}

export class JobNotFoundError extends BunMQError {
  constructor(id: string) {
    super(`Job not found: ${id}`);
    this.name = "JobNotFoundError";
  }
}

export class QueuePausedError extends BunMQError {
  constructor(queue: string) {
    super(`Queue is paused: ${queue}`);
    this.name = "QueuePausedError";
  }
}

export class LockError extends BunMQError {
  constructor(id: string) {
    super(`Job lock is held by another worker: ${id}`);
    this.name = "LockError";
  }
}

export class UnrecoverableError extends BunMQError {
  constructor(message: string) {
    super(message);
    this.name = "UnrecoverableError";
  }
}

export class RateLimitError extends BunMQError {
  retryAfter: number;
  constructor(retryAfter: number) {
    super(`Rate limited; retry after ${retryAfter}ms`);
    this.name = "RateLimitError";
    this.retryAfter = retryAfter;
  }
}

export class TimeoutError extends BunMQError {
  constructor(id: string, ms: number) {
    super(`Job ${id} timed out after ${ms}ms`);
    this.name = "TimeoutError";
  }
}

export class WorkflowError extends BunMQError {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowError";
  }
}
