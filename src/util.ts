import type { Backoff } from "./types";

export const clock = {
  now: () => Date.now(),
};

export function id(prefix = "job"): string {
  const t = clock.now().toString(36);
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return `${prefix}_${t}${s}`;
}

export function clone<T>(value: T): T {
  return structuredClone(value);
}

export function qk(namespace: string, queue: string): string {
  return `${namespace}::${queue}`;
}

export function backoffDelay(backoff: Backoff | undefined, attemptsMade: number): number {
  if (backoff == null) return 0;
  if (typeof backoff === "number") return backoff;
  if (backoff.type === "custom") return backoff.delay(attemptsMade);
  if (backoff.type === "exponential") {
    return backoff.delay * Math.pow(2, Math.max(0, attemptsMade - 1));
  }
  return backoff.delay;
}

export function stackLines(err: unknown): string[] {
  if (err instanceof Error && err.stack) return err.stack.split("\n").slice(0, 24);
  return [String(err)];
}

export function errMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

export function asArray<T>(value: T | T[] | undefined): T[] | undefined {
  if (value == null) return undefined;
  return Array.isArray(value) ? value : [value];
}

export class Mutex {
  private chain: Promise<void> = Promise.resolve();

  run<T>(fn: () => Promise<T> | T): Promise<T> {
    const run = this.chain.then(fn, fn);
    this.chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }
}

/** Token bucket. Returns wait ms if empty, else 0. */
export class Limiter {
  private stamps: number[] = [];
  constructor(
    private max: number,
    private duration: number,
  ) {}

  take(now = clock.now()): number {
    const cut = now - this.duration;
    this.stamps = this.stamps.filter((t) => t > cut);
    if (this.stamps.length >= this.max) {
      return this.stamps[0]! + this.duration - now;
    }
    this.stamps.push(now);
    return 0;
  }
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      reject(signal?.reason ?? new Error("aborted"));
    };
    if (signal) {
      if (signal.aborted) {
        clearTimeout(t);
        onAbort();
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}

const CRON_RE = /^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$/;

function parseField(field: string, min: number, max: number): Set<number> {
  const out = new Set<number>();
  for (const part of field.split(",")) {
    if (part === "*") {
      for (let i = min; i <= max; i++) out.add(i);
      continue;
    }
    const stepMatch = part.match(/^(?:\*|(\d+)(?:-(\d+))?)\/(\d+)$/);
    if (stepMatch) {
      const a = stepMatch[1] == null ? min : Number(stepMatch[1]);
      const b = stepMatch[2] == null ? max : Number(stepMatch[2]);
      const step = Number(stepMatch[3]);
      for (let i = a; i <= b; i += step) if (i >= min && i <= max) out.add(i);
      continue;
    }
    const range = part.match(/^(\d+)-(\d+)$/);
    if (range) {
      const a = Number(range[1]);
      const b = Number(range[2]);
      for (let i = a; i <= b; i++) if (i >= min && i <= max) out.add(i);
      continue;
    }
    const n = Number(part);
    if (Number.isFinite(n) && n >= min && n <= max) out.add(n);
  }
  return out;
}

export function parseCron(expr: string) {
  const m = expr.trim().match(CRON_RE);
  if (!m) throw new Error(`Invalid cron: ${expr}`);
  return {
    minute: parseField(m[1]!, 0, 59),
    hour: parseField(m[2]!, 0, 23),
    date: parseField(m[3]!, 1, 31),
    month: parseField(m[4]!, 1, 12),
    day: parseField(m[5]!, 0, 6),
  };
}

export function nextCron(expr: string, from: number): number {
  const c = parseCron(expr);
  const d = new Date(from);
  d.setUTCSeconds(0, 0);
  d.setUTCMinutes(d.getUTCMinutes() + 1);
  for (let i = 0; i < 366 * 24 * 60; i++) {
    const month = d.getUTCMonth() + 1;
    const date = d.getUTCDate();
    const hour = d.getUTCHours();
    const minute = d.getUTCMinutes();
    const day = d.getUTCDay();
    if (
      c.month.has(month) &&
      c.date.has(date) &&
      c.hour.has(hour) &&
      c.minute.has(minute) &&
      c.day.has(day)
    ) {
      return d.getTime();
    }
    d.setUTCMinutes(d.getUTCMinutes() + 1);
  }
  throw new Error(`No next date for cron: ${expr}`);
}

export function serializeBackoff(backoff: Backoff | undefined): unknown {
  if (backoff == null) return null;
  if (typeof backoff === "number") return backoff;
  if (backoff.type === "custom") return { type: "exponential", delay: 1000 };
  return backoff;
}
