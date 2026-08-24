import type { Adapter } from "./adapter";
import { WorkflowError } from "./errors";
import { Job, createRecord } from "./job";
import { Worker } from "./worker";
import type { JobOptions, JobRecord, WorkflowRecord } from "./types";
import { clock, id } from "./util";

export type StepContext<TInput = unknown, TSteps extends Record<string, unknown> = Record<string, unknown>> = {
  input: TInput;
  steps: Partial<TSteps>;
  job: Job;
  signal: AbortSignal;
};

export type StepDef<TInput = unknown, TSteps extends Record<string, unknown> = Record<string, unknown>> = {
  needs?: string[];
  queue?: string;
  opts?: JobOptions;
  compensate?: (ctx: StepContext<TInput, TSteps>) => Promise<void> | void;
  run: (ctx: StepContext<TInput, TSteps>) => Promise<unknown> | unknown;
};

export type WorkflowDef<TInput = unknown> = {
  name: string;
  queue?: string;
  steps: Record<string, StepDef<TInput>>;
};

export function defineWorkflow<TInput = unknown>(
  name: string,
  steps: Record<string, StepDef<TInput>>,
  opts?: { queue?: string },
): WorkflowDef<TInput> {
  validateGraph(steps);
  return { name, steps, queue: opts?.queue };
}

function validateGraph(steps: Record<string, { needs?: string[] }>) {
  const keys = new Set(Object.keys(steps));
  for (const [name, step] of Object.entries(steps)) {
    for (const need of step.needs ?? []) {
      if (!keys.has(need)) throw new WorkflowError(`Step "${name}" needs unknown step "${need}"`);
      if (need === name) throw new WorkflowError(`Step "${name}" cannot need itself`);
    }
  }
  const visiting = new Set<string>();
  const seen = new Set<string>();
  const visit = (k: string) => {
    if (seen.has(k)) return;
    if (visiting.has(k)) throw new WorkflowError(`Cycle in workflow at "${k}"`);
    visiting.add(k);
    for (const n of steps[k]?.needs ?? []) visit(n);
    visiting.delete(k);
    seen.add(k);
  };
  for (const k of keys) visit(k);
}

const WORKFLOW_QUEUE = "__bunmq_workflow";

export type OrchestratorOptions = {
  adapter: Adapter;
  namespace?: string;
  concurrency?: number;
  autorun?: boolean;
};

export class Orchestrator {
  readonly adapter: Adapter;
  readonly namespace: string;
  private defs = new Map<string, WorkflowDef<any>>();
  private worker: Worker;
  private runningCompensations = new Set<string>();

  constructor(opts: OrchestratorOptions) {
    this.adapter = opts.adapter;
    this.namespace = opts.namespace ?? "default";
    this.worker = new Worker(
      WORKFLOW_QUEUE,
      (job, ctx) => this.processStep(job, ctx.signal),
      {
        adapter: this.adapter,
        namespace: this.namespace,
        concurrency: opts.concurrency ?? 8,
        autorun: opts.autorun !== false,
      },
    );
    this.worker.on("finished", (record, ok) => {
      void this.afterStep(record as JobRecord, ok as boolean);
    });
  }

  register(def: WorkflowDef<any>): this {
    validateGraph(def.steps);
    this.defs.set(def.name, def);
    return this;
  }

  async run<TInput>(name: string, input: TInput, opts?: { namespace?: string }): Promise<WorkflowRecord> {
    const def = this.defs.get(name);
    if (!def) throw new WorkflowError(`Unknown workflow: ${name}`);
    const namespace = opts?.namespace ?? this.namespace;
    const wf: WorkflowRecord = {
      id: id("wf"),
      namespace,
      name,
      status: "running",
      input,
      output: null,
      stepResults: {},
      completedSteps: [],
      failedStep: null,
      failedReason: null,
      createdAt: clock.now(),
      finishedAt: null,
    };
    await this.adapter.saveWorkflow(wf);
    await this.enqueueReady(def, wf);
    return wf;
  }

  async get(id: string): Promise<WorkflowRecord | null> {
    return this.adapter.getWorkflow(id);
  }

  async list(limit = 40): Promise<WorkflowRecord[]> {
    return this.adapter.listWorkflows(this.namespace, limit);
  }

  async close(): Promise<void> {
    await this.worker.close();
  }

  private readySteps(def: WorkflowDef, wf: WorkflowRecord): string[] {
    const done = new Set(wf.completedSteps);
    const out: string[] = [];
    for (const [key, step] of Object.entries(def.steps)) {
      if (done.has(key)) continue;
      if (wf.stepResults[key] !== undefined && key === wf.failedStep) continue;
      const needs = step.needs ?? [];
      if (needs.every((n) => done.has(n))) out.push(key);
    }
    return out;
  }

  private async enqueueReady(def: WorkflowDef, wf: WorkflowRecord) {
    const ready = this.readySteps(def, wf);
    const inflight = await this.adapter.listJobs({
      namespace: wf.namespace,
      queue: WORKFLOW_QUEUE,
      workflowId: wf.id,
      status: ["waiting", "delayed", "active"],
    });
    const inflightKeys = new Set(inflight.map((j) => j.stepKey).filter(Boolean));
    const jobs: JobRecord[] = [];
    for (const key of ready) {
      if (inflightKeys.has(key)) continue;
      const step = def.steps[key]!;
      jobs.push(
        createRecord({
          namespace: wf.namespace,
          queue: WORKFLOW_QUEUE,
          name: `${def.name}:${key}`,
          data: { workflowId: wf.id, step: key, input: wf.input },
          opts: { attempts: 2, backoff: { type: "exponential", delay: 200 }, ...step.opts },
          workflowId: wf.id,
          stepKey: key,
        }),
      );
    }
    if (jobs.length) await this.adapter.addJobs(jobs);
    if (ready.length === 0 && inflight.length === 0) {
      const remaining = Object.keys(def.steps).filter((k) => !wf.completedSteps.includes(k));
      if (remaining.length === 0 && wf.status === "running") {
        wf.status = "completed";
        wf.finishedAt = clock.now();
        wf.output = wf.stepResults;
        await this.adapter.saveWorkflow(wf);
        await this.adapter.appendEvent({
          id: id("evt"),
          namespace: wf.namespace,
          queue: WORKFLOW_QUEUE,
          jobId: wf.id,
          type: "workflow-completed",
          payload: { name: wf.name },
          timestamp: clock.now(),
        });
      }
    }
  }

  private async processStep(job: Job, signal: AbortSignal) {
    const workflowId = (job.data as { workflowId: string }).workflowId;
    const stepKey = job.record.stepKey;
    const wf = await this.adapter.getWorkflow(workflowId);
    if (!wf) throw new WorkflowError(`Workflow missing: ${workflowId}`);
    const def = this.defs.get(wf.name);
    if (!def || !stepKey) throw new WorkflowError(`Definition missing for ${wf.name}`);
    const step = def.steps[stepKey];
    if (!step) throw new WorkflowError(`Step missing: ${stepKey}`);
    const ctx = {
      input: wf.input,
      steps: wf.stepResults,
      job,
      signal,
    };
    if (wf.status === "compensating") {
      if (step.compensate) await step.compensate(ctx);
      return { compensated: true };
    }
    return step.run(ctx);
  }

  private async afterStep(record: JobRecord, ok: boolean) {
    const wfId = record.workflowId;
    const stepKey = record.stepKey;
    if (!wfId || !stepKey) return;
    const wf = await this.adapter.getWorkflow(wfId);
    if (!wf) return;
    const def = this.defs.get(wf.name);
    if (!def) return;

    if (wf.status === "compensating") {
      if (!ok) {
        wf.status = "failed";
        wf.failedReason = record.failedReason ?? "compensation failed";
        wf.finishedAt = clock.now();
        await this.adapter.saveWorkflow(wf);
        return;
      }
      wf.completedSteps = wf.completedSteps.filter((s) => s !== stepKey);
      await this.adapter.saveWorkflow(wf);
      await this.continueCompensate(def, wf);
      return;
    }

    if (!ok) {
      wf.status = "failed";
      wf.failedStep = stepKey;
      wf.failedReason = record.failedReason;
      await this.adapter.saveWorkflow(wf);
      await this.adapter.appendEvent({
        id: id("evt"),
        namespace: wf.namespace,
        queue: WORKFLOW_QUEUE,
        jobId: wf.id,
        type: "workflow-failed",
        payload: { step: stepKey, reason: record.failedReason },
        timestamp: clock.now(),
      });
      const hasCompensate = Object.values(def.steps).some((s) => s.compensate);
      if (hasCompensate) await this.startCompensate(def, wf);
      return;
    }

    wf.stepResults[stepKey] = record.returnvalue;
    if (!wf.completedSteps.includes(stepKey)) wf.completedSteps.push(stepKey);
    await this.adapter.saveWorkflow(wf);
    await this.enqueueReady(def, wf);
  }

  private async startCompensate(def: WorkflowDef, wf: WorkflowRecord) {
    if (this.runningCompensations.has(wf.id)) return;
    this.runningCompensations.add(wf.id);
    wf.status = "compensating";
    await this.adapter.saveWorkflow(wf);
    await this.continueCompensate(def, wf);
  }

  private async continueCompensate(def: WorkflowDef, wf: WorkflowRecord) {
    const remaining = [...wf.completedSteps].reverse();
    const next = remaining.find((k) => def.steps[k]?.compensate);
    if (!next) {
      wf.status = "failed";
      wf.finishedAt = clock.now();
      await this.adapter.saveWorkflow(wf);
      this.runningCompensations.delete(wf.id);
      return;
    }
    const step = def.steps[next]!;
    const job = createRecord({
      namespace: wf.namespace,
      queue: WORKFLOW_QUEUE,
      name: `${def.name}:${next}:compensate`,
      data: { workflowId: wf.id, step: next, input: wf.input, compensate: true },
      opts: { attempts: 1, ...step.opts },
      workflowId: wf.id,
      stepKey: next,
    });
    await this.adapter.addJob(job);
  }
}
