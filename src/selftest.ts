import { MemoryAdapter } from "./memory";
import { AzureStorageBusAdapter, createMemoryAzureBus } from "./azure";
import { Queue } from "./queue";
import { Worker, type Processor } from "./worker";
import { FlowProducer } from "./flow";
import { Orchestrator, defineWorkflow } from "./workflow";
import { UnrecoverableError } from "./errors";
import { clock, sleep } from "./util";
import type { Adapter } from "./adapter";

export type TestResult = {
  name: string;
  pass: boolean;
  ms: number;
  error?: string;
};

async function waitFor(
  fn: () => Promise<boolean> | boolean,
  timeout = 4000,
  interval = 15,
): Promise<void> {
  const start = clock.now();
  while (clock.now() - start < timeout) {
    if (await fn()) return;
    await sleep(interval);
  }
  throw new Error("timeout");
}

async function withWorker<T>(
  adapter: Adapter,
  queue: string,
  processor: Processor,
  fn: (w: Worker) => Promise<T>,
  opts?: { concurrency?: number; namespace?: string },
): Promise<T> {
  const w = new Worker(queue, processor, {
    adapter,
    namespace: opts?.namespace ?? "t",
    concurrency: opts?.concurrency ?? 1,
    pollInterval: 10,
    stallInterval: 200,
    lockDuration: 2000,
  });
  try {
    return await fn(w);
  } finally {
    await w.close();
  }
}

export async function runSelfTest(adapter?: Adapter): Promise<TestResult[]> {
  const results: TestResult[] = [];
  const tests: Array<[string, (a: Adapter) => Promise<void>]> = [
    ["fifo order", testFifo],
    ["priority order", testPriority],
    ["delayed jobs", testDelayed],
    ["retries with backoff", testRetry],
    ["unrecoverable errors skip retry", testUnrecoverable],
    ["worker concurrency", testConcurrency],
    ["namespace isolation", testNamespaces],
    ["idempotency keys", testIdempotency],
    ["pause and resume", testPause],
    ["group concurrency", testGroup],
    ["parent waits for children", testFlow],
    ["workflow dag + fan-in", testWorkflow],
    ["job progress and logs", testProgress],
  ];

  for (const [name, test] of tests) {
    const a = adapter ?? new MemoryAdapter();
    if (a.reset) await a.reset();
    const t0 = clock.now();
    try {
      await test(a);
      results.push({ name, pass: true, ms: clock.now() - t0 });
    } catch (err) {
      results.push({
        name,
        pass: false,
        ms: clock.now() - t0,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
  if (!adapter) {
    const t0 = clock.now();
    try {
      await testAzureAdapter();
      results.push({ name: "azure storage bus adapter", pass: true, ms: clock.now() - t0 });
    } catch (err) {
      results.push({
        name: "azure storage bus adapter",
        pass: false,
        ms: clock.now() - t0,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
  return results;
}

async function testFifo(a: Adapter) {
  const q = new Queue<number>("q", { adapter: a, namespace: "t" });
  const seen: number[] = [];
  await q.add("n", 1);
  await q.add("n", 2);
  await q.add("n", 3);
  await withWorker(a, "q", async (job) => {
    seen.push(job.data as number);
  }, async () => {
    await waitFor(() => seen.length === 3);
  });
  if (seen.join() !== "1,2,3") throw new Error(`got ${seen.join()}`);
}

async function testPriority(a: Adapter) {
  const q = new Queue<string>("q", { adapter: a, namespace: "t" });
  await q.add("n", "low", { priority: 1 });
  await q.add("n", "high", { priority: 10 });
  await q.add("n", "mid", { priority: 5 });
  const seen: string[] = [];
  await withWorker(a, "q", async (job) => {
    seen.push(job.data as string);
  }, async () => {
    await waitFor(() => seen.length === 3);
  });
  if (seen[0] !== "high") throw new Error(`first was ${seen[0]}`);
}

async function testDelayed(a: Adapter) {
  const q = new Queue("q", { adapter: a, namespace: "t" });
  const t0 = clock.now();
  await q.add("n", {}, { delay: 80 });
  let done = 0;
  await withWorker(a, "q", async () => {
    done += 1;
  }, async () => {
    await waitFor(() => done === 1);
  });
  if (clock.now() - t0 < 70) throw new Error("fired too early");
}

async function testRetry(a: Adapter) {
  const q = new Queue("q", { adapter: a, namespace: "t" });
  let n = 0;
  await q.add("n", {}, { attempts: 3, backoff: { type: "fixed", delay: 20 } });
  await withWorker(a, "q", async () => {
    n += 1;
    if (n < 3) throw new Error("boom");
    return "ok";
  }, async () => {
    await waitFor(() => n === 3);
  });
  const counts = await q.getJobCounts();
  if (counts.completed < 1) throw new Error("not completed");
}

async function testUnrecoverable(a: Adapter) {
  const q = new Queue("q", { adapter: a, namespace: "t" });
  let n = 0;
  await q.add("n", {}, { attempts: 5 });
  await withWorker(a, "q", async () => {
    n += 1;
    throw new UnrecoverableError("nope");
  }, async () => {
    await waitFor(async () => (await q.getJobCounts()).failed === 1);
  });
  if (n !== 1) throw new Error(`tried ${n} times`);
}

async function testConcurrency(a: Adapter) {
  const q = new Queue("q", { adapter: a, namespace: "t" });
  let max = 0;
  let current = 0;
  for (let i = 0; i < 8; i++) await q.add("n", i);
  await withWorker(
    a,
    "q",
    async () => {
      current += 1;
      max = Math.max(max, current);
      await sleep(40);
      current -= 1;
    },
    async () => {
      await waitFor(async () => (await q.getJobCounts()).completed === 8, 5000);
    },
    { concurrency: 4 },
  );
  if (max < 3) throw new Error(`max concurrency ${max}`);
}

async function testNamespaces(a: Adapter) {
  const acme = new Queue("mail", { adapter: a, namespace: "acme" });
  const solo = new Queue("mail", { adapter: a, namespace: "solo" });
  await acme.add("a", { t: "acme" });
  await solo.add("a", { t: "solo" });
  const seen: string[] = [];
  await withWorker(
    a,
    "mail",
    async (job) => {
      seen.push((job.data as { t: string }).t);
    },
    async () => {
      await waitFor(() => seen.length === 1);
    },
    { namespace: "acme" },
  );
  if (seen.join() !== "acme") throw new Error(`leaked ${seen.join()}`);
  const soloCounts = await solo.getJobCounts();
  if (soloCounts.waiting !== 1) throw new Error("solo job was consumed");
}

async function testIdempotency(a: Adapter) {
  const q = new Queue("q", { adapter: a, namespace: "t" });
  const a1 = await q.add("n", { n: 1 }, { idempotencyKey: "k1" });
  const a2 = await q.add("n", { n: 2 }, { idempotencyKey: "k1" });
  if (a1.id !== a2.id) throw new Error("expected same job");
  const counts = await q.getJobCounts();
  if (counts.waiting !== 1) throw new Error("duplicated");
}

async function testPause(a: Adapter) {
  const q = new Queue("q", { adapter: a, namespace: "t" });
  await q.add("n", 1);
  await q.pause();
  let n = 0;
  await withWorker(a, "q", async () => {
    n += 1;
  }, async () => {
    await sleep(80);
    if (n !== 0) throw new Error("processed while paused");
    await q.resume();
    await waitFor(() => n === 1);
  });
}

async function testGroup(a: Adapter) {
  const q = new Queue("q", { adapter: a, namespace: "t" });
  for (let i = 0; i < 4; i++) {
    await q.add("n", i, { group: { id: "user-1", max: 1 } });
  }
  let max = 0;
  let current = 0;
  await withWorker(
    a,
    "q",
    async () => {
      current += 1;
      max = Math.max(max, current);
      await sleep(30);
      current -= 1;
    },
    async () => {
      await waitFor(async () => (await q.getJobCounts()).completed === 4, 5000);
    },
    { concurrency: 4 },
  );
  if (max !== 1) throw new Error(`group max was ${max}`);
}

async function testFlow(a: Adapter) {
  const flow = new FlowProducer({ adapter: a, namespace: "t" });
  const { job } = await flow.add({
    name: "parent",
    queueName: "q",
    data: { kind: "parent" },
    children: [
      { name: "c1", queueName: "q", data: { kind: "c" } },
      { name: "c2", queueName: "q", data: { kind: "c" } },
    ],
  });
  const order: string[] = [];
  await withWorker(a, "q", async (j) => {
    order.push(j.name);
  }, async () => {
    await waitFor(() => order.length === 3, 4000);
  });
  const parentLast = order[2] === "parent";
  if (!parentLast) throw new Error(`order ${order.join()}`);
  const refreshed = await job.refresh();
  if (refreshed.status !== "completed") throw new Error(refreshed.status);
}

async function testWorkflow(a: Adapter) {
  const orch = new Orchestrator({ adapter: a, namespace: "t", concurrency: 4 });
  const def = defineWorkflow<{ n: number }>("sum", {
    a: { run: async ({ input }) => input.n + 1 },
    b: { run: async ({ input }) => input.n + 2 },
    c: {
      needs: ["a", "b"],
      run: async ({ steps }) => (steps.a as number) + (steps.b as number),
    },
  });
  orch.register(def);
  const wf = await orch.run("sum", { n: 10 });
  await waitFor(async () => {
    const cur = await orch.get(wf.id);
    return cur?.status === "completed";
  }, 5000);
  const done = await orch.get(wf.id);
  await orch.close();
  if (done?.stepResults.c !== 23) throw new Error(`got ${JSON.stringify(done?.stepResults)}`);
}

async function testProgress(a: Adapter) {
  const q = new Queue("q", { adapter: a, namespace: "t" });
  const job = await q.add("n", {});
  await withWorker(a, "q", async (j) => {
    await j.updateProgress(40);
    await j.log("halfway");
    await j.updateProgress(100);
  }, async () => {
    await waitFor(async () => (await q.getJobCounts()).completed === 1);
  });
  const fresh = await q.getJob(job.id);
  if (!fresh) throw new Error("missing");
  if (fresh.progress !== 100) throw new Error("progress");
  if (!fresh.record.logs.some((l) => l.message === "halfway")) throw new Error("log");
}

async function testAzureAdapter() {
  const a = new AzureStorageBusAdapter(createMemoryAzureBus());
  await testFifo(a);
  await a.reset();
  await testPriority(a);
  await a.reset();
  await testDelayed(a);
  await a.reset();
  await testNamespaces(a);
  await a.reset();
  await testIdempotency(a);
  await a.reset();
  await testPause(a);
  await a.reset();
  await testGroup(a);
  await a.reset();
  await testWorkflow(a);
}

export async function assertAllPass(adapter?: Adapter) {
  const results = await runSelfTest(adapter);
  const failed = results.filter((r) => !r.pass);
  if (failed.length) {
    const msg = failed.map((f) => `${f.name}: ${f.error}`).join("\n");
    throw new Error(msg);
  }
  return results;
}
