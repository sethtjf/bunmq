# BunMQ

Durable queues for Bun. Small surface. Pluggable storage. Multi-tenant. Workflows.

Inspired by BullMQ. Built only for Bun — Node is not a goal.

```ts
import { Database } from "bun:sqlite"
import { createBunMQ, SqliteAdapter } from "bunmq"

const bunmq = createBunMQ({
  adapter: new SqliteAdapter(new Database("bunmq.db")),
  tenant: "acme",
})

await bunmq.queue("mail").add("welcome", { to: "ada@bunmq.dev" }, {
  attempts: 5,
  backoff: { type: "exponential", delay: 400 },
})

bunmq.worker("mail", async (job) => {
  await send(job.data)
})
```

## Install

Bun can import this repo directly until it is on npm:

```sh
bun add github:sethtjf/bunmq
```

Or vendor `src/`. There are **no runtime dependencies**.

## Adapters

Storage is one interface. Swap them without changing queues or workers.

| Adapter | Use |
|---|---|
| `MemoryAdapter` | Tests, in-process |
| `SqliteAdapter` | `bun:sqlite`. One file, local or edge |
| `PostgresAdapter` | Cloud default. Any tagged-query client |
| `RedisAdapter` | Inject Bun’s `RedisClient` or anything like it |
| `FileAdapter` | JSON snapshot via `Bun.write` or `localStorage` |
| `AzureStorageBusAdapter` | Table Storage for records, Service Bus for dispatch |

You inject the client. BunMQ never imports `pg`, `ioredis`, or `@azure/*`.

```ts
import { TableClient } from "@azure/data-tables"
import { ServiceBusClient } from "@azure/service-bus"
import { AzureStorageBusAdapter, azureFromSdk, createBunMQ } from "bunmq"

const bus = new ServiceBusClient(process.env.AZURE_SERVICEBUS!)
const bunmq = createBunMQ({
  adapter: new AzureStorageBusAdapter(azureFromSdk({
    table: TableClient.fromConnectionString(process.env.AZURE_STORAGE!, "bunmq"),
    sender: bus.createSender("bunmq"),
    receiver: bus.createReceiver("bunmq"),
  })),
  tenant: "acme",
})
```

Write your own by implementing `Adapter`.

## Workers

Workers claim with a lock, heartbeat while running, and respect pause, tenant scope, and group caps.

```ts
const w = bunmq.worker("mail", async (job, { signal }) => {
  await job.updateProgress(10)
  await job.log("sending")
  await send(job.data, signal)
}, { concurrency: 8 })

w.use(async (job, next) => {
  const t = Date.now()
  try { return await next() }
  finally { await job.log(`${Date.now() - t}ms`) }
})
```

Jobs support delay, priority, retries with backoff, idempotency keys, repeat (`cron` / `every`), and per-group concurrency.

## Tenancy

Every record carries a tenant. Workers bind to one tenant or `*` to drain them all. Pause, counts, and workflows stay isolated.

## Workflows

`defineWorkflow` is a DAG of steps. Fan-out, fan-in, retries, and compensation. `FlowProducer` if you only need parent-waits-for-children.

```ts
const onboard = defineWorkflow("onboard", {
  createUser: { run: ({ input }) => users.create(input) },
  mail: { needs: ["createUser"], run: ({ steps }) => send(steps.createUser) },
  provision: { needs: ["createUser"], run: ({ steps }) => slots.grant(steps.createUser) },
  notify: { needs: ["mail", "provision"], run: () => "ok" },
})

const orch = bunmq.orchestrator()
orch.register(onboard)
await orch.run("onboard", { email: "ada@bunmq.dev" })
```

## Test

```sh
bun src/run-selftest.ts
```

Covers FIFO, priority, delays, retries, concurrency, tenants, idempotency, pause, groups, parent/child flows, workflow DAGs, and the Azure Storage Bus adapter.

## License

MIT
