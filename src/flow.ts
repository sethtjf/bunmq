import type { Adapter } from "./adapter";
import { Job, createRecord } from "./job";
import type { JobOptions, JobRecord } from "./types";
import { clock, id } from "./util";

export type FlowNode<T = unknown> = {
  name: string;
  queueName: string;
  data: T;
  opts?: JobOptions;
  children?: FlowNode[];
};

export type FlowProducerOptions = {
  adapter: Adapter;
  namespace?: string;
};

export class FlowProducer {
  readonly adapter: Adapter;
  readonly namespace: string;

  constructor(opts: FlowProducerOptions) {
    this.adapter = opts.adapter;
    this.namespace = opts.namespace ?? "default";
  }

  async add<T>(node: FlowNode<T>): Promise<{ job: Job<T>; children: Job[] }> {
    const built = this.build(node, this.namespace);
    await this.adapter.addJobs(built.records);
    const ts = clock.now();
    await Promise.all(
      built.records.map((r) =>
        this.adapter.appendEvent({
          id: id("evt"),
          namespace: r.namespace,
          queue: r.queue,
          jobId: r.id,
          type: r.status === "waiting-children" ? "waiting" : "added",
          payload: { flow: true, name: r.name },
          timestamp: ts,
        }),
      ),
    );
    const parent = built.records[0]!;
    return {
      job: new Job(parent as JobRecord<T>, this.adapter),
      children: built.records.slice(1).map((r) => new Job(r, this.adapter)),
    };
  }

  private build(node: FlowNode, namespace: string): { records: JobRecord[]; root: JobRecord } {
    const children = (node.children ?? []).map((c) => this.build(c, namespace));
    const childRecords = children.flatMap((c) => c.records);
    const roots = children.map((c) => c.root);
    const record = createRecord({
      namespace: node.opts?.namespace ?? namespace,
      queue: node.queueName,
      name: node.name,
      data: node.data,
      opts: node.opts ?? {},
    });
    if (roots.length > 0) {
      record.status = "waiting-children";
      record.pendingChildren = roots.length;
      for (const child of roots) {
        child.parentId = record.id;
        child.parentQueue = record.queue;
      }
    }
    return { records: [record, ...childRecords], root: record };
  }
}
