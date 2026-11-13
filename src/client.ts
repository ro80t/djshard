import { Pending, respond, toSource, type EvalOptions, type Msg, type Script } from "./shared.ts";

export interface ClusterInfo {
  id: number;
  /** Pass to your Discord client as `shards`. */
  shards: number[];
  /** Pass to your Discord client as `shardCount`. */
  totalShards: number;
  totalClusters: number;
}

/** Cluster info injected by ClusterManager. Throws outside a managed cluster process. */
export function getInfo(): ClusterInfo {
  const raw = process.env.DJSHARD_CLUSTER;
  if (!raw) throw new Error("not running inside a djshard cluster");
  return JSON.parse(raw);
}

/** Runs inside a cluster process. `client` is what broadcastEval scripts receive. */
export class ClusterClient<C = unknown> {
  readonly info = getInfo();
  #pending = new Pending();

  constructor(readonly client: C) {
    process.on("message", (m: Msg) => {
      if (m.t === "res") this.#pending.settle(m);
      else if (m.t === "eval")
        respond(
          m.id,
          async () => evaluate(m.script, this.client, m.context),
          (r) => process.send!(r),
        );
    });
  }

  get id() {
    return this.info.id;
  }

  /** Runs `script` on every cluster across every machine; results ordered by cluster. */
  broadcastEval<R, X = undefined>(
    script: Script<C, X, R>,
    opts: EvalOptions & { context?: X } = {},
  ): Promise<Awaited<R>[]> {
    const { id, promise } = this.#pending.open(opts.timeout);
    process.send!({
      t: "eval",
      id,
      script: toSource(script),
      context: opts.context,
      cluster: opts.cluster,
    } satisfies Msg);
    return promise;
  }

  /** e.g. fetchClientValues("guilds.cache.size") */
  fetchClientValues(path: string): Promise<unknown[]> {
    return this.broadcastEval((c: any, p: string) => p.split(".").reduce((o, k) => o?.[k], c), { context: path });
  }
}

function evaluate(script: string, client: unknown, context: unknown) {
  // Scripts only come from our own manager/bridge (authenticated), same trust as the bot code.
  return new Function("client", "context", `return (${script})(client, context)`)(client, context);
}
