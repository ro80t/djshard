import { EventEmitter } from "node:events";
import { hostname } from "node:os";
import type { Subprocess } from "bun";
import {
  Pending,
  chunk,
  fetchRecommendedShards,
  respond,
  toSource,
  type Assign,
  type EvalOptions,
  type Msg,
  type Script,
} from "./shared.ts";

export interface ManagerOptions {
  /** Ignored when a bridge is used (the bridge decides). Default "auto". */
  totalShards?: number | "auto";
  shardsPerCluster?: number;
  /** Needed for totalShards "auto". Default DISCORD_TOKEN. */
  token?: string;
  /** Delay between cluster spawns, for identify rate limits. */
  spawnDelay?: number;
  respawn?: boolean;
  /** Cross-host mode. Defaults from DJSHARD_BRIDGE_URL / DJSHARD_AUTH / DJSHARD_ID (set by the k8s operator). */
  bridge?: { url: string; auth: string; id?: string };
}

interface Cluster {
  id: number;
  shards: number[];
  proc?: Subprocess;
  pending: Pending;
  stopped: boolean;
}

/**
 * Spawns one process per cluster, each running several shards.
 * Events: "spawn" (clusterId), "exit" (clusterId, code), "debug" (message).
 */
export class ClusterManager extends EventEmitter {
  readonly clusters = new Map<number, Cluster>();
  totalShards = 0;
  totalClusters = 0;
  #opts;
  #bridge;
  #ws?: WebSocket;
  #online = false;
  #wsPending = new Pending();
  #assignKey = "";
  #queue = Promise.resolve();
  #closed = false;

  constructor(
    readonly file: string,
    opts: ManagerOptions = {},
  ) {
    super();
    const env = process.env;
    this.#opts = {
      totalShards: "auto" as number | "auto",
      shardsPerCluster: 4,
      token: env.DISCORD_TOKEN,
      spawnDelay: 5500,
      respawn: true,
      ...opts,
    };
    const url = opts.bridge?.url ?? env.DJSHARD_BRIDGE_URL;
    this.#bridge = url
      ? {
          url,
          auth: opts.bridge?.auth ?? env.DJSHARD_AUTH ?? "",
          id: opts.bridge?.id ?? env.DJSHARD_ID ?? hostname(),
        }
      : undefined;
  }

  /** Resolves once the first set of clusters has been spawned. */
  async spawn(): Promise<void> {
    if (this.#bridge) {
      const { promise, resolve, reject } = Promise.withResolvers<void>();
      this.once("assigned", resolve);
      this.once("rejected", reject);
      this.#connect();
      return promise;
    }
    const { shardsPerCluster, token } = this.#opts;
    let total = this.#opts.totalShards;
    if (total === "auto") {
      if (!token) throw new Error('totalShards "auto" needs a token');
      total = await fetchRecommendedShards(token);
    }
    const clusters = chunk(total, shardsPerCluster).map((shards, id) => ({ id, shards }));
    return this.#apply({ t: "assign", totalShards: total, totalClusters: clusters.length, clusters });
  }

  /** Runs `script` on every cluster (across all machines when bridged). */
  broadcastEval<R>(script: Script<any, any, R>, opts: EvalOptions = {}): Promise<Awaited<R>[]> {
    const source = toSource(script);
    if (!this.#bridge) return this.evalLocal(source, opts);
    if (!this.#online) return Promise.reject(new Error("bridge not connected"));
    const { id, promise } = this.#wsPending.open(opts.timeout);
    this.#send({ t: "eval", id, script: source, context: opts.context, cluster: opts.cluster });
    return promise;
  }

  /** Runs `script` on this machine's clusters only. */
  evalLocal(script: string, { context, cluster, timeout }: EvalOptions = {}): Promise<any[]> {
    const targets = [...this.clusters.values()].filter((c) => cluster === undefined || c.id === cluster);
    return Promise.all(
      targets.map((c) => {
        if (!c.proc || c.proc.killed) return Promise.reject(new Error(`cluster ${c.id} is not running`));
        const { id, promise } = c.pending.open(timeout);
        c.proc.send({ t: "eval", id, script, context } satisfies Msg);
        return promise;
      }),
    );
  }

  /** Kills every cluster in order and spawns them again. */
  respawnAll(): Promise<void> {
    const clusters = [...this.clusters.values()].map(({ id, shards }) => ({ id, shards }));
    return this.#apply(
      { t: "assign", totalShards: this.totalShards, totalClusters: this.totalClusters, clusters },
      true,
    );
  }

  destroy() {
    this.#closed = true;
    this.#ws?.close();
    for (const c of this.clusters.values()) this.#stop(c);
  }

  // Serialized so a reassignment arriving mid-spawn waits its turn.
  #apply(a: Assign, force = false): Promise<void> {
    // A failed step must not poison the queue, or every later reassignment is skipped.
    const run = this.#queue
      .catch(() => {})
      .then(async () => {
        const key = JSON.stringify(a);
        if (key === this.#assignKey && !force) return;
        this.#assignKey = key;
        for (const c of this.clusters.values()) this.#stop(c);
        this.clusters.clear();
        this.totalShards = a.totalShards;
        this.totalClusters = a.totalClusters;
        for (const [i, { id, shards }] of a.clusters.entries()) {
          if (this.#closed) return;
          if (i > 0) await Bun.sleep(this.#opts.spawnDelay);
          const c: Cluster = { id, shards, pending: new Pending(), stopped: false };
          this.clusters.set(id, c);
          this.#start(c);
        }
      });
    this.#queue = run;
    return run;
  }

  #start(c: Cluster) {
    const info = { id: c.id, shards: c.shards, totalShards: this.totalShards, totalClusters: this.totalClusters };
    c.proc = Bun.spawn([process.execPath, this.file], {
      env: { ...process.env, DJSHARD_CLUSTER: JSON.stringify(info) },
      stdio: ["inherit", "inherit", "inherit"],
      ipc: (m: Msg) => {
        if (m.t === "res") c.pending.settle(m);
        else if (m.t === "eval")
          respond(
            m.id,
            () => this.broadcastEval(m.script, m),
            (r) => c.proc?.send(r),
          );
      },
      onExit: (_, code) => {
        c.pending.rejectAll(`cluster ${c.id} exited`);
        this.emit("exit", c.id, code);
        // ponytail: fixed 5s respawn delay, no heartbeat; hung-but-alive processes are not detected.
        if (!c.stopped && !this.#closed && this.#opts.respawn) setTimeout(() => c.stopped || this.#start(c), 5000);
      },
    });
    this.emit("spawn", c.id);
  }

  #stop(c: Cluster) {
    c.stopped = true;
    c.proc?.kill();
  }

  #send(m: Msg) {
    this.#ws?.send(JSON.stringify(m));
  }

  #connect() {
    const b = this.#bridge!;
    const ws = (this.#ws = new WebSocket(b.url));
    ws.onopen = () => this.#send({ t: "hello", auth: b.auth, name: b.id });
    ws.onmessage = (e) => {
      let m: Msg;
      try {
        m = JSON.parse(String(e.data));
      } catch {
        return this.emit("debug", "bridge sent bad json");
      }
      this.#onBridge(m);
    };
    ws.onclose = () => {
      this.#online = false;
      this.#wsPending.rejectAll("bridge disconnected");
      this.emit("debug", "bridge disconnected");
      // Clusters keep running; reconnect and only respawn if the assignment changed.
      if (!this.#closed) setTimeout(() => this.#connect(), 5000);
    };
  }

  #onBridge(m: Msg) {
    switch (m.t) {
      case "assign":
        this.#online = true;
        this.#apply(m).then(
          () => this.emit("assigned"),
          (e) => this.emit("rejected", e),
        );
        break;
      case "eval":
        respond(
          m.id,
          () => this.evalLocal(m.script, m),
          (r) => this.#send(r),
        );
        break;
      case "res":
        this.#wsPending.settle(m);
        break;
      case "error":
        this.emit("debug", `bridge: ${m.message}`);
        this.emit("rejected", new Error(m.message));
    }
  }
}
