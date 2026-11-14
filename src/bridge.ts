import type { Server, ServerWebSocket } from "bun";
import {
  Pending,
  assignment,
  fetchRecommendedShards,
  respond,
  safeEqual,
  toSource,
  type EvalOptions,
  type Msg,
  type Script,
} from "./shared.ts";

export interface BridgeOptions {
  port?: number;
  /** Shared secret every machine must present. */
  auth: string;
  /** Number of machines the clusters are split across. */
  machines: number;
  totalShards: number | "auto";
  shardsPerCluster?: number;
  token?: string;
}

interface Machine {
  id: string;
  slot: number;
  pending: Pending;
}

/** Hands out shard ranges to ClusterManagers on several hosts and routes cross-host broadcastEval. */
export class Bridge {
  totalShards = 0;
  #slots: (ServerWebSocket<Machine> | undefined)[];
  #sticky = new Map<string, number>(); // machine id -> last slot, so reconnects keep their shards
  #server?: Server<Machine>;

  constructor(readonly opts: BridgeOptions) {
    this.#slots = Array(opts.machines).fill(undefined);
  }

  /** Starts listening; returns the bound port. */
  async listen(): Promise<number> {
    const { totalShards, token } = this.opts;
    if (totalShards === "auto" && !token) throw new Error('totalShards "auto" needs a token');
    this.totalShards = totalShards === "auto" ? await fetchRecommendedShards(token!) : totalShards;
    this.#server = Bun.serve<Machine>({
      port: this.opts.port ?? 4444,
      fetch: (req, server) =>
        server.upgrade(req, { data: { id: "", slot: -1, pending: new Pending() } })
          ? undefined
          : new Response("djshard bridge", { status: 426 }),
      websocket: {
        message: (ws, raw) => {
          let m: Msg;
          try {
            m = JSON.parse(String(raw));
          } catch {
            return ws.close(1003, "bad json");
          }
          this.#onMessage(ws, m);
        },
        close: (ws) => {
          if (this.#slots[ws.data.slot] === ws) this.#slots[ws.data.slot] = undefined;
          ws.data.pending.rejectAll(`machine ${ws.data.id} disconnected`);
        },
      },
    });
    return this.#server.port!;
  }

  /** Runs `script` on every cluster of every connected machine. */
  async broadcastEval<R>(script: Script<any, any, R>, opts: EvalOptions = {}): Promise<Awaited<R>[]> {
    const parts = await Promise.all(
      this.#slots
        .filter((ws) => ws !== undefined)
        .map((ws) => {
          const { id, promise } = ws.data.pending.open(opts.timeout);
          send(ws, { t: "eval", id, script: toSource(script), context: opts.context, cluster: opts.cluster });
          return promise;
        }),
    );
    return parts.flat();
  }

  close() {
    this.#server?.stop(true);
  }

  #onMessage(ws: ServerWebSocket<Machine>, m: Msg) {
    if (ws.data.slot < 0) {
      if (m.t !== "hello" || typeof m.auth !== "string" || !safeEqual(m.auth, this.opts.auth)) {
        send(ws, { t: "error", message: "unauthorized" });
        return ws.close(1008, "unauthorized");
      }
      const prev = this.#sticky.get(m.name);
      const slot = prev !== undefined && !this.#slots[prev] ? prev : this.#slots.findIndex((s) => !s);
      if (slot < 0) {
        send(ws, { t: "error", message: "no free machine slot" });
        return ws.close(1008, "full");
      }
      Object.assign(ws.data, { id: String(m.name), slot });
      this.#slots[slot] = ws;
      this.#sticky.set(ws.data.id, slot);
      return send(ws, assignment(this.totalShards, this.opts.shardsPerCluster ?? 4, this.opts.machines, slot));
    }
    if (m.t === "eval")
      respond(
        m.id,
        () => this.broadcastEval(m.script, m),
        (r) => send(ws, r),
      );
    else if (m.t === "res") ws.data.pending.settle(m);
  }
}

const send = (ws: ServerWebSocket<Machine>, m: Msg) => ws.send(JSON.stringify(m));
