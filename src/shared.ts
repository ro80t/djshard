import { timingSafeEqual } from "node:crypto";

// Wire protocol, shared by manager <-> cluster IPC and manager <-> bridge WebSocket.
// The Go operator's bridge speaks the same JSON (operator/bridge.go).
export type Msg =
  | { t: "hello"; auth: string; name: string }
  | { t: "assign"; totalShards: number; totalClusters: number; clusters: { id: number; shards: number[] }[] }
  | { t: "eval"; id: number; script: string; context?: unknown; cluster?: number }
  | { t: "res"; id: number; ok: boolean; data?: unknown; error?: string }
  | { t: "error"; message: string };

export type Assign = Extract<Msg, { t: "assign" }>;
export type Script<C = any, X = any, R = unknown> = string | ((client: C, context: X) => R);
export interface EvalOptions {
  context?: unknown;
  /** Only run on this cluster id. */
  cluster?: number;
  timeout?: number;
}

export const toSource = (s: Script) => (typeof s === "function" ? s.toString() : s);

/** Correlates request ids with responses; one instance per connection. */
export class Pending {
  #next = 0;
  #map = new Map<number, { resolve(v: unknown): void; reject(e: Error): void; timer: Timer }>();

  open(timeout = 30_000): { id: number; promise: Promise<any> } {
    const id = ++this.#next;
    const { promise, resolve, reject } = Promise.withResolvers<unknown>();
    const timer = setTimeout(() => this.#map.delete(id) && reject(new Error(`request ${id} timed out`)), timeout);
    this.#map.set(id, { resolve, reject, timer });
    return { id, promise };
  }

  settle(m: Extract<Msg, { t: "res" }>) {
    const p = this.#map.get(m.id);
    if (!p) return;
    this.#map.delete(m.id);
    clearTimeout(p.timer);
    if (m.ok) p.resolve(m.data);
    else p.reject(new Error(m.error));
  }

  rejectAll(reason: string) {
    for (const p of this.#map.values()) {
      clearTimeout(p.timer);
      p.reject(new Error(reason));
    }
    this.#map.clear();
  }
}

/** Await `work` and send its outcome back as a `res` message. */
export function respond(id: number, work: () => Promise<unknown>, send: (m: Msg) => void) {
  work().then(
    (data) => send({ t: "res", id, ok: true, data }),
    (e) => send({ t: "res", id, ok: false, error: String(e?.message ?? e) }),
  );
}

export function chunk(totalShards: number, shardsPerCluster: number): number[][] {
  return Array.from({ length: Math.ceil(totalShards / shardsPerCluster) }, (_, c) =>
    Array.from(
      { length: Math.min(shardsPerCluster, totalShards - c * shardsPerCluster) },
      (_, i) => c * shardsPerCluster + i,
    ),
  );
}

/** Clusters for machine `slot` out of `machines`. Mirrors assignment() in operator/bridge.go. */
export function assignment(totalShards: number, shardsPerCluster: number, machines: number, slot: number): Assign {
  const all = chunk(totalShards, shardsPerCluster);
  const per = Math.ceil(all.length / machines);
  const clusters = all.slice(slot * per, slot * per + per).map((shards, i) => ({ id: slot * per + i, shards }));
  return { t: "assign", totalShards, totalClusters: all.length, clusters };
}

export async function fetchRecommendedShards(token: string): Promise<number> {
  const res = await fetch("https://discord.com/api/v10/gateway/bot", { headers: { Authorization: `Bot ${token}` } });
  if (!res.ok) throw new Error(`GET /gateway/bot failed: ${res.status}`);
  return ((await res.json()) as { shards: number }).shards;
}

export const safeEqual = (a: string, b: string) => {
  const x = Buffer.from(a),
    y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
};
