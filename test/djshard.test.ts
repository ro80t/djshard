import { afterAll, expect, test } from "bun:test";
import { Bridge, ClusterManager, assignment } from "../src/index.ts";

const bot = `${import.meta.dir}/fixture-bot.ts`;

test("assignment splits clusters across machines", () => {
  expect(assignment(10, 4, 2, 0).clusters).toEqual([
    { id: 0, shards: [0, 1, 2, 3] },
    { id: 1, shards: [4, 5, 6, 7] },
  ]);
  expect(assignment(10, 4, 2, 1).clusters).toEqual([{ id: 2, shards: [8, 9] }]);
  expect(assignment(10, 4, 5, 4).clusters).toEqual([]);
});

const bridge = new Bridge({ port: 0, auth: "s3cret", machines: 2, totalShards: 8, shardsPerCluster: 2 });
const port = await bridge.listen();
const url = `ws://localhost:${port}`;
const managers = ["a", "b"].map(
  (id) => new ClusterManager(bot, { spawnDelay: 0, bridge: { url, auth: "s3cret", id } }),
);
afterAll(() => {
  managers.forEach((m) => m.destroy());
  bridge.close();
});

test("cross-host broadcastEval, from manager and from inside a cluster", async () => {
  await Promise.all(managers.map((m) => m.spawn()));
  expect(await managers[0]!.broadcastEval((c: any) => c.shards)).toEqual([
    [0, 1],
    [2, 3],
    [4, 5],
    [6, 7],
  ]);
  const nested = await managers[1]!.broadcastEval((c: any) => c.cluster.broadcastEval((x: any) => x.shards[0]), {
    cluster: 3,
  });
  expect(nested).toEqual([[0, 2, 4, 6]]);
  expect(await bridge.broadcastEval((c: any, n: number) => c.shards.length * n, { context: 10 })).toEqual([
    20, 20, 20, 20,
  ]);
});

test("bad auth is rejected", async () => {
  const m = new ClusterManager(bot, { bridge: { url, auth: "nope" } });
  await expect(m.spawn()).rejects.toThrow("unauthorized");
  m.destroy();
});
