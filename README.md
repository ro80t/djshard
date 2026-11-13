# djshard

Clustered and cross-host sharding for Discord bots on [Bun](https://bun.sh), plus a Kubernetes operator.

djshard combines what [discord-hybrid-sharding](https://github.com/meister03/discord-hybrid-sharding) and [discord-cross-hosting](https://github.com/meister03/discord-cross-hosting) do:

- **Clusters**: one process runs several shards, so you need far fewer processes than shards.
- **Cross-host**: shards are split across machines, and `broadcastEval` reaches every cluster on every machine.
- **Kubernetes**: a Go operator turns a `ShardCluster` resource into a StatefulSet and acts as the bridge between pods.

djshard is not tied to any Discord library. It tells each cluster which shards to run, and you pass them to your client.

> [!NOTE]
> djshard runs on Bun only. It uses `Bun.spawn` and `Bun.serve`.

## Install

```sh
bun add djshard
```

Both ESM (`import`) and CommonJS (`require`) builds are published.

## Single machine

```ts
// manager.ts
import { ClusterManager } from "djshard";

const manager = new ClusterManager("./bot.ts", { shardsPerCluster: 4 });
manager.on("exit", (id, code) => console.warn(`cluster ${id} exited with ${code}`));
await manager.spawn();
```

```ts
// bot.ts: runs once per cluster
import { Client } from "discord.js";
import { ClusterClient, getInfo } from "djshard";

const info = getInfo();
const client = new Client({ intents: [], shards: info.shards, shardCount: info.totalShards });
client.cluster = new ClusterClient(client);
await client.login();

// Runs on every cluster; results are ordered by cluster id.
const counts = await client.cluster.broadcastEval((c) => c.guilds.cache.size);
const same = await client.cluster.fetchClientValues("guilds.cache.size");
```

When `totalShards` is `"auto"` (the default), the manager asks Discord for the recommended shard count using `DISCORD_TOKEN`.

### `ClusterManager` options

| Option             | Default              | Description                                                           |
| ------------------ | -------------------- | --------------------------------------------------------------------- |
| `totalShards`      | `"auto"`             | Total shard count. Ignored when a bridge is used, because it decides. |
| `shardsPerCluster` | `4`                  | Shards per process.                                                   |
| `token`            | `DISCORD_TOKEN`      | Used to look up the recommended shard count.                          |
| `spawnDelay`       | `5500`               | Milliseconds between cluster spawns, to respect identify rate limits. |
| `respawn`          | `true`               | Restart clusters that exit.                                           |
| `bridge`           | from `DJSHARD_*` env | `{ url, auth, id? }`. Turns on cross-host mode.                       |

`broadcastEval(script, { context, cluster, timeout })` passes `context` to the script as its second argument and runs it only on `cluster` when one is given. Scripts are serialized with `toString()`, so they must not capture outer variables. Pass values through `context` instead. Return values must be JSON- or structured-clone-serializable.

## Multiple machines (without Kubernetes)

Run one bridge:

```ts
import { Bridge } from "djshard";

const bridge = new Bridge({ port: 4444, auth: process.env.BRIDGE_AUTH!, machines: 3, totalShards: "auto", token });
await bridge.listen();
```

On every machine:

```ts
const manager = new ClusterManager("./bot.ts", {
  bridge: { url: "ws://bridge-host:4444", auth: process.env.BRIDGE_AUTH!, id: "machine-a" },
});
await manager.spawn();
```

The bridge gives each machine a contiguous range of clusters. A machine that reconnects with the same `id` gets the same range back. Its clusters keep running while it is disconnected.

## Kubernetes

```sh
kubectl apply -f operator/config/crd.yaml -f operator/config/operator.yaml
kubectl create secret generic discord --from-literal=token=YOUR_TOKEN
kubectl apply -f operator/config/example.yaml
```

```yaml
apiVersion: djshard.ro80t.com/v1alpha1
kind: ShardCluster
metadata: { name: mybot }
spec:
  replicas: 3
  shardsPerCluster: 4
  # totalShards: 32   # omit to use Discord's recommendation (resolved once)
  tokenSecretRef: { name: discord, key: token }
  template:
    spec:
      containers:
        - name: bot
          image: mybot:latest
          command: [bun, manager.ts]
```

For each `ShardCluster`, the operator:

- creates a StatefulSet and a Secret holding a random bridge token,
- injects `DJSHARD_BRIDGE_URL`, `DJSHARD_AUTH`, `DJSHARD_ID` and `DISCORD_TOKEN` into the first container,
- serves the bridge itself and assigns shards by pod ordinal (`mybot-2` gets slot 2),
- rolls every pod when `replicas`, `totalShards` or `shardsPerCluster` changes.

Your `manager.ts` is the same as in single-machine mode, because `ClusterManager` picks up the `DJSHARD_*` variables automatically.

The operator image is published to `ghcr.io/ro80t/djshard-operator` (`latest` from `main`, plus a tag per `v*` release). To run your own build, build `operator/Dockerfile` and change the image in `operator/config/operator.yaml`.

## Limitations

- Run the operator as a single replica. Bot connections live in its memory.
- During a rolling restart, an old pod and a new pod can briefly own the same shard. Discord drops the older session.
- Bridge traffic is plain WebSocket protected only by the shared token. Keep it inside the cluster or private network.
- A cluster that hangs without exiting is not detected (there is no heartbeat).
- Identify rate limits (`max_concurrency`) are only respected within one machine, through `spawnDelay`.
- Only the process mode exists. There is no worker-thread mode or custom IPC messages yet.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
