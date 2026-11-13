# djshard

Discord ボット向けのシャーディングライブラリ（Bun 用）。discord-hybrid-sharding（1 プロセスで複数シャード）と discord-cross-hosting（複数マシンへの分散）の機能をまとめたもので、Kubernetes 用のオペレーター（Go）も付いています。

## 単一マシンで使う

```ts
// manager.ts
import { ClusterManager } from "djshard";
const manager = new ClusterManager("./bot.ts", { shardsPerCluster: 4 }); // totalShards: "auto" + DISCORD_TOKEN
await manager.spawn();
```

```ts
// bot.ts
import { Client } from "discord.js";
import { ClusterClient, getInfo } from "djshard";
const info = getInfo();
const client = new Client({ intents: [], shards: info.shards, shardCount: info.totalShards });
client.cluster = new ClusterClient(client);
await client.login();

const guilds = await client.cluster.broadcastEval((c) => c.guilds.cache.size); // 全クラスタ分
```

## 複数マシンで使う（k8s なし）

```ts
import { Bridge } from "djshard";
await new Bridge({ port: 4444, auth: "secret", machines: 3, totalShards: "auto", token }).listen();
```

各マシンで `new ClusterManager("./bot.ts", { bridge: { url: "ws://bridge:4444", auth: "secret" } })` を起動します。`broadcastEval` は全マシンにまたがって実行されます。

## Kubernetes

```sh
kubectl apply -f operator/config/crd.yaml -f operator/config/operator.yaml
kubectl create secret generic discord --from-literal=token=...
kubectl apply -f operator/config/example.yaml
```

オペレーターは `ShardCluster` ごとに StatefulSet と認証用の Secret を作り、`DJSHARD_BRIDGE_URL` / `DJSHARD_AUTH` / `DJSHARD_ID` を最初のコンテナに注入します。`ClusterManager` はこれらの環境変数を自動で読み込みます。ブリッジはオペレーター本体の中で動き、Pod 名の序数（`mybot-2` → 2）でシャードを割り当てます。

## 制約

- オペレーターは 1 レプリカのみで動かしてください（接続はメモリ上で保持しています）。
- `replicas` / `totalShards` / `shardsPerCluster` を変更すると全 Pod がローリング再起動されます。再起動中は、同じシャードを新旧の Pod が一時的に重複して持つことがあります。
- ブリッジ通信は平文の ws です。認証トークンでしか保護していないため、クラスタ外へは公開しないでください。
- broadcastEval の戻り値は JSON にシリアライズできる値に限ります。

## 開発

```sh
bun run build               # tsdown -> dist/
bun run ci                  # oxlint + oxfmt --check + bun check + bun test + build
cd operator && go test ./...
```
