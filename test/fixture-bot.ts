import { ClusterClient, getInfo } from "../src/index.ts";

// Stand-in for a discord.js Client: new Client({ shards: info.shards, shardCount: info.totalShards })
const client: any = { shards: getInfo().shards };
client.cluster = new ClusterClient(client);
