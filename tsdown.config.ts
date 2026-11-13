import { defineConfig } from "tsdown";

export default defineConfig({
  entry: "src/index.ts",
  platform: "node", // runs on Bun (Bun.spawn / Bun.serve); "bun" types are import-only
  format: ["esm", "cjs"],
  dts: true,
  deps: { neverBundle: ["bun"] }, // consumers bring @types/bun
});
