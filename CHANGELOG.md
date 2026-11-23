# djshard

## 0.2.0

### Minor Changes

- 2904813: Kubernetes: when a pod has been gone for 30s (`--failover-grace`), the operator hands its clusters to the remaining pods until it comes back. `ClusterManager` now applies reassignments incrementally, so clusters it keeps are not restarted.

## 0.1.1

### Patch Changes

- b668ea9: Document installing with npm, pnpm and yarn, and note that djshard must run on Bun.
