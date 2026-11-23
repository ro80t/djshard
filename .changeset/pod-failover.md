---
"djshard": minor
---

Kubernetes: when a pod has been gone for 30s (`--failover-grace`), the operator hands its clusters to the remaining pods until it comes back. `ClusterManager` now applies reassignments incrementally, so clusters it keeps are not restarted.
