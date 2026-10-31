// Package balancer is the front's per-ModelDeployment balancer: KubeAI's
// bounded-load consistent hashing (CHWBL), copied unchanged under
// third_party/forked/kubeai, and the wrapper HBEP-5 requires around it
// (Load bound and cap).
//
// Provenance (HBEP-15):
//   - KubeAI's group and its CHWBL pick: copied code, Apache-2.0, at
//     third_party/forked/kubeai (commit 89dfff46), reached through the fork's
//     exported handle (loadbalancer.Group). Nothing here re-implements its
//     pick; the replica walk applies its bound check (LoadOK) only to the
//     members a filter admits.
//   - The wrapper is product code, rewritten from spike S10's
//     spikes/s10/front (serialised pick, once-guarded release, release at a
//     drain), S10b's ring replica and capped pick, and S10c's adaptive cap
//     (this repository, PRs #34, #50 and #53).
//   - The ring hash, github.com/cespare/xxhash v1.1.0 (MIT): the version the
//     fork uses, so the replica's points are the fork's.
package balancer
