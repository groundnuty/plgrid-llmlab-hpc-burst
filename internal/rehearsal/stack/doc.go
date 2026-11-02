// Package stack is test support that starts and drives the whole stack on
// this host: hpcburst up (k3s, the controller, frps and the front), the API,
// the front's client side and usage records, and, for a stand-in run, the fake
// Slurm in run mode behind the rehearsal's login node and a Hub double. The
// alpha rows' live steps (internal/alpha, issue #245) drive a real cluster
// with it, and their stand-in proof drives the fake one. Only _test.go files
// import it, as they do internal/rehearsal.
//
// Every helper here is a near-copy of the one-host rehearsal's own, from
// internal/rehearsal/scenario's _test.go files (issue #146, PR #161), which no
// other package can import: the same names, exported, and the same behaviour,
// with the rehearsal's fixed ports, names and timing passed in. The copies are
// temporary: issue #251 moves the scenario onto this package once PR #247
// merges, and removes them there. Change a helper here and in the scenario
// together until then.
//
// Everything is asserted from the API, events, the front's usage files,
// /healthz and /metrics, and frps's proxy list, never from log lines.
//
// Provenance: new package; copied from internal/rehearsal/scenario
// (harness_test.go, objects_test.go, watch_test.go, client_test.go,
// front_test.go, hub_test.go and steps_test.go), each file saying which.
package stack
