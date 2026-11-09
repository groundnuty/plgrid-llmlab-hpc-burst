// Package stack is test support that starts and drives the whole stack on
// this host: hpcburst up (k3s, the controller, frps and the front), the API,
// the front's client side and usage records, and, for a stand-in run, the fake
// Slurm in run mode behind the rehearsal's login node and a Hub double. The
// one-host rehearsal (internal/rehearsal/scenario, issue #146) drives the fake
// cluster with it, the alpha rows' live steps (internal/alpha, issue #245) a
// real one, and their stand-in proof the fake one. Only _test.go files import
// it, as they do internal/rehearsal.
//
// It holds the one implementation of each helper (issue #251, held by the
// scenario's TestNoCopyOfStack). Where the rehearsal and the alpha rows need
// different behaviour, the choice is explicit: ClusterShape's Around and
// WrapApptainer, the rehearsal's own fixtures, whose zero values leave them out;
// Send, SendCtx and SendTimed; and StartHost's frontTuning.
//
// Everything is asserted from the API, events, the front's usage files,
// /healthz and /metrics, and frps's proxy list, never from log lines.
//
// Provenance: new package; moved from internal/rehearsal/scenario's _test.go
// files (issue #146, PR #161), with what issue #245 added for the alpha rows.
package stack
