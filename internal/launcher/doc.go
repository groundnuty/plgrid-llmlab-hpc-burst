// Package launcher starts and supervises the single-user control-plane host of HBEP-11: a
// pinned k3s running as the invoking user (server --rootless --disable-agent, SQLite).
//
// It is the only code in this module that starts k3s. The package is Linux-only, because it
// ties every health and stop decision to /proc: a process counts as ours only if its PID, its
// start time and the machine's boot ID match what the launcher recorded, and a listener
// counts as ours only if a socket of that process holds it. Nothing here decides anything
// from "something answers on that port", because another k3s may run on the same machine.
//
// Several files adapt code from spike S3's harness (spikes/s3/cmd/s3bench/proc.go,
// hosts.go, flow.go) in this repository; each says so. Nothing imports from spikes/.
package launcher
