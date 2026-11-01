// Package alpha holds the live runs of the alpha runbook (issue #194,
// docs/alpha-runbook.md): the rows of hack/live-checks.sh that put the whole
// stack on a real cluster, behind the owner's dial-home route.
//
//	alpha-athena-handover   HBEP-7 and HBEP-5: a handover with a stream in flight cuts nothing
//	alpha-helios-nooverlap  HBEP-7: no-overlap mode with the replacement queued ahead, the gap measured
//	tunnel-hour-helios/-athena  HBEP-6: S1's one-hour run over this wiring, the path rate measured
//	gpu-accounting-*        HBEP-8: the kept accounting record's GPU entry (internal/providerctl/conformance)
//
// The package splits a run in two. The scenario, which starts the stack and
// drives it, produces an observation (observe.go) through the Steps interface
// (steps.go). The pass criteria (verdict.go) are pure functions of that
// observation and of the cluster's own record of the run's jobs (the Ledger),
// each quoting the HBEP line it enforces. Run (run.go) joins the two: it hands
// the scenario a Recorder whose job-tags.txt is written ahead of every
// submission (the script cancels a run's jobs by those tags), judges the
// observation, and writes the text to post.
//
// Steps are supplied by the test files: the stand-in steps of the dry run
// (dry_test.go), which drive a fake Slurm in run mode, and the live steps
// (steps_live_test.go), which wait for the one-host rehearsal's scenario code
// (PR #161) and fail loudly until it is wired.
//
// Provenance: new package. The judged criteria are HBEP-5, HBEP-6, HBEP-7 and
// HBEP-8's; the path-rate rule is spike S1's verdict C6 (spikes/s1-frp).
package alpha
