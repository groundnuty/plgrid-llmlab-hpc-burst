// Package obscheck holds the checks of HBEP-8's observability that the one-host rehearsal's part 3 makes, so that the
// alpha rows on a real cluster (internal/alpha, issue #376's P3 and P4) can make the same ones: the form of the
// events a run raised, with an expected set that must all appear and a forbidden set that must not (EventProblems),
// and the engine gauges per Instance as the lifecycle controller's /metrics shows them, with unknowns shown as
// unknown (ParseExposition, Scrape, GaugeProblems, PendingGaugeProblems, RestartReadProblems).
//
// Each check is a pure function of what a run saw. The expected values are the caller's, written from HBEP-8's
// Reasons table and Metrics section and the run's own steps, never from what the run emitted (issue #193,
// Verifications that prove nothing); the package's own tests feed each check a correct fixture, which must pass,
// and fixtures that break one rule each, which must fail.
//
// Only _test.go files import it, as they do internal/rehearsal, and TestOnlyTestFilesImportIt holds that, so no
// product binary links it.
//
// What is the stand-in's and not yet a parameter: notReported lists the two prefix-cache gauges that the
// rehearsal's engine double does not export (part3EngineModes, its no-prefix-cache-metrics mode), so the checks
// treat them as never known. A row on a real engine that exports them has to make that a parameter first: this
// package moved the checks as they were, and changed no behaviour.
//
// Provenance: new package; moved from internal/rehearsal/scenario's part3_events_test.go, part3_metrics_test.go and
// part3_guards_test.go (issues #146 and #193; issue #376, P2). The move is mechanical: each declaration is the
// original's, with these renames, made at the token level (comments and strings untouched) and the first word of a
// doc comment following its name, and gofmt's realignment:
//   - the names a caller needs, exported: wantEvent (with its fields action, reason, kind, ns, name, controller,
//     related and why, as Action, Reason, Kind, Namespace, Name, Controller, Related and Why, and its method matches
//     as Matches), eventProblems, typedEvent, lifecycleController and frontController; sample (name, labels and
//     value as Name, Labels and Value), parseExposition, scrape and find; engineGauges; gaugeView (uid, namespace,
//     name, deployment, provider, gpuModel, reported, staleReason and minGenerated as UID, Namespace, Name,
//     Deployment, Provider, GPUModel, Reported, StaleReason and MinGenerated), gaugeProblems,
//     pendingGaugeProblems and restartReadProblems;
//   - the engine gauges' names gRunning, gWaiting, gTPOT, gPreempt, gPrefixQ, gPrefixHit, gKV and gGenerated, as
//     GaugeRunning, GaugeWaiting, GaugeTPOT, GaugePreempt, GaugePrefixQ, GaugePrefixHit, GaugeKV and GaugeGenerated.
//
// The checks' own tests moved with them (events_test.go, metrics_test.go) from part3_guards_test.go, and the
// scenario's code that drives a run (expectedEvents, checkEvents, frontMetricsBothKinds, gauges, the restart
// scraper) stayed and names them through this package.
package obscheck
