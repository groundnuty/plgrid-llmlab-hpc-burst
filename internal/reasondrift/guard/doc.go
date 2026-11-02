// Package guard holds the drift guard of #239: tests only. It compares HBEP-8's
// event reasons and HBEP-7's Degraded ranking with the code (see package
// reasondrift) and runs the mutant checks of the comparison.
//
// It is a package of its own because it runs the scan of the module, which
// holds the module's syntax and types in memory (about 0.8 GB). The scans run
// once per test binary, so -count=N repeats the comparisons but not the scans.
package guard
