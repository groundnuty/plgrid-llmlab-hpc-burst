// Package guard holds the drift guard of #239: tests only. It compares HBEP-8's
// event reasons and HBEP-7's Degraded ranking with the code (see package
// reasondrift) and runs the mutant checks of the comparison.
//
// It is a package of its own because it runs the scan of the module, which holds
// its syntax and types in memory (about 0.8 GB). Two scans run, the tree as it
// is and the tree with every mutant at once, and each runs once per test binary:
// under -count=N the comparisons repeat and the scans do not.
package guard
