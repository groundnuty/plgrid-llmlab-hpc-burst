// Package e2e runs the managed Provider's controller (package managed) in its
// own controller-runtime manager against the test host of internal/testenv:
// a real API server, the committed Provider CRD, leader election on the
// Provider's Lease, and the LLMLab double. It holds tests only.
package e2e
