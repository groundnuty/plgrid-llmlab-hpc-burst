//go:build !linux

package ctltest

import "testing"

// startFullQueue needs Linux's behaviour for a listener whose accept queue is full.
func startFullQueue(t testing.TB, host string, port int) ReachStub {
	t.Helper()
	t.Fatal("the full-queue stub of the reach tests needs Linux")
	return ReachStub{}
}
