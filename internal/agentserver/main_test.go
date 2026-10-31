package agentserver

import (
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// TestMain fails the run when a goroutine of the server outlives its test: a
// handler or a connection that Serve's shutdown did not end. The tests close
// their own clients, so what is left is the server's.
func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if left := serverGoroutines(3 * time.Second); left != "" {
			fmt.Fprintf(os.Stderr, "goroutines left after the tests:\n%s\n", left)
			code = 1
		}
	}
	os.Exit(code)
}

// serverGoroutines returns the stacks of goroutines that are in this package or
// serve a connection, after waiting up to wait for them to end.
func serverGoroutines(wait time.Duration) string {
	for deadline := time.Now().Add(wait); ; time.Sleep(20 * time.Millisecond) {
		left := currentServerGoroutines()
		if left == "" || time.Now().After(deadline) {
			return left
		}
	}
}

func currentServerGoroutines() string {
	var b strings.Builder
	if err := pprof.Lookup("goroutine").WriteTo(&b, 2); err != nil {
		return "reading the goroutines: " + err.Error()
	}
	var left []string
	for _, stack := range strings.Split(b.String(), "\n\n") {
		if strings.Contains(stack, "agentserver.TestMain") || strings.Contains(stack, "runtime/pprof") {
			continue // this goroutine
		}
		if strings.Contains(stack, "internal/agentserver.") || strings.Contains(stack, "net/http.(*conn).serve") {
			left = append(left, stack)
		}
	}
	return strings.Join(left, "\n\n")
}
