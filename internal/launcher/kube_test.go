package launcher

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// writeAdminLater overwrites the fake host's admin kubeconfig with the given steps, the way k3s
// fills its file in: each step truncates and rewrites the file after its delay (counted from the
// start of the previous step). The returned func waits for the last step, so a test never ends
// with a writer still running.
func writeAdminLater(t *testing.T, h *fakeHost, steps ...adminStep) (wait func()) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, s := range steps {
			time.Sleep(s.after)
			if err := os.WriteFile(h.l.adminKubeconfig(), []byte(s.content), 0o600); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	return wg.Wait
}

type adminStep struct {
	after   time.Duration
	content string
}

// TestWaitReadyWaitsForAWholeAdminKubeconfig: k3s writes its admin kubeconfig early in its start
// and not atomically, so waitReady can find the file empty or cut short. That is "not ready yet",
// not a failure. The values that must hold come from the file k3s would write once done, which
// the fake host holds as the launcher's own kubeconfig (the same text, written by newFakeHost).
func TestWaitReadyWaitsForAWholeAdminKubeconfig(t *testing.T) {
	validOf := func(t *testing.T, h *fakeHost) string {
		t.Helper()
		b, err := os.ReadFile(h.l.kubeconfig())
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	// Each partial state is one that clientcmd refuses (empty, valid YAML with nothing in it, cut
	// before the contexts, cut inside the server address), and each must make the launcher wait.
	for _, tc := range []struct {
		name    string
		partial func(valid string, h *fakeHost) string
	}{
		{"empty", func(string, *fakeHost) string { return "" }},
		{"cut before the contexts", func(v string, _ *fakeHost) string { return v[:strings.Index(v, "contexts:")] }},
		{"cut inside the server address", func(v string, h *fakeHost) string {
			return v[:strings.Index(v, "127.0.0.1:")+len("127.0.0.1:")+1]
		}},
	} {
		t.Run("fills in after a delay, "+tc.name, func(t *testing.T) {
			h := newFakeHost(t)
			valid := validOf(t, h)
			partial := tc.partial(valid, h)
			if err := os.WriteFile(h.l.adminKubeconfig(), []byte(partial), 0o600); err != nil {
				t.Fatal(err)
			}
			// Incomplete at first, rewritten (still incomplete) at 150 ms, whole at 400 ms.
			wait := writeAdminLater(t, h, adminStep{150 * time.Millisecond, partial}, adminStep{250 * time.Millisecond, valid})
			defer wait()
			t0 := time.Now()
			_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), 10*time.Second)
			if err != nil {
				t.Fatalf("a kubeconfig that was only briefly incomplete: %v", err)
			}
			if d := time.Since(t0); d < 380*time.Millisecond {
				t.Errorf("ready after %s, before the file was whole (400 ms); the launcher read a partial file as good", d)
			}
		})
	}

	t.Run("never becomes valid, the wait ends at the deadline with the parse error", func(t *testing.T) {
		h := newFakeHost(t)
		if err := os.WriteFile(h.l.adminKubeconfig(), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		const timeout = 600 * time.Millisecond
		t0 := time.Now()
		_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), timeout)
		d := time.Since(t0)
		if err == nil {
			t.Fatal("an empty kubeconfig counted as ready")
		}
		for _, want := range []string{"not ready after", h.l.adminKubeconfig(), "no configuration has been provided"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %q", err, want)
			}
		}
		if d < timeout {
			t.Errorf("gave up after %s, before the deadline %s", d, timeout)
		}
	})

	t.Run("a kubeconfig naming another server is waited out, never used", func(t *testing.T) {
		h := newFakeHost(t)
		other := strings.ReplaceAll(validOf(t, h), "127.0.0.1:"+strconv.Itoa(h.st.Ports.HTTPS), "127.0.0.1:6443")
		if err := os.WriteFile(h.l.adminKubeconfig(), []byte(other), 0o600); err != nil {
			t.Fatal(err)
		}
		const timeout = 400 * time.Millisecond
		t0 := time.Now()
		_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), timeout)
		if err == nil || !strings.Contains(err.Error(), "127.0.0.1:6443") || !strings.Contains(err.Error(), "not at this launcher's") {
			t.Fatalf("got %v; want a refusal naming the other server", err)
		}
		// Cut-short files are indistinguishable from this one, so it too is retried to the deadline.
		if !strings.Contains(err.Error(), "not ready after") || time.Since(t0) < timeout {
			t.Errorf("gave up after %s with %q; want the wait to run to the deadline", time.Since(t0), err)
		}
	})
}
