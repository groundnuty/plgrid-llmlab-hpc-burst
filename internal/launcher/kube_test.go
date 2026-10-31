package launcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// writeAdmin overwrites the fake host's admin kubeconfig the way k3s writes it: in place, not
// atomically.
func writeAdmin(t *testing.T, h *fakeHost, content string) {
	t.Helper()
	if err := os.WriteFile(h.l.adminKubeconfig(), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// useWaitReadyHook installs hook as waitReadyHook for the test.
func useWaitReadyHook(t *testing.T, hook func(last error)) {
	t.Helper()
	waitReadyHook = hook
	t.Cleanup(func() { waitReadyHook = nil })
}

// requireClientcmdOrder fails unless text has the field order that makes a cut kubeconfig
// dangerous: a cluster's server before its name, and all of that before the contexts and the
// current-context. That is the order of clientcmd.Write, which k3s uses, and a fixture that
// put "name" first would hide the cut file that parses as an unnamed cluster.
func requireClientcmdOrder(t *testing.T, text string) {
	t.Helper()
	server, name := strings.Index(text, "server:"), strings.Index(text, "name:")
	contexts, current := strings.Index(text, "contexts:"), strings.Index(text, "current-context:")
	if server < 0 || name < 0 || contexts < 0 || current < 0 || server >= name || name >= contexts || contexts >= current {
		t.Fatalf("the fixture is not in clientcmd's field order (server %d, name %d, contexts %d, current-context %d):\n%s", server, name, contexts, current, text)
	}
}

// TestWaitReadyWaitsForAWholeAdminKubeconfig: k3s writes its admin kubeconfig early in its start
// and not atomically, so waitReady can find the file empty or cut short. That is "not ready yet",
// not a failure. The values that must hold come from the file k3s would write once done, which
// the fake host holds as the launcher's own kubeconfig (the same text, written by newFakeHost).
//
// The file changes only inside waitReadyHook, which runs after each round that failed: so the test
// publishes the whole file only after two rounds have failed on the incomplete one, and nothing in
// it depends on how long a round takes.
func TestWaitReadyWaitsForAWholeAdminKubeconfig(t *testing.T) {
	validOf := func(t *testing.T, h *fakeHost) string {
		t.Helper()
		b, err := os.ReadFile(h.l.kubeconfig())
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	// Each incomplete state must make the launcher wait. Four are refused by restConfig before any
	// request: empty, cut before the contexts, cut before the current-context, and cut inside the
	// server address (the case where clientcmd's field order matters: the file then holds a cluster
	// with no name yet, which client-go uses although no context selects it, and the address reads
	// as another server's). The fifth is a cut that parses and resolves, inside a base64 blob on a
	// 4-character boundary: clientcmd accepts it and only the first use of the config fails, so the
	// file must be read again after /readyz failed, not kept from the first parse.
	for _, tc := range []struct {
		name    string
		partial func(t *testing.T, h *fakeHost, valid string) string
	}{
		{"empty", func(*testing.T, *fakeHost, string) string { return "" }},
		{"cut before the contexts", func(_ *testing.T, _ *fakeHost, v string) string { return v[:strings.Index(v, "contexts:")] }},
		{"cut before the current-context", func(_ *testing.T, _ *fakeHost, v string) string { return v[:strings.Index(v, "current-context:")] }},
		{"cut inside the server address, before the cluster has a name", func(t *testing.T, _ *fakeHost, v string) string {
			requireClientcmdOrder(t, v)
			cut := v[:strings.Index(v, "127.0.0.1:")+len("127.0.0.1:")+1]
			if !strings.Contains(cut, "server: https://127.0.0.1:") || strings.Contains(cut, "name:") {
				t.Fatalf("the cut is not inside the first cluster's server address:\n%s", cut)
			}
			return cut
		}},
		{"parses, but its client certificate is not PEM", func(t *testing.T, h *fakeHost, _ string) string {
			cfg := fakeKubeconfig("https://127.0.0.1:" + strconv.Itoa(h.st.Ports.HTTPS))
			// "notpem" in base64: what a certificate or key blob cut on a 4-character boundary decodes to.
			cfg.AuthInfos["default"].Token = ""
			cfg.AuthInfos["default"].ClientCertificateData = []byte("notpem")
			cfg.AuthInfos["default"].ClientKeyData = []byte("notpem")
			return kubeconfigText(t, cfg)
		}},
	} {
		t.Run("fills in after two failed rounds, "+tc.name, func(t *testing.T) {
			h := newFakeHost(t)
			valid := validOf(t, h)
			partial := tc.partial(t, h, valid)
			writeAdmin(t, h, partial)
			var failed []error
			published := false
			useWaitReadyHook(t, func(last error) {
				if last == nil {
					return // a round that got a step further (the whole file's /readyz), not a failure
				}
				failed = append(failed, last)
				switch len(failed) {
				case 1:
					writeAdmin(t, h, partial) // k3s writes the file again, still incomplete
				case 2:
					writeAdmin(t, h, valid)
					published = true
				}
			})
			_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), 10*time.Second)
			if err != nil {
				t.Fatalf("a kubeconfig that was only briefly incomplete: %v", err)
			}
			if !published {
				t.Fatalf("ready before the whole file was published (%d failed rounds): the launcher read a partial file as good", len(failed))
			}
		})
	}

	t.Run("never becomes valid, the wait ends at the deadline with the reason", func(t *testing.T) {
		h := newFakeHost(t)
		writeAdmin(t, h, "")
		rounds := 0
		useWaitReadyHook(t, func(error) { rounds++ })
		const timeout = 600 * time.Millisecond
		t0 := time.Now()
		_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), timeout)
		d := time.Since(t0)
		if err == nil {
			t.Fatal("an empty kubeconfig counted as ready")
		}
		for _, want := range []string{"not ready after", h.l.adminKubeconfig(), "no current-context"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %q", err, want)
			}
		}
		// The loop cannot end before its deadline, which is set after t0; this guards against an
		// early return that skips the wait.
		if d < timeout {
			t.Errorf("gave up after %s, before the deadline %s", d, timeout)
		}
		if rounds == 0 {
			t.Error("the file was never read again")
		}
	})

	t.Run("a whole kubeconfig naming another server fails at once", func(t *testing.T) {
		h := newFakeHost(t)
		other := kubeconfigText(t, fakeKubeconfig("https://127.0.0.1:6443"))
		writeAdmin(t, h, other)
		rounds := 0
		useWaitReadyHook(t, func(error) { rounds++ })
		// The timeout is long only so that a wait would show: this file will not change, so the
		// refusal must come out of the first round, with no round failed before it.
		_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), 5*time.Second)
		var wrong *wrongServerError
		if !errors.As(err, &wrong) || !strings.Contains(err.Error(), "127.0.0.1:6443") || !strings.Contains(err.Error(), "not at this launcher's") {
			t.Fatalf("got %v; want a wrongServerError naming the other server", err)
		}
		if rounds != 0 {
			t.Errorf("%d round(s) failed before the refusal; want it at once", rounds)
		}
	})
}

// TestRestConfigNeverCallsACutKubeconfigAWrongServer cuts the serialised kubeconfig at every byte.
// A file cut inside the first cluster's server address names an address that is not this
// launcher's, and client-go builds a config from it all the same (the cluster has no name yet).
// That is not a refusal to make: the address is not final until the file resolves, which is
// when current-context is whole, and until then restConfig must give an ordinary error.
func TestRestConfigNeverCallsACutKubeconfigAWrongServer(t *testing.T) {
	ports := Ports{API: 29220, HTTPS: 29221, KCM: 29222, LB: 29225}
	own := "https://127.0.0.1:" + strconv.Itoa(ports.HTTPS)
	for _, tc := range []struct {
		name, server string
		wantWrong    bool // for the whole file
	}{
		{"this launcher's server", own, false},
		{"another server", "https://127.0.0.1:6443", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full := kubeconfigText(t, fakeKubeconfig(tc.server))
			requireClientcmdOrder(t, full)
			// Resolved once the current-context is whole: the clusters are then final, since they
			// come before it.
			resolvedAt := strings.Index(full, "current-context: default") + len("current-context: default")
			path := filepath.Join(t.TempDir(), "k3s-admin.yaml")
			for n := 0; n < len(full); n++ {
				if err := os.WriteFile(path, []byte(full[:n]), 0o600); err != nil {
					t.Fatal(err)
				}
				_, err := restConfig(path, ports, time.Second)
				var wrong *wrongServerError
				// Only a file that resolves can be refused for its address, and only when it names
				// another server.
				if errors.As(err, &wrong) && (n < resolvedAt || !tc.wantWrong) {
					t.Fatalf("the first %d bytes were refused as a wrong server (%v); a cut file is not ready yet:\n%s", n, err, full[:n])
				}
				if n < resolvedAt && err == nil {
					t.Fatalf("the first %d bytes were accepted, before the current-context is whole:\n%s", n, full[:n])
				}
			}
			if err := os.WriteFile(path, []byte(full), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := restConfig(path, ports, time.Second)
			var wrong *wrongServerError
			switch {
			case tc.wantWrong && !errors.As(err, &wrong):
				t.Errorf("the whole file naming %s: got %v; want a wrongServerError", tc.server, err)
			case !tc.wantWrong && err != nil:
				t.Errorf("the whole file naming this launcher's server: %v", err)
			}
		})
	}
}

// TestWaitReadyDoesNotAnswerAfterItsDeadline: the timeout is a deadline for the whole wait. The
// controller manager answers 200, but only after the deadline; a probe that starts shortly before
// the deadline used to get its own fresh 1.5 s, and so reported ready late. The fake answers a
// fixed 1.2 s after it is asked, which is after the 1 s deadline whenever the probe began, and
// within a fresh 1.5 s whenever the probe began.
func TestWaitReadyDoesNotAnswerAfterItsDeadline(t *testing.T) {
	h := newFakeHost(t)
	var asked atomic.Bool
	h.onKCM = func() {
		asked.Store(true)
		time.Sleep(1200 * time.Millisecond)
	}
	_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), time.Second)
	if !asked.Load() {
		t.Fatal("the controller manager was never asked: the probe this test is about did not run")
	}
	if err == nil {
		t.Fatal("ready on an answer that came after the deadline")
	}
	if !strings.Contains(err.Error(), "controller manager /healthz not ready after 1s") {
		t.Errorf("got %v; want a timeout in the controller manager stage", err)
	}
}

// TestWaitReadyDeadlineHoldsWhenARoundIsSlowBeforeItsRequest: a round fixes its probe timeout when
// it begins, but sends its request only after it has checked who holds the port, and a slow check
// moves the request later, past where a timeout counted from the round's start would end. The
// fake slows that check by 0.8 s in the controller manager's round, then answers 0.3 s after it
// is asked: the answer comes after the 1 s deadline whenever the round began, and well within
// the probe's own timeout (about 0.95 s from the request).
func TestWaitReadyDeadlineHoldsWhenARoundIsSlowBeforeItsRequest(t *testing.T) {
	h := newFakeHost(t)
	var asked atomic.Bool
	h.onKCM = func() {
		asked.Store(true)
		time.Sleep(300 * time.Millisecond)
	}
	// The hook runs after /readyz has answered, so the next round is the controller manager's. Its
	// first exited check is the round's own verify, before the probe timeout is fixed; the second is
	// the port check, after it.
	slow := false
	checks := 0
	useWaitReadyHook(t, func(error) { slow = true })
	h.f.onExited = func() {
		if !slow {
			return
		}
		if checks++; checks == 2 {
			time.Sleep(800 * time.Millisecond)
		}
	}
	_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), time.Second)
	if !asked.Load() {
		t.Fatal("the controller manager was never asked: the round this test is about did not run")
	}
	if err == nil {
		t.Fatal("ready on an answer that came after the deadline")
	}
	if !strings.Contains(err.Error(), "controller manager /healthz not ready after 1s") {
		t.Errorf("got %v; want a timeout in the controller manager stage", err)
	}
}
