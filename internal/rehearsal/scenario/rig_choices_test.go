package scenario_test

// The rehearsal's own choices where it and the alpha rows differ (issue #251):
// each test takes the path the rig takes, through requireRehearsal, startRigHost
// or rigClient, so that an option left out there fails here. No stack starts: the
// binaries and the front are doubles.

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// frontNeverReading answers every request at once and never reads its body, so a
// client sending a body larger than the sockets' buffers never finishes writing
// it: its WroteRequest hook never reports a written request. Each reply names
// request req-1, whose usage record, for Instance uid, is on file.
func frontNeverReading(t *testing.T, uid string) *stack.Host {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Hpcburst-Request-Id", "req-1")
		w.Header().Set("Content-Length", "2")
		_, _ = w.Write([]byte("{}"))
		w.(http.Flusher).Flush()
		<-release
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // first, as the server's Close waits for its handlers
	dir := t.TempDir()
	row := `{"requestID":"req-1","instanceUID":"` + uid + `"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "usage.jsonl"), []byte(row), 0o600); err != nil {
		t.Fatal(err)
	}
	return &stack.Host{T: t, UsageDir: dir, ClientKey: "k", Ports: stack.Ports{ClientAddr: strings.TrimPrefix(srv.URL, "http://")}}
}

// neverWrittenModel is a model name that makes KeyFor's short chat request larger
// than any socket buffers: Linux's default TCP maximums are 4 and 6 MiB.
func neverWrittenModel() string { return strings.Repeat("m", 64<<20) }

// setWroteWait sets stack.WroteWait for this test.
func setWroteWait(t *testing.T, d time.Duration) {
	t.Helper()
	old := stack.WroteWait
	stack.WroteWait = d
	t.Cleanup(func() { stack.WroteWait = old })
}

// The rig's key search returns once the reply is read, with the write hook not
// called, as its own send did (issue #251, Astra's review of PR #283). The wait it
// must not have is raised far past the deadline here, so taking it fails the test
// by a generous deadline, not a narrow window.
//
//livecheck:none a test of the rig's client on a double; nothing live runs
func TestRigKeySearchDoesNotWaitForTheWriteHook(t *testing.T) {
	setWroteWait(t, 10*time.Minute)
	in := &v1alpha1.Instance{}
	in.Name, in.UID = "primary", "uid-1"
	c := rigClient(t, frontNeverReading(t, "uid-1"))
	c.Model = neverWrittenModel()
	done := make(chan string, 1)
	go func() { done <- c.KeyFor(in) }()
	select {
	case key := <-done:
		if key != "rehearsal-conversation-0" {
			t.Errorf("KeyFor = %q, want the first candidate", key)
		}
	case <-time.After(time.Minute):
		t.Fatal("the rig's KeyFor did not return within a minute of a reply with its write hook not called: it waits for the hook")
	}
}

// A client without the rig's option keeps the alpha rows' behaviour: its key search
// waits for the write hook, up to WroteWait. Its elapsed time is a floor set by a
// timer, so no load can make this fail, and it fails on a host whose buffers took the
// whole body, where the test above would prove nothing.
//
//livecheck:none a test of the alpha rows' default on a double; nothing live runs
func TestPlainClientKeySearchWaitsForTheWriteHook(t *testing.T) {
	const wait = time.Second
	setWroteWait(t, wait)
	in := &v1alpha1.Instance{}
	in.Name, in.UID = "primary", "uid-1"
	c := &stack.Client{T: t, H: frontNeverReading(t, "uid-1"), Model: neverWrittenModel(), MaxRequest: time.Minute}
	start := time.Now()
	if key := c.KeyFor(in); key != "rehearsal-conversation-0" {
		t.Errorf("KeyFor = %q, want the first candidate", key)
	}
	if took := time.Since(start); took < wait {
		t.Errorf("KeyFor took %s, under the %s wait for a hook that is never called", took, wait)
	}
}

// An executable directory in a build's place passes the rehearsal's first check, so
// a host without frp still skips at testenv.FrpBinaries rather than failing before
// it. Astra's review of PR #283: requireRehearsal's order, with the check it had.
//
//livecheck:none a test of the rig's binaries check on empty files; nothing live runs
func TestRequireRehearsalLeavesADirectoryToFrp(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "hpcburst"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "k3s-1.36.5"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hpcburst-controller", "hpcburst-front", "hb-agent", filepath.Join("k3s-1.36.5", "k3s")} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("HPCBURST_REHEARSAL", "1")
	t.Setenv("HPCBURST_REHEARSAL_BIN", dir)
	t.Setenv("HPCBURST_BIN_DIR", dir)
	t.Setenv("HPCBURST_FRP_DIR", filepath.Join(dir, "no-frp"))
	t.Setenv("HPCBURST_REQUIRE_FRP", "")
	var skipped bool
	t.Run("frp absent", func(t *testing.T) {
		defer func() { skipped = t.Skipped() }()
		requireRehearsal(t)
	})
	if !skipped {
		t.Error("requireRehearsal did not skip at frp with a directory in hpcburst's place")
	}
}

// The rig's hpcburst down past its bound says so in its error, as its own down did
// (Astra's review of PR #283). The double's up only makes the state directory, and
// its down runs until the bound kills it; exec, so that the kill leaves no child.
//
//livecheck:none a test of the rig's host on a double of hpcburst; nothing live runs
func TestRigDownPastItsBoundSaysSo(t *testing.T) {
	dir := stack.ShortDir(t)
	bin := filepath.Join(dir, "hpcburst")
	script := "#!/bin/sh\nif [ \"$1\" = down ]; then exec sleep 600; fi\nmkdir -p \"$3\"\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// The controller's /readyz, on up's base+6: a kernel-chosen port, and the base it implies.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	h := startRigHost(t, stack.Binaries{HPCBurst: bin, K3sDir: dir, Repo: dir}, dir, stack.DialHome{Mode: "direct", Addr: "127.0.0.1:1"},
		os.Environ(), stack.Ports{Base: n - 6, ClientAddr: "127.0.0.1:1", DialHomeListen: "127.0.0.1:1"})
	h.DownBound = 3 * time.Second
	want := "hpcburst down: signal: killed\nhpcburst did not end within 3s\n"
	if err := h.DownErr(); err == nil || err.Error() != want {
		t.Errorf("a down past its bound: %q, want %q", err, want)
	}
}
