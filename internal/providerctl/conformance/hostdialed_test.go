package conformance

// The reach-host-dialed rows' body over the fake (hostdialed_dry_test.go), without the script: a healthy run passes with
// every line of its text to post, each fault fails the check it stands for and names it, and a job kept pending past
// its start wait is NOT RUN. hack/livechecks runs the same through hack/live-checks.sh with the compiled test.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// hostDialedDryRun runs a row's body over the fake with the fault and returns how it ended and the text it posted.
func hostDialedDryRun(t *testing.T, row, fault string) (*stopRecorder, string, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}
	w, g := dryHostDialedWorld(t, row, fault, out)
	rec := &stopRecorder{TB: t}
	w.t = rec
	ends(func() { hostDialedBody(rec, w, g, row) })
	posts, _ := os.ReadFile(filepath.Join(out, alpha.PostsFile))
	if d := os.Getenv("HB_K305_DEBUG"); d != "" {
		logs, _ := filepath.Glob(filepath.Join(out, "run-*.log"))
		for _, l := range logs {
			b, _ := os.ReadFile(l)
			_ = os.WriteFile(filepath.Join(d, filepath.Base(t.Name())+".log"), b, 0o600)
		}
	}
	return rec, string(posts), out
}

func TestAReachHostDialedRunPassesOverTheFake(t *testing.T) {
	for _, r := range alpha.HostDialedRows() {
		t.Run(r.Name, func(t *testing.T) {
			t.Parallel()
			rec, posts, out := hostDialedDryRun(t, r.Name, "")
			if rec.failed != "" || rec.skipped != "" || len(rec.errors) > 0 {
				t.Fatalf("the healthy run did not pass: failed %q, skipped %q, errors %q\n%s", rec.failed, rec.skipped, rec.errors, posts)
			}
			for _, want := range []string{
				r.Name + ": PASS (posts to #305 and #145)",
				"- " + checkControl + ": ok: up",
				"- " + checkServing + ": ok: up",
				"- " + checkSrunU + ": ok:",
				"- " + checkRendezvous + ": ok:",
				"- " + checkPreamble + ": ok:",
				"- " + checkEpoch + ": ok:",
				"- " + checkBytes + ": ok:",
				"- frps and frpc: not run",
			} {
				if !strings.Contains(posts, want) {
					t.Errorf("the text to post lacks %q:\n%s", want, posts)
				}
			}
			hostDialedPostsAreClean(t, posts, out, true)
		})
	}
}

// hostDialedPostsAreClean fails when the text to post holds anything but plain printable ASCII and line ends, is longer
// than its fixed lines can be, or holds what the run keeps private: the job's ID, the Allocation's UID, or a path the
// probe read (the job's TMPDIR, the socket), which can name the job and the user.
func hostDialedPostsAreClean(t *testing.T, posts, out string, healthy bool) {
	t.Helper()
	if len(posts) > 4096 {
		t.Errorf("the text to post is %d bytes, more than its fixed lines can be", len(posts))
	}
	for i := 0; i < len(posts); i++ {
		if c := posts[i]; c != '\n' && (c < 0x20 || c > 0x7e) {
			t.Errorf("the text to post holds the byte %#x at %d", c, i)
			break
		}
	}
	var private []string
	rec, _ := os.ReadFile(filepath.Join(out, alpha.RecordFile))
	for _, field := range []string{"tmpdir", "socket"} {
		if m := regexp.MustCompile(`"` + field + `": "([^"]+)"`).FindSubmatch(rec); m != nil {
			private = append(private, string(m[1]))
		}
	}
	tags, _ := os.ReadFile(filepath.Join(out, jobTagsFile))
	for _, tag := range strings.Fields(string(tags)) {
		private = append(private, tag)
	}
	if healthy && len(private) < 3 {
		t.Errorf("the record holds %d private value(s) to look for; the search would mean nothing", len(private))
	}
	private = append(private, "conf-", os.TempDir())
	for _, p := range private {
		if strings.Contains(posts, p) {
			t.Errorf("the text to post holds a private value (%d bytes long)", len(p))
		}
	}
	if regexp.MustCompile(`jobid=[0-9]|job [0-9]`).MatchString(posts) {
		t.Error("the text to post holds a job ID")
	}
}

// hostDialedFaultChecks are the dry faults and the check each must fail: the one it stands for. A fault may fail others
// too (a channel that never comes up fails the preamble, and what needs it), which the test allows.
var hostDialedFaultChecks = map[string]string{
	"no-u":          checkSrunU,
	"banner":        checkPreamble,
	"epoch":         checkEpoch,
	"control-down":  checkControl,
	"serving-down":  checkServing,
	"bytes-to-host": checkBytes,
	"bytes-to-node": checkBytes,
	"socket-mode":   checkRendezvous,
	"unreadable":    checkReadable,
}

// Each fault fails the row, exit by t.Fatal with the check it stands for named, and the text to post says FAIL on that
// check's line. Every relay and node fault has a case, so a fault added without one fails here.
func TestEachHostDialedFaultFailsTheCheckItStandsFor(t *testing.T) {
	for fault := range dryRelayFaults {
		if _, ok := hostDialedFaultChecks[fault]; !ok {
			t.Errorf("the relay fault %s has no check here", fault)
		}
	}
	for fault := range dryNodeFaults {
		if _, ok := hostDialedFaultChecks[fault]; !ok {
			t.Errorf("the node fault %s has no check here", fault)
		}
	}
	for fault, check := range hostDialedFaultChecks {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			row := "reach-host-dialed-athena"
			rec, posts, out := hostDialedDryRun(t, row, fault)
			if rec.skipped != "" {
				t.Fatalf("the run skipped (%s), not a failure", rec.skipped)
			}
			if !strings.HasPrefix(rec.failed, row+" failed: ") || !strings.Contains(rec.failed, check) {
				t.Fatalf("the run ended %q, want a failure naming %q:\n%s", rec.failed, check, posts)
			}
			for _, want := range []string{row + ": FAIL (posts to #305 and #145)", "- " + check + ": FAIL: "} {
				if !strings.Contains(posts, want) {
					t.Errorf("the text to post lacks %q:\n%s", want, posts)
				}
			}
			hostDialedPostsAreClean(t, posts, out, false)
		})
	}
}

// The owner's other job holds the slot, so this run's job pends past its start wait: the row is NOT RUN, the job is
// cancelled by its tag, no relay is ever started, and there is no text to post.
func TestAReachHostDialedRowWhoseSlotIsHeldIsNotRun(t *testing.T) {
	for _, r := range alpha.HostDialedRows() {
		t.Run(r.Name, func(t *testing.T) {
			t.Parallel()
			rec, posts, out := hostDialedDryRun(t, r.Name, "slot-held")
			if rec.failed != "" || !strings.Contains(rec.skipped, "did not start within 10m0s of its acceptance") {
				t.Fatalf("the run ended failed %q, skipped %q; want NOT RUN on the start wait", rec.failed, rec.skipped)
			}
			if _, err := os.Stat(filepath.Join(out, "not-run.txt")); err != nil {
				t.Errorf("no NOT RUN marker: %v", err)
			}
			if posts != "" {
				t.Errorf("a NOT RUN run left a text to post:\n%s", posts)
			}
		})
	}
}
