package conformance

// The reach rows' body over a fake Slurm in run mode (issue #225), without the script: the job is the probe script a
// live run stages, run on this machine by the fake against the stand-ins; the body then reads the line it left and
// writes the text to post. hack/livechecks runs the same through hack/live-checks.sh with the compiled test.

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
)

// walk returns every file under dir with its content.
func walk(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		files[p] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// A reach row's run over the fake, for each outcome the probe can write, on each row: the verdict, the text to post
// and the record say what the probe saw; and the target, host and port, is in no file the run wrote, the text to post
// and the record included. A stand-in that always connected would pass the first and prove nothing about the second,
// so the first asks the stand-in for the connection it saw, and the second finds the target in the job's script (the
// stand-ins' own seed addresses are distinctive: 127.77.x.y, not 127.0.0.1).
func TestAReachRowOverTheFakeReportsEachOutcomeAndLeaksNoTarget(t *testing.T) {
	cases := []struct {
		kind    ctltest.ReachKind
		outcome string
		answer  string
		passes  bool
	}{
		{ctltest.StubSSH, "reachable", "ssh", true},
		{ctltest.StubTLS, "reachable", "tls", true},
		{ctltest.StubSilent, "reachable", "silent", true},
		{ctltest.StubRefused, "refused", "", false},
		{ctltest.StubTimeout, "timeout", "", false},
		{ctltest.StubUnresolved, "unresolved", "", false},
	}
	for i, row := range []string{"reach-helios", "reach-athena"} {
		for j, tc := range cases {
			t.Run(row+"/"+string(tc.kind), func(t *testing.T) {
				t.Parallel()
				out := filepath.Join(t.TempDir(), "out")
				if err := os.Mkdir(out, 0o700); err != nil {
					t.Fatal(err)
				}
				stub := ctltest.StartReachStub(t, tc.kind, "127.77."+strconv.Itoa(i+1)+"."+strconv.Itoa(j+1), 0)
				target := probeTarget{host: stub.Host, port: stub.Port, label: "example-gateway"}
				r, _ := alpha.RowByName(row)
				w, _ := dryWorld(t, r, "", out, dryShape{cpus: "1", memory: "1Gi"}, target.agent(testConnectSeconds, testReadSeconds), nil)
				rec := &stopRecorder{TB: t}
				w.t = rec
				ends(func() { reachBody(rec, w, row, target) })

				if tc.passes && rec.failed != "" {
					t.Fatalf("the run failed: %s", rec.failed)
				}
				if !tc.passes && !strings.Contains(rec.failed, "was not reached: "+tc.outcome) {
					t.Fatalf("the run ended %q (skipped %q), want a failure that says %s", rec.failed, rec.skipped, tc.outcome)
				}
				if tc.passes != (stub.Accepted() > 0) {
					t.Errorf("the stand-in accepted %d connection(s) for outcome %s: the outcome may be another socket's", stub.Accepted(), tc.outcome)
				}
				posts, err := os.ReadFile(filepath.Join(out, alpha.PostsFile))
				if err != nil {
					t.Fatalf("no text to post: %v", err)
				}
				verdict := "FAIL"
				if tc.passes {
					verdict = "PASS"
				}
				for _, want := range []string{row + ": " + verdict, "as example-gateway: outcome " + tc.outcome, "from a compute node of partition " + r.Partition + ", qos ", "1 GPU, 1 CPU and 1Gi"} {
					if !strings.Contains(string(posts), want) {
						t.Errorf("the text to post lacks %q:\n%s", want, posts)
					}
				}
				wantQOS := "qos now (requested"
				if row == "reach-athena" {
					wantQOS = "qos plgrid-now (the partition's own; none requested)"
				}
				if !strings.Contains(string(posts), wantQOS) {
					t.Errorf("the text to post does not name the QOS used (%q):\n%s", wantQOS, posts)
				}
				record, err := os.ReadFile(filepath.Join(out, alpha.RecordFile))
				if err != nil || !strings.Contains(string(record), `"outcome": "`+tc.outcome+`"`) {
					t.Errorf("the record lacks the outcome %s: %v\n%s", tc.outcome, err, record)
				}
				// The target is in no file of the run's record directory, the private log and the tags included: the
				// job's script holds it, on the cluster's side, and the screen of a live run never shows the log.
				var portWord *regexp.Regexp // the unresolved stand-in's port, 9, is any digit in a log
				if tc.kind != ctltest.StubUnresolved {
					portWord = regexp.MustCompile(`\b` + strconv.Itoa(stub.Port) + `\b`)
				}
				for name, content := range walk(t, out) {
					if strings.Contains(content, stub.Host) || (portWord != nil && portWord.MatchString(content)) {
						t.Errorf("%s holds the target (host %q or port):\n%s", filepath.Base(name), stub.Host, content)
					}
				}
			})
		}
	}
}
