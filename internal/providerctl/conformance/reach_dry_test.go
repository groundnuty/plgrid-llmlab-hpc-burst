package conformance

// The reach rows' body over a fake Slurm in run mode (issue #225), without the script: the job is the probe script a
// live run stages, run on this machine by the fake against the stand-ins; the body then reads the line it left and
// writes the text to post. hack/livechecks runs the same through hack/live-checks.sh with the compiled test.

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// postsAreClean fails when the text to post holds anything but plain printable ASCII and line ends, is longer than a
// few lines can be, or holds a word or a metacharacter that a hostile gateway sent (ctltest.HostileMarker, the
// shell's $( and backquote): what the other side sends must never reach a post, whatever it sends.
func postsAreClean(t *testing.T, posts string) {
	t.Helper()
	if len(posts) > 1024 {
		t.Errorf("the text to post is %d bytes, more than a few lines of fixed words can be", len(posts))
	}
	for i := 0; i < len(posts); i++ {
		if c := posts[i]; c != '\n' && (c < 0x20 || c > 0x7e) {
			t.Errorf("the text to post holds the byte %#x at %d", c, i)
			break
		}
	}
	for _, bad := range []string{ctltest.HostileMarker, "$(", "`", "\x1b"} {
		if strings.Contains(posts, bad) {
			t.Errorf("the text to post holds %q", bad)
		}
	}
}

// reachDryRun runs the body of a reach row over the fake with the probe script (payload) and the node's environment, in
// the way a dry TestReach does, and returns the world, the fake and the recorder that says how the body ended.
func reachDryRun(t *testing.T, row string, target probeTarget, payload []byte, jobEnv map[string]string) (*world, *fakeslurm.Platform, *stopRecorder, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}
	r, _ := alpha.RowByName(row)
	w, plat := dryWorld(t, r, "", out, dryShape{cpus: "1", memory: "1Gi"}, payload, jobEnv)
	rec := &stopRecorder{TB: t}
	w.t = rec
	ends(func() { reachBody(rec, w, row, target) })
	return w, plat, rec, out
}

// A reach row's run over the fake, for each outcome the probe can write, on each row: the verdict, the text to post
// and the record say what the probe saw; and the target, host and port, is in no file the run left outside the job's
// script (targetLeaks: the job's output and error files, the staging, the agent builds, the records, the scheduler's
// copies). A stand-in that always connected would pass the first and prove nothing about the second, so the first asks
// the stand-in for the connection it saw. The stand-ins' own seed addresses are distinctive: 127.77.x.y. The cases of
// reason are the node's: a bash that fails its connect as a kernel does, which no loopback socket can make. A hostile
// gateway's bytes are in no post.
func TestAReachRowOverTheFakeReportsEachOutcomeAndLeaksNoTarget(t *testing.T) {
	cases := []struct {
		kind    ctltest.ReachKind
		reason  string // the node's connect fails with this reason; "" for the stand-in's own behaviour
		outcome string
		passes  bool
	}{
		{ctltest.StubSSH, "", "reachable", true},
		{ctltest.StubTLS, "", "reachable", true},
		{ctltest.StubSilent, "", "reachable", true},
		{ctltest.StubBinary, "", "reachable", true},
		{ctltest.StubHostile, "", "reachable", true},
		{ctltest.StubRefused, "", "refused", false},
		{ctltest.StubTimeout, "", "timeout", false},
		{ctltest.StubUnresolved, "", "unresolved", false},
		{ctltest.StubSSH, "No route to host", "unreachable", false},
		{ctltest.StubSSH, "Permission denied", "error", false},
	}
	for i, row := range []string{"reach-helios", "reach-athena"} {
		for j, tc := range cases {
			name := string(tc.kind)
			if tc.reason != "" {
				name = tc.outcome + "-from-the-node"
			}
			t.Run(row+"/"+name, func(t *testing.T) {
				t.Parallel()
				stub := ctltest.StartReachStub(t, tc.kind, "127.77."+strconv.Itoa(i+1)+"."+strconv.Itoa(j+1), 0)
				target := probeTarget{host: stub.Host, port: stub.Port, label: "example-gateway"}
				payload := target.agent(testConnectSeconds, testReadSeconds)
				var jobEnv map[string]string
				if tc.reason != "" {
					jobEnv = connectFailureEnv(t, tc.reason)
				}
				w, plat, rec, out := reachDryRun(t, row, target, payload, jobEnv)

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
				r, _ := alpha.RowByName(row)
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
				postsAreClean(t, string(posts))
				record, err := os.ReadFile(filepath.Join(out, alpha.RecordFile))
				if err != nil || !strings.Contains(string(record), `"outcome": "`+tc.outcome+`"`) {
					t.Errorf("the record lacks the outcome %s: %v\n%s", tc.outcome, err, record)
				}
				// The job asked for its own two minutes, not the harness's five (the review of PR 235, B3).
				var jobs int
				for _, j := range plat.Jobs() {
					if strings.HasPrefix(j.Name, "hb-") {
						jobs++
						if j.TimeLimit != 2 {
							t.Errorf("the job's time limit is %d minutes, want 2", j.TimeLimit)
						}
					}
				}
				if jobs != 1 {
					t.Errorf("%d job(s) of this run, want 1", jobs)
				}
				// The target is in the job's script and in no other file or record. The search looks at the files of
				// the fake cluster and the scheduler's copies too, not only the record directory, and says how many.
				leaks, scanned := w.targetLeaks(target, payload, plat)
				if scanned < 15 {
					t.Errorf("the search looked at %d file(s) and record(s), too few for it to mean anything", scanned)
				}
				if len(leaks) > 0 {
					t.Errorf("the target is in %v", leaks)
				}
			})
		}
	}
}

// The search for the target finds where a probe leaks it, or it proves nothing: each variant of the probe puts the
// target somewhere the run is not meant to (the stderr echo that the review of PR 235 named, the standard output, a
// file in the Allocation's directory, a copy of the script that is not byte for byte the script, a job named for the
// host), and the search must name that place. The unchanged probe, and a copy that is byte for byte the script, are
// not leaks.
func TestTheTargetSearchFindsWhereAProbeLeaksIt(t *testing.T) {
	anchor := "out=\"$1/reach\"\n"
	if !strings.Contains(reachProbeScript, anchor) {
		t.Fatalf("the probe has no line %q to put a leak after", anchor)
	}
	leaky := func(extra string) func(probeTarget) []byte {
		return func(p probeTarget) []byte {
			return []byte(strings.Replace(string(p.agent(testConnectSeconds, testReadSeconds)), anchor, anchor+extra+"\n", 1))
		}
	}
	for _, tc := range []struct {
		name  string
		agent func(probeTarget) []byte // nil: the probe as it is
		extra func(t *testing.T, w *world, plat *fakeslurm.Platform, p probeTarget, payload []byte)
		want  string // a part of a place the search must name; "" for none
	}{
		{"the probe as it is", nil, nil, ""},
		{"an echo to the error output", leaky(`echo "$host:$port" >&2`), nil, "job.err"},
		{"an echo to the standard output", leaky(`echo "$host"`), nil, "job.out"},
		{"the port in a file of the Allocation's directory", leaky(`echo "$port" > "$1/note"`), nil, "note"},
		{"a copy of the script, byte for byte", nil, func(t *testing.T, w *world, _ *fakeslurm.Platform, _ probeTarget, payload []byte) {
			if err := os.WriteFile(filepath.Join(w.artefacts[0], "copy-of-the-script"), payload, 0o600); err != nil {
				t.Fatal(err)
			}
		}, ""},
		{"a copy of the script with a line more", nil, func(t *testing.T, w *world, _ *fakeslurm.Platform, _ probeTarget, payload []byte) {
			if err := os.WriteFile(filepath.Join(w.artefacts[0], "copy-of-the-script"), append(append([]byte(nil), payload...), "# a line more\n"...), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "copy-of-the-script"},
		{"a job named for the host", nil, func(t *testing.T, _ *world, plat *fakeslurm.Platform, p probeTarget, _ []byte) {
			res, err := plat.Login(fakeslurm.Login{User: "example-user"}).Run(context.Background(), slurm.Command{Line: "sbatch --parsable --no-requeue --job-name=" + p.host +
				" --partition=plgrid-gpu-gh200 --account=example-account --time=00:01:00 --gres=gpu:1 --cpus-per-task=1 --mem=1024 --wrap=true"})
			if err != nil || res.ExitCode != 0 {
				t.Fatalf("the fake did not take the job: %v exit %d %s", err, res.ExitCode, res.Stderr)
			}
		}, "the scheduler's record of job"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stub := ctltest.StartReachStub(t, ctltest.StubSSH, "127.77.9.1", 0)
			target := probeTarget{host: stub.Host, port: stub.Port, label: "example-gateway"}
			payload := target.agent(testConnectSeconds, testReadSeconds)
			if tc.agent != nil {
				payload = tc.agent(target)
			}
			w, plat, rec, _ := reachDryRun(t, "reach-helios", target, payload, nil)
			if rec.failed != "" {
				t.Fatalf("the run failed: %s", rec.failed)
			}
			if tc.extra != nil {
				tc.extra(t, w, plat, target, payload)
			}
			leaks, scanned := w.targetLeaks(target, payload, plat)
			if scanned < 15 {
				t.Errorf("the search looked at %d file(s) and record(s), too few for it to mean anything", scanned)
			}
			switch {
			case tc.want == "" && len(leaks) > 0:
				t.Errorf("the search reports %v, want no leak", leaks)
			case tc.want != "" && !strings.Contains(strings.Join(leaks, "\n"), tc.want):
				t.Errorf("the search reports %v, want a place that holds %q", leaks, tc.want)
			}
		})
	}
}

// The hostile gateway's bytes: the probe reads four bytes of what is sent first and then stops, so a megabyte with no
// line end, an escape sequence and a shell command in a banner cost it nothing. The row passes, and the text to post
// holds the fixed words of "something answered" and none of the bytes (postsAreClean is asserted for every outcome in
// the test above too); the time it takes is bounded by the probe's own limits, not by what the gateway sends.
func TestAHostileBannerReachesNoPostAndCostsNoTime(t *testing.T) {
	for _, kind := range []ctltest.ReachKind{ctltest.StubBinary, ctltest.StubHostile} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			stub := ctltest.StartReachStub(t, kind, "127.77.9.2", 0)
			target := probeTarget{host: stub.Host, port: stub.Port, label: "example-gateway"}
			_, _, rec, out := reachDryRun(t, "reach-athena", target, target.agent(testConnectSeconds, testReadSeconds), nil)
			if rec.failed != "" {
				t.Fatalf("the run failed: %s", rec.failed)
			}
			posts, err := os.ReadFile(filepath.Join(out, alpha.PostsFile))
			if err != nil {
				t.Fatal(err)
			}
			postsAreClean(t, string(posts))
			if !strings.Contains(string(posts), "something answered") {
				t.Errorf("the text to post does not say that something answered:\n%s", posts)
			}
		})
	}
}
