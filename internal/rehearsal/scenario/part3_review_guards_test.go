package scenario_test

// The checks the review of PR #247 added or changed, on their own, in an
// ordinary go test (no HPCBURST_REHEARSAL): each is fed what a correct run
// gives, which must pass, and fixtures that break one rule each, which must
// fail.

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// streamBody is a streamed chat reply as the engine double sends it: a role
// event, one event per token (the last with the finish reason), the usage
// event, [DONE].
func streamBody(tokens int, finish string, usage int) string {
	var b strings.Builder
	b.WriteString(`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}` + "\n\n")
	for i := 0; i < tokens; i++ {
		text := fmt.Sprintf("w%d", i)
		if i > 0 {
			text = " " + text
		}
		fr := "null"
		if i == tokens-1 {
			fr = `"` + finish + `"`
		}
		fmt.Fprintf(&b, `data: {"choices":[{"index":0,"delta":{"content":%q},"finish_reason":%s}]}`+"\n\n", text, fr)
	}
	fmt.Fprintf(&b, `data: {"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":%d,"total_tokens":%d}}`+"\n\n", usage, usage+9)
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

func TestStreamProblems(t *testing.T) {
	if p := streamProblems(sseParse([]byte(streamBody(50, "length", 50))), 50); len(p) != 0 {
		t.Fatalf("a correct stream of 50 tokens: %v", p)
	}
	dropped := func() string {
		var keep []string
		for i, ev := range strings.Split(streamBody(50, "length", 50), "\n\n") {
			if i < 2 || i > 40 { // the role event and the first token, then the last ten and the rest
				keep = append(keep, ev)
			}
		}
		return strings.Join(keep, "\n\n")
	}
	for name, body := range map[string]string{
		// Review of PR #247: every frame intact but most content dropped.
		"most content events dropped, finish, usage and [DONE] intact": dropped(),
		"one token short":                       streamBody(49, "length", 50),
		"finish stop":                           streamBody(50, "stop", 50),
		"usage that disagrees":                  streamBody(50, "length", 49),
		"no [DONE]":                             strings.TrimSuffix(streamBody(50, "length", 50), "data: [DONE]\n\n"),
		"content after the finish":              strings.Replace(streamBody(50, "length", 50), `data: {"choices":[],`, `data: {"choices":[{"index":0,"delta":{"content":" more"},"finish_reason":null}]}`+"\n\n"+`data: {"choices":[],`, 1),
		"a second finish":                       strings.Replace(streamBody(50, "length", 50), `"finish_reason":null}]}`, `"finish_reason":"length"}]}`, 2),
		"an event that does not parse":          strings.Replace(streamBody(50, "length", 50), `data: [DONE]`, "data: {\"choices\":\ndata: [DONE]", 1),
		"[DONE] before the end":                 strings.Replace(streamBody(50, "length", 50), `data: {"choices":[],`, "data: [DONE]\n\n"+`data: {"choices":[],`, 1),
		"a stream that only carries its record": `data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}` + "\n\n" + `data: {"choices":[],"usage":{"completion_tokens":50}}` + "\n\ndata: [DONE]\n\n",
	} {
		if p := streamProblems(sseParse([]byte(body)), 50); len(p) == 0 {
			t.Errorf("%s: streamProblems found nothing", name)
		}
	}
}

func TestFinalizerCancelProblems(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	cancel := func(s int) fakeslurm.Call { return fakeslurm.Call{Words: []string{"scancel", "1003"}, Time: at(s)} }
	other := fakeslurm.Call{Words: []string{"scancel", "1004"}, Time: at(5)}
	if p := finalizerCancelProblems([]fakeslurm.Call{other, cancel(11)}, 1003, at(0), at(10), at(12)); len(p) != 0 {
		t.Fatalf("a cancel inside the deletion: %v", p)
	}
	for name, c := range map[string]struct {
		calls             []fakeslurm.Call
		deletion, deleted time.Time
	}{
		// Review of PR #247: a cancel before the deletion is not the finalizer's.
		"a cancel 5 s before the deletionTimestamp": {[]fakeslurm.Call{cancel(5)}, at(10), at(12)},
		"a cancel before it and one inside":         {[]fakeslurm.Call{cancel(5), cancel(11)}, at(10), at(12)},
		"no cancel of the job":                      {[]fakeslurm.Call{other}, at(10), at(12)},
		"a cancel only after the DELETED":           {[]fakeslurm.Call{cancel(13)}, at(10), at(12)},
		"no deletionTimestamp seen":                 {[]fakeslurm.Call{cancel(11)}, time.Time{}, at(12)},
	} {
		if p := finalizerCancelProblems(c.calls, 1003, at(0), c.deletion, c.deleted); len(p) == 0 {
			t.Errorf("%s: finalizerCancelProblems found nothing", name)
		}
	}
}

// TestCmdLogRecordsHoldersBeforeEachSbatch runs sbatch through the login
// node's hook on an in-memory fake cluster: the attempt records the jobs that
// held the partition's slot before it ran, and its own outcome.
func TestCmdLogRecordsHoldersBeforeEachSbatch(t *testing.T) {
	plat := fakeslurm.New(slurm.RealClock, fakeslurm.Config{
		Partitions: []fakeslurm.Partition{{Name: "one"}},
		QOS:        []fakeslurm.QOS{{Name: "q1", MaxSubmitPerUser: fakeslurm.Cap(1)}},
	})
	sess := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user"}})
	l := &cmdLog{plat: plat}
	run := func(line string) slurm.Result {
		res, err := l.around(line, func() (slurm.Result, error) { return sess.Run(context.Background(), slurm.Command{Line: line}) })
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		return res
	}
	submit := func(name string) string {
		return "sbatch --parsable --job-name=" + name + " --partition=one --qos=q1 --time=5 --wrap=true"
	}
	run(submit("hb-a"))
	run("squeue --me --json")
	run(submit("hb-b")) // refused: hb-a, pending, holds the QOS's one slot
	atts := l.submissions("one")
	if len(atts) != 2 {
		t.Fatalf("%d attempts recorded, want 2: %+v", len(atts), atts)
	}
	if a := atts[0]; a.name != "hb-a" || a.jobID == 0 || a.exit != 0 || len(a.holders) != 0 {
		t.Errorf("the first attempt: %+v, want hb-a accepted with no holder before it", a)
	}
	if a := atts[1]; a.name != "hb-b" || a.jobID != 0 || a.exit == 0 || !slices.Equal(a.holders, []int{atts[0].jobID}) {
		t.Errorf("the second attempt: %+v, want hb-b refused, with job %d holding the slot before it", a, atts[0].jobID)
	}
	if last, n := l.lastHeld("one", atts[0].jobID); n != 3 || !last.Equal(atts[1].at) {
		t.Errorf("job %d last held the slot at %v over %d commands, want the third command's %v", atts[0].jobID, last, n, atts[1].at)
	}
}

// TestEngineGate runs the wrapper with a stand-in apptainer: an exec waits
// while the gate is armed, leaves its marker, and runs once released; a
// build passes at once.
func TestEngineGate(t *testing.T) {
	dir := t.TempDir()
	fakes := filepath.Join(dir, "fk")
	if err := os.MkdirAll(fakes, 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "ran")
	stand := "#!/bin/sh\necho \"$@\" >> '" + out + "'\n"
	if err := os.WriteFile(filepath.Join(fakes, "apptainer"), []byte(stand), 0o755); err != nil {
		t.Fatal(err)
	}
	g, wrapper := installEngineGate(t, dir, fakes)
	start := func(args ...string) chan error {
		done := make(chan error, 1)
		cmd := exec.Command(wrapper, args...)
		cmd.Env = append(os.Environ(), "SLURM_JOB_ID=7")
		go func() { done <- cmd.Run() }()
		return done
	}
	if err := <-start("build", "--sandbox"); err != nil {
		t.Fatalf("a build before the gate is armed: %v", err)
	}
	if err := g.arm(); err != nil {
		t.Fatal(err)
	}
	if err := <-start("build", "--sandbox"); err != nil {
		t.Fatalf("a build while armed: %v", err)
	}
	done := start("exec", "image", "vllm")
	stack.WaitFor(t, 30*time.Second, "the exec's marker at the gate", func() bool {
		_, ok := g.held(7)
		return ok
	})
	if b, _ := os.ReadFile(out); strings.Contains(string(b), "exec") {
		t.Fatalf("the exec ran while the gate held it: %q", b)
	}
	if jobs, err := g.heldJobs(); err != nil || !slices.Equal(jobs, []int{7}) {
		t.Fatalf("held jobs %v, %v; want [7]", jobs, err)
	}
	if err := g.release(7); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the released exec: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the released exec did not run")
	}
	if b, _ := os.ReadFile(out); string(b) != "build --sandbox\nbuild --sandbox\nexec image vllm\n" {
		t.Errorf("the stand-in ran %q", b)
	}
}

func TestGapReplies(t *testing.T) {
	from := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	gap := func(sent time.Time) probe {
		h := http.Header{}
		h.Set(hdrEmitter, "front")
		h.Set("Retry-After", "30")
		return probe{model: "m", sent: sent, rp: stack.Reply{Status: 503, Header: h, Body: []byte(`{"error":{"code":"model_inactive","detail":"no_ready_target"}}`)}}
	}
	good := []probe{gap(from), gap(from.Add(time.Second))}
	if n := gapReplies(good, "m", from); n != 2 {
		t.Fatalf("two gap replies counted as %d", n)
	}
	for name, pb := range map[string]func(probe) probe{
		"sent before the gap": func(p probe) probe { p.sent = from.Add(-time.Millisecond); return p },
		"another model":       func(p probe) probe { p.model = "x"; return p },
		"a 200":               func(p probe) probe { p.rp.Status = 200; return p },
		"from the instance": func(p probe) probe {
			p.rp.Header = p.rp.Header.Clone()
			p.rp.Header.Set(hdrEmitter, "instance")
			return p
		},
		"another detail": func(p probe) probe {
			p.rp.Body = []byte(`{"error":{"code":"model_inactive","detail":"not_attached"}}`)
			return p
		},
		"no Retry-After": func(p probe) probe { p.rp.Header = p.rp.Header.Clone(); p.rp.Header.Del("Retry-After"); return p },
		"unanswered":     func(p probe) probe { p.rp.Err = fmt.Errorf("reset"); return p },
	} {
		if n := gapReplies([]probe{pb(gap(from))}, "m", from); n != 0 {
			t.Errorf("%s: counted as a gap reply", name)
		}
	}
}
