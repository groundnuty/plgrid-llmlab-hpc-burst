package fakeslurm

// New. Test helpers. The expected values in the tests are literals worked out
// from S4's tests, the Slurm source it cites, S7's captures or by hand; they
// never come from this fake's own output fed back as a reference.

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// t0 is the clock's start: 2026-10-30 12:00:00 UTC.
var t0 = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

type rig struct {
	t     *testing.T
	clock *fake.Clock
	p     *Platform
	s     *Session
}

func newRig(t *testing.T, cfg Config) *rig {
	t.Helper()
	clock := fake.NewClock(t0)
	p := New(clock, cfg)
	return &rig{t: t, clock: clock, p: p, s: loginAs(p, "example-user")}
}

// loginAs is a Session with HOME and SCRATCH set under the user's name.
func loginAs(p *Platform, user string) *Session {
	return p.Login(Login{User: user, Env: map[string]string{
		"HOME": "/home/" + user, "SCRATCH": "/scratch/" + user,
	}})
}

// runAs runs one line as s, with stdin.
func (r *rig) runAs(s *Session, line string, stdin []byte) (stdout, stderr string, code int) {
	r.t.Helper()
	res, err := s.Run(context.Background(), slurm.Command{Line: line, Stdin: stdin})
	if err != nil {
		r.t.Fatalf("Run(%q): unexpected runner error %v", line, err)
	}
	return string(res.Stdout), string(res.Stderr), res.ExitCode
}

func (r *rig) run(line string) (stdout, stderr string, code int) {
	r.t.Helper()
	return r.runAs(r.s, line, nil)
}

// must runs a line that has to exit 0 and returns its stdout.
func (r *rig) must(line string) string {
	r.t.Helper()
	out, errOut, code := r.run(line)
	if code != 0 {
		r.t.Fatalf("%q: exit %d, stderr %q", line, code, errOut)
	}
	return out
}

// sbatchAs submits as s with --parsable and a trivial --wrap, and returns the
// job ID.
func (r *rig) sbatchAs(s *Session, args ...string) int {
	r.t.Helper()
	line := strings.Join(append([]string{"sbatch", "--parsable", "--wrap=true"}, args...), " ")
	out, errOut, code := r.runAs(s, line, nil)
	if code != 0 {
		r.t.Fatalf("%q: exit %d, stderr %q", line, code, errOut)
	}
	id, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		r.t.Fatalf("%q printed %q, want a job ID", line, out)
	}
	return id
}

func (r *rig) sbatch(args ...string) int { r.t.Helper(); return r.sbatchAs(r.s, args...) }

func (r *rig) squeue(args ...string) map[string]any {
	r.t.Helper()
	return decode(r.t, r.must(strings.Join(append([]string{"squeue", "--me", "--json"}, args...), " ")))
}

func (r *rig) sacct(args ...string) map[string]any {
	r.t.Helper()
	return decode(r.t, r.must(strings.Join(append([]string{"sacct", "--json"}, args...), " ")))
}

// squeueJob is the squeue record of job id, or fails.
func (r *rig) squeueJob(id int) map[string]any {
	r.t.Helper()
	js := jobsOf(r.t, r.squeue("--jobs="+strconv.Itoa(id)))
	if len(js) != 1 {
		r.t.Fatalf("squeue --jobs=%d lists %d jobs, want 1", id, len(js))
	}
	return js[0]
}

// start, end and friends fail the test on an error from the job controls.
func (r *rig) start(id int) {
	r.t.Helper()
	if err := r.p.Start(id); err != nil {
		r.t.Fatalf("Start(%d): %v", id, err)
	}
}

func (r *rig) end(id int, state string) {
	r.t.Helper()
	if err := r.p.End(id, state); err != nil {
		r.t.Fatalf("End(%d, %s): %v", id, state, err)
	}
}

func decode(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("output is not a JSON object: %v\n%s", err, s)
	}
	return m
}

// jobsOf returns the "jobs" array of a squeue or sacct response.
func jobsOf(t *testing.T, resp map[string]any) []map[string]any {
	t.Helper()
	list, ok := resp["jobs"].([]any)
	if !ok {
		t.Fatalf(`"jobs" is %T, want a JSON array`, resp["jobs"])
	}
	out := []map[string]any{}
	for _, j := range list {
		out = append(out, j.(map[string]any))
	}
	return out
}

// idsOf returns the job IDs of a response, in output order.
func idsOf(t *testing.T, resp map[string]any) []int {
	t.Helper()
	out := []int{}
	for _, j := range jobsOf(t, resp) {
		out = append(out, int(j["job_id"].(float64)))
	}
	return out
}

func keysOf(m map[string]any) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func wantKeys(t *testing.T, what string, m map[string]any, want ...string) {
	t.Helper()
	sort.Strings(want)
	if got := keysOf(m); !reflect.DeepEqual(got, want) {
		t.Errorf("%s keys\n got  %v\n want %v", what, got, want)
	}
}

// wantSubset asserts that every key of m is in the allowed set.
func wantSubset(t *testing.T, what string, m map[string]any, allowed map[string]bool) {
	t.Helper()
	for _, k := range keysOf(m) {
		if !allowed[k] {
			t.Errorf("%s has key %q, which S7's capture does not list", what, k)
		}
	}
}

// wantNV asserts a Slurm "number with set and infinite flags".
func wantNV(t *testing.T, what string, v any, set bool, number float64) {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Errorf("%s is %T, want an object {set, infinite, number}", what, v)
		return
	}
	wantKeys(t, what, m, "set", "infinite", "number")
	if m["set"] != set || m["infinite"] != false || m["number"] != number {
		t.Errorf("%s = %v, want set=%v infinite=false number=%v", what, m, set, number)
	}
}

func wantStrings(t *testing.T, what string, v any, want ...string) {
	t.Helper()
	list, ok := v.([]any)
	if !ok {
		t.Errorf("%s is %T, want a list of strings", what, v)
		return
	}
	got := []string{}
	for _, s := range list {
		got = append(got, s.(string))
	}
	if want == nil {
		want = []string{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

// num reads a JSON number out of a decoded object.
func num(t *testing.T, m map[string]any, path ...string) float64 {
	t.Helper()
	var v any = m
	for _, k := range path {
		o, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("%v: %v is not an object", path, k)
		}
		v = o[k]
	}
	switch x := v.(type) {
	case float64:
		return x
	case map[string]any:
		return x["number"].(float64)
	}
	t.Fatalf("%v is %T, not a number or a triple", path, v)
	return 0
}

func itoa(n int) string { return strconv.Itoa(n) }
