package fakeslurm

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	_ "time/tzdata" // zone tests must not depend on the host's zoneinfo
)

// t0 is the clock's start: 2026-10-30 12:00:00 UTC.
var t0 = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

// clock is an injectable, goroutine-safe fake clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func (c *clock) Advance(d time.Duration) { c.Set(c.Now().Add(d)) }

// rig runs fake Slurm commands in-process against a private state and log in
// a temp dir, with a fake clock and no real sleeps. Its env map is the whole
// process environment of the fake.
type rig struct {
	t   *testing.T
	env map[string]string
	clk *clock

	sleep       func(time.Duration)
	block       func()
	inLock      func() func()
	outsideLock func(string)
}

func newRig(t *testing.T) *rig {
	t.Helper()
	dir := t.TempDir()
	return &rig{
		t: t,
		env: map[string]string{
			"FAKE_SLURM_STATE": filepath.Join(dir, "state.json"),
			"FAKE_SLURM_LOG":   filepath.Join(dir, "log.jsonl"),
		},
		clk: &clock{t: t0},
	}
}

func (r *rig) Env() (Env, *bytes.Buffer, *bytes.Buffer) {
	var out, errOut bytes.Buffer
	return Env{
		Getenv: func(k string) string { return r.env[k] },
		Now:    r.clk.Now,
		Sleep:  func(d time.Duration) { r.callSleep(d) },
		Block:  func() { r.callBlock() },
		Stdout: &out, Stderr: &errOut, Pid: 4242,
		InLock: r.inLock, OutsideLock: r.outsideLock,
	}, &out, &errOut
}

func (r *rig) callSleep(d time.Duration) {
	if r.sleep != nil {
		r.sleep(d)
	}
}

func (r *rig) callBlock() {
	if r.block != nil {
		r.block()
	}
}

// run executes one command line; argv[0] is the command word as invoked.
func (r *rig) run(argv ...string) (stdout, stderr string, code int) {
	env, out, errOut := r.Env()
	code = Main(env, argv)
	return out.String(), errOut.String(), code
}

// must runs a command that has to exit 0 and returns its stdout.
func (r *rig) must(argv ...string) string {
	r.t.Helper()
	out, errOut, code := r.run(argv...)
	if code != 0 {
		r.t.Fatalf("%v: exit %d, stderr %q", argv, code, errOut)
	}
	return out
}

// sbatch submits with --parsable and returns the job id.
func (r *rig) sbatch(args ...string) int {
	r.t.Helper()
	out := r.must(append([]string{"sbatch", "--parsable"}, args...)...)
	id, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		r.t.Fatalf("sbatch --parsable printed %q, want a job id", out)
	}
	return id
}

func (r *rig) ctl(args ...string) string {
	r.t.Helper()
	return r.must(append([]string{"fakeslurmctl"}, args...)...)
}

func (r *rig) squeue(args ...string) map[string]any {
	r.t.Helper()
	return decodeJSON(r.t, r.must(append([]string{"squeue", "--json"}, args...)...))
}

func (r *rig) sacct(args ...string) map[string]any {
	r.t.Helper()
	return decodeJSON(r.t, r.must(append([]string{"sacct", "--json"}, args...)...))
}

func decodeJSON(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("output is not a JSON object: %v\n%s", err, s)
	}
	return m
}

// jobs returns the "jobs" array of a squeue or sacct response.
func jobs(t *testing.T, resp map[string]any) []map[string]any {
	t.Helper()
	list, ok := resp["jobs"].([]any)
	if !ok {
		t.Fatalf(`"jobs" is %T, want a JSON array`, resp["jobs"])
	}
	var out []map[string]any
	for _, j := range list {
		out = append(out, j.(map[string]any))
	}
	return out
}

// ids returns the job ids of a response, in output order.
func ids(t *testing.T, resp map[string]any) []int {
	t.Helper()
	out := []int{}
	for _, j := range jobs(t, resp) {
		out = append(out, int(j["job_id"].(float64)))
	}
	return out
}

func sameInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// logEvents decodes the event log into generic maps.
func (r *rig) logEvents() []map[string]any {
	r.t.Helper()
	b, err := os.ReadFile(r.env["FAKE_SLURM_LOG"])
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		r.t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			r.t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func (r *rig) countEvents(event string) int {
	n := 0
	for _, ev := range r.logEvents() {
		if ev["event"] == event {
			n++
		}
	}
	return n
}
