package fakeslurm

// New. Faults, gates, the generation counter and concurrency. S4 drove the
// same cases with environment variables (FAKE_SLURM_SBATCH_FAIL_ONCE,
// _GATE, _LOST_REPLY_ONCE); here they are method calls.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

func TestGenerationStartsAtOneAndRisesOnReconnects(t *testing.T) {
	r := newRig(t, Config{})
	if g := r.s.Generation(); g != 1 {
		t.Fatalf("initial Generation = %d, want 1", g)
	}
	r.p.FailNext(2, slurm.ErrNotStarted) // each refused login is a reconnect
	for i := 0; i < 2; i++ {
		if _, err := r.s.Run(context.Background(), slurm.Command{Line: "printenv HOME"}); !errors.Is(err, slurm.ErrNotStarted) {
			t.Fatalf("Run #%d = %v, want ErrNotStarted", i+1, err)
		}
	}
	if g := r.s.Generation(); g != 3 {
		t.Errorf("Generation after two refused logins = %d, want 3", g)
	}
	// A transport error or a time-out is not a new connection.
	for _, e := range []error{slurm.ErrTransport, slurm.ErrTimeout} {
		r.p.FailNext(1, e)
		r.s.Run(context.Background(), slurm.Command{Line: "printenv HOME"})
	}
	if g := r.s.Generation(); g != 3 {
		t.Errorf("Generation after a transport error and a time-out = %d, want 3", g)
	}
	r.s.Reconnect()
	if g := r.s.Generation(); g != 4 {
		t.Errorf("Generation after Reconnect = %d, want 4", g)
	}
	// A wrapped ErrNotStarted counts too.
	r.p.FailNext(1, fmt.Errorf("ssh: %w", slurm.ErrNotStarted))
	r.s.Run(context.Background(), slurm.Command{Line: "printenv HOME"})
	if g := r.s.Generation(); g != 5 {
		t.Errorf("Generation after a wrapped ErrNotStarted = %d, want 5", g)
	}
}

func TestFailNext(t *testing.T) {
	r := newRig(t, Config{})
	r.p.FailNext(2, slurm.ErrTransport)
	for i := 0; i < 2; i++ {
		res, err := r.s.Run(context.Background(), slurm.Command{Line: "sbatch --parsable --wrap=true"})
		if !errors.Is(err, slurm.ErrTransport) || len(res.Stdout) != 0 {
			t.Fatalf("Run #%d = %+v, %v, want ErrTransport and no result", i+1, res, err)
		}
	}
	// A faulted command did not run: no job, not in the call log, but in Faulted.
	if len(r.p.Jobs()) != 0 || r.p.Count("sbatch") != 0 {
		t.Errorf("a faulted sbatch ran: %d jobs, %d calls", len(r.p.Jobs()), r.p.Count("sbatch"))
	}
	f := r.p.Faulted()
	if len(f) != 2 || !errors.Is(f[0].Err, slurm.ErrTransport) || f[0].User != "example-user" ||
		strings.Join(f[0].Words, " ") != "sbatch --parsable --wrap=true" || !f[0].Time.Equal(t0) {
		t.Errorf("Faulted = %+v", f)
	}
	// The third command passes.
	if id := r.sbatch(); id != 1001 {
		t.Errorf("job ID after the faults = %d, want 1001", id)
	}
	if n := len(r.p.Faulted()); n != 2 {
		t.Errorf("Faulted has %d entries after a normal command, want 2", n)
	}
}

func TestFailNextOfOneSession(t *testing.T) {
	r := newRig(t, Config{})
	bob := loginAs(r.p, "bob")
	r.s.FailNext(1, slurm.ErrNotStarted)
	// Bob's command passes, and the fault stays for the Session it was set on.
	if _, _, code := r.runAs(bob, "printenv HOME", nil); code != 0 {
		t.Errorf("bob's command exit %d, want 0: the fault is for the other Session", code)
	}
	if _, err := r.s.Run(context.Background(), slurm.Command{Line: "printenv HOME"}); !errors.Is(err, slurm.ErrNotStarted) {
		t.Errorf("the faulted Session's command = %v, want ErrNotStarted", err)
	}
	if r.s.Generation() != 2 || bob.Generation() != 1 {
		t.Errorf("generations %d and %d, want 2 and 1: only the Session that was refused reconnects", r.s.Generation(), bob.Generation())
	}
	// A platform-wide fault hits whichever Session comes first.
	r.p.FailNext(1, slurm.ErrTimeout)
	if _, err := bob.Run(context.Background(), slurm.Command{Line: "printenv HOME"}); !errors.Is(err, slurm.ErrTimeout) {
		t.Errorf("bob's command = %v, want ErrTimeout from the platform-wide fault", err)
	}
	if _, _, code := r.run("printenv HOME"); code != 0 {
		t.Errorf("the platform-wide fault was used twice")
	}
}

func TestCancelledContextNeverStarts(t *testing.T) {
	r := newRig(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.s.Run(ctx, slurm.Command{Line: "sbatch --parsable --wrap=true"})
	if !errors.Is(err, slurm.ErrNotStarted) {
		t.Errorf("Run with an ended context = %v, want ErrNotStarted", err)
	}
	if len(r.p.Jobs()) != 0 || r.p.Count("sbatch") != 0 {
		t.Errorf("the command ran")
	}
}

// ---- gates ----

// runAsync runs a command in a goroutine and returns channels for its outcome.
type asyncResult struct {
	res slurm.Result
	err error
}

func runAsync(ctx context.Context, s *Session, line string) <-chan asyncResult {
	ch := make(chan asyncResult, 1)
	go func() {
		res, err := s.Run(ctx, slurm.Command{Line: line})
		ch <- asyncResult{res, err}
	}()
	return ch
}

func receive(t *testing.T, ch <-chan asyncResult) asyncResult {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("a command did not finish within 5 s")
		return asyncResult{}
	}
}

// A gate orders a race: the gated sbatch registers after one that started later.
func TestGateOrdersARace(t *testing.T) {
	r := newRig(t, Config{})
	bob := loginAs(r.p, "bob")
	g := r.p.Gate("sbatch")
	first := runAsync(context.Background(), r.s, "sbatch --parsable --wrap=true")
	g.Wait() // the first sbatch is blocked
	if n := r.p.Count("sbatch"); n != 0 || len(r.p.Jobs()) != 0 {
		t.Errorf("a blocked sbatch has begun: %d calls, %d jobs", n, len(r.p.Jobs()))
	}
	// The second sbatch is not gated (a gate holds one command) and overtakes.
	if id := r.sbatchAs(bob); id != 1001 {
		t.Errorf("the overtaking job has ID %d, want 1001", id)
	}
	g.Release()
	got := receive(t, first)
	if got.err != nil || string(got.res.Stdout) != "1002\n" {
		t.Errorf("the released sbatch = %+v, %v, want job 1002", got.res, got.err)
	}
	if j, _ := r.p.Job(1001); j.User != "bob" {
		t.Errorf("job 1001 belongs to %s, want bob", j.User)
	}
	if n := r.p.Count("sbatch"); n != 2 {
		t.Errorf("sbatch count = %d, want 2", n)
	}
}

func TestGateReleasedEarlyPassesAtOnce(t *testing.T) {
	r := newRig(t, Config{})
	g := r.p.Gate("squeue")
	g.Release()
	g.Release() // twice is fine
	r.squeue()
	select {
	case <-g.Blocked():
	default:
		t.Errorf("the command did not meet the gate")
	}
	r.squeue() // the gate is used up
}

func TestGatesHoldCommandsInTheOrderTheyWereSet(t *testing.T) {
	r := newRig(t, Config{})
	g1, g2 := r.p.Gate("printenv"), r.p.Gate("printenv")
	a := runAsync(context.Background(), r.s, "printenv HOME")
	g1.Wait()
	b := runAsync(context.Background(), r.s, "printenv SCRATCH")
	g2.Wait()
	g2.Release() // b goes first
	if got := receive(t, b); string(got.res.Stdout) != "/scratch/example-user\n" {
		t.Errorf("b = %q", got.res.Stdout)
	}
	select {
	case <-a:
		t.Fatal("a finished before its gate was released")
	default:
	}
	g1.Release()
	if got := receive(t, a); string(got.res.Stdout) != "/home/example-user\n" {
		t.Errorf("a = %q", got.res.Stdout)
	}
}

// A context that ends while the command waits at a gate frees it with an error
// and the command never runs: it registers nothing and is not in the call log.
func TestGateReleasedByTheContext(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		r := newRig(t, Config{})
		g := r.p.Gate("sbatch")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ch := runAsync(ctx, r.s, "sbatch --parsable --wrap=true")
		g.Wait()
		cancel()
		wantFreedByContext(t, r, receive(t, ch), slurm.ErrTransport)
	})
	t.Run("deadline", func(t *testing.T) {
		r := newRig(t, Config{})
		g := r.p.Gate("sbatch")
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		ch := runAsync(ctx, r.s, "sbatch --parsable --wrap=true")
		g.Wait() // the deadline then frees it
		wantFreedByContext(t, r, receive(t, ch), slurm.ErrTimeout)
	})
}

func wantFreedByContext(t *testing.T, r *rig, got asyncResult, want error) {
	t.Helper()
	if !errors.Is(got.err, want) {
		t.Errorf("Run = %v, want %v", got.err, want)
	}
	if len(r.p.Jobs()) != 0 || r.p.Count("sbatch") != 0 {
		t.Errorf("a command freed by its context ran")
	}
	// The gate is used up and the platform still works.
	if id := r.sbatch(); id != 1001 {
		t.Errorf("job ID %d, want 1001", id)
	}
}

// ---- concurrency ----

func TestMaxConcurrentCountsBlockedCommands(t *testing.T) {
	r := newRig(t, Config{})
	for i := 0; i < 5; i++ {
		r.must("printenv HOME")
	}
	if n := r.p.MaxConcurrent(); n != 1 {
		t.Fatalf("MaxConcurrent after sequential commands = %d, want 1", n)
	}
	gates := []*Gate{r.p.Gate("printenv"), r.p.Gate("printenv"), r.p.Gate("printenv")}
	var results []<-chan asyncResult
	for _, g := range gates {
		results = append(results, runAsync(context.Background(), r.s, "printenv HOME"))
		g.Wait()
	}
	for _, g := range gates {
		g.Release()
	}
	for _, ch := range results {
		receive(t, ch)
	}
	if n := r.p.MaxConcurrent(); n != 3 {
		t.Errorf("MaxConcurrent with three blocked commands = %d, want 3", n)
	}
}

// Run with -race: many Sessions hammer one platform.
func TestConcurrentUseIsSafe(t *testing.T) {
	r := newRig(t, Config{})
	const workers, each = 8, 40
	var wg sync.WaitGroup
	ids := make(chan string, workers*each)
	for w := 0; w < workers; w++ {
		s := loginAs(r.p, fmt.Sprintf("worker%d", w))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				res, err := s.Run(context.Background(), slurm.Command{Line: "sbatch --parsable --wrap=true"})
				if err != nil || res.ExitCode != 0 {
					t.Errorf("sbatch = %+v, %v", res, err)
					return
				}
				ids <- string(res.Stdout)
				for _, line := range []string{"squeue --me --json", "sacct --json -X -D", "printenv HOME", "mkdir -p /d && cat > /d/f"} {
					if _, err := s.Run(context.Background(), slurm.Command{Line: line, Stdin: []byte("x")}); err != nil {
						t.Errorf("%s: %v", line, err)
					}
				}
			}
		}()
	}
	// A reader of the control and log API runs alongside.
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				r.p.Calls()
				r.p.Jobs()
				r.p.Files()
				r.p.MaxConcurrent()
			}
		}
	}()
	wg.Wait()
	close(stop)
	readers.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Errorf("job ID %q was handed out twice", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*each || r.p.Count("sbatch") != workers*each || len(r.p.Jobs()) != workers*each {
		t.Errorf("%d IDs, %d sbatch calls, %d jobs, want %d of each", len(seen), r.p.Count("sbatch"), len(r.p.Jobs()), workers*each)
	}
	if n := r.p.MaxConcurrent(); n < 1 || n > workers {
		t.Errorf("MaxConcurrent = %d, want between 1 and %d", n, workers)
	}
}
