package rehearsal

// New. The login node driven by the controller's own transport, sshrun.New,
// over a real sshd. The expected values come from fakeslurm's documented
// behaviour (doc.go: the grammar, the exit codes, run mode, the per-user
// squeue view) and from the content and commands this file writes; none is
// read back from the relay. Every wait is on observable state (a gate being
// reached, a job's snapshot, a file) with a generous deadline.

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// squeueReply is the part of squeue --json that the tests read.
type squeueReply struct {
	Jobs []struct {
		JobID    int      `json:"job_id"`
		Hold     bool     `json:"hold"`
		JobState []string `json:"job_state"`
		UserName string   `json:"user_name"`
	} `json:"jobs"`
	Meta struct {
		Plugin struct {
			DataParser string `json:"data_parser"`
		} `json:"plugin"`
		Client struct {
			User string `json:"user"`
		} `json:"client"`
	} `json:"meta"`
}

func decodeSqueue(t *testing.T, out []byte) squeueReply {
	t.Helper()
	var q squeueReply
	if err := json.Unmarshal(out, &q); err != nil {
		t.Fatalf("squeue --json printed %q, not JSON: %v", out, err)
	}
	return q
}

// The credentials file parses with the controller's own parser, names the
// current account (read here from the OS) and the key in Dir, points at the
// listener, and is written with mode 600.
func TestCredentialsFile(t *testing.T) {
	r, _ := newRig(t)
	r.watch.wantSSH = false // no command runs here
	path := filepath.Join(r.base, "creds")
	fi, err := os.Stat(path)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("credentials file: %v, %v; want mode 600", fi, err)
	}
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	got := r.creds
	if got.HostName != "127.0.0.1" || got.User != me.Username || got.IdentityFile != filepath.Join(r.base, "n", "id") ||
		got.UserKnownHostsFile != filepath.Join(r.base, "n", "known_hosts") {
		t.Errorf("parsed credentials %+v: want host 127.0.0.1, user %q, the key and known_hosts in Dir", got, me.Username)
	}
	if want := r.node.Addr(); want != "127.0.0.1:"+strconv.Itoa(got.Port) {
		t.Errorf("credentials port %d, listener %s", got.Port, want)
	}
	// Rewriting replaces the file and keeps the mode.
	if err := r.node.WriteCredentials(path); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("rewritten credentials file: %v, %v; want mode 600", fi, err)
	}
}

// squeue --me --json shows the cluster user's jobs only, in the JSON shape of
// the documented data parser, over the controller's own transport.
func TestSqueueMeJSON(t *testing.T) {
	r, s := newRig(t)
	other := r.plat.Login(fakeslurm.Login{User: "other-user", Env: map[string]string{"HOME": r.home}})
	if res, err := other.Run(t.Context(), slurm.Command{Line: "sbatch --parsable --hold '--wrap=true'"}); err != nil || res.ExitCode != 0 {
		t.Fatalf("the other user's sbatch: %v, %+v", err, res)
	}
	id := atoi(t, must(t, s, "sbatch --parsable --hold '--wrap=true'"))

	q := decodeSqueue(t, []byte(must(t, s, "squeue --me --json")))
	if len(q.Jobs) != 1 {
		t.Fatalf("squeue --me listed %d jobs, want only the cluster user's one: %+v", len(q.Jobs), q.Jobs)
	}
	j := q.Jobs[0]
	if j.JobID != id || !j.Hold || len(j.JobState) != 1 || j.JobState[0] != "PENDING" || j.UserName != clusterUser {
		t.Errorf("the listed job %+v: want job %d, held, PENDING, user %s", j, id, clusterUser)
	}
	if q.Meta.Plugin.DataParser != "data_parser/v0.0.42" || q.Meta.Client.User != clusterUser {
		t.Errorf("meta %+v: want data_parser/v0.0.42 and client user %s", q.Meta, clusterUser)
	}
	if got := r.plat.CountFor(clusterUser, "squeue"); got != 1 {
		t.Errorf("the platform logged %d squeue calls of %s, want 1", got, clusterUser)
	}
}

// A submission held with --hold has no process; scontrol release starts it
// within the command (doc.go, Run mode), and its --wrap runs for real.
func TestHoldReleaseRunsTheJob(t *testing.T) {
	r, s := newRig(t)
	ran := filepath.Join(r.root, "ran.txt")
	id := atoi(t, must(t, s, "sbatch --parsable --hold '--wrap=echo ran > "+ran+"'"))
	j, ok := r.plat.Job(id)
	if !ok || !j.Held || j.PID != 0 || j.State != "PENDING" {
		t.Fatalf("after sbatch --hold: %+v (found %v), want PENDING, held, no process", j, ok)
	}
	if _, err := os.Stat(ran); err == nil {
		t.Fatal("the held job's --wrap ran")
	}
	must(t, s, "scontrol release "+strconv.Itoa(id))
	if j, _ := r.plat.Job(id); j.PID == 0 || j.Held {
		t.Fatalf("after scontrol release returned: %+v, want a started process and no hold", j)
	}
	waitFor(t, "the job's processes to end", func() bool { j, _ := r.plat.Job(id); return j.Finished })
	b, err := os.ReadFile(ran)
	if err != nil || string(b) != "ran\n" {
		t.Errorf("the job wrote %q (%v), want \"ran\\n\"", b, err)
	}
	if j, _ := r.plat.Job(id); j.State != "COMPLETED" || j.ExitCode != 0 {
		t.Errorf("the job ended %s with exit %d, want COMPLETED, 0", j.State, j.ExitCode)
	}
}

// Content goes through standard input to a file under the run-mode root, byte
// for byte, with the mode the line's umask gives: 600 for a file, 700 for the
// directory made under it.
func TestStdinWritesLandOnDisk(t *testing.T) {
	r, s := newRig(t)
	// 3 MiB with NUL bytes, no trailing newline: a short read, a lost window
	// or an added byte would change the digest.
	content := make([]byte, 3<<20+7)
	rand.New(rand.NewSource(1)).Read(content)
	for i := 0; i < len(content); i += 4099 {
		content[i] = 0
	}
	dir := filepath.Join(r.home, "stage", "sub")
	file := filepath.Join(dir, "blob")
	line := "umask 077 && mkdir -p " + dir + " && cat > " + file
	if res := run(t, s, line, content); res.ExitCode != 0 {
		t.Fatalf("%q: exit %d, stderr %q", line, res.ExitCode, res.Stderr)
	}
	got, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(content) || len(got) != len(content) {
		t.Errorf("the file holds %d bytes with another digest than the %d written", len(got), len(content))
	}
	for path, want := range map[string]os.FileMode{file: 0o600, dir: 0o700} {
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s: %v, %v; want mode %o", filepath.Base(path), fi, err, want)
		}
	}
	// No standard input is an empty file.
	empty := filepath.Join(dir, "empty")
	if res := run(t, s, "cat > "+empty, nil); res.ExitCode != 0 {
		t.Fatalf("cat > with no standard input: exit %d, stderr %q", res.ExitCode, res.Stderr)
	}
	if fi, err := os.Stat(empty); err != nil || fi.Size() != 0 {
		t.Errorf("the empty write: %v, %v", fi, err)
	}
}

// A command that fails passes its exit code and its standard error back, and
// the two streams stay apart.
func TestFailuresPassExitCodeAndStderr(t *testing.T) {
	r, s := newRig(t)
	missing := filepath.Join(r.home, "missing")
	for _, c := range []struct {
		line, stdout, stderr string
		exit                 int
	}{
		{"frobnicate", "", "bash: line 1: frobnicate: command not found\n", 127},
		{"sha256sum " + missing, "", "sha256sum: " + missing + ": No such file or directory\n", 1},
		{"echo $HOME", "", "", 2}, // a "$" outside quotes is a syntax error (doc.go)
		{"printenv HOME && sha256sum " + missing, r.home + "\n", "sha256sum: " + missing + ": No such file or directory\n", 1},
	} {
		res := run(t, s, c.line, nil)
		if res.ExitCode != c.exit || string(res.Stdout) != c.stdout {
			t.Errorf("%q: exit %d, stdout %q; want exit %d, stdout %q", c.line, res.ExitCode, res.Stdout, c.exit, c.stdout)
		}
		if c.stderr != "" && string(res.Stderr) != c.stderr || c.stderr == "" && len(res.Stderr) == 0 {
			t.Errorf("%q: stderr %q, want %q", c.line, res.Stderr, c.stderr)
		}
	}
}

// A runner error of the fake (a timeout here) reaches the controller's
// transport as exit 255, which sshrun reads as slurm.ErrTransport; the master
// survives it.
func TestRunnerErrorIsATransportError(t *testing.T) {
	r, s := newRig(t)
	must(t, s, "printenv HOME")
	r.plat.FailNext(1, slurm.ErrTimeout)
	_, err := s.Run(t.Context(), slurm.Command{Line: "printenv HOME"})
	if !errors.Is(err, slurm.ErrTransport) {
		t.Fatalf("a fake timeout: error %v, want one wrapping slurm.ErrTransport", err)
	}
	if got := strings.TrimSpace(must(t, s, "printenv HOME")); got != r.home {
		t.Errorf("after the error: HOME %q, want %q", got, r.home)
	}
	if s.Starts() != 1 {
		t.Errorf("the master started %d times, want 1: the error must not cost the connection", s.Starts())
	}
}

// Commands run at once over the one master, each in a session of its own: the
// fake holds n of them at a gate at the same moment, then lets them all go.
func TestConcurrentSessions(t *testing.T) {
	for _, n := range []int{2, 4} { // sshrun allows four at once (HBEP-4)
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			r, s := newRig(t)
			gates := make([]*fakeslurm.Gate, n)
			for i := range gates {
				gates[i] = r.plat.Gate("squeue")
				defer gates[i].Release() // a failed test must not strand the commands
			}
			outs := make([]string, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res, err := s.Run(t.Context(), slurm.Command{Line: "squeue --me --json"})
					if err != nil || res.ExitCode != 0 {
						t.Errorf("command %d: %v, %+v", i, err, res)
					}
					outs[i] = string(res.Stdout)
				}()
			}
			for i, g := range gates {
				select {
				case <-g.Blocked():
				case <-t.Context().Done():
					t.Fatalf("gate %d was never reached", i)
				}
			}
			if got := r.plat.MaxConcurrent(); got != n {
				t.Errorf("the platform saw at most %d commands at once, want %d", got, n)
			}
			for _, g := range gates {
				g.Release()
			}
			wg.Wait()
			for i, o := range outs {
				if q := decodeSqueue(t, []byte(o)); q.Meta.Client.User != clusterUser {
					t.Errorf("command %d: client user %q", i, q.Meta.Client.User)
				}
			}
			if s.Starts() != 1 {
				t.Errorf("the master started %d times, want 1 for all %d sessions", s.Starts(), n)
			}
		})
	}
}

// The platform belongs to the test: a job submitted through one transport is
// there for the next, as for a restarted controller.
func TestPlatformOutlivesTheTransport(t *testing.T) {
	r, first := newRig(t)
	id := atoi(t, must(t, first, "sbatch --parsable --hold '--wrap=true'"))
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := r.transport()
	q := decodeSqueue(t, []byte(must(t, second, "squeue --me --json")))
	if len(q.Jobs) != 1 || q.Jobs[0].JobID != id {
		t.Errorf("a second transport lists %+v, want job %d", q.Jobs, id)
	}
}

// Close with a command in flight does not hang on it: the command ends with a
// transport error and nothing is left behind (the rig's check).
func TestCloseEndsACommandInFlight(t *testing.T) {
	r, s := newRig(t)
	g := r.plat.Gate("squeue")
	defer g.Release()
	errc := make(chan error, 1)
	go func() {
		_, err := s.Run(t.Context(), slurm.Command{Line: "squeue --me --json"})
		errc <- err
	}()
	select {
	case <-g.Blocked():
	case <-t.Context().Done():
		t.Fatal("the command never reached the gate")
	}
	if err := r.node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, slurm.ErrTransport) {
		t.Errorf("the command in flight at Close: error %v, want one wrapping slurm.ErrTransport", err)
	}
	if err := r.node.Close(); err != nil {
		t.Errorf("a second Close: %v", err)
	}
}
