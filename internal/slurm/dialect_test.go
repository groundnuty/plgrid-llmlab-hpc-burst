package slurm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// These tests check the command templates and the reading of sbatch's reply
// against HBEP-4 (The Slurm dialect, Submission, Transport). The shell is the
// oracle for quoting: lines are run through /bin/sh against a stand-in sbatch
// that prints its arguments, and the bootstrap is run for real.

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func needTools(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := exec.LookPath(n); err != nil {
			t.Skipf("%s is not on PATH; the login node has it, and this test runs the line in a shell", n)
		}
	}
}

func exampleSubmission() submission {
	dir := "/scratch/example-user/hpcburst/alloc/hb-example-0123456789ab"
	return submission{
		tag: "hb-example-0123456789ab", partition: "example-gpu", account: "example-account", hold: true,
		minutes: 2880, gpus: 2, cpusPerTask: 16, memMiB: 98304,
		allocDir:    dir,
		agentPath:   "/scratch/example-user/hpcburst/agent/" + testDigest + "/hb-agent",
		agentDigest: testDigest,
	}
}

// sbatchArgv runs line in /bin/sh with a stand-in sbatch first on PATH and
// returns the arguments it received, one element per argument.
func sbatchArgv(t *testing.T, line string) []string {
	t.Helper()
	needTools(t, "sh")
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "argv")
	script := "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\0' \"$a\"; done > '" + out + "'\n"
	if err := os.WriteFile(filepath.Join(bin, "sbatch"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/sh", "-c", line)
	cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=/nonexistent-home"}
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("running %q: %v\n%s", line, err, b)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
	return args
}

// ---- the submission line ----

// HBEP-4, Submission: sbatch --parsable --job-name=<tag> --no-requeue [--hold]
// --partition=<p> --account=<a> --time=<limit> --nodes=1 --gres=gpu:<n>
// --cpus-per-task=<c> --mem=<m> --output=<dir>/job.out --error=<dir>/job.err
// --wrap=<bootstrap>.
func TestSubmitLineWordsAndOrder(t *testing.T) {
	const dir = "/scratch/example-user/hpcburst/alloc/hb-example-0123456789ab"
	words := func(parts ...string) []string { return parts }
	for _, c := range []struct {
		name string
		edit func(s *submission)
		pre  []string // the words before --wrap
	}{
		{"held GPU job with an account", func(s *submission) {}, words(
			"--parsable", "--job-name=hb-example-0123456789ab", "--no-requeue", "--hold", "--partition=example-gpu",
			"--account=example-account", "--time=2880", "--nodes=1", "--gres=gpu:2", "--cpus-per-task=16", "--mem=98304M",
			"--output="+dir+"/job.out", "--error="+dir+"/job.err")},
		{"not held", func(s *submission) { s.hold = false }, words(
			"--parsable", "--job-name=hb-example-0123456789ab", "--no-requeue", "--partition=example-gpu",
			"--account=example-account", "--time=2880", "--nodes=1", "--gres=gpu:2", "--cpus-per-task=16", "--mem=98304M",
			"--output="+dir+"/job.out", "--error="+dir+"/job.err")},
		{"no GPU: --gres is left out", func(s *submission) { s.gpus = 0 }, words(
			"--parsable", "--job-name=hb-example-0123456789ab", "--no-requeue", "--hold", "--partition=example-gpu",
			"--account=example-account", "--time=2880", "--nodes=1", "--cpus-per-task=16", "--mem=98304M",
			"--output="+dir+"/job.out", "--error="+dir+"/job.err")},
		{"four GPUs, untyped", func(s *submission) { s.gpus = 4 }, words(
			"--parsable", "--job-name=hb-example-0123456789ab", "--no-requeue", "--hold", "--partition=example-gpu",
			"--account=example-account", "--time=2880", "--nodes=1", "--gres=gpu:4", "--cpus-per-task=16", "--mem=98304M",
			"--output="+dir+"/job.out", "--error="+dir+"/job.err")},
		{"the Provider names no account", func(s *submission) { s.account = "" }, words(
			"--parsable", "--job-name=hb-example-0123456789ab", "--no-requeue", "--hold", "--partition=example-gpu",
			"--time=2880", "--nodes=1", "--gres=gpu:2", "--cpus-per-task=16", "--mem=98304M",
			"--output="+dir+"/job.out", "--error="+dir+"/job.err")},
		{"minutes and MiB are plain numbers", func(s *submission) { s.minutes, s.memMiB = 90, 1500 }, words(
			"--parsable", "--job-name=hb-example-0123456789ab", "--no-requeue", "--hold", "--partition=example-gpu",
			"--account=example-account", "--time=90", "--nodes=1", "--gres=gpu:2", "--cpus-per-task=16", "--mem=1500M",
			"--output="+dir+"/job.out", "--error="+dir+"/job.err")},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := exampleSubmission()
			c.edit(&s)
			line, err := submitLine(s)
			if err != nil {
				t.Fatal(err)
			}
			// As text: the words before --wrap, in HBEP-4's order, then the
			// bootstrap as one single-quoted word.
			wantPrefix := "sbatch " + strings.Join(c.pre, " ") + " '--wrap="
			if !strings.HasPrefix(line, wantPrefix) {
				t.Errorf("the line starts\n  %s\nwant\n  %s", line, wantPrefix)
			}
			if !strings.HasSuffix(line, "'") || strings.Count(line, "'") != 2 {
				t.Errorf("the --wrap word is not one single-quoted word: %s", line)
			}
			// As the shell reads it: one argument per word, the bootstrap whole.
			args := sbatchArgv(t, line)
			if len(args) != len(c.pre)+1 {
				t.Fatalf("the shell split the line into %d words, want %d: %q", len(args), len(c.pre)+1, args)
			}
			if !reflect.DeepEqual(args[:len(c.pre)], c.pre) {
				t.Errorf("words %q, want %q", args[:len(c.pre)], c.pre)
			}
			if got, want := args[len(c.pre)], "--wrap="+bootstrap(s.agentPath, s.agentDigest, s.allocDir); got != want {
				t.Errorf("--wrap word %q, want %q", got, want)
			}
		})
	}
}

// The bootstrap travels as one single-quoted word, so it holds no single quote
// (nor a newline); the login node's shell must not expand its $( ).
func TestBootstrapIsQuotable(t *testing.T) {
	b := bootstrap("/scratch/example-user/hpcburst/agent/"+testDigest+"/hb-agent", testDigest, "/scratch/example-user/hpcburst/alloc/hb-x")
	if strings.ContainsAny(b, "'\n\r\x00") {
		t.Errorf("the bootstrap holds a quote or a newline: %q", b)
	}
	if !strings.Contains(b, testDigest) {
		t.Errorf("the bootstrap does not name the digest: %q", b)
	}
}

func TestSubmitLineRefusesAWordItCannotQuote(t *testing.T) {
	for _, edit := range []func(s *submission){
		func(s *submission) { s.tag = "hb-x'y" },
		func(s *submission) { s.account = "a\nb" },
		func(s *submission) { s.allocDir = "/scratch/it's" },
	} {
		s := exampleSubmission()
		edit(&s)
		if line, err := submitLine(s); err == nil || line != "" {
			t.Errorf("submitLine(%+v) = %q, %v; want a refusal and no line", s, line, err)
		}
	}
}

// The bootstrap, run for real: it checks the staged binary's SHA-256 and then
// runs it with the Allocation's directory as its only argument.
func TestBootstrapRunsTheAgentWithTheAllocDir(t *testing.T) {
	needTools(t, "sh", "sha256sum", "cut")
	dir := t.TempDir()
	agent := filepath.Join(dir, "hb-agent")
	alloc := filepath.Join(dir, "alloc", "hb-example-0123456789ab")
	marker := filepath.Join(dir, "ran")
	body := "#!/bin/sh\nprintf '%s\\n%s\\n' \"$#\" \"$1\" > " + marker + "\nexit 7\n"
	if err := os.WriteFile(agent, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	digest := hex.EncodeToString(sum[:])

	cmd := exec.Command("/bin/sh", "-c", bootstrap(agent, digest, alloc))
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 {
		t.Fatalf("the bootstrap ended with %v (output %q); want the agent's exit status 7", err, out)
	}
	got, rerr := os.ReadFile(marker)
	if rerr != nil {
		t.Fatalf("the agent did not run: %v", rerr)
	}
	if want := "1\n" + alloc + "\n"; string(got) != want {
		t.Errorf("the agent saw %q, want one argument, the Allocation's directory: %q", got, want)
	}
}

// A binary that does not match the digest is not run: a wrong digest, a binary
// changed after staging, no binary at all.
func TestBootstrapRefusesTheWrongBinary(t *testing.T) {
	needTools(t, "sh", "sha256sum", "cut")
	for _, c := range []struct {
		name  string
		stage bool
		// digest is computed from the staged bytes unless it returns a fixed one.
		digest func(staged []byte) string
		after  string // appended to the staged file after the digest was taken
	}{
		{"digest of other bytes", true, func([]byte) string { return strings.Repeat("0", 64) }, ""},
		{"digest off by one character", true, func(b []byte) string {
			d := sha256Hex(b)
			if d[0] == 'f' {
				return "e" + d[1:]
			}
			return "f" + d[1:]
		}, ""},
		{"binary changed after staging", true, sha256Hex, "# changed\n"},
		{"no binary", false, sha256Hex, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			agent := filepath.Join(dir, "hb-agent")
			marker := filepath.Join(dir, "ran")
			staged := []byte("#!/bin/sh\ntouch " + marker + "\n")
			digest := c.digest(staged)
			if c.stage {
				if err := os.WriteFile(agent, append(append([]byte{}, staged...), c.after...), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			err := exec.Command("/bin/sh", "-c", bootstrap(agent, digest, filepath.Join(dir, "alloc"))).Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() == 0 {
				t.Errorf("the bootstrap ended with %v; want a non-zero exit", err)
			}
			if _, serr := os.Stat(marker); !os.IsNotExist(serr) {
				t.Errorf("the agent ran (marker: %v)", serr)
			}
		})
	}
	// Control: the right digest runs the same file.
	dir := t.TempDir()
	agent, marker := filepath.Join(dir, "hb-agent"), filepath.Join(dir, "ran")
	staged := []byte("#!/bin/sh\ntouch " + marker + "\n")
	if err := os.WriteFile(agent, staged, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("/bin/sh", "-c", bootstrap(agent, sha256Hex(staged), filepath.Join(dir, "alloc"))).Run(); err != nil {
		t.Fatalf("the control run failed: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("the control agent did not run: %v", err)
	}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// The whole path: submitLine, read by the shell, gives a --wrap word that runs
// the staged agent.
func TestWrapFromTheSubmitLineRuns(t *testing.T) {
	needTools(t, "sh", "sha256sum", "cut")
	dir := t.TempDir()
	agent := filepath.Join(dir, "hb-agent")
	alloc := filepath.Join(dir, "alloc")
	marker := filepath.Join(dir, "ran")
	body := "#!/bin/sh\nprintf '%s\\n' \"$1\" > " + marker + "\n"
	if err := os.WriteFile(agent, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	s := exampleSubmission()
	s.agentPath, s.allocDir, s.agentDigest = agent, alloc, hex.EncodeToString(sum[:])
	line, err := submitLine(s)
	if err != nil {
		t.Fatal(err)
	}
	args := sbatchArgv(t, line)
	wrap := strings.TrimPrefix(args[len(args)-1], "--wrap=")
	if wrap == args[len(args)-1] {
		t.Fatalf("the last word is not --wrap: %q", args[len(args)-1])
	}
	if out, err := exec.Command("/bin/sh", "-c", wrap).CombinedOutput(); err != nil {
		t.Fatalf("the wrapped bootstrap failed: %v\n%s", err, out)
	}
	if got, _ := os.ReadFile(marker); string(got) != alloc+"\n" {
		t.Errorf("the agent saw %q, want %q", got, alloc+"\n")
	}
}

// ---- Line, CheckValue ----

func TestLineQuotesWhatTheShellWouldRead(t *testing.T) {
	needTools(t, "sh")
	words := []string{"a b", "$HOME", "`id`", "x;y", "a|b", "$(echo hi)", "*", "~", "a&b", `"dq"`, `\`, "!x", "(", "a<b>c", "#c", "tab\there"}
	line, err := Line(append([]string{"printf", `%s\0`}, words...)...)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/sh", "-c", line)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=SENTINEL_HOME_EXPANDED"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	got := strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
	if !reflect.DeepEqual(got, words) {
		t.Errorf("the shell read %q, want %q", got, words)
	}
}

func TestLineLeavesPlainWordsAlone(t *testing.T) {
	line, err := Line("sbatch", "--time=60", "/scratch/example-user/x-y_z.w:v=u,t", "--mem=1500M")
	if err != nil || line != "sbatch --time=60 /scratch/example-user/x-y_z.w:v=u,t --mem=1500M" {
		t.Errorf("Line = %q, %v", line, err)
	}
	if line, err := Line(); err != nil || line != "" {
		t.Errorf("Line() = %q, %v", line, err)
	}
}

func TestLineRefusesAQuoteANewlineOrNothing(t *testing.T) {
	for _, w := range []string{"it's", "'", "a\nb", "a\rb", "a\x00b", "pass'word"} {
		line, err := Line("ok", w)
		if err == nil || line != "" {
			t.Errorf("Line(ok, %q) = %q, %v; want a refusal and no line", w, line, err)
		}
		if err != nil && strings.Contains(err.Error(), "pass'word") {
			t.Errorf("the error repeats the word: %v", err)
		}
	}
	if line, err := Line("ok", ""); err == nil || line != "" {
		t.Errorf("Line(ok, \"\") = %q, %v; want a refusal: an empty word would vanish", line, err)
	}
}

func TestCheckValue(t *testing.T) {
	for _, v := range []string{
		"hb-example-0123456789ab", "plgrid", "/scratch/example-user/x_y.z", "a:b=c,d/e-f.g", "0", "A.B",
	} {
		if err := CheckValue("value", v); err != nil {
			t.Errorf("CheckValue(%q) = %v, want none", v, err)
		}
	}
	for _, v := range []string{
		"", " ", "a b", "a;b", "a$b", "$(id)", "a`b`", "a'b", `a"b`, "a\nb", "a\tb", "a|b", "a&b", "a(b)", "a*b",
		"a?b", "a<b", "a>b", `a\b`, "a!b", "~", "a@b", "a#b", "a%b", "a+b", "a{b}", "é", "a\x00b", "ab\n", "\nab",
	} {
		if err := CheckValue("value", v); err == nil {
			t.Errorf("CheckValue(%q) = nil, want a refusal", v)
		}
	}
	err := CheckValue("account", "sec;ret")
	if err == nil || strings.Contains(err.Error(), "sec;ret") || !strings.Contains(err.Error(), "account") {
		t.Errorf("CheckValue(account, sec;ret) = %v; want a refusal naming the value's role and not the value", err)
	}
}

func TestCheckJobID(t *testing.T) {
	for _, id := range []string{"4242", "0", "18446744073709551615"} {
		if err := checkJobID(id); err != nil {
			t.Errorf("checkJobID(%q) = %v", id, err)
		}
	}
	for _, id := range []string{"", "42a", "4242;ls", "-1", " 4242", "4242 ", "4242\n", "123456789012345678901", "4.2"} {
		if err := checkJobID(id); !errors.Is(err, provider.ErrInvalidRequest) {
			t.Errorf("checkJobID(%q) = %v, want ErrInvalidRequest", id, err)
		}
	}
}

// ---- the read, cancel and release lines (HBEP-4, The Slurm dialect) ----

func TestReadLines(t *testing.T) {
	for _, c := range []struct{ name, got, want string }{
		{"the queue listing", queueLine(), "squeue --me --json"},
		{"one job from the queue", queueJobLine("4242"), "squeue --me --json --jobs=4242"},
		{"accounting by tag", accountingTagLine("hb-example-0123456789ab", 7200), "sacct --json -X -D --name=hb-example-0123456789ab -S now-7200 -E now"},
		{"accounting by job", accountingJobLine("4242"), "sacct --json -X -D -j 4242"},
		{"scancel", cancelLine("4242"), "scancel 4242"},
		{"scontrol release", releaseLine("4242"), "scontrol release 4242"},
		{"printenv", printenvLine("HOME"), "printenv HOME"},
	} {
		if c.got != c.want {
			t.Errorf("%s: %q, want %q", c.name, c.got, c.want)
		}
	}
}

// sacct without -S starts at midnight in the login session's zone (S4): the
// start is always given, and always relative.
func TestAccountingTagLineAlwaysHasARelativeStart(t *testing.T) {
	for _, secs := range []int64{-5, 0, 1, 59, 60, 3600, 90000} {
		l := accountingTagLine("hb-x-0123456789ab", secs)
		if !strings.Contains(l, " -S now-") || !strings.HasSuffix(l, " -E now") || strings.Contains(l, "now--") || strings.Contains(l, "now-0 ") {
			t.Errorf("accountingTagLine(%d) = %q", secs, l)
		}
	}
}

// ---- reading sbatch's answer ----

func TestClassifySubmit(t *testing.T) {
	const prefix = "sbatch: error: Batch job submission failed: "
	notStarted := fmt.Errorf("dial: %w", ErrNotStarted)
	for _, c := range []struct {
		name    string
		res     Result
		runErr  error
		id      string
		outcome provider.SubmitOutcome
		errIs   error // wanted in the error's chain; nil: none required
	}{
		// Accepted: --parsable prints <jobid>[;<cluster>].
		{"accepted", Result{Stdout: []byte("1234\n")}, nil, "1234", provider.Accepted, nil},
		{"accepted with a cluster", Result{Stdout: []byte("1234;example-cluster\n")}, nil, "1234", provider.Accepted, nil},
		{"accepted, no newline", Result{Stdout: []byte("1234")}, nil, "1234", provider.Accepted, nil},
		{"accepted with a warning on stderr", Result{Stdout: []byte("1234\n"), Stderr: []byte("sbatch: warning: x\n")}, nil, "1234", provider.Accepted, nil},

		// Rejected: a definite refusal, or a command that never started.
		{"invalid account", Result{ExitCode: 1, Stderr: []byte(prefix + "Invalid account or account/partition combination specified\n")}, nil, "", provider.Rejected, nil},
		{"invalid account, no sbatch prefix", Result{ExitCode: 1, Stderr: []byte("Batch job submission failed: Invalid account or account/partition combination specified")}, nil, "", provider.Rejected, nil},
		{"invalid partition", Result{ExitCode: 1, Stderr: []byte(prefix + "Invalid partition name specified\n")}, nil, "", provider.Rejected, nil},
		{"node configuration", Result{ExitCode: 1, Stderr: []byte(prefix + "Requested node configuration is not available\n")}, nil, "", provider.Rejected, nil},
		{"policy limit", Result{ExitCode: 1, Stderr: []byte(prefix + "Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n")}, nil, "", provider.Rejected, nil},
		{"sbatch not found", Result{ExitCode: 127, Stderr: []byte("sh: 1: sbatch: not found\n")}, nil, "", provider.Rejected, nil},
		{"sbatch not executable", Result{ExitCode: 126, Stderr: []byte("sh: 1: sbatch: Permission denied\n")}, nil, "", provider.Rejected, nil},
		{"never started", Result{}, notStarted, "", provider.Rejected, ErrNotStarted},

		// Unknown: Slurm may have registered the job.
		{"timeout", Result{}, fmt.Errorf("run: %w", ErrTimeout), "", provider.Unknown, ErrTimeout},
		{"transport", Result{}, fmt.Errorf("run: %w", ErrTransport), "", provider.Unknown, ErrTransport},
		{"context deadline", Result{}, context.DeadlineExceeded, "", provider.Unknown, context.DeadlineExceeded},
		{"any other runner error", Result{}, errors.New("boom"), "", provider.Unknown, nil},
		{"socket timeout after the prefix", Result{ExitCode: 1, Stderr: []byte(prefix + "Socket timed out on send/recv operation\n")}, nil, "", provider.Unknown, nil},
		{"controller unreachable after the prefix", Result{ExitCode: 1, Stderr: []byte(prefix + "Unable to contact slurm controller (connect failure)\n")}, nil, "", provider.Unknown, nil},
		{"zero bytes after the prefix", Result{ExitCode: 1, Stderr: []byte(prefix + "Zero Bytes were transmitted or received\n")}, nil, "", provider.Unknown, nil},
		{"standby after the prefix", Result{ExitCode: 1, Stderr: []byte(prefix + "Slurm backup controller in standby mode\n")}, nil, "", provider.Unknown, nil},
		{"connection refused after the prefix", Result{ExitCode: 1, Stderr: []byte(prefix + "Connection refused\n")}, nil, "", provider.Unknown, nil},
		{"exit 0, text instead of an ID", Result{Stdout: []byte("Submitted batch job 1234\n")}, nil, "", provider.Unknown, nil},
		{"exit 0, nothing", Result{}, nil, "", provider.Unknown, nil},
		{"exit 0, ID with junk", Result{Stdout: []byte("1234abc\n")}, nil, "", provider.Unknown, nil},
		{"exit 0, negative ID", Result{Stdout: []byte("-5\n")}, nil, "", provider.Unknown, nil},
		{"exit 0, ID on the second line", Result{Stdout: []byte("sbatch: note\n1234\n")}, nil, "", provider.Unknown, nil},
		{"exit 255, ssh failed", Result{ExitCode: 255, Stderr: []byte("ssh: connect to host example-login-node port 22: Connection refused\n")}, nil, "", provider.Unknown, nil},
		{"exit 1 without the prefix", Result{ExitCode: 1, Stderr: []byte("sbatch: error: something else\n")}, nil, "", provider.Unknown, nil},
		{"exit 1, nothing on stderr", Result{ExitCode: 1}, nil, "", provider.Unknown, nil},
		{"killed by a signal", Result{ExitCode: 137}, nil, "", provider.Unknown, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			id, outcome, err := classifySubmit(c.res, c.runErr)
			if id != c.id || outcome != c.outcome {
				t.Errorf("classifySubmit = %q, %s; want %q, %s (error: %v)", id, outcome, c.id, c.outcome, err)
			}
			if (err == nil) != (c.outcome == provider.Accepted) {
				t.Errorf("error %v: it is nil exactly when the outcome is Accepted", err)
			}
			if c.errIs != nil && !errors.Is(err, c.errIs) {
				t.Errorf("error %v does not wrap %v", err, c.errIs)
			}
		})
	}
}

// Error texts never carry an absolute path (or the host) that stderr held.
func TestClassifySubmitErrorsHoldNoPath(t *testing.T) {
	const prefix = "sbatch: error: Batch job submission failed: "
	for _, c := range []struct {
		name string
		res  Result
	}{
		{"rejected, path in the reason", Result{ExitCode: 1, Stderr: []byte(prefix + "Unable to open file /scratch/example-user/hpcburst/alloc/hb-x/job.err\n")}},
		{"transient, path in the reason", Result{ExitCode: 1, Stderr: []byte(prefix + "Socket timed out on send/recv operation, file /scratch/example-user/x\n")}},
		{"no prefix, path in the error line", Result{ExitCode: 1, Stderr: []byte("sbatch: error: cannot read /home/example-user/.slurm/conf: Permission denied\n")}},
		{"path on an earlier line", Result{ExitCode: 1, Stderr: []byte("sbatch: error: reading /home/example-user/.slurm/conf\n" + prefix + "Invalid partition name specified\n")}},
		{"command not found", Result{ExitCode: 127, Stderr: []byte("sh: 1: /opt/example/bin/sbatch: not found\n")}},
		{"garbage on stdout", Result{Stdout: []byte("sbatch: /scratch/example-user/secret\n")}},
		{"ssh failure with the host", Result{ExitCode: 255, Stderr: []byte("ssh: connect to host example-login-node port 22: Connection refused\n")}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := classifySubmit(c.res, nil)
			if err == nil {
				t.Fatal("no error")
			}
			for _, leak := range []string{"/scratch", "/home", "/opt", "example-user", "example-login-node"} {
				if strings.Contains(err.Error(), leak) {
					t.Errorf("the error %q holds %q", err, leak)
				}
			}
			if len(err.Error()) > 400 {
				t.Errorf("the error is %d characters long", len(err.Error()))
			}
		})
	}
	long := Result{ExitCode: 1, Stderr: []byte("sbatch: error: Batch job submission failed: " + strings.Repeat("reason ", 400))}
	if _, _, err := classifySubmit(long, nil); err == nil || len(err.Error()) > 400 {
		t.Errorf("a 2800-character reason gives an error of %d characters", len(err.Error()))
	}
}

// A rejection keeps Slurm's own words, so an operator can read why. Only a real
// absolute path is removed.
func TestRejectedReasonKeepsSlurmsText(t *testing.T) {
	for _, reason := range []string{
		"Invalid account or account/partition combination specified",
		"Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)",
	} {
		_, outcome, err := classifySubmit(Result{ExitCode: 1, Stderr: []byte("sbatch: error: Batch job submission failed: " + reason + "\n")}, nil)
		if outcome != provider.Rejected || err == nil || !strings.Contains(err.Error(), reason) {
			t.Errorf("reason %q: outcome %s, error %v; want Rejected and the reason kept", reason, outcome, err)
		}
	}
	_, _, err := classifySubmit(Result{ExitCode: 1, Stderr: []byte("sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n")}, nil)
	if err == nil || !strings.Contains(err.Error(), "send/recv") {
		t.Errorf("the transient reason lost its text: %v", err)
	}
}

func TestRedactPathsRemovesRealPaths(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"open /scratch/example-user/x/job.err failed", "open <path> failed"},
		{"cannot read /home/example-user/.slurm/conf: Permission denied", "cannot read <path>: Permission denied"},
		{"a /one and /two/three", "a <path> and <path>"},
		{"no path here", "no path here"},
	} {
		if got := redactPaths(c.in); got != c.want {
			t.Errorf("redactPaths(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	long := redactPaths(strings.Repeat("x", 1000))
	if len(long) > 303 || !strings.HasSuffix(long, "...") {
		t.Errorf("a 1000-character text became %d characters: %.20s...", len(long), long)
	}
}

func TestSlurmMessage(t *testing.T) {
	for _, c := range []struct{ name, stderr, want string }{
		{"the error line", "sbatch: warning: x\nsbatch: error: Unable to contact slurm controller (connect failure)\n", "sbatch: error: Unable to contact slurm controller (connect failure)"},
		{"the first error line", "squeue: error: first\nsqueue: error: second\n", "squeue: error: first"},
		{"a capital Error", "Error: nope\n", "Error: nope"},
		{"a path removed", "sbatch: error: cannot open /scratch/example-user/x\n", "sbatch: error: cannot open <path>"},
		{"no error text", "ssh: connect to host example-login-node port 22: Connection refused\n", "no error text"},
		{"empty", "", "no error text"},
	} {
		if got := slurmMessage(c.stderr); got != c.want {
			t.Errorf("%s: slurmMessage = %q, want %q", c.name, got, c.want)
		}
	}
}

// ---- requests refused before any call ----

type requestCase struct {
	name string
	edit func(r *provider.SubmitRequest, p *provider.Profile, s *provider.Settings)
}

func exampleRequest() (provider.SubmitRequest, provider.Profile, provider.Settings) {
	maxTime := v1alpha1.Duration("48h")
	perNode := int32(4)
	profile := provider.Profile{Name: "example", Partition: v1alpha1.PartitionProfile{
		Name:             "example-gpu",
		AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "GH200", CPUArch: "aarch64"},
		MaxTime:          &maxTime,
		GPUsPerNode:      &perNode,
	}}
	settings := provider.Settings{Provider: "example", Account: "example-account", Partition: "example-gpu"}
	req := provider.SubmitRequest{
		Tag: "hb-example-0123456789ab", Hold: true, Account: "example-account",
		Params: v1alpha1.BatchParameters{
			Partition: "example-gpu", TimeLimit: "48h", GPUs: 2, CPUsPerTask: 16,
			Memory: resource.MustParse("96Gi"), Nodes: 1, AgentDigest: testDigest,
		},
	}
	return req, profile, settings
}

func setLimit(l string) func(*provider.SubmitRequest, *provider.Profile, *provider.Settings) {
	return func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) {
		r.Params.TimeLimit = v1alpha1.Duration(l)
	}
}

func setDigest(d string) func(*provider.SubmitRequest, *provider.Profile, *provider.Settings) {
	return func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Params.AgentDigest = d }
}

func setTag(tag string) func(*provider.SubmitRequest, *provider.Profile, *provider.Settings) {
	return func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Tag = tag }
}

func setAccount(a string) func(*provider.SubmitRequest, *provider.Profile, *provider.Settings) {
	return func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Account = a }
}

// HBEP-4 (Submission, Refused before any call) and the safe pattern
// (Transport), plus the single-node alpha rule.
var requestRefusals = []requestCase{
	{"partition is not the Provider's", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) {
		r.Params.Partition = "example-other"
	}},
	{"partition is the Provider's but the profile lists another", func(_ *provider.SubmitRequest, p *provider.Profile, _ *provider.Settings) {
		p.Partition.Name = "example-listed"
	}},
	{"partition is the profile's but the Provider allows another", func(_ *provider.SubmitRequest, _ *provider.Profile, s *provider.Settings) {
		s.Partition = "example-allowed"
	}},
	{"unsafe partition", func(r *provider.SubmitRequest, p *provider.Profile, s *provider.Settings) {
		r.Params.Partition, p.Partition.Name, s.Partition = "example-gpu;ls", "example-gpu;ls", "example-gpu;ls"
	}},
	{"no GPU on a GPU partition", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Params.GPUs = 0 }},
	{"negative GPUs", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Params.GPUs = -1 }},
	{"more GPUs than gpusPerNode", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Params.GPUs = 5 }},
	{"time limit above maxTime", setLimit("49h")},
	{"time limit one minute above maxTime", setLimit("48h1m")},
	{"time limit not whole minutes", setLimit("90s")},
	{"time limit minutes and seconds", setLimit("1h30m30s")},
	{"time limit below a minute", setLimit("30s")},
	{"time limit zero", setLimit("0s")},
	{"time limit negative", setLimit("-5m")},
	{"time limit not a duration", setLimit("soon")},
	{"time limit empty", setLimit("")},
	{"tag with a semicolon", setTag("hb-x;rm -rf /")},
	{"tag with a space", setTag("hb x")},
	{"tag with a command substitution", setTag("hb-$(id)")},
	{"tag with a newline", setTag("hb-x\nhb-y")},
	{"empty tag", setTag("")},
	{"tag longer than JobTagLength", setTag(strings.Repeat("a", v1alpha1.JobTagLength+1))},
	{"account with a semicolon", setAccount("acct;x")},
	{"account with a command substitution", setAccount("acct$(id)")},
	{"account with a space", setAccount("a b")},
	{"digest empty", setDigest("")},
	{"digest of 63 characters", setDigest(testDigest[:63])},
	{"digest of 65 characters", setDigest(testDigest + "0")},
	{"digest in upper case", setDigest(strings.ToUpper(testDigest))},
	{"digest not hex", setDigest(strings.Repeat("g", 64))},
	{"digest with a sha256: prefix", setDigest("sha256:" + testDigest)},
	{"digest with a path", setDigest("../../" + testDigest[:58])},
	{"no CPUs", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Params.CPUsPerTask = 0 }},
	{"no memory", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) {
		r.Params.Memory = resource.MustParse("0")
	}},
	{"two nodes", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Params.Nodes = 2 }},
}

func TestCheckRequestRefusals(t *testing.T) {
	for _, c := range requestRefusals {
		t.Run(c.name, func(t *testing.T) {
			req, profile, settings := exampleRequest()
			c.edit(&req, &profile, &settings)
			_, err := checkRequest(req, profile, settings)
			if !errors.Is(err, provider.ErrInvalidRequest) {
				t.Errorf("checkRequest = %v, want a refusal wrapping ErrInvalidRequest", err)
			}
		})
	}
}

// countingRunner fails the test's expectation if the Batch runs any command.
type countingRunner struct{ calls atomic.Int32 }

func (c *countingRunner) Run(context.Context, Command) (Result, error) {
	c.calls.Add(1)
	return Result{}, errors.New("the test runner must not be called")
}

func (c *countingRunner) Generation() uint64 { return 1 }

// A refused submission is Rejected, wraps ErrInvalidRequest, and makes no
// call at all: not even the one that resolves the login node's paths.
func TestSubmitRefusesBeforeAnyCall(t *testing.T) {
	for _, c := range requestRefusals {
		t.Run(c.name, func(t *testing.T) {
			req, profile, settings := exampleRequest()
			c.edit(&req, &profile, &settings)
			runner := &countingRunner{}
			b := NewBatch(runner, profile, settings, Options{})
			id, outcome, err := b.Submit(context.Background(), req)
			if id != "" || outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) {
				t.Errorf("Submit = %q, %s, %v; want no ID, Rejected and ErrInvalidRequest", id, outcome, err)
			}
			if n := runner.calls.Load(); n != 0 {
				t.Errorf("the runner was called %d times", n)
			}
		})
	}
}

func TestCheckRequestAccepts(t *testing.T) {
	example := func() submission {
		return submission{
			tag: "hb-example-0123456789ab", partition: "example-gpu", account: "example-account", hold: true,
			minutes: 2880, gpus: 2, cpusPerTask: 16, memMiB: 98304, agentDigest: testDigest,
		}
	}
	longTag := strings.Repeat("a", v1alpha1.JobTagLength)
	for _, c := range []struct {
		name   string
		edit   func(r *provider.SubmitRequest, p *provider.Profile, s *provider.Settings)
		adjust func(s *submission)
	}{
		{"the example", func(*provider.SubmitRequest, *provider.Profile, *provider.Settings) {}, func(*submission) {}},
		{"time limit equal to maxTime", setLimit("48h"), func(*submission) {}},
		{"90 minutes", setLimit("90m"), func(s *submission) { s.minutes = 90 }},
		{"all GPUs of the node", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Params.GPUs = 4 },
			func(s *submission) { s.gpus = 4 }},
		{"no account named", setAccount(""), func(s *submission) { s.account = "" }},
		{"not held", func(r *provider.SubmitRequest, _ *provider.Profile, _ *provider.Settings) { r.Hold = false },
			func(s *submission) { s.hold = false }},
		{"a tag of exactly JobTagLength", setTag(longTag), func(s *submission) { s.tag = longTag }},
		{"a CPU partition takes no GPU and has no limits", func(r *provider.SubmitRequest, p *provider.Profile, _ *provider.Settings) {
			p.Partition.GPUModel, p.Partition.MaxTime, p.Partition.GPUsPerNode = "none", nil, nil
			r.Params.GPUs, r.Params.TimeLimit = 0, "9999h"
		}, func(s *submission) { s.gpus, s.minutes = 0, 9999*60 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			req, profile, settings := exampleRequest()
			c.edit(&req, &profile, &settings)
			got, err := checkRequest(req, profile, settings)
			if err != nil {
				t.Fatal(err)
			}
			want := example()
			c.adjust(&want)
			if got != want {
				t.Errorf("checkRequest = %+v\nwant %+v", got, want)
			}
		})
	}
}

// Memory goes to Slurm in whole MiB, rounded up: a job never gets less than it
// asked for. The expected numbers are worked out by hand: 1G is 10^9 bytes,
// 953.67 MiB; 1025Ki is 1.0009766 MiB.
func TestMemoryInWholeMiB(t *testing.T) {
	for _, c := range []struct {
		memory string
		mib    int64
	}{
		{"96Gi", 98304},
		{"1500Mi", 1500},
		{"1Mi", 1},
		{"1G", 954},
		{"1025Ki", 2},
		{"1", 1},
		{"64G", 61036}, // 64e9 / 1048576 = 61035.16
	} {
		req, profile, settings := exampleRequest()
		req.Params.Memory = resource.MustParse(c.memory)
		s, err := checkRequest(req, profile, settings)
		if err != nil {
			t.Errorf("%s: %v", c.memory, err)
			continue
		}
		if s.memMiB != c.mib {
			t.Errorf("memory %s: %d MiB, want %d", c.memory, s.memMiB, c.mib)
		}
		s.allocDir, s.agentPath = "/scratch/example-user/a", "/scratch/example-user/b"
		line, _ := submitLine(s)
		if want := fmt.Sprintf(" --mem=%dM ", c.mib); !strings.Contains(line, want) {
			t.Errorf("memory %s: the line lacks %q: %s", c.memory, want, line)
		}
	}
}

// slurmMessage's comment says raw output never goes into an error because it
// may name a host or a path. A host that Slurm's own text names must not reach
// a condition or an event either.
func TestErrorTextsHoldNoHostName(t *testing.T) {
	stderr := "sbatch: error: slurm_persist_conn_open_without_init: failed to open persistent connection to host:ctl-example-host:6819: Connection refused\n"
	if got := slurmMessage(stderr); strings.Contains(got, "ctl-example-host") {
		t.Errorf("slurmMessage = %q holds the controller's host name", got)
	}
	_, _, err := classifySubmit(Result{ExitCode: 1, Stderr: []byte(stderr)}, nil)
	if err == nil || strings.Contains(err.Error(), "ctl-example-host") {
		t.Errorf("classifySubmit error %v holds the controller's host name", err)
	}
}
