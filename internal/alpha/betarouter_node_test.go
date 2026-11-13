package alpha

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The node probes of the router row (betarouter.go): the Envoy pick off a listing and the step that reads its executable's digest, run
// here against a real process under the name envoy, as the node's would be, so that the script is the one the row sends.

func TestPickEnvoyReadsTheJobsEnvoyAndWhetherItIsBare(t *testing.T) {
	agent := NodeProc{PID: 100, PPID: 1, InJob: true, Argv: []string{"/staged/hb-agent", "run"}}
	envoyUnderAgent := NodeProc{PID: 110, PPID: 100, Argv: []string{"envoy", "-c", "/run/x/envoy.yaml", "--disable-hot-restart"}}
	apptainer := NodeProc{PID: 120, PPID: 100, Argv: []string{"apptainer", "exec", "image.sif", "envoy"}}
	envoyUnderApptainer := NodeProc{PID: 130, PPID: 120, Argv: []string{"envoy", "-c", "/x"}}
	otherUsersJob := NodeProc{PID: 200, PPID: 1, Argv: []string{"envoy", "-c", "/elsewhere"}}
	for _, c := range []struct {
		name      string
		procs     []NodeProc
		wantPIDs  []int
		contained bool
	}{
		{"bare under the agent", []NodeProc{agent, envoyUnderAgent}, []int{110}, false},
		{"none", []NodeProc{agent}, nil, false},
		{"one in a container", []NodeProc{agent, apptainer, envoyUnderApptainer}, []int{130}, true},
		{"a bare one and a contained one", []NodeProc{agent, envoyUnderAgent, apptainer, envoyUnderApptainer}, []int{110, 130}, true},
		{"one that is not of the job", []NodeProc{agent, envoyUnderAgent, otherUsersJob}, []int{110}, false},
		{"a program that only mentions envoy", []NodeProc{agent, {PID: 140, PPID: 100, Argv: []string{"/bin/sleep", "envoy"}}}, nil, false},
		{"a path to envoy is envoy", []NodeProc{agent, {PID: 150, PPID: 100, Argv: []string{"/staged/envoy/abc/envoy", "-c", "/x"}}}, []int{150}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, contained := PickEnvoy(c.procs)
			var pids []int
			for _, p := range got {
				pids = append(pids, p.PID)
			}
			if !equalInts(pids, c.wantPIDs) || contained != c.contained {
				t.Errorf("PickEnvoy = %v, contained %t; want %v, %t", pids, contained, c.wantPIDs, c.contained)
			}
		})
	}
}

func equalInts(a, b []int) bool {
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

// startNamed runs the real program at path with argv[0] name, in an environment that names the job, and returns it with a stop that
// ends it and waits for it: the child only, through its os.Process, which signals by pidfd.
func startNamed(t *testing.T, name, path, job string, args ...string) (pid int) {
	t.Helper()
	cmd := &exec.Cmd{Path: path, Args: append([]string{name}, args...), Env: append(os.Environ(), "SLURM_JOB_ID="+job)}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd.Process.Pid
}

func runScriptHere(t *testing.T, script string) []byte {
	t.Helper()
	cmd := exec.Command("bash", "-s")
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("the script failed: %v\n%s", err, out)
	}
	return out
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// A real process named envoy, in a job's environment, is listed, picked, and its executable's digest is the file's own: the step reads the
// file the kernel runs. Changed, gone and bad fields are each their own word.
func TestTheDigestStepReadsTheRunningExecutableOfARealProcess(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep program")
	}
	real, err := filepath.EvalSymlinks(sleep)
	if err != nil {
		t.Fatal(err)
	}
	const job = "4242001"
	pid := startNamed(t, "envoy", real, job, "60")
	var envoy NodeProc
	deadline := time.Now().Add(10 * time.Second)
	for {
		procs, err := ParseListing(runScriptHere(t, ListScript(job)))
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := PickEnvoy(procs); len(got) == 1 && got[0].PID == pid {
			envoy = got[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the listing never showed the process %d as the job's one Envoy", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
	script, err := EnvoyDigestScript(envoy)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := ParseEnvoyDigest(runScriptHere(t, script))
	if want := fileDigest(t, real); !ok || got != want {
		t.Fatalf("the step read digest %q (ok %t), want the executable's own %s", got, ok, want)
	}
	// The process at the PID is not the one listed: a different start time says changed, and reads nothing.
	other := envoy
	other.Start = "1"
	script, err = EnvoyDigestScript(other)
	if err != nil {
		t.Fatal(err)
	}
	if out := strings.TrimSpace(string(runScriptHere(t, script))); out != "changed" {
		t.Errorf("a process with another start time read %q, want changed", out)
	}
	// A different command line says changed too.
	other = envoy
	other.cmdline = "ZWdn"
	script, _ = EnvoyDigestScript(other)
	if out := strings.TrimSpace(string(runScriptHere(t, script))); out != "changed" {
		t.Errorf("a process with another command line read %q, want changed", out)
	}
	// Fields a script cannot carry are refused before anything is written into one.
	for _, bad := range []NodeProc{{PID: 0, Start: "1", cmdline: "YQ=="}, {PID: 1, Start: "1; echo x", cmdline: "YQ=="}, {PID: 1, Start: "1", cmdline: "a b"}, {PID: 1, Start: "", cmdline: "YQ=="}} {
		if _, err := EnvoyDigestScript(bad); err == nil {
			t.Errorf("EnvoyDigestScript accepted %+v", bad)
		}
	}
}

func TestTheDigestStepSaysGoneForAProcessThatEnded(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep program")
	}
	const job = "4242002"
	cmd := &exec.Cmd{Path: sleep, Args: []string{"envoy", "60"}, Env: append(os.Environ(), "SLURM_JOB_ID="+job)}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	var envoy NodeProc
	deadline := time.Now().Add(10 * time.Second)
	for {
		procs, err := ParseListing(runScriptHere(t, ListScript(job)))
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := PickEnvoy(procs); len(got) == 1 && got[0].PID == pid {
			envoy = got[0]
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("the listing never showed the process")
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	script, err := EnvoyDigestScript(envoy)
	if err != nil {
		t.Fatal(err)
	}
	if out := strings.TrimSpace(string(runScriptHere(t, script))); out != "gone" {
		t.Errorf("a process that ended read %q, want gone", out)
	}
}

func TestParseEnvoyDigest(t *testing.T) {
	good := strings.Repeat("ab", 32)
	for _, c := range []struct {
		in string
		ok bool
	}{
		{"digest " + good + "\n", true},
		{"gone\n", false},
		{"changed\n", false},
		{"unreadable\n", false},
		{"sum " + good + "\n", false},
		{"digest " + good[:63] + "\n", false},
		{"digest " + strings.ToUpper(good) + "\n", false},
		{"digest " + good + " extra\n", false},
		{"", false},
	} {
		got, ok := ParseEnvoyDigest([]byte(c.in))
		if ok != c.ok || (ok && got != good) {
			t.Errorf("ParseEnvoyDigest(%q) = %q, %t; want ok %t", c.in, got, ok, c.ok)
		}
	}
}
