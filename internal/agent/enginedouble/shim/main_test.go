package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func digestOf(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// staging makes a staging root holding the agent file (the harness's place) and a stand-in shim file at the Provider's
// place, and returns the root, the shim's path and the agent's digest.
func staging(t *testing.T, agentBody, shimBody []byte) (root, shim, agentDigest string) {
	t.Helper()
	root = t.TempDir()
	s := sha256.Sum256(agentBody)
	agentDigest = hex.EncodeToString(s[:])
	if err := os.MkdirAll(filepath.Join(root, TestAgentDir, agentDigest), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, TestAgentDir, agentDigest, "hb-agent"), agentBody, 0o700); err != nil {
		t.Fatal(err)
	}
	ss := sha256.Sum256(shimBody)
	shimDir := filepath.Join(root, "agent", hex.EncodeToString(ss[:]))
	if err := os.MkdirAll(shimDir, 0o700); err != nil {
		t.Fatal(err)
	}
	shim = filepath.Join(shimDir, "hb-agent")
	if err := os.WriteFile(shim, shimBody, 0o700); err != nil {
		t.Fatal(err)
	}
	return root, shim, agentDigest
}

type execCall struct {
	path      string
	argv, env []string
	called    bool
}

func recorder(c *execCall) execFunc {
	return func(path string, argv, env []string) error {
		*c = execCall{path: path, argv: append([]string(nil), argv...), env: append([]string(nil), env...), called: true}
		return errors.New("the test's exec returns")
	}
}

func envValue(env []string, key string) (string, bool) {
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, key+"="); ok {
			return v, true
		}
	}
	return "", false
}

// The agent's run: the shipped agent is exec'd with the argv unchanged and the environment unchanged but for
// HPCBURST_APPTAINER, which names a symlink called apptainer to the shim itself.
func TestTheShimExecsTheShippedAgentWithTheDoubleAsApptainer(t *testing.T) {
	root, shim, want := staging(t, []byte("the shipped agent"), []byte("the shim"))
	var c execCall
	argv := []string{shim, filepath.Join(root, "alloc", "t-1")}
	env := []string{"A=1", EnvApptainer + "=/elsewhere/apptainer", "SLURM_JOB_ID=1"}
	_ = run(want, shim, argv, env, recorder(&c))
	if !c.called || c.path != filepath.Join(root, TestAgentDir, want, "hb-agent") {
		t.Fatalf("exec %+v, want the staged shipped agent", c)
	}
	if strings.Join(c.argv, " ") != strings.Join(argv, " ") {
		t.Errorf("argv %q, want %q unchanged", c.argv, argv)
	}
	link, ok := envValue(c.env, EnvApptainer)
	if !ok || filepath.Base(link) != "apptainer" {
		t.Fatalf("%s = %q, want a path whose name is apptainer", EnvApptainer, link)
	}
	if target, err := os.Readlink(link); err != nil || target != shim {
		t.Errorf("%s -> %q (%v), want the shim %s", link, target, err, shim)
	}
	if v, _ := envValue(c.env, "A"); v != "1" || len(c.env) != len(env) {
		t.Errorf("env %q: want the shim's own, with only %s replaced", c.env, EnvApptainer)
	}
}

// A relay and -version run no engine: the agent is exec'd with the shim's environment as it is, and no link is made.
func TestARelayIsExecdAsItIs(t *testing.T) {
	for _, argv1 := range []string{"relay", "-version"} {
		root, shim, want := staging(t, []byte("the shipped agent"), []byte("the shim"))
		var c execCall
		argv := []string{shim, argv1, "control", "--dir", filepath.Join(root, "alloc", "t-1")}
		env := []string{"A=1"}
		_ = run(want, shim, argv, env, recorder(&c))
		if !c.called || strings.Join(c.argv, " ") != strings.Join(argv, " ") || strings.Join(c.env, " ") != "A=1" {
			t.Errorf("%s: exec %+v, want the agent with argv and env unchanged", argv1, c)
		}
		if ents, _ := filepath.Glob(filepath.Join(root, TestAgentDir, want, "shim-*")); len(ents) != 0 {
			t.Errorf("%s: an apptainer link was made: %v", argv1, ents)
		}
	}
}

// The refusals: nothing is exec'd.
func TestTheShimRefusesWithoutExecuting(t *testing.T) {
	root, shim, want := staging(t, []byte("the shipped agent"), []byte("the shim"))
	other := strings.Repeat("ab", 32)
	for name, c := range map[string]struct {
		want, exe string
		edit      func()
		says      string
	}{
		"unbaked":               {"", shim, nil, "built without the shipped agent's digest"},
		"a malformed digest":    {"abc", shim, nil, "built without the shipped agent's digest"},
		"not the staged layout": {want, filepath.Join(root, "hb-agent"), nil, "not run from the Provider's staging layout"},
		"no staged agent":       {other, shim, nil, "the shipped agent the harness stages, digest " + other},
		"another agent staged": {want, shim, func() {
			if err := os.WriteFile(filepath.Join(root, TestAgentDir, want, "hb-agent"), []byte("not the shipped agent"), 0o700); err != nil {
				t.Fatal(err)
			}
		}, "not " + want},
	} {
		if c.edit != nil {
			c.edit()
		}
		var e execCall
		err := run(c.want, c.exe, []string{c.exe, "/alloc"}, nil, recorder(&e))
		if err == nil || e.called || !strings.Contains(err.Error(), c.says) {
			t.Errorf("%s: run = %v, exec'd %t; want a refusal saying %q and no exec", name, err, e.called, c.says)
		}
	}
}

// The built shim, end to end: run from the Provider's layout it execs a stub agent, writing nothing to standard output
// itself; run as apptainer it is the double's Apptainer; built without the digest it refuses with exit 1.
func TestTheBuiltShim(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the shim runs on Linux nodes")
	}
	stub := []byte("#!/bin/sh\necho \"agent argv: $*\"\necho \"agent apptainer: $" + EnvApptainer + "\"\n")
	s := sha256.Sum256(stub)
	want := hex.EncodeToString(s[:])
	tmp := t.TempDir()
	build := func(out string, ldflags string) {
		t.Helper()
		args := []string{"build", "-trimpath", "-o", out}
		if ldflags != "" {
			args = append(args, "-ldflags", ldflags)
		}
		cmd := exec.Command("go", append(args, ".")...)
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOTOOLCHAIN=local", "GOPROXY=off")
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build: %v\n%s", err, b)
		}
	}
	built := filepath.Join(tmp, "shim")
	build(built, "-X main.wantAgentDigest="+want)
	shimBody, err := os.ReadFile(built)
	if err != nil {
		t.Fatal(err)
	}
	root, shim, _ := staging(t, stub, shimBody)

	cmd := exec.Command(shim, "/the/allocation/dir")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("the shim: %v\n%s", err, stderr.String())
	}
	link := filepath.Join(root, TestAgentDir, want, "shim-"+digestOf(t, shim)[:12], "apptainer")
	if wantOut := "agent argv: /the/allocation/dir\nagent apptainer: " + link + "\n"; stdout.String() != wantOut {
		t.Errorf("standard output %q, want only the agent's %q: the shim writes nothing", stdout.String(), wantOut)
	}

	// Run as apptainer, it is the double's Apptainer: with no command, its own fatal line and exit 255.
	cmd = exec.Command(link)
	stderr.Reset()
	cmd.Stderr = &stderr
	err = cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 255 || !strings.Contains(stderr.String(), "FATAL: no command") {
		t.Errorf("the shim as apptainer: %v, %q; want the double's Apptainer's exit 255", err, stderr.String())
	}

	// Run as apptainer to pull or build, it refuses before the double runs: exit 1, its own line, and no image written (the science agent's
	// rulings on #579). The double's own Apptainer would write one.
	for _, verb := range [][]string{{"pull", filepath.Join(tmp, "out.sif"), "docker://example.test/vllm@sha256:" + want}, {"build", "--sandbox", filepath.Join(tmp, "out.sandbox"), "docker://example.test/vllm@sha256:" + want}} {
		cmd = exec.Command(link, verb...)
		stdout.Reset()
		stderr.Reset()
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err = cmd.Run()
		if !errors.As(err, &ee) || ee.ExitCode() != 1 || !strings.Contains(stderr.String(), "refused: apptainer "+verb[0]+": the double stages no image") || stdout.Len() != 0 {
			t.Errorf("the shim as apptainer %s: %v, stdout %q, stderr %q; want exit 1 and the refusal", verb[0], err, stdout.String(), stderr.String())
		}
	}
	for _, out := range []string{"out.sif", "out.sandbox"} {
		if _, err := os.Lstat(filepath.Join(tmp, out)); !os.IsNotExist(err) {
			t.Errorf("a refused staging wrote %s (%v)", out, err)
		}
	}

	// Built without the digest it refuses, with exit 1 and nothing on standard output.
	plain := filepath.Join(tmp, "plain")
	build(plain, "")
	plainBody, err := os.ReadFile(plain)
	if err != nil {
		t.Fatal(err)
	}
	_, plainShim, _ := staging(t, stub, plainBody)
	cmd = exec.Command(plainShim, "/the/allocation/dir")
	stdout.Reset()
	stderr.Reset()
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	if !errors.As(err, &ee) || ee.ExitCode() != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "built without the shipped agent's digest") {
		t.Errorf("an unbaked shim: %v, stdout %q, stderr %q; want exit 1, a refusal and no output", err, stdout.String(), stderr.String())
	}
}

func TestRefusesStaging(t *testing.T) {
	for _, c := range []struct {
		argv []string
		want bool
	}{
		{[]string{"/x/apptainer", "pull", "out", "docker://i"}, true},
		{[]string{"/x/apptainer", "build", "--sandbox", "out", "docker://i"}, true},
		{[]string{"/x/apptainer", "exec", "image", "vllm"}, false},
		{[]string{"/x/apptainer"}, false},
		{[]string{"/x/hb-agent", "pull"}, false},
	} {
		if got := refusesStaging(c.argv); got != c.want {
			t.Errorf("refusesStaging(%q) = %t, want %t", c.argv, got, c.want)
		}
	}
}
