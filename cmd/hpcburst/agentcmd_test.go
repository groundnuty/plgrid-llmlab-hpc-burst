package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// These tests need no k3s. Each agent command is wired to internal/agentcli: with no kubeconfig it exits 2
// and names both ways to give one, where an unwired command would say "unknown command".
func TestAgentCommandsAreWired(t *testing.T) {
	t.Setenv("KUBECONFIG", "")
	for _, args := range [][]string{
		{"scale", "m", "--instances", "1"}, {"end", "m"}, {"release", "a"}, {"confirm", "op", "--plan", "p"},
		{"approvals"}, {"approve", "op", "--plan", "p"}, {"operation", "op"},
	} {
		code, out, errs := runInProcess(t, args...)
		if code != exitUsage || !strings.Contains(errs, "KUBECONFIG") || !strings.Contains(errs, "--kubeconfig") || strings.Contains(errs, "unknown command") {
			t.Errorf("%v: exit %d, stdout %q, stderr %q; want exit 2 naming KUBECONFIG and --kubeconfig", args, code, out, errs)
		}
	}
}

func TestAgentEnvCarriesTheVariablesAndAsksOnlyAtATerminal(t *testing.T) {
	t.Setenv("KUBECONFIG", "/a/kubeconfig")
	t.Setenv("HOME", "/h")
	t.Setenv("XDG_STATE_HOME", "/s")
	env := agentEnv(&bytes.Buffer{})
	if env.Kubeconfig != "/a/kubeconfig" || env.Home != "/h" || env.XDGStateHome != "/s" {
		t.Errorf("env %+v", env)
	}
	if env.IsTerminal() {
		t.Error("a buffer is not a terminal")
	}
}

// A command asks only when stdin AND stdout are terminals. The files are real, so that their descriptors differ,
// and the check on a descriptor is a stand-in that names which of them is a terminal.
func TestAskOnlyWhenStdinAndStdoutAreBothTerminals(t *testing.T) {
	in, err := os.CreateTemp(t.TempDir(), "stdin")
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct {
		name          string
		inTTY, outTTY bool
		stdout        any
		want          bool
	}{
		{"both are terminals", true, true, out, true},
		{"stdout is a terminal and stdin is not (a pipe feeds the answers)", false, true, out, false},
		{"stdin is a terminal and stdout is not (the output is redirected)", true, false, out, false},
		{"neither is", false, false, out, false},
		{"stdout is not a file at all", true, true, &bytes.Buffer{}, false},
	} {
		isTerm := func(fd int) bool { return (fd == int(in.Fd()) && r.inTTY) || (fd == int(out.Fd()) && r.outTTY) }
		w, _ := r.stdout.(interface{ Write([]byte) (int, error) })
		if got := bothTerminals(isTerm, in, w); got != r.want {
			t.Errorf("%s: %v, want %v", r.name, got, r.want)
		}
	}
}
