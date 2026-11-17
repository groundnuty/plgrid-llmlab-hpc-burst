package main

import (
	"bytes"
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
