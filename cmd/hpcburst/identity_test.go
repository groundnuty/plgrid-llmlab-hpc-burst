package main

// hpcburst identity add|renew, the parts that need no host: the usage errors, the refusal with no
// host running, the help, and the status text about the agents. The commands against a real k3s are
// in identity_k3s_test.go; the roles are judged by the API server's decisions in api/v1alpha1/e2e.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

func TestIdentityUsageErrors(t *testing.T) {
	dir := filepath.Join(shortTmp(t, false), "st")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"no subcommand", []string{"identity"}},
		{"an unknown subcommand", []string{"identity", "remove", "agent-a"}},
		{"no name", []string{"identity", "add", "--state-dir", dir}},
		{"no name for renew", []string{"identity", "renew", "--state-dir", dir}},
		{"two names", []string{"identity", "add", "agent-a", "agent-b", "--state-dir", dir}},
		{"upper case", []string{"identity", "add", "Agent-A", "--state-dir", dir}},
		{"an underscore", []string{"identity", "add", "agent_a", "--state-dir", dir}},
		{"a leading hyphen after the flags", []string{"identity", "add", "--state-dir", dir, "-agent"}},
		{"a dot", []string{"identity", "renew", "agent.a", "--state-dir", dir}},
		{"a path", []string{"identity", "add", "../agent-a", "--state-dir", dir}},
		{"64 characters", []string{"identity", "add", strings.Repeat("a", 64), "--state-dir", dir}},
		{"an unknown flag", []string{"identity", "add", "agent-a", "--no-such-flag", "--state-dir", dir}},
	} {
		if r := cli(t, tc.args...); r.code != exitUsage {
			t.Errorf("%s: hpcburst %s exited %d (stderr %q), want the usage exit %d", tc.name, strings.Join(tc.args, " "), r.code, r.err, exitUsage)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "identities")); err == nil {
		t.Errorf("a refused name left an identities directory behind")
	}
}

// With no host recorded, add and renew are refused as guard remove is, and write nothing.
func TestIdentityRefusesWithoutAHost(t *testing.T) {
	dir := filepath.Join(shortTmp(t, false), "st")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, verb := range []string{"add", "renew"} {
		// The name comes before the flags in one run and after them in the other.
		for _, args := range [][]string{
			{"identity", verb, "agent-a", "--state-dir", dir},
			{"identity", verb, "--state-dir", dir, "agent-a"},
		} {
			r := cli(t, args...)
			if r.code != exitFail || !strings.Contains(r.err, "no host is recorded") {
				t.Errorf("hpcburst %s: exit %d, stderr %q, want exit %d and 'no host is recorded'", strings.Join(args, " "), r.code, r.err, exitFail)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "identities")); err == nil {
		t.Errorf("a refused add or renew left an identities directory behind")
	}
}

// The usage summary and the help name identity, and the help says what the command does and what it
// never prints.
func TestIdentityIsInTheUsageAndHelp(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {"help"}} {
		if r := cli(t, args...); r.code != exitOK || !strings.Contains(r.out, "identity") {
			t.Errorf("hpcburst %v: exit %d, stdout %q, want exit 0 and a summary that lists identity", args, r.code, r.out)
		}
	}
	if r := cli(t); r.code != exitUsage || !strings.Contains(r.err, "identity") {
		t.Errorf("hpcburst with no command: exit %d, stderr %q, want the usage naming identity", r.code, r.err)
	}
	for _, args := range [][]string{{"identity", "-h"}, {"identity", "add", "-h"}, {"identity", "renew", "-h"}} {
		r := cli(t, args...)
		out := r.out + r.err
		if r.code != exitOK {
			t.Errorf("hpcburst %v exited %d, want 0", args, r.code)
		}
		for _, want := range []string{"identity add NAME", "identity renew NAME", "mode 600", "never the token", "for 30"} {
			if !strings.Contains(out, want) {
				t.Errorf("hpcburst %v: the help lacks %q:\n%s", args, want, out)
			}
		}
	}
}

// The status lines about the agents are literals: roles present or different, and each identity valid
// or not, with its expiry. Nothing about them is printed when the API server was not asked.
func TestPrintStatusAgents(t *testing.T) {
	text := func(r *launcher.StatusReport) string {
		var b bytes.Buffer
		printStatus(&b, r)
		return b.String()
	}
	exp := time.Date(2026, 12, 15, 10, 0, 0, 0, time.UTC)
	out := text(&launcher.StatusReport{Health: launcher.Healthy, AgentRBAC: &launcher.AgentRBACState{Present: true}, Identities: []launcher.IdentityState{
		{Name: "agent-a", Kubeconfig: "/state/identities/agent-a.kubeconfig", Expires: &exp, Valid: true},
		{Name: "agent-b", Kubeconfig: "/state/identities/agent-b.kubeconfig", Expires: &exp, Detail: "the API server rejected the token (401)"},
		{Name: "agent-c", Detail: "no kubeconfig file"},
	}})
	for _, want := range []string{
		"agent roles: present (",
		"agent identity agent-a: valid, expires 2026-12-15T10:00:00Z (/state/identities/agent-a.kubeconfig)\n",
		"agent identity agent-b: NOT VALID, expires 2026-12-15T10:00:00Z (the API server rejected the token (401))\n",
		"agent identity agent-c: NOT VALID, expiry unknown (no kubeconfig file)\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status lacks %q:\n%s", want, out)
		}
	}
	out = text(&launcher.StatusReport{Health: launcher.Healthy, AgentRBAC: &launcher.AgentRBACState{Detail: "the ClusterRole hpcburst-agent has rules that differ from the manifest's"}})
	for _, want := range []string{"agent roles: DIFFER FROM THE MANIFEST (the ClusterRole hpcburst-agent has rules that differ", "the next hpcburst up resets them", "agent identities: none"} {
		if !strings.Contains(out, want) {
			t.Errorf("status of different roles lacks %q:\n%s", want, out)
		}
	}
	if out := text(&launcher.StatusReport{Health: launcher.Healthy}); strings.Contains(out, "agent ") {
		t.Errorf("a report that did not ask the API server prints about the agents:\n%s", out)
	}
}
