package main

import (
	"maps"
	"os"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Under --instance-router envoy the Provider's controller stages Envoy by the pinned digest of each
// cpuArch (#454); without it, the front settings carry no Envoy digest and alpha stages nothing new.
// The digests are Envoy 1.40.2's published release sums, written out.
func TestTheRouterFlagGivesTheProviderEnvoysDigests(t *testing.T) {
	state := t.TempDir()
	writeEndpointCert(t, agentapi.Host{StateDir: state, PortBase: 29300})
	base := []string{"--state-dir", state, "--port-base", "29300", "--health-addr", "127.0.0.1:29306", "--front-mode", "direct", "--front-addr", "front.example.org:29443"}
	d := deps{
		frpcDigests: func() (map[string]string, error) { return map[string]string{"x86_64": strings.Repeat("a", 64)}, nil },
		username:    func() (string, error) { return "ExampleUser", nil },
	}
	f, err := parse(base, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	fs, err := frontSettings(f, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs.EnvoyDigests) != 0 {
		t.Errorf("without --instance-router: Envoy digests %v, want none", fs.EnvoyDigests)
	}
	f, err = parse(append(append([]string{}, base...), "--instance-router", "envoy"), os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	fs, err = frontSettings(f, d)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"x86_64":  "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc",
		"aarch64": "b9d6d1e06dd5838a3506de9f4deebad3de9a2ce6cadb87cd76222ca286213baf",
	}
	if !maps.Equal(fs.EnvoyDigests, want) {
		t.Errorf("with --instance-router envoy: Envoy digests %v, want %v", fs.EnvoyDigests, want)
	}
}
