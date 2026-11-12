package main

// The router's opt-in (beta; HBEP-5, Inside an Instance; #429): --instance-router unset is alpha,
// no router; envoy is the one router it takes, and any other value is refused rather than read as
// none.

import (
	"io"
	"strings"
	"testing"
)

func TestTheInstanceRouterIsOptIn(t *testing.T) {
	f, err := parse(keeperArgs(), io.Discard)
	if err != nil || f.instanceRouter != "" {
		t.Fatalf("without the flag: %q, %v; want alpha's empty router", f.instanceRouter, err)
	}
	f, err = parse(keeperArgs("--instance-router", "envoy"), io.Discard)
	if err != nil || f.instanceRouter != "envoy" {
		t.Fatalf("with --instance-router envoy: %q, %v", f.instanceRouter, err)
	}
	for _, v := range []string{"vllm-router", "Envoy", "envoy ", "none"} {
		if _, err := parse(keeperArgs("--instance-router", v), io.Discard); err == nil || !strings.Contains(err.Error(), "--instance-router") {
			t.Errorf("--instance-router %q: %v; want a refusal naming the flag", v, err)
		}
	}
}
