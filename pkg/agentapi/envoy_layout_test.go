package agentapi

import "testing"

// Envoy's staged path and build name (#454), written out in full, not built from the code under test.
func TestEnvoyLayout(t *testing.T) {
	const sum = "1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc"
	for got, want := range map[string]string{
		StagedEnvoyPath(sum):      "envoy/1d64506e25c1844f146162ddb2ece7010fa4e784faddb40a079021491f1f03cc/envoy",
		EnvoyBuildName("x86_64"):  "envoy-x86_64",
		EnvoyBuildName("aarch64"): "envoy-aarch64",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
