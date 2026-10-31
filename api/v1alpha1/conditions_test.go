package v1alpha1

import "testing"

// The names HBEP-6 (Path state and the inventory) and HBEP-7 (Instance start)
// give the tunnel condition, its three reasons and the end reason of an
// Instance whose tunnel never came up, written out by hand: the front, the
// lifecycle controller and the tests of both match these strings exactly.
func TestTunnelConditionNames(t *testing.T) {
	for got, want := range map[string]string{
		ConditionTunnelConnected:  "TunnelConnected",
		ReasonProxyOnline:         "ProxyOnline",
		ReasonProxyOffline:        "ProxyOffline",
		ReasonProxyListUnreadable: "ProxyListUnreadable",
		ReasonTunnelNotConnected:  "TunnelNotConnected",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	// The end reasons of a start that timed out are two different strings, so
	// the one that names the tunnel can be told from the plain timeout.
	if ReasonTunnelNotConnected == ReasonStartTimeout {
		t.Error("TunnelNotConnected and StartTimeout must differ")
	}
}

// The read-back annotation's key, written out by hand from HBEP-4 (Read-back)
// and the other Allocation annotations' form (Group + "/name").
func TestReadBackAnnotationKey(t *testing.T) {
	if AnnotationReadBack != "hpcburst.groundnuty.github.io/read-back" {
		t.Errorf("AnnotationReadBack = %q", AnnotationReadBack)
	}
	if AnnotationReadBack == AnnotationJobID || AnnotationReadBack == AnnotationHoldLifted {
		t.Error("the read-back annotation shares a key with another")
	}
}
