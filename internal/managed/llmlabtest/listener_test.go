package llmlabtest

import (
	"net"
	"net/http"
	"testing"
)

// A Server given a listener serves on it, at its address, and takes no port of
// the range (issue #219: a test holds its port until the Server starts).
func TestStartServesOnTheListenerItIsGiven(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := Start(t, Options{Listener: ln, Keys: []string{testKey}, Models: []Model{{ID: "example-9b"}}})
	if want := "http://" + ln.Addr().String(); s.URL() != want {
		t.Fatalf("URL = %q, want %q", s.URL(), want)
	}
	if resp, _ := do(t, s, http.MethodGet, "/v1/models", testKey, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("listing = %d, want 200", resp.StatusCode)
	}
}
