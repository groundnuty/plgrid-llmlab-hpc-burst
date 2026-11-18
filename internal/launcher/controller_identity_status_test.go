package launcher

// status's expiry line and its 30-day warning for the controllers' identities (HBEP-10, Audit; #660, PR A),
// with an injected clock and no sleep: the check takes the time it is given, never the real one. The
// kubeconfigs are the ones the issue helper writes (identityKubeconfigBytes), holding a token whose exp claim
// the test chooses; the API server's answer to the SelfSubjectReview is a fake one.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
)

// tokenExpiring is a JWT-shaped token whose payload carries exp; the check reads the claim and never
// verifies the signature, as status does.
func tokenExpiring(exp time.Time) string {
	payload, _ := json.Marshal(map[string]any{"exp": exp.Unix(), "sub": "system:serviceaccount:hpcburst-system:x"})
	return "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".c2ln"
}

// writeControllerKubeconfigsExpiring writes the three controller kubeconfigs as up writes them, on ports'
// loopback server, each with a token expiring at exps[name]; a name that is not in exps gets no file.
func writeControllerKubeconfigsExpiring(t *testing.T, l layout, ports Ports, exps map[string]time.Time) {
	t.Helper()
	dir := l.systemIdentitiesDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	admin := &rest.Config{Host: fmt.Sprintf("https://127.0.0.1:%d", ports.HTTPS), TLSClientConfig: rest.TLSClientConfig{Insecure: true}}
	for name, exp := range exps {
		b, err := identityKubeconfigBytes(admin, name, tokenExpiring(exp))
		if err != nil {
			t.Fatal(err)
		}
		if err := writeFileAtomic(l.controllerKubeconfig(name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStatusControllerIdentityExpiryAndTheThirtyDayWarning(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	ports, err := PortsFromBase(28520)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	for _, c := range []struct {
		name         string
		left         time.Duration
		wantExpiring bool
		wantWarning  string // a phrase the warning holds; "" for none
	}{
		{"a year left", 365 * day, false, ""},
		{"31 days left", 31 * day, false, ""},
		{"30 days and a second", 30*day + time.Second, false, ""},
		{"exactly 30 days", 30 * day, true, "30 days left, within the 30 days"},
		{"a day left", day, true, "1 day left, within the 30 days"},
		{"an hour left", time.Hour, true, "1 hour left, within the 30 days"},
		{"just expired", -time.Second, true, "expired at 2026-11-17T07:59:59Z"},
		{"long expired", -400 * day, true, "expired at"},
	} {
		t.Run(c.name, func(t *testing.T) {
			exps := map[string]time.Time{}
			for _, n := range ControllerIdentityNames() {
				exps[n] = now.Add(c.left)
			}
			writeControllerKubeconfigsExpiring(t, l, ports, exps)
			ask := func(ctx context.Context, kubeconfig string, p Ports) (string, error) {
				// The API server's answer names the ServiceAccount of the file asked through.
				name := strings.TrimSuffix(filepath.Base(kubeconfig), ".kubeconfig")
				return "system:serviceaccount:hpcburst-system:" + name, nil
			}
			got, err := checkControllerIdentities(context.Background(), l, ports, now, ask)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 3 {
				t.Fatalf("%d identities: %+v", len(got), got)
			}
			for i, s := range got {
				if s.Name != ControllerIdentityNames()[i] || !s.Valid || s.Detail != "" || s.Expires == nil || !s.Expires.Equal(now.Add(c.left)) {
					t.Errorf("%s: %+v, want valid and expiring at %s", s.Name, s, now.Add(c.left))
				}
				if s.Expiring != c.wantExpiring || (c.wantWarning == "") != (s.Warning == "") || !strings.Contains(s.Warning, c.wantWarning) {
					t.Errorf("%s with %s left: expiring %v, warning %q; want expiring %v and a warning holding %q", s.Name, c.left, s.Expiring, s.Warning, c.wantExpiring, c.wantWarning)
				}
				// The text of the line: the expiry always, the WARNING line only when expiring, and no token.
				line := ControllerIdentityLine(s)
				if !strings.Contains(line, "valid, expires "+now.Add(c.left).Format(time.RFC3339)) {
					t.Errorf("the line %q lacks the expiry", line)
				}
				if strings.Contains(line, "WARNING") != c.wantExpiring {
					t.Errorf("the line %q: a WARNING is %v, want %v", line, strings.Contains(line, "WARNING"), c.wantExpiring)
				}
				if strings.Contains(line, tokenExpiring(now.Add(c.left))) {
					t.Errorf("the line holds the token: %q", line)
				}
			}
		})
	}
}

// A missing file, a 401, a token for another ServiceAccount and a file that names another server are each a
// state with its reason, and none is an error of the check; only the context ending is.
func TestStatusControllerIdentityStates(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	ports, err := PortsFromBase(28520)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)
	exp := now.Add(300 * 24 * time.Hour)
	// The lifecycle's file is there; the provider's is not; the front's names another server.
	writeControllerKubeconfigsExpiring(t, l, ports, map[string]time.Time{"hpcburst-lifecycle": exp})
	other := &rest.Config{Host: "https://127.0.0.1:1", TLSClientConfig: rest.TLSClientConfig{Insecure: true}}
	b, err := identityKubeconfigBytes(other, "hpcburst-front", tokenExpiring(exp))
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(l.controllerKubeconfig("hpcburst-front"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	ask := func(context.Context, string, Ports) (string, error) {
		return "", apierrors.NewUnauthorized("the token is not accepted")
	}
	got, err := checkControllerIdentities(context.Background(), l, ports, now, ask)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("%+v", got)
	}
	if s := got[0]; s.Valid || !strings.Contains(s.Detail, "rejected the token (401)") || s.Expires == nil || s.Expiring {
		t.Errorf("a 401: %+v", s)
	}
	if s := got[1]; s.Valid || !strings.Contains(s.Detail, "no kubeconfig file") || s.Expires != nil {
		t.Errorf("a missing file: %+v", s)
	}
	if s := got[2]; s.Valid || !strings.Contains(s.Detail, "unreadable") || !strings.Contains(s.Detail, "not at this launcher") {
		t.Errorf("a file that names another server: %+v", s)
	}
	// The same expired token that the API server rejects says so.
	writeControllerKubeconfigsExpiring(t, l, ports, map[string]time.Time{"hpcburst-lifecycle": now.Add(-time.Hour)})
	got, _ = checkControllerIdentities(context.Background(), l, ports, now, ask)
	if s := got[0]; !strings.Contains(s.Detail, "expired at") || !s.Expiring {
		t.Errorf("an expired token the API server rejects: %+v", s)
	}
	// Another ServiceAccount's token in the lifecycle's file.
	writeControllerKubeconfigsExpiring(t, l, ports, map[string]time.Time{"hpcburst-lifecycle": exp})
	got, _ = checkControllerIdentities(context.Background(), l, ports, now, func(context.Context, string, Ports) (string, error) {
		return "system:serviceaccount:hpcburst-system:hpcburst-front", nil
	})
	if s := got[0]; s.Valid || !strings.Contains(s.Detail, "authenticates as") {
		t.Errorf("a token for another ServiceAccount: %+v", s)
	}
	// A request that fails for another reason is that identity's own state; the context ending is the check's error.
	got, err = checkControllerIdentities(context.Background(), l, ports, now, func(context.Context, string, Ports) (string, error) {
		return "", apierrors.NewServiceUnavailable("a test")
	})
	if err != nil || got[0].Valid || !strings.Contains(got[0].Detail, "unreadable") {
		t.Errorf("a failing request: %+v, %v", got[0], err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := checkControllerIdentities(ctx, l, ports, now, func(context.Context, string, Ports) (string, error) {
		return "", errors.New("context canceled")
	}); err == nil {
		t.Error("the context ending was not an error")
	}
}

// statusControllerIdentities runs only for a healthy host with controllers, and an expiring token does not make
// it unhealthy.
func TestStatusControllerIdentitiesDoNotChangeTheHostsHealth(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	ports, err := PortsFromBase(28520)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)
	exps := map[string]time.Time{}
	for _, n := range ControllerIdentityNames() {
		exps[n] = now.Add(10 * 24 * time.Hour)
	}
	writeControllerKubeconfigsExpiring(t, l, ports, exps)
	yes := true
	bad := func(f string, a ...any) { t.Errorf("the host was marked bad: "+f, a...) }
	st := &State{Ports: ports, Controllers: &ControllerConfig{Binary: "/x"}}
	rep := &StatusReport{Health: Healthy, Readyz: &yes}
	// No API server answers on ports.HTTPS, so every identity is "unreadable", which is a state.
	statusControllerIdentities(context.Background(), l, st, now, rep, bad)
	if len(rep.ControllerIdentities) != 3 || rep.Health != Healthy {
		t.Fatalf("%d identities, health %s", len(rep.ControllerIdentities), rep.Health)
	}
	for _, s := range rep.ControllerIdentities {
		if !s.Expiring || s.Warning == "" {
			t.Errorf("%s with 10 days left: %+v, want the warning", s.Name, s)
		}
	}
	// Not asked without controllers, or of a host that is not healthy.
	for name, c := range map[string]struct {
		st  *State
		rep *StatusReport
	}{
		"no controllers": {&State{Ports: ports}, &StatusReport{Health: Healthy, Readyz: &yes}},
		"not healthy":    {st, &StatusReport{Health: Unhealthy, Readyz: &yes}},
		"no readyz":      {st, &StatusReport{Health: Healthy}},
	} {
		statusControllerIdentities(context.Background(), l, c.st, now, c.rep, bad)
		if len(c.rep.ControllerIdentities) != 0 {
			t.Errorf("%s: the identities were asked", name)
		}
	}
}
