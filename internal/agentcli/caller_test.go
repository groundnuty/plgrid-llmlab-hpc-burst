package agentcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// apiServer is a stand-in API server that answers SelfSubjectReview for one identity and records every
// request it gets. The real API server's judgement is the end-to-end test's.
type apiServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []string
}

func newAPIServer(t *testing.T, user string, groups []string) *apiServer {
	t.Helper()
	s := &apiServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests = append(s.requests, r.Method+" "+r.URL.Path)
		s.mu.Unlock()
		if r.Method != http.MethodPost || r.URL.Path != "/apis/authentication.k8s.io/v1/selfsubjectreviews" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview",
			"status": map[string]any{"userInfo": map[string]any{"username": user, "groups": groups}},
		})
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *apiServer) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

// kubeconfigFor writes a kubeconfig that names the server, with the given context namespace.
func kubeconfigFor(t *testing.T, dir, name, server, namespace string) string {
	t.Helper()
	ns := ""
	if namespace != "" {
		ns = ", namespace: " + namespace
	}
	body := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: %q}}]
users: [{name: u, user: {token: not-a-real-token}}]
contexts: [{name: x, context: {cluster: c, user: u%s}}]
current-context: x
`, server, ns)
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadCallerNeedsAnExplicitKubeconfig(t *testing.T) {
	srv := newAPIServer(t, userA, groupsOfAgent)
	home := t.TempDir()
	// A kubeconfig that a fallback would find, at the default path and in a launcher's state directory.
	for _, rel := range []string{".kube/config", ".local/state/hpcburst/kubeconfig"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(home, rel)), 0o700); err != nil {
			t.Fatal(err)
		}
		kubeconfigFor(t, filepath.Dir(filepath.Join(home, rel)), filepath.Base(rel), srv.URL, "")
	}
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".local/state"))
	t.Setenv("KUBECONFIG", "") // the process variable; the CLI reads only what it is handed

	var stderr bytes.Buffer
	_, err := loadCaller(context.Background(), "", "", &stderr)
	var u usageError
	if !errors.As(err, &u) {
		t.Fatalf("no kubeconfig: %v, want a usage error", err)
	}
	for _, want := range []string{"--kubeconfig", "KUBECONFIG"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message %q does not name %s", err, want)
		}
	}
	if n := len(srv.seen()); n != 0 {
		t.Errorf("the API server got %d requests: a default kubeconfig was used", n)
	}
}

func TestLoadCallerFlagBeforeVariable(t *testing.T) {
	flagSrv := newAPIServer(t, userA, groupsOfAgent)
	envSrv := newAPIServer(t, userB, groupsOfAgent)
	dir := t.TempDir()
	flagPath := kubeconfigFor(t, dir, "flag", flagSrv.URL, "team")
	envPath := kubeconfigFor(t, dir, "env", envSrv.URL, "")

	var stderr bytes.Buffer
	c, err := loadCaller(context.Background(), flagPath, envPath, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if c.user != userA || c.namespace != "team" {
		t.Errorf("caller %q in %q, want %q in team: the flag comes first, and the context names the namespace", c.user, c.namespace, userA)
	}
	if got := flagSrv.seen(); len(got) != 1 || got[0] != "POST /apis/authentication.k8s.io/v1/selfsubjectreviews" {
		t.Errorf("requests to the flag's server: %v, want one SelfSubjectReview", got)
	}
	if n := len(envSrv.seen()); n != 0 {
		t.Errorf("the variable's server got %d requests", n)
	}
	if stderr.Len() != 0 {
		t.Errorf("an agent identity printed %q", stderr.String())
	}

	// The variable alone, and a context with no namespace: default.
	c, err = loadCaller(context.Background(), "", envPath, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if c.user != userB || c.namespace != "default" {
		t.Errorf("caller %q in %q, want %q in default", c.user, c.namespace, userB)
	}
}

func TestLoadCallerIdentities(t *testing.T) {
	controller := "system:serviceaccount:hpcburst-system:hpcburst-lifecycle"
	rows := []struct {
		name    string
		user    string
		groups  []string
		wantErr string
		note    bool
	}{
		{"an agent", userA, groupsOfAgent, "", false},
		{"the person with the admin kubeconfig: allowed, with a note", "admin", []string{"system:masters", "system:authenticated"}, "", true},
		{"a controller's ServiceAccount, by user and group", controller, []string{"system:serviceaccounts", "system:serviceaccounts:hpcburst-system", "system:authenticated"}, "a controller's credential is outside the capacity guard; use your own identity", false},
		{"a controller's group alone", "someone", []string{"system:serviceaccounts:hpcburst-system"}, "a controller's credential is outside the capacity guard", false},
		{"a controller's user alone", controller, []string{"system:authenticated"}, "a controller's credential is outside the capacity guard", false},
		{"a ServiceAccount of another namespace is not a controller", "system:serviceaccount:other:x", []string{"system:serviceaccounts:other"}, "", true},
		{"no user name", "", nil, "reported no user name", false},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			srv := newAPIServer(t, r.user, r.groups)
			path := kubeconfigFor(t, t.TempDir(), "kc", srv.URL, "")
			var stderr bytes.Buffer
			c, err := loadCaller(context.Background(), path, "", &stderr)
			if r.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), r.wantErr) {
					t.Fatalf("error %v, want one holding %q", err, r.wantErr)
				}
				var u usageError
				if errors.As(err, &u) {
					t.Errorf("a refused identity is a failure, not a usage error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.user != r.user {
				t.Errorf("user %q, want %q", c.user, r.user)
			}
			want := fmt.Sprintf("acting as %s, outside the capacity guard", r.user)
			if got := strings.Contains(stderr.String(), want); got != r.note {
				t.Errorf("stderr %q: note %v, want %v", stderr.String(), got, r.note)
			}
		})
	}
}

func TestLoadCallerRefusesWhatItCannotUse(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"an empty file": empty, "a missing file": filepath.Join(dir, "absent")} {
		_, err := loadCaller(context.Background(), path, "", &bytes.Buffer{})
		if err == nil {
			t.Errorf("%s: no error", name)
			continue
		}
		if strings.Contains(err.Error(), "in-cluster") {
			t.Errorf("%s: %v: the in-cluster configuration was tried", name, err)
		}
	}
	_, err := loadCaller(context.Background(), "", "/a/kubeconfig:/another", &bytes.Buffer{})
	var u usageError
	if !errors.As(err, &u) || !strings.Contains(err.Error(), "more than one file") {
		t.Errorf("a KUBECONFIG list: %v, want a usage error", err)
	}
}

// Run with no credential exits 2 for every command and names both ways to give one.
func TestRunWithoutAKubeconfig(t *testing.T) {
	srv := newAPIServer(t, userA, groupsOfAgent)
	home := t.TempDir()
	kubeconfigFor(t, home, "config", srv.URL, "")
	cmds := [][]string{
		{"scale", "m", "--instances", "1"}, {"end", "m"}, {"release", "alloc-a"},
		{"confirm", "op", "--plan", "p"}, {"approvals"}, {"approve", "op", "--plan", "p"}, {"operation", "op"},
	}
	for _, args := range cmds {
		var out, errs bytes.Buffer
		env := Env{Home: home, XDGStateHome: home}
		if code := Run(context.Background(), args, env, strings.NewReader(""), &out, &errs); code != exitUsage {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
		contains(t, strings.Join(args, " ")+" stderr", errs.String(), "KUBECONFIG", "--kubeconfig")
		lacks(t, strings.Join(args, " ")+" output", out.String()+errs.String(), "permission denied", home)
	}
	if n := len(srv.seen()); n != 0 {
		t.Errorf("the API server got %d requests", n)
	}
}

func TestRunUsage(t *testing.T) {
	for _, r := range []struct {
		args []string
		want int
	}{{nil, exitUsage}, {[]string{"frobnicate"}, exitUsage}} {
		var out, errs bytes.Buffer
		if code := Run(context.Background(), r.args, Env{}, strings.NewReader(""), &out, &errs); code != r.want {
			t.Errorf("%v: exit %d, want %d", r.args, code, r.want)
		}
		contains(t, "usage", errs.String(), "usage: hpcburst scale")
	}
}
