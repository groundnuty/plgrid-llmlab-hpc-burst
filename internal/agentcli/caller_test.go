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
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
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

// Run with no credential exits 2 for every command and names both ways to give one. The commands run in a CHILD
// process of the test binary, whose HOME and XDG_STATE_HOME are the test's: client-go computes the default
// kubeconfig path from HOME when the process starts, so only a process started with the planted HOME can see a
// fallback to it. An admin-style kubeconfig that works is planted at $HOME/.kube/config and at the launcher's
// state directory, once readable and once at mode 000, and KUBECONFIG is unset. A command that tried either
// would reach the stand-in server (readable) or print "permission denied" (mode 000); the server must see
// nothing, and the output neither "permission denied" nor the state path.
func TestRunWithoutAKubeconfig(t *testing.T) {
	for _, mode := range []os.FileMode{0o600, 0o000} {
		t.Run(fmt.Sprintf("planted kubeconfigs of mode %04o", mode), func(t *testing.T) {
			if mode == 0 && os.Geteuid() == 0 {
				t.Skip("a process of root reads a file of mode 000, so there is nothing to refuse")
			}
			srv := newAPIServer(t, "admin", []string{"system:masters"})
			home, xdg := t.TempDir(), t.TempDir()
			state := filepath.Join(xdg, "hpcburst")
			for _, dir := range []string{filepath.Join(home, ".kube"), state} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			for _, p := range []string{kubeconfigFor(t, filepath.Join(home, ".kube"), "config", srv.URL, ""), kubeconfigFor(t, state, "kubeconfig", srv.URL, "")} {
				if err := os.Chmod(p, mode); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(os.Args[0], "-test.run", "^TestCommandsWithoutAKubeconfigInAChildProcess$", "-test.v")
			cmd.Env = append(withoutEnv(os.Environ(), "KUBECONFIG", "HOME", "XDG_STATE_HOME"), childEnv+"="+state, "HOME="+home, "XDG_STATE_HOME="+xdg)
			out, err := cmd.CombinedOutput()
			if err != nil || !strings.Contains(string(out), "--- PASS: TestCommandsWithoutAKubeconfigInAChildProcess") {
				t.Fatalf("the child process: %v\n%s", err, out)
			}
			if n := len(srv.seen()); n != 0 {
				t.Errorf("the API server got %d requests: a planted kubeconfig was used", n)
			}
		})
	}
}

// childEnv marks the child process of TestRunWithoutAKubeconfig and carries the launcher's state directory.
const childEnv = "AGENTCLI_TEST_CHILD_STATE_DIR"

func withoutEnv(env []string, names ...string) []string {
	var out []string
	for _, kv := range env {
		keep := true
		for _, n := range names {
			keep = keep && !strings.HasPrefix(kv, n+"=")
		}
		if keep {
			out = append(out, kv)
		}
	}
	return out
}

// TestCommandsWithoutAKubeconfigInAChildProcess is the child: it runs every command as hpcburst's main does,
// with the three variables read from the process, and does nothing when it is not the child.
func TestCommandsWithoutAKubeconfigInAChildProcess(t *testing.T) {
	state := os.Getenv(childEnv)
	if state == "" {
		t.Skip("run by TestRunWithoutAKubeconfig")
	}
	if os.Getenv("KUBECONFIG") != "" {
		t.Fatal("the child has a KUBECONFIG")
	}
	env := Env{Kubeconfig: os.Getenv("KUBECONFIG"), Home: os.Getenv("HOME"), XDGStateHome: os.Getenv("XDG_STATE_HOME")}
	for _, args := range [][]string{
		{"scale", "m", "--instances", "1"}, {"end", "m"}, {"release", "alloc-a"},
		{"confirm", "op", "--plan", "p"}, {"approvals"}, {"approve", "op", "--plan", "p"}, {"operation", "op"},
	} {
		var out, errs bytes.Buffer
		if code := Run(context.Background(), args, env, strings.NewReader(""), &out, &errs); code != exitUsage {
			t.Errorf("%v: exit %d, want 2; stderr %q", args, code, errs.String())
		}
		contains(t, strings.Join(args, " ")+" stderr", errs.String(), "KUBECONFIG", "--kubeconfig")
		lacks(t, strings.Join(args, " ")+" output", out.String()+errs.String(), "permission denied", state)
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

// Two tokens of an invented shape, for a kubeconfig with two users of one name. client-go's own error for that
// names the whole list of users, bearer tokens included, so a parser error must never be printed as it is.
const (
	leakTokenOne = "tok-invented-one-0123456789"
	leakTokenTwo = "tok-invented-two-9876543210"
)

func duplicateUserKubeconfig(t *testing.T, dir string) string {
	t.Helper()
	body := `apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: "http://127.0.0.1:1"}}]
users:
- {name: u, user: {token: ` + leakTokenOne + `}}
- {name: u, user: {token: ` + leakTokenTwo + `}}
contexts: [{name: x, context: {cluster: c, user: u}}]
current-context: x
`
	p := filepath.Join(dir, "dup")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// A kubeconfig that cannot be parsed or used is reported by a fixed message that names the file and the kind
// of fault, and never by the parser's text, which can hold the credentials of the file.
func TestAKubeconfigErrorNeverPrintsACredential(t *testing.T) {
	dir := t.TempDir()
	dup := duplicateUserKubeconfig(t, dir)
	garbled := filepath.Join(dir, "garbled")
	if err := os.WriteFile(garbled, []byte("users: [{name: u, user: {token: "+leakTokenOne+"}"+"\n\t not yaml: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	// Valid, but its current context does not exist: ClientConfig's own error, which must not be printed either.
	dangling := filepath.Join(dir, "dangling")
	if err := os.WriteFile(dangling, []byte("apiVersion: v1\nkind: Config\nclusters: [{name: c, cluster: {server: \"http://127.0.0.1:1\"}}]\n"+
		"users: [{name: other, user: {token: "+leakTokenTwo+"}}]\ncontexts: [{name: x, context: {cluster: c, user: other}}]\ncurrent-context: nope\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"two users of one name": dup, "unparsable": garbled, "a current context that does not exist": dangling} {
		t.Run(name, func(t *testing.T) {
			_, err := loadFor(path)
			if err == nil {
				t.Fatal("no error")
			}
			lacks(t, "the error", err.Error(), leakTokenOne, leakTokenTwo)
			contains(t, "the error", err.Error(), path)

			// Through the command: nothing printed holds a token either.
			var out, errs bytes.Buffer
			if code := Run(context.Background(), []string{"approvals"}, Env{Kubeconfig: path}, strings.NewReader(""), &out, &errs); code != exitFail {
				t.Errorf("exit %d, want 1", code)
			}
			lacks(t, "output", out.String()+errs.String(), leakTokenOne, leakTokenTwo)
		})
	}
	_, err := loadFor(dup)
	contains(t, "the message for two users of one name", err.Error(), "is not a valid kubeconfig")
}

// loadFor is loadCaller on one kubeconfig file, with no variable and nothing to say on stderr.
func loadFor(path string) (*caller, error) {
	return loadCaller(context.Background(), path, "", &bytes.Buffer{})
}

// tokenServer answers SelfSubjectReview with the user that owns the bearer token of the request, and records
// the tokens it saw. A request with another token is Unauthorized.
type tokenServer struct {
	*httptest.Server
	mu   sync.Mutex
	seen []string
}

func newTokenServer(t *testing.T, users map[string]string) *tokenServer {
	t.Helper()
	s := &tokenServer{}
	// TLS, because client-go reads a kubeconfig's credentials only when the transport is secure.
	s.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		s.mu.Lock()
		s.seen = append(s.seen, tok)
		s.mu.Unlock()
		user, ok := users[tok]
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview",
			"status": map[string]any{"userInfo": map[string]any{"username": user, "groups": groupsOfAgent}},
		})
	}))
	t.Cleanup(s.Close)
	return s
}

// A relative credential path in a kubeconfig is relative to the kubeconfig's own directory, never to the
// process's. For dir/config with tokenFile: token, the token is dir/token, agent A's; a ./token in the
// working directory, which is another identity's, must not be read.
func TestARelativeCredentialPathIsResolvedAgainstTheKubeconfigsDirectory(t *testing.T) {
	const tokA, tokB = "tok-invented-agent-a", "tok-invented-other"
	srv := newTokenServer(t, map[string]string{tokA: userA, tokB: userB})
	dir, cwd := t.TempDir(), t.TempDir()
	for path, tok := range map[string]string{filepath.Join(dir, "token"): tokA, filepath.Join(cwd, "token"): tokB} {
		if err := os.WriteFile(path, []byte(tok), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	body := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters: [{name: c, cluster: {server: %q, insecure-skip-tls-verify: true}}]\nusers: [{name: u, user: {tokenFile: token}}]\n"+
		"contexts: [{name: x, context: {cluster: c, user: u}}]\ncurrent-context: x\n", srv.URL)
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(cwd)

	c, err := loadFor(filepath.Join(dir, "config"))
	if err != nil {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		t.Fatalf("%v (the API server saw the tokens %q)", err, srv.seen)
	}
	if c.user != userA {
		t.Errorf("the command acts as %q, want %q: the token came from the working directory, not from the kubeconfig's", c.user, userA)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.seen) != 1 || srv.seen[0] != tokA {
		t.Errorf("the API server saw the tokens %q, want only the kubeconfig directory's", srv.seen)
	}
}

// A reference that is still relative after the resolution is refused, so the working directory is never read.
func TestARelativeCredentialPathThatCannotBeResolvedIsRefused(t *testing.T) {
	cfg := clientcmdapi.NewConfig()
	cfg.AuthInfos["u"] = &clientcmdapi.AuthInfo{TokenFile: "token"} // no LocationOfOrigin: the loader did not set it
	if err := resolveCredentialPaths(cfg, "/agent/config"); err != nil {
		t.Fatal(err)
	}
	if got := cfg.AuthInfos["u"].TokenFile; got != "/agent/token" {
		t.Errorf("tokenFile %q, want it resolved against the kubeconfig's directory: /agent/token", got)
	}
	// Every kind of reference is resolved the same way.
	cfg = clientcmdapi.NewConfig()
	cfg.Clusters["c"] = &clientcmdapi.Cluster{CertificateAuthority: "ca.crt"}
	cfg.AuthInfos["u"] = &clientcmdapi.AuthInfo{ClientCertificate: "tls.crt", ClientKey: "tls.key", TokenFile: "sub/token"}
	if err := resolveCredentialPaths(cfg, "/agent/config"); err != nil {
		t.Fatal(err)
	}
	for got, want := range map[string]string{cfg.Clusters["c"].CertificateAuthority: "/agent/ca.crt", cfg.AuthInfos["u"].ClientCertificate: "/agent/tls.crt",
		cfg.AuthInfos["u"].ClientKey: "/agent/tls.key", cfg.AuthInfos["u"].TokenFile: "/agent/sub/token"} {
		if got != want {
			t.Errorf("resolved to %q, want %q", got, want)
		}
	}
}

// With no file to resolve against, a relative reference is refused, never left for the working directory.
func TestARelativeCredentialPathWithNoDirectoryToResolveAgainstIsRefused(t *testing.T) {
	cfg := clientcmdapi.NewConfig()
	cfg.AuthInfos["u"] = &clientcmdapi.AuthInfo{TokenFile: "token"}
	if err := resolveCredentialPaths(cfg, ""); err == nil {
		t.Error("a relative tokenFile was accepted with no kubeconfig directory to resolve it against")
	}
}
