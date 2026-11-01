package quickstart

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// root is the repository root, relative to this package.
const root = "../.."

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(b)
}

// block is one fenced code block of the quickstart.
type block struct {
	lang string // the fence's info string
	text string
}

var fence = regexp.MustCompile("(?ms)^```([a-z-]*)\n(.*?)^```$")

// blocks returns the fenced code blocks of docs/quickstart.md. A document with none fails: a
// changed fence style must not leave every check below with nothing to read.
func blocks(t *testing.T) []block {
	t.Helper()
	var out []block
	for _, m := range fence.FindAllStringSubmatch(readRepoFile(t, "docs/quickstart.md"), -1) {
		out = append(out, block{lang: m[1], text: m[2]})
	}
	if len(out) == 0 {
		t.Fatal("docs/quickstart.md has no fenced code block")
	}
	return out
}

// only returns the one block that satisfies match, and fails when there are none or several.
func only(t *testing.T, what string, match func(block) bool) block {
	t.Helper()
	var found []block
	for _, b := range blocks(t) {
		if match(b) {
			found = append(found, b)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one %s in docs/quickstart.md, found %d", what, len(found))
	}
	return found[0]
}

// fill replaces the quickstart's placeholders with values of the right shape, since a placeholder
// is a user's job to replace and not a value any reader accepts.
func fill(s string) string {
	return strings.NewReplacer(
		"<login-node>", "login.example",
		"<user>", "exampleuser",
		"<llmlab-key>", strings.Repeat("k", 32),
		"<llmlab-gateway>", "gateway.example",
	).Replace(s)
}

func privateFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "creds")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// A batch Provider's credentials file, as the quickstart shows it, is accepted by the parser the
// Slurm provider uses (HBEP-4, Provider settings).
func TestSSHCredentialsSampleParses(t *testing.T) {
	b := only(t, "ssh-config block", func(b block) bool { return b.lang == "ssh-config" })
	c, err := slurm.ParseSSHConfig([]byte(fill(b.text)), "/home/exampleuser")
	if err != nil {
		t.Fatalf("the sample is refused: %v", err)
	}
	if c.HostName != "login.example" || c.User != "exampleuser" || c.IdentityFile == "" {
		t.Errorf("the sample parsed to %+v, want its HostName, User and IdentityFile", c)
	}
}

// The LLMLab Provider's credentials file, as the quickstart shows it, is accepted by the reader the
// managed Provider and the front share, from a file of mode 600.
func TestManagedCredentialsSampleParses(t *testing.T) {
	b := only(t, "json block", func(b block) bool { return b.lang == "json" })
	c, err := provider.ReadManagedCredentials(privateFile(t, fill(b.text)))
	if err != nil {
		t.Fatalf("the sample is refused: %v", err)
	}
	if !strings.HasSuffix(c.BaseURL, "/v1") {
		t.Errorf("the sample's baseURL is %q, want one that ends in /v1", c.BaseURL)
	}
}

// The shell snippets that create the front's files really produce files the front accepts, in the
// mode it demands. They run in a home of their own, and the key never leaves it.
func TestFrontFileSnippetsProduceWhatTheFrontReads(t *testing.T) {
	keys := only(t, "block that generates the client key", func(b block) bool { return strings.Contains(b.text, "openssl rand") })
	cfgBlock := only(t, "block that writes front.json", func(b block) bool { return strings.Contains(b.text, "front.json") && strings.Contains(b.text, "cat >") })
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Fatalf("the quickstart generates the client key with openssl, and this machine has none: %v", err)
	}
	home := t.TempDir()
	script := "set -eu\nSTATE=$HOME/.local/state/hpcburst\ninstall -d -m 700 \"$STATE\" ~/.config/hpcburst\n" + keys.text + "\n" + cfgBlock.text
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH")}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the snippets fail: %v\n%s", err, out)
	}
	dir := filepath.Join(home, ".config", "hpcburst")

	ck, err := front.ReadClientKeys(filepath.Join(dir, "client-keys.json"))
	if err != nil {
		t.Fatalf("the front refuses client-keys.json: %v", err)
	}
	secret, err := os.ReadFile(filepath.Join(dir, "client.key"))
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := ck.Match(strings.TrimSpace(string(secret))); !ok || id != "me" {
		t.Errorf("the generated key does not match its entry: id %q, found %v", id, ok)
	}
	hdr, err := os.ReadFile(filepath.Join(dir, "client.hdr"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "Authorization: Bearer " + strings.TrimSpace(string(secret)); strings.TrimSpace(string(hdr)) != want {
		t.Errorf("client.hdr is not a Bearer header of the generated key")
	}
	for _, f := range []string{"client.key", "client-keys.json", "client.hdr"} {
		if fi, err := os.Stat(filepath.Join(dir, f)); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("%s: want mode 600, got %v (%v)", f, fi, err)
		}
	}
	if _, err := front.LoadConfig(filepath.Join(dir, "front.json")); err != nil {
		t.Errorf("the front refuses front.json: %v", err)
	}
}

// Every path the quickstart links or names exists, so a rename in the repository shows here.
func TestNamedPathsExist(t *testing.T) {
	doc := readRepoFile(t, "docs/quickstart.md")
	link := regexp.MustCompile(`\]\(([^)#]+)(#[^)]*)?\)`)
	n := 0
	for _, m := range link.FindAllStringSubmatch(doc, -1) {
		if strings.Contains(m[1], "://") {
			continue
		}
		n++
		if _, err := os.Stat(filepath.Join(root, "docs", m[1])); err != nil {
			t.Errorf("link %q: %v", m[1], err)
		}
	}
	// Paths in code: scripts, manifests, profiles, commands and the CRD directory.
	code := regexp.MustCompile("(?:hack/[a-z0-9-]+\\.sh|examples/[a-z0-9-]+\\.yaml|profiles/[a-z0-9-]+\\.yaml|config/crd|\\./cmd/[a-z-]+)")
	for _, p := range code.FindAllString(doc, -1) {
		n++
		if _, err := os.Stat(filepath.Join(root, strings.TrimPrefix(p, "./"))); err != nil {
			t.Errorf("path %q: %v", p, err)
		}
	}
	if n == 0 {
		t.Fatal("found no link or path to check in docs/quickstart.md")
	}
}

// Every file in examples/ appears in the quickstart, so a new example is not left undocumented.
func TestEveryExampleIsMentioned(t *testing.T) {
	doc := readRepoFile(t, "docs/quickstart.md")
	files, err := filepath.Glob(filepath.Join(root, "examples", "*"))
	if err != nil || len(files) == 0 {
		t.Fatalf("examples/ lists no file (%v)", err)
	}
	for _, f := range files {
		if !strings.Contains(doc, "`"+filepath.Base(f)+"`") {
			t.Errorf("examples/%s is not named in docs/quickstart.md", filepath.Base(f))
		}
	}
}

// Every flag the quickstart passes to hpcburst up is one that command defines. The flags are read
// from the command's source: cmd/hpcburst is package main and cannot be imported.
func TestUpFlagsExist(t *testing.T) {
	src := readRepoFile(t, "cmd/hpcburst/main.go")
	start := strings.Index(src, "func cmdUp(")
	end := strings.Index(src, "func cmdStatus(")
	if start < 0 || end < start {
		t.Fatal("cannot find cmdUp in cmd/hpcburst/main.go")
	}
	defined := map[string]bool{}
	for _, m := range regexp.MustCompile(`fs\.\w+Var\(&[\w.]+, "([a-z-]+)"`).FindAllStringSubmatch(src[start:end], -1) {
		defined[m[1]] = true
	}
	if len(defined) < 5 {
		t.Fatalf("read only %d flags of hpcburst up from its source: the pattern is stale", len(defined))
	}
	flag := regexp.MustCompile(`--[a-z][a-z-]*`)
	checked := 0
	for _, b := range blocks(t) {
		if !strings.Contains(b.text, "bin/hpcburst up") {
			continue
		}
		for _, f := range flag.FindAllString(b.text, -1) {
			checked++
			if !defined[strings.TrimPrefix(f, "--")] {
				t.Errorf("the quickstart passes %s to hpcburst up, which defines no such flag", f)
			}
		}
	}
	// Flags named in prose, in a span that starts with hpcburst up or a flag of the command.
	for _, m := range regexp.MustCompile("`(?:hpcburst up )?(--[a-z][a-z-]*)[^`]*`").FindAllStringSubmatch(readRepoFile(t, "docs/quickstart.md"), -1) {
		name := strings.TrimPrefix(m[1], "--")
		if name == "agent-builds" { // hack/fetch-frp.sh's flag, not hpcburst up's
			continue
		}
		checked++
		if !defined[name] {
			t.Errorf("the quickstart names %s as a flag of hpcburst up, which defines no such flag", m[1])
		}
	}
	if checked == 0 {
		t.Fatal("found no hpcburst up flag in docs/quickstart.md to check")
	}
}
