package quickstart

import (
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
)

// The commands of steps 4 to 7, checked against what they name. The binary's flags are checked in
// cli_test.go; this file adds what a flag list cannot show: that the start command's combination of
// flags is one the binary accepts, that each kubectl line is one kubectl takes and names an object
// the page applies, that the request asks for the model the page deploys, at the address the front
// listens on, and that the credentials files of step 3 are the ones the Provider names.

// frontFiles runs the page's snippets that make the client key, its files and front.json, in the
// test's home with the test's STATE, as the snippets of step 3 do for a user, and returns the
// front's configuration file.
func frontFiles(t *testing.T, d dirs, home string) string {
	t.Helper()
	keys := only(t, "block that generates the client key", func(b block) bool { return strings.Contains(b.text, "openssl rand") })
	cfg := only(t, "block that writes front.json", func(b block) bool { return strings.Contains(b.text, "front.json") && strings.Contains(b.text, "cat >") })
	cmd := exec.Command("bash", "-c", "set -eu\nmkdir -p ~/.config/hpcburst\n"+keys.text+"\n"+cfg.text)
	cmd.Env = append(baseEnv(home), "STATE="+d.state)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the front's snippets fail: %v\n%s", err, out)
	}
	return filepath.Join(home, ".config", "hpcburst", "front.json")
}

// startCommand is the quickstart's one `hpcburst up` command.
func startCommand(t *testing.T, d dirs, home string) command {
	t.Helper()
	var found []command
	for _, c := range repoCommands(t, d, home) {
		if len(c.words) > 1 && c.words[0] == "bin/hpcburst" && c.words[1] == "up" {
			// The beta router section's up, with --instance-router, has its own check:
			// TestTheRouterSectionFetchesEnvoyThenStartsWithTheRouter (router_section_test.go).
			if slices.Contains(c.words, "--instance-router") {
				continue
			}
			found = append(found, c)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one hpcburst up command in docs/quickstart.md, found %d: a second one needs its own check here", len(found))
	}
	return found[0]
}

// valueOf returns the value that follows a flag in a command's words.
func valueOf(c command, flag string) (string, bool) {
	i := slices.Index(c.words, flag)
	if i < 0 || i+1 == len(c.words) {
		return "", false
	}
	return c.words[i+1], true
}

var missingK3s = regexp.MustCompile(`open (\S+)/k3s-[0-9.]+/k3s: no such file`)

// upBinary is a copy of the built hpcburst with the two other host binaries beside it, as step 2
// leaves bin/: `up` looks for the controller and the front beside itself, and with a front
// configuration it refuses to go on without the front. The two are stubs that fail if run, and the
// tests that use them stop `up` at k3s, before it starts anything.
func upBinary(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile(hpcburst(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "hpcburst")
	if err := os.WriteFile(bin, src, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"hpcburst-controller", "hpcburst-front"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return bin
}

// The start command of step 4 is in host-dialed mode, and the binary accepts it as it is written:
// with the front's files the page's snippets make, and an empty binary directory, `up` gets past
// every check of its flags, the mode and the front's configuration, and stops where it looks for
// k3s. The controls show that the same command with an address or a key, which host-dialed mode
// refuses, stops earlier, so reaching k3s is not what every command does.
func TestStartCommandIsHostDialedAndTheBinaryAcceptsIt(t *testing.T) {
	d, home := testDirs(t)
	frontFiles(t, d, home)
	c := startCommand(t, d, home)
	if mode, _ := valueOf(c, "--front-mode"); mode != "host-dialed" {
		t.Fatalf("the quickstart's start command runs in mode %q, want host-dialed: %q", mode, c.line)
	}
	if cfg, ok := valueOf(c, "--front-config"); !ok || cfg != filepath.Join(home, ".config", "hpcburst", "front.json") {
		t.Errorf("--front-config is %q, want the front.json that step 3 writes", cfg)
	}
	for _, f := range []string{"--front-addr", "--front-key"} {
		if _, ok := valueOf(c, f); ok {
			t.Errorf("the host-dialed command passes %s, which that mode refuses", f)
		}
	}

	up := upBinary(t)
	out, code := run(t, baseEnv(home), up, c.words[1:]...)
	if m := missingK3s.FindStringSubmatch(out); code == 0 || m == nil || m[1] != d.bin {
		t.Errorf("the command should stop where up looks for k3s in %s (exit %d):\n%s", d.bin, code, out)
	}
	for _, extra := range [][]string{{"--front-addr", "front.example:22"}, {"--front-key", filepath.Join(home, "key")}} {
		t.Run("control: with "+extra[0], func(t *testing.T) {
			out, code := run(t, baseEnv(home), up, append(slices.Clone(c.words[1:]), extra...)...)
			if code == 0 || missingK3s.MatchString(out) || !strings.Contains(out, "host-dialed") {
				t.Errorf("up should refuse %s in host-dialed mode before it looks for k3s (exit %d):\n%s", extra[0], code, out)
			}
		})
	}
}

// object is what a manifest creates, and what it asks for.
type object struct {
	kind, name, model, credentials string
	providers                      []string
}

// objectsIn reads a manifest of the repository.
func objectsIn(t *testing.T, rel string) []object {
	t.Helper()
	var out []object
	for _, doc := range strings.Split(readRepoFile(t, rel), "\n---") {
		var o struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Model     string `json:"model"`
				Providers struct {
					Names []string `json:"names"`
				} `json:"providers"`
				CredentialsRef struct {
					File string `json:"file"`
				} `json:"credentialsRef"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &o); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if o.Kind != "" {
			out = append(out, object{kind: strings.ToLower(o.Kind), name: o.Metadata.Name, model: o.Spec.Model, credentials: o.Spec.CredentialsRef.File, providers: o.Spec.Providers.Names})
		}
	}
	return out
}

// plainLines returns the command lines of the quickstart's plain shell blocks, with continuations
// joined and comments left out.
func plainLines(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, b := range blocks(t) {
		if b.lang != "" {
			continue
		}
		for _, l := range joinContinuations(b.text) {
			if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
				out = append(out, l)
			}
		}
	}
	return out
}

// Every kubectl line of the page is one kubectl takes (verbs, flags and values as the table of the
// operations guide's checks holds them), the file an apply names is a file of the repository, or
// the copy of an example that an earlier cp line made for the user to edit, and the names that a
// wait or a delete asks for are those of objects the page applies.
func TestKubectlLinesAreKubectlsAndNameWhatThePageApplies(t *testing.T) {
	d, home := testDirs(t)
	crds := readCRDNames(t)
	userDir := filepath.Join(home, ".config", "hpcburst") + "/"
	copies := map[string]string{} // a file the user edits -> the example it is a copy of
	var hbk []command
	for _, l := range plainLines(t) {
		// Only the lines this test reads are expanded: the shell evaluates what it is given, and a
		// line with a redirection would write a file.
		switch strings.Fields(l)[0] {
		case "cp":
			if w := shellWords(t, l, d, home); len(w) == 3 && strings.HasPrefix(w[1], "examples/") {
				if filepath.Base(w[2]) != filepath.Base(w[1]) || !strings.HasPrefix(w[2], userDir) {
					t.Errorf("%q: a copy of an example goes under ~/.config/hpcburst with the example's own name", l)
				}
				if _, err := os.Stat(filepath.Join(root, w[1])); err != nil {
					t.Errorf("%q: %v", l, err)
				}
				copies[w[2]] = w[1]
			}
		case "hbk":
			hbk = append(hbk, command{line: l, words: shellWords(t, l, d, home)})
		}
	}
	if len(hbk) == 0 || len(copies) == 0 {
		t.Fatalf("found %d hbk lines and %d copies of examples in docs/quickstart.md", len(hbk), len(copies))
	}

	applied := map[string]object{} // kind/name -> object, as the page applies them
	var models []string
	verbs := map[string]int{}
	for _, c := range hbk {
		words := slices.Clone(c.words)
		verbs[words[1]]++
		if words[1] == "apply" {
			for i, w := range words {
				if w == "-f" && i+1 < len(words) {
					if src, ok := copies[words[i+1]]; ok {
						words[i+1] = src
					}
					for _, o := range objectsIn(t, words[i+1]) {
						applied[o.kind+"/"+o.name] = o
					}
				}
			}
		}
		checkHbk(t, c.line, words, crds)
	}
	for _, verb := range []string{"apply", "get", "wait", "delete"} {
		if verbs[verb] == 0 {
			t.Errorf("the quickstart has no kubectl %s line", verb)
		}
	}
	for _, c := range hbk {
		// The object a wait or a delete asks for, as kind/name: wait takes it so, delete takes the
		// kind and the name as two words (checkHbk has accepted the line, so they are there).
		var ref string
		switch w := c.words; w[1] {
		case "wait", "delete":
			var pos []string
			for _, a := range w[2:] {
				if !strings.HasPrefix(a, "-") {
					pos = append(pos, a)
				}
			}
			if len(pos) == 0 {
				continue // checkHbk reported it
			}
			if ref = strings.Join(pos, "/"); w[1] == "wait" {
				ref = pos[0]
			}
		default:
			continue
		}
		o, ok := applied[ref]
		if !ok {
			t.Errorf("%q asks for %s, which no apply line of the page creates", c.line, ref)
			continue
		}
		if o.kind == "modeldeployment" {
			models = append(models, o.model)
			for _, p := range o.providers {
				if _, ok := applied["provider/"+p]; !ok {
					t.Errorf("%s names the Provider %s, which the page does not apply (its Provider keeps the example's name)", ref, p)
				}
			}
		}
	}

	// A request asks for a model the page deploys.
	var curl *command
	for _, l := range plainLines(t) {
		if strings.HasPrefix(l, "curl ") {
			if curl != nil {
				t.Fatalf("a second curl line in docs/quickstart.md: %q", l)
			}
			curl = &command{line: l, words: shellWords(t, l, d, home)}
		}
	}
	if curl == nil {
		t.Fatal("the quickstart has no curl line")
	}
	checkRequest(t, *curl, d, home, models)
}

// checkRequest checks the curl command of step 6: its address is where the front listens by
// default (read from the front's own configuration, loaded from the file the page's snippet
// writes), its header file is the one step 3 makes, its body is JSON, and its model is one that
// a ModelDeployment the page applies serves.
func checkRequest(t *testing.T, c command, d dirs, home string, models []string) {
	t.Helper()
	cfg, err := front.LoadConfig(frontFiles(t, d, home))
	if err != nil {
		t.Fatalf("the front refuses the page's front.json: %v", err)
	}
	var target, body string
	var headers []string
	for i := 1; i < len(c.words); i++ {
		switch w := c.words[i]; {
		case strings.HasPrefix(w, "http://"):
			target = w
		case w == "-H" && i+1 < len(c.words):
			i++
			headers = append(headers, c.words[i])
		case w == "-d" && i+1 < len(c.words):
			i++
			body = c.words[i]
		}
	}
	u, err := url.Parse(target)
	if err != nil || u.Host != cfg.ClientListen || u.Path != "/v1/chat/completions" {
		t.Errorf("the request goes to %q; want the front's default address %s and the path /v1/chat/completions (%v)", target, cfg.ClientListen, err)
	}
	want := "@" + filepath.Join(home, ".config", "hpcburst", "client.hdr")
	if !slices.Contains(headers, want) || !slices.Contains(headers, "Content-Type: application/json") {
		t.Errorf("the request's headers are %q; want the header file %s that step 3 makes, and a JSON content type", headers, want)
	}
	if _, err := os.Stat(strings.TrimPrefix(want, "@")); err != nil {
		t.Errorf("the header file the request names is not one step 3 makes: %v", err)
	}
	var req struct {
		Model     string           `json:"model"`
		Messages  []map[string]any `json:"messages"`
		MaxTokens int              `json:"max_tokens"`
	}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("the request's body is not JSON: %v\n%s", err, body)
	}
	if !slices.Contains(models, req.Model) {
		t.Errorf("the request asks for model %q; the page deploys %q", req.Model, models)
	}
	if len(req.Messages) == 0 || req.MaxTokens <= 0 {
		t.Errorf("the request has %d messages and max_tokens %d", len(req.Messages), req.MaxTokens)
	}
}

// The credentials file step 3 creates is the file the Provider example names, with ~ for
// /home/<user>, so a user who follows both ends with a Provider whose credentials file exists.
func TestCredentialsFilesOfStep3AreTheOnesTheProvidersName(t *testing.T) {
	page := readRepoFile(t, "docs/quickstart.md")
	created := map[string]bool{}
	for _, l := range plainLines(t) {
		if rest, ok := strings.CutPrefix(l, "install -m 600 /dev/null "); ok {
			created[rest] = true
		}
	}
	// The Athena Provider is the page's own; the Helios note says "as in step 3" and has no line.
	for _, o := range objectsIn(t, "examples/provider-athena-a100.yaml") {
		if p := strings.Replace(o.credentials, "/home/<user>", "~", 1); !created[p] {
			t.Errorf("examples/provider-athena-a100.yaml names the credentials file %s, which no install -m 600 /dev/null line of the page creates (%v)", o.credentials, created)
		}
	}
	for _, o := range objectsIn(t, "examples/provider-llmlab.yaml") {
		if p := strings.Replace(o.credentials, "/home/<user>", "~", 1); !strings.Contains(page, "`"+p+"`") {
			t.Errorf("examples/provider-llmlab.yaml names %s, which the page's LLMLab section does not give", o.credentials)
		}
	}
}
