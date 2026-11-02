package quickstart

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The operator's guide (issue #240) is checked like the quickstart: its hpcburst commands against
// the binary's -h, its hack/ scripts by running them up to their arguments, and the status output
// it shows against the binary's own. Its kubectl lines (hbk) are checked per verb against the CRDs
// and the constants (operations_kubectl_test.go), and its login-node lines against Slurm's man
// pages (operations_slurm_test.go). Every condition, reason, phase and mode it names is checked
// against the constant that holds it. A line or a name the test cannot place fails, so nothing in
// the guide goes unchecked by default.

const opsGuide = "docs/operations.md"

// opsFence matches the guide's fenced blocks, also those indented under a list item.
var opsFence = regexp.MustCompile("(?ms)^( *)```([a-z-]*)\n(.*?)^ *```$")

// opsSegment is a run of the guide: prose, or a fenced block with its info string and its text
// with the fence's indentation removed.
type opsSegment struct {
	fenced bool
	lang   string
	text   string
}

// opsSegments splits the guide into prose and fenced blocks, in order. Every fence line must
// belong to a block it found, so a fence the pattern misses fails here instead of reading as prose.
func opsSegments(t *testing.T) []opsSegment {
	t.Helper()
	doc := readRepoFile(t, opsGuide)
	var out []opsSegment
	last := 0
	for _, m := range opsFence.FindAllStringSubmatchIndex(doc, -1) {
		out = append(out, opsSegment{text: doc[last:m[0]]})
		indent := doc[m[2]:m[3]]
		var lines []string
		for _, l := range strings.Split(doc[m[6]:m[7]], "\n") {
			lines = append(lines, strings.TrimPrefix(l, indent))
		}
		out = append(out, opsSegment{fenced: true, lang: doc[m[4]:m[5]], text: strings.Join(lines, "\n")})
		last = m[1]
	}
	out = append(out, opsSegment{text: doc[last:]})
	fences, blocks := 0, 0
	for _, s := range out {
		if s.fenced {
			blocks++
		} else {
			fences += len(regexp.MustCompile("(?m)^ *```").FindAllString(s.text, -1))
		}
	}
	if blocks == 0 || fences != 0 {
		t.Fatalf("%s: %d fenced blocks found, and %d fence lines outside them", opsGuide, blocks, fences)
	}
	return out
}

// fillOps replaces the guide's placeholders with values of the right shape; the quickstart's own
// placeholders are filled by fill. An unknown placeholder stays, and the shell then refuses it.
func fillOps(s string) string {
	return fill(strings.NewReplacer(
		"<name>", "example-deployment",
		"<instance>", "example-instance",
		"<allocation>", "example-allocation",
		"<provider>", "example-provider",
		"<record>", "acct-00000000-0000-4000-8000-000000000000",
		"<attempt>", "2",
		"<version>", "1.36.6",
		"<backup>", "/nonexistent/backup",
		"<tag>", "example-tag",
		"<job-id>", "1",
	).Replace(s))
}

// The fences the guide may use: plain for commands on the control-plane host, cluster for commands
// on a login node, which nothing here can run, and text for output.
var opsFenceLangs = map[string]bool{"": true, "cluster": true, "text": true}

// Every command in the guide's blocks and prose is accepted by what runs it: an hpcburst command by
// the binary's -h, a hack/ script by the script, an hbk line by the CRDs and constants, a login-node
// line by Slurm's man pages. A bare flag in the prose belongs to the hpcburst subcommand or the
// kubectl verb named last before it.
func TestOperationsCommands(t *testing.T) {
	d, home := testDirs(t)
	crds := readCRDNames(t)
	// bare checks a bare flag of the prose against the hpcburst subcommand or the kubectl verb named
	// last before it; nil before the guide names one.
	var bare func(span string, f []string)
	hpcburstCtx := func(sub string) func(string, []string) {
		return func(_ string, f []string) { checkHpcburst(t, home, append([]string{sub}, f...)) }
	}
	hbkCtx := func(verb string) func(string, []string) {
		return func(span string, f []string) {
			for _, w := range f {
				name, _, _ := strings.Cut(w, "=")
				if _, ok := kubectlVerbs[verb].flags[name]; strings.HasPrefix(w, "-") && !ok {
					t.Errorf("the span %q holds %s, which kubectl %s, named last before it, does not take", span, name, verb)
				}
			}
		}
	}
	var hpcburstLines, hbkLines, scripts, clusterLines int
	for _, s := range opsSegments(t) {
		if !s.fenced {
			for _, m := range regexp.MustCompile("`([^`\n]+)`").FindAllStringSubmatch(s.text, -1) {
				f := strings.Fields(m[1])
				switch {
				case f[0] == "hpcburst" || f[0] == "bin/hpcburst":
					checkHpcburst(t, home, f[1:])
					if len(f) > 1 {
						bare = hpcburstCtx(f[1])
					}
				case f[0] == "hbk":
					checkHbk(t, m[1], shellWords(t, fillOps(m[1]), d, home), crds)
					if len(f) > 1 {
						bare = hbkCtx(f[1])
					}
				case strings.HasPrefix(f[0], "--"):
					if bare == nil {
						t.Errorf("the span %q holds a flag before the guide names any command", m[1])
						continue
					}
					bare(m[1], f)
				default:
					for _, w := range f[1:] {
						if strings.HasPrefix(w, "--") {
							t.Errorf("the span %q holds the flag %s, and the test cannot tell which command it belongs to: start the span with the command, or with the flag", m[1], w)
						}
					}
				}
			}
			continue
		}
		if !opsFenceLangs[s.lang] {
			t.Errorf("a block is fenced as %q, which the test does not know; use a plain fence for commands on the host, cluster or text", s.lang)
			continue
		}
		if s.lang == "text" {
			continue // output, checked by TestOperationsStoppedOutputIsTheBinarys
		}
		for _, l := range joinContinuations(s.text) {
			l = strings.TrimSpace(l)
			if l == "" || strings.HasPrefix(l, "#") {
				continue
			}
			prog := strings.Fields(l)[0]
			if s.lang == "cluster" {
				clusterLines++
				checkCluster(t, l, shellWords(t, fillOps(l), d, home))
				continue
			}
			switch {
			case strings.HasPrefix(l, "STATE=") || strings.HasPrefix(l, "BIN=") || strings.HasPrefix(l, "alias hbk="):
				// The definitions are the quickstart's, which its own tests compare with the launcher.
				if !strings.Contains(readRepoFile(t, "docs/quickstart.md"), "\n"+l+"\n") {
					t.Errorf("%q is not the quickstart's line", l)
				}
			case prog == "bin/hpcburst":
				c := command{line: l, words: shellWords(t, fillOps(l), d, home)}
				hpcburstLines++
				if len(c.words) < 2 {
					t.Errorf("%q names no subcommand", l)
					continue
				}
				checkHpcburst(t, home, c.words[1:])
				bare = hpcburstCtx(c.words[1])
			case strings.HasPrefix(prog, "hack/"):
				c := command{line: l, words: shellWords(t, fillOps(l), d, home)}
				if _, ok := scriptStops[c.words[0]]; !ok {
					t.Errorf("%q: the test has no way to run %s without side effects; add it to scriptStops", l, c.words[0])
					continue
				}
				scripts++
				runScript(t, home, c)
			case prog == "hbk":
				hbkLines++
				w := shellWords(t, fillOps(l), d, home)
				checkHbk(t, l, w, crds)
				if len(w) > 1 {
					bare = hbkCtx(w[1])
				}
			case prog == "go":
				checkGoBuild(t, l, shellWords(t, fillOps(l), d, home))
			default:
				t.Errorf("%q: the test cannot check a line that starts with %s", l, prog)
			}
		}
	}
	if hpcburstLines == 0 || hbkLines == 0 || scripts == 0 || clusterLines == 0 {
		t.Fatalf("found %d bin/hpcburst, %d hbk, %d hack/ and %d login-node lines in the guide's blocks; a changed fence style must not leave nothing to check", hpcburstLines, hbkLines, scripts, clusterLines)
	}
}

// checkGoBuild accepts the build line of the quickstart's step 2: go build -o bin/ and programs of
// this repository.
func checkGoBuild(t *testing.T, line string, words []string) {
	t.Helper()
	if len(words) < 5 || words[1] != "build" || words[2] != "-o" || words[3] != "bin/" {
		t.Errorf("%q: the test checks only go build -o bin/ <programs>", line)
		return
	}
	for _, p := range words[4:] {
		if fi, err := os.Stat(filepath.Join(root, p)); !strings.HasPrefix(p, "./cmd/") || err != nil || !fi.IsDir() {
			t.Errorf("%q builds %s, which is not a program of this repository", line, p)
		}
	}
}

// The two stopped reports the guide shows are what the binary prints, with the exit code the guide
// gives: the first for a state directory that does not exist, the second for one that exists and
// holds no record of a start (internal/launcher/status.go).
func TestOperationsStoppedOutputIsTheBinarys(t *testing.T) {
	var samples []string
	for _, s := range opsSegments(t) {
		if s.fenced && s.lang == "text" {
			samples = append(samples, s.text)
		}
	}
	if len(samples) != 2 {
		t.Fatalf("want two text blocks in %s, found %d", opsGuide, len(samples))
	}
	// A short path: the launcher refuses a state directory of 80 bytes or more, which t.TempDir's
	// names can reach.
	_, home := testDirs(t)
	empty := filepath.Join(home, "empty")
	if err := os.Mkdir(empty, 0o700); err != nil {
		t.Fatal(err)
	}
	for i, dir := range []string{filepath.Join(home, "none"), empty} {
		out, code := run(t, baseEnv(home), hpcburst(t), "status", "--state-dir", dir)
		if code != 3 {
			t.Errorf("status on %s exits %d; the guide says 3", dir, code)
		}
		if out != samples[i] {
			t.Errorf("status on %s prints\n%s\nand the guide shows\n%s", dir, out, samples[i])
		}
	}
	doc := readRepoFile(t, opsGuide)
	for _, want := range []string{"does not exist, it prints this and exits 3:", "the exit code is 3 too:"} {
		if !strings.Contains(doc, want) {
			t.Errorf("the guide no longer gives the exit code beside a sample (%q)", want)
		}
	}
}

// The tag form the guide gives for finding orphaned jobs after a restore is JobTag's: hb-, the first
// 40 characters of the Allocation's name, a hyphen and 12 hex characters.
func TestOperationsTagFormIsJobTags(t *testing.T) {
	name := strings.Repeat("a", 50)
	tag := v1alpha1.JobTag("default", name, "00000000-0000-4000-8000-000000000000")
	if !regexp.MustCompile("^hb-a{40}-[0-9a-f]{12}$").MatchString(tag) {
		t.Errorf("JobTag gives %q, which is not the form the guide states", tag)
	}
	if !strings.Contains(readRepoFile(t, opsGuide), "`hb-`, the first 40 characters of the Allocation's name, a hyphen and 12 hex characters") {
		t.Error("the guide no longer states the tag's form as the test checks it")
	}
}

// Every link of the guide leads to a file of the repository, and every anchor to a heading of its
// file, by GitHub's rule for heading anchors.
func TestOperationsLinksResolve(t *testing.T) {
	doc := readRepoFile(t, opsGuide)
	n := 0
	for _, m := range regexp.MustCompile(`\]\(([^)#]*)(#[^)]*)?\)`).FindAllStringSubmatch(doc, -1) {
		if strings.Contains(m[1], "://") {
			continue
		}
		n++
		target := opsGuide
		if m[1] != "" {
			target = filepath.Join("docs", m[1])
			if _, err := os.Stat(filepath.Join(root, target)); err != nil {
				t.Errorf("link %q: %v", m[1], err)
				continue
			}
		}
		if m[2] == "" {
			continue
		}
		if fi, err := os.Stat(filepath.Join(root, target)); err != nil || fi.IsDir() {
			t.Errorf("link %q: an anchor into a directory", m[1]+m[2])
			continue
		}
		if !anchors(t, target)[strings.TrimPrefix(m[2], "#")] {
			t.Errorf("link %q: %s has no heading with that anchor", m[1]+m[2], target)
		}
	}
	if n == 0 {
		t.Fatal("found no link in the guide")
	}
}

// anchors returns the anchors GitHub gives the headings of a Markdown file: lowercased, with every
// character but letters, digits, spaces, hyphens and underscores dropped, and spaces made hyphens.
func anchors(t *testing.T, rel string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	drop := regexp.MustCompile(`[^a-z0-9 _-]`)
	for _, m := range regexp.MustCompile(`(?m)^#+ (.+)$`).FindAllStringSubmatch(readRepoFile(t, rel), -1) {
		out[strings.ReplaceAll(drop.ReplaceAllString(strings.ToLower(m[1]), ""), " ", "-")] = true
	}
	return out
}
