package release

// Issue #401, PR B: the release workflow (.github/workflows/release.yml) and the one change to ci.yml
// that it needs, tested like ci.yml's own checks (hack/gotoolchain/ci_workflow_test.go). The workflows
// are read as YAML, and every expectation below is written out here, not taken from the workflow, so a
// change to a workflow and none to this file fails. The scripts the workflow calls are tested in
// scripts_test.go.

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// repoRoot is the repository's root, two directories above this package.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

type doc = map[string]any

func readWorkflow(t *testing.T, name string) doc {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	var d doc
	if err := yaml.Unmarshal(b, &d); err != nil {
		t.Fatalf("%s is not valid YAML: %v", name, err)
	}
	return d
}

// asMap and asList read a value of the YAML as the JSON the yaml package turns it into.
func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// triggers returns the on: map. YAML 1.1 reads an unquoted on as a boolean, which the yaml package
// writes as the key "true".
func triggers(t *testing.T, d doc) map[string]any {
	t.Helper()
	for _, k := range []string{"on", "true"} {
		if v, ok := d[k]; ok {
			m := asMap(v)
			if m == nil {
				t.Fatalf("on: is %T, not a map", v)
			}
			return m
		}
	}
	t.Fatal("the workflow has no on:")
	return nil
}

func jobsOf(t *testing.T, d doc) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for id, j := range asMap(d["jobs"]) {
		out[id] = asMap(j)
	}
	if len(out) == 0 {
		t.Fatal("the workflow has no jobs")
	}
	return out
}

func stepsOf(job map[string]any) []map[string]any {
	var out []map[string]any
	for _, s := range asList(job["steps"]) {
		out = append(out, asMap(s))
	}
	return out
}

// stepNamed returns the one step with that name.
func stepNamed(t *testing.T, job map[string]any, name string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, s := range stepsOf(job) {
		if str(s["name"]) == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d steps named %q, want 1", len(found), name)
	}
	return found[0]
}

// needs lists a job's needs, which YAML allows as a string or a list.
func needs(job map[string]any) []string {
	switch n := job["needs"].(type) {
	case string:
		return []string{n}
	case []any:
		var out []string
		for _, x := range n {
			out = append(out, str(x))
		}
		return out
	}
	return nil
}

// ---- a small evaluator for the if: expressions of the workflow ------------------------------------
//
// The release workflow's conditions are a few == and != comparisons of github.event_name and
// github.ref_type with strings, joined by && and ||. Evaluating them for each kind of run checks what the
// condition does, not how it is spelled: a run started by hand on a tag's ref has ref_type tag.

type ifParser struct {
	t    *testing.T
	toks []string
	i    int
	env  map[string]string
}

var ifToken = regexp.MustCompile(`\s*(==|!=|&&|\|\||\(|\)|'[^']*'|[A-Za-z_][A-Za-z0-9_.]*)`)

func evalIf(t *testing.T, expr string, env map[string]string) bool {
	t.Helper()
	expr = strings.TrimSpace(expr)
	expr = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expr, "${{"), "}}"))
	var toks []string
	for rest := expr; strings.TrimSpace(rest) != ""; {
		m := ifToken.FindStringSubmatchIndex(rest)
		if m == nil || m[0] != 0 {
			t.Fatalf("cannot read %q in the condition %q", rest, expr)
		}
		toks = append(toks, rest[m[2]:m[3]])
		rest = rest[m[1]:]
	}
	p := &ifParser{t: t, toks: toks, env: env}
	v := p.or()
	if p.i != len(toks) {
		t.Fatalf("the condition %q has %q left over", expr, toks[p.i:])
	}
	return v
}

func (p *ifParser) peek() string {
	if p.i < len(p.toks) {
		return p.toks[p.i]
	}
	return ""
}

func (p *ifParser) next() string {
	tok := p.peek()
	p.i++
	return tok
}

func (p *ifParser) or() bool {
	v := p.and()
	for p.peek() == "||" {
		p.next()
		r := p.and()
		v = v || r
	}
	return v
}

func (p *ifParser) and() bool {
	v := p.cmp()
	for p.peek() == "&&" {
		p.next()
		r := p.cmp()
		v = v && r
	}
	return v
}

func (p *ifParser) cmp() bool {
	if p.peek() == "(" {
		p.next()
		v := p.or()
		if p.next() != ")" {
			p.t.Fatalf("a condition lacks its closing parenthesis: %q", p.toks)
		}
		return v
	}
	l := p.operand()
	op := p.next()
	r := p.operand()
	switch op {
	case "==":
		return l == r
	case "!=":
		return l != r
	}
	p.t.Fatalf("the condition %q compares with %q, which this test does not read", p.toks, op)
	return false
}

func (p *ifParser) operand() string {
	tok := p.next()
	if strings.HasPrefix(tok, "'") {
		return strings.Trim(tok, "'")
	}
	v, ok := p.env[tok]
	if !ok {
		p.t.Fatalf("the condition %q reads %q, which this test does not set", p.toks, tok)
	}
	return v
}

// The four runs a release workflow can see, and what each must do.
var runs = []struct {
	name     string
	env      map[string]string
	releases bool // a tag push: the one run that checks the tag, calls CI and drafts a release
}{
	{"a tag push", map[string]string{"github.event_name": "push", "github.ref_type": "tag"}, true},
	{"a run started by hand on a branch", map[string]string{"github.event_name": "workflow_dispatch", "github.ref_type": "branch"}, false},
	{"a run started by hand on a tag's ref", map[string]string{"github.event_name": "workflow_dispatch", "github.ref_type": "tag"}, false},
	{"a push to a branch", map[string]string{"github.event_name": "push", "github.ref_type": "branch"}, false},
}

// ---- ci.yml ------------------------------------------------------------------------------------------

// ci.yml can be called, so that the release workflow runs the required checks rather than a copy of
// them, and no job is renamed for it: the five required checks of the branch protection (go, go
// own-timeout, tidy, crd and envtest, launcher-k3s) keep their names, each under its own job id.
func TestCICanBeCalledAndKeepsItsRequiredCheckNames(t *testing.T) {
	d := readWorkflow(t, "ci.yml")
	on := triggers(t, d)
	for _, trigger := range []string{"pull_request", "push", "workflow_dispatch", "workflow_call"} {
		if _, ok := on[trigger]; !ok {
			t.Errorf("ci.yml has no %s trigger; it has %v", trigger, keys(on))
		}
	}
	want := map[string]string{
		"go":           "go",
		"tidy":         "tidy",
		"crd":          "crd and envtest",
		"launcher-k3s": "launcher-k3s",
		"go-own":       "go own-timeout",
	}
	jobs := jobsOf(t, d)
	for id, name := range want {
		j, ok := jobs[id]
		if !ok {
			t.Errorf("ci.yml has no job %q", id)
			continue
		}
		if got := str(j["name"]); got != name {
			t.Errorf("ci.yml's job %q is named %q, want %q: a required check would never report", id, got, name)
		}
	}
}

func keys(m map[string]any) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ---- release.yml -----------------------------------------------------------------------------------

// It runs on pushed version tags and when started by hand, and on nothing else: no pull request can
// start a workflow that has a token able to write.
func TestReleaseRunsOnPushedVersionTagsAndByHandOnly(t *testing.T) {
	on := triggers(t, readWorkflow(t, "release.yml"))
	if len(on) != 2 {
		t.Errorf("the triggers are %v, want push and workflow_dispatch only", keys(on))
	}
	if _, ok := on["workflow_dispatch"]; !ok {
		t.Error("no workflow_dispatch trigger")
	}
	push := asMap(on["push"])
	if !reflect.DeepEqual(push, map[string]any{"tags": []any{"v*"}}) {
		t.Errorf("push is %v, want only tags: [v*]", push)
	}
}

// The workflow is read-only. The draft job alone has contents: write, and nothing else is writable in
// it; nothing in the workflow reads a secret.
func TestReleasePermissionsAreReadOnlyExceptTheDraftJob(t *testing.T) {
	d := readWorkflow(t, "release.yml")
	if got := d["permissions"]; !reflect.DeepEqual(got, map[string]any{"contents": "read"}) {
		t.Errorf("the workflow's permissions are %v, want only contents: read", got)
	}
	for id, j := range jobsOf(t, d) {
		got, has := j["permissions"]
		switch id {
		case "draft":
			if !reflect.DeepEqual(got, map[string]any{"contents": "write"}) {
				t.Errorf("the draft job's permissions are %v, want only contents: write", got)
			}
		default:
			if has && !reflect.DeepEqual(got, map[string]any{"contents": "read"}) {
				t.Errorf("job %q has permissions %v, want none or only contents: read", id, got)
			}
		}
		if _, ok := j["secrets"]; ok {
			t.Errorf("job %q passes secrets", id)
		}
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var writes int
	for _, line := range strings.Split(string(raw), "\n") {
		code, _, _ := strings.Cut(line, "#")
		if strings.Contains(code, "secrets.") || strings.Contains(code, "secrets:") {
			t.Errorf("the workflow reads a secret: %q", line)
		}
		if strings.Contains(code, ": write") || strings.Contains(code, ": admin") {
			writes++
		}
	}
	if writes != 1 {
		t.Errorf("the workflow has %d write permissions, want exactly the draft job's one", writes)
	}
}

// Only the draft step has the token, and the draft job runs no code of the repository.
func TestReleaseOnlyTheDraftStepHasTheTokenAndNoRepositoryCodeRunsInThatJob(t *testing.T) {
	jobs := jobsOf(t, readWorkflow(t, "release.yml"))
	for id, j := range jobs {
		for _, s := range stepsOf(j) {
			name, run := str(s["name"]), str(s["run"])
			usesToken := strings.Contains(fmt.Sprint(s["env"]), "github.token") || strings.Contains(run, "github.token")
			_, hasGHToken := asMap(s["env"])["GH_TOKEN"]
			isDraft := strings.Contains(run, "gh release create")
			if usesToken != isDraft || hasGHToken != isDraft {
				t.Errorf("job %q step %q: uses the job token %v, has GH_TOKEN %v, creates a release %v; the three go together", id, name, usesToken, hasGHToken, isDraft)
			}
			if id == "draft" {
				for _, banned := range []string{"hack/", "bash ", "go ", "make ", "./"} {
					if strings.Contains(run, banned) {
						t.Errorf("the draft job's step %q runs %q: code of the repository must not run beside the write token", name, banned)
					}
				}
			}
		}
	}
}

// Every action of every workflow is pinned by a full commit SHA, and the call of ci.yml is a local path.
func TestReleaseEveryActionIsPinnedByCommitSHA(t *testing.T) {
	pinned := regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}$`)
	local := regexp.MustCompile(`^\./\.github/workflows/[A-Za-z0-9_.-]+\.yml$`)
	files, err := filepath.Glob(filepath.Join(repoRoot(t), ".github", "workflows", "*.yml"))
	if err != nil || len(files) < 2 {
		t.Fatalf("workflow files %v, %v", files, err)
	}
	seen := map[string]bool{}
	for _, f := range files {
		d := readWorkflow(t, filepath.Base(f))
		for id, j := range jobsOf(t, d) {
			uses := []string{}
			if u := str(j["uses"]); u != "" {
				uses = append(uses, u)
			}
			for _, s := range stepsOf(j) {
				if u := str(s["uses"]); u != "" {
					uses = append(uses, u)
				}
			}
			for _, u := range uses {
				seen[filepath.Base(f)+" "+strings.SplitN(u, "@", 2)[0]] = true
				if !pinned.MatchString(u) && !local.MatchString(u) {
					t.Errorf("%s job %q uses %q: not pinned by a 40-hex commit SHA", filepath.Base(f), id, u)
				}
			}
		}
	}
	// So that the test cannot pass by finding nothing.
	for _, want := range []string{
		"ci.yml actions/checkout", "ci.yml actions/cache/restore",
		"release.yml actions/checkout", "release.yml actions/cache/restore", "release.yml actions/cache/save",
		"release.yml actions/upload-artifact", "release.yml actions/download-artifact", "release.yml ./.github/workflows/ci.yml",
	} {
		if !seen[want] {
			t.Errorf("no use of %q was found, so the pin check did not see it", want)
		}
	}
}

// The tag is checked before anything is built, with the one script, from a checkout that has the history
// of origin/main.
func TestReleaseChecksTheTagBeforeAnythingIsBuilt(t *testing.T) {
	jobs := jobsOf(t, readWorkflow(t, "release.yml"))
	check := jobs["check"]
	if check == nil {
		t.Fatal("no check job")
	}
	checkout := stepNamed(t, check, "Check out the repository")
	if str(checkout["uses"]) == "" || !strings.HasPrefix(str(checkout["uses"]), "actions/checkout@") {
		t.Errorf("the check job's checkout is %v", checkout["uses"])
	}
	with := asMap(checkout["with"])
	if with["fetch-depth"] != 0.0 && with["fetch-depth"] != 0 {
		t.Errorf("the check job's checkout has fetch-depth %v, want 0 (the history of origin/main, for is-ancestor)", with["fetch-depth"])
	}
	if with["persist-credentials"] != false {
		t.Errorf("the check job's checkout persists credentials: %v", with["persist-credentials"])
	}
	step := stepNamed(t, check, "Check the tag can be released")
	if got, want := strings.TrimSpace(str(step["run"])), `bash hack/release-check.sh "$TAG"`; got != want {
		t.Errorf("the check step runs %q, want %q", got, want)
	}
	if got, want := asMap(step["env"])["TAG"], "${{ github.ref_name }}"; got != want {
		t.Errorf("the check step's TAG is %v, want %q: the tag name reaches the script by env, not in the command line", got, want)
	}
	for _, r := range runs {
		if got := evalIf(t, str(step["if"]), r.env); got != r.releases {
			t.Errorf("the tag check runs for %s: %v, want %v", r.name, got, r.releases)
		}
	}
	if got := needs(jobs["archive"]); !reflect.DeepEqual(got, []string{"check"}) {
		t.Errorf("the archive job needs %v, want [check]", got)
	}
	if got := needs(jobs["ci"]); !reflect.DeepEqual(got, []string{"check"}) {
		t.Errorf("the ci job needs %v, want [check]", got)
	}
}

// CI is called, not copied, and only for a tag push.
func TestReleaseCallsCIThroughWorkflowCall(t *testing.T) {
	jobs := jobsOf(t, readWorkflow(t, "release.yml"))
	ci := jobs["ci"]
	if got := str(ci["uses"]); got != "./.github/workflows/ci.yml" {
		t.Errorf("the ci job uses %q, want ./.github/workflows/ci.yml", got)
	}
	if _, ok := ci["steps"]; ok {
		t.Error("the ci job has steps of its own: CI is called, not copied")
	}
	if got := ci["permissions"]; !reflect.DeepEqual(got, map[string]any{"contents": "read"}) {
		t.Errorf("the ci job's permissions are %v, want only contents: read", got)
	}
	for _, r := range runs {
		if got := evalIf(t, str(ci["if"]), r.env); got != r.releases {
			t.Errorf("CI is called for %s: %v, want %v", r.name, got, r.releases)
		}
	}
}

// The draft is made on a tag push and in no other run, after the check, CI and the archive have all
// passed; it is a draft; it carries the committed summary; and no other job or step runs gh at all.
func TestReleaseDraftIsMadeOnlyOnATagPush(t *testing.T) {
	jobs := jobsOf(t, readWorkflow(t, "release.yml"))
	draft := jobs["draft"]
	if draft == nil {
		t.Fatal("no draft job")
	}
	for _, r := range runs {
		if got := evalIf(t, str(draft["if"]), r.env); got != r.releases {
			t.Errorf("the draft job runs for %s: %v, want %v", r.name, got, r.releases)
		}
	}
	if got := needs(draft); !reflect.DeepEqual(got, []string{"check", "ci", "archive"}) {
		t.Errorf("the draft job needs %v, want [check ci archive]", got)
	}
	if _, ok := jobs["archive"]["if"]; ok {
		t.Errorf("the archive job has the condition %v: a run started by hand must build the archive", jobs["archive"]["if"])
	}

	var creating int
	for id, j := range jobs {
		for _, s := range stepsOf(j) {
			run := str(s["run"])
			if !strings.Contains(run, "gh ") {
				continue
			}
			if id != "draft" || !strings.Contains(run, "gh release create") {
				t.Errorf("job %q step %q runs gh: %q", id, str(s["name"]), run)
				continue
			}
			creating++
			for _, want := range []string{
				`gh release create "$TAG"`, "--draft", "--verify-tag", "--generate-notes", `--notes-file "docs/releases/$TAG.md"`,
				`"$RUNNER_TEMP"/release/hpcburst-*-linux-amd64.tar.gz`, `"$RUNNER_TEMP/release/SHA256SUMS"`,
			} {
				if !strings.Contains(run, want) {
					t.Errorf("the draft step lacks %q: %q", want, run)
				}
			}
			for _, banned := range []string{"--draft=false", "--latest", "--prerelease=false", "gh release edit", "gh release upload", "gh release delete", "git push", "git tag"} {
				if strings.Contains(run, banned) {
					t.Errorf("the draft step has %q: nothing is published or moved", banned)
				}
			}
			if got := asMap(s["env"])["TAG"]; got != "${{ github.ref_name }}" {
				t.Errorf("the draft step's TAG is %v, want ${{ github.ref_name }}", got)
			}
		}
	}
	if creating != 1 {
		t.Errorf("%d steps create a release, want 1", creating)
	}
}

// The archive job builds with the same pinned Go as CI (the same steps, word for word), calls the one
// script from outside the checkout, hands the tag to it only on a tag, and keeps the two files.
func TestReleaseArchiveJobBuildsWithTheScriptAndTheSameGoAsCI(t *testing.T) {
	jobs := jobsOf(t, readWorkflow(t, "release.yml"))
	ci := jobsOf(t, readWorkflow(t, "ci.yml"))["go"]
	archive := jobs["archive"]
	for _, name := range []string{
		"Read the Go version from go.mod",
		"Restore the cached Go archive",
		"Install the pinned Go toolchain",
		"Save the Go archive to the cache",
		"Check the Go on PATH is the pinned toolchain",
		"Download and verify the Go modules",
		"Go version",
	} {
		if !reflect.DeepEqual(stepNamed(t, archive, name), stepNamed(t, ci, name)) {
			t.Errorf("the archive job's step %q is not CI's go job's, word for word", name)
		}
	}
	checkout := asMap(stepNamed(t, archive, "Check out the repository")["with"])
	if checkout["fetch-depth"] != 0.0 && checkout["fetch-depth"] != 0 {
		t.Errorf("the archive job's checkout has fetch-depth %v, want 0 (Go finds the tag from the history)", checkout["fetch-depth"])
	}
	build := stepNamed(t, archive, "Build the archive")
	if got, want := asMap(build["env"])["TAG"], "${{ github.ref_type == 'tag' && github.ref_name || '' }}"; got != want {
		t.Errorf("the build step's TAG is %v, want %q", got, want)
	}
	run := str(build["run"])
	for _, want := range []string{`args+=(--tag "$TAG")`, `bash hack/release-archive.sh ${args[@]+"${args[@]}"} "$RUNNER_TEMP/release"`} {
		if !strings.Contains(run, want) {
			t.Errorf("the build step lacks %q: %q", want, run)
		}
	}
	upload := stepNamed(t, archive, "Keep the archive for the next job")
	with := asMap(upload["with"])
	if with["name"] != "release" || with["if-no-files-found"] != "error" {
		t.Errorf("the upload is %v", with)
	}
	if got, want := strings.TrimSpace(str(with["path"])), "${{ runner.temp }}/release/hpcburst-*-linux-amd64.tar.gz\n${{ runner.temp }}/release/SHA256SUMS"; got != want {
		t.Errorf("the upload's paths are %q, want %q", got, want)
	}
}
