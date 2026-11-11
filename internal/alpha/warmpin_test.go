package alpha

// The warm start's reason words (nowserve.go: StagedAlreadyVerified, StagedByAnother, StagedPresentMatching) pinned to
// what the stager really logs (issue #392, PR #393): the real internal/agent/stage, over a temporary staging directory,
// the engine double's fake Apptainer and the rehearsal's Hub double, its log captured and read by this package's own
// parser and seam. A cold staging and the agent's after-start verification (RehashOnly) show no warm start; a second,
// fresh stager over the same directory, configured as cmd/hb-agent configures its own, shows it. The reason a lock's
// re-check gives (StagedByAnother) needs two stagers racing for one digest's lock, so it is pinned to the stager's
// source instead: every format string the stager logs with one of the reason words holds the constant, and every
// constant is in one.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/stage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// pinLog is a stager's log that its writers share.
type pinLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *pinLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *pinLog) Bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.b.Bytes()...)
}

// pinFiles is the Hub double's checkpoint as an identity record lists it, built here from what the double serves: the
// listing's paths and the SHA-256 of each file's bytes, read over plain net/http, by none of the stager's code.
func pinFiles(t *testing.T, hubURL string) []agentapi.FileDigest {
	t.Helper()
	get := func(url string) []byte {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: %d %v", url, resp.StatusCode, err)
		}
		return b
	}
	var listing []struct{ Path, Type string }
	if err := json.Unmarshal(get(hubURL+"/api/models/"+stack.CheckpointRepo+"/tree/"+stack.CheckpointRev), &listing); err != nil {
		t.Fatal(err)
	}
	var files []agentapi.FileDigest
	for _, e := range listing {
		if e.Type == "file" {
			sum := sha256.Sum256(get(hubURL + "/" + stack.CheckpointRepo + "/resolve/" + stack.CheckpointRev + "/" + e.Path))
			files = append(files, agentapi.FileDigest{Path: e.Path, SHA256: hex.EncodeToString(sum[:])})
		}
	}
	if len(files) < 2 {
		t.Fatalf("the Hub double lists %d files", len(files))
	}
	return files
}

func TestTheWarmStartsReasonWordsAreTheStagersOwnLines(t *testing.T) {
	root := t.TempDir()
	staging, bin := filepath.Join(root, "staging"), filepath.Join(root, "bin")
	for _, d := range []string{staging, bin} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := enginedouble.Install(bin); err != nil {
		t.Fatal(err)
	}
	hub := stack.StartHub(t)
	files := pinFiles(t, hub.URL)
	const image = "example.org/engine/server"
	digest := "sha256:" + strings.Repeat("cd", 32)
	// A stager as cmd/hb-agent configures its own (cmd/hb-agent/main.go): the staging directory, Apptainer, the job's run
	// directory, the Hub, the environment and the log.
	newStager := func() (*stage.Stager, *pinLog) {
		run, err := os.MkdirTemp(root, "run")
		if err != nil {
			t.Fatal(err)
		}
		log := &pinLog{}
		s, err := stage.New(stage.Config{StagingDir: staging, Apptainer: filepath.Join(bin, "apptainer"), TmpDir: run, HubEndpoint: hub.URL, BaseEnv: os.Environ(), Log: log})
		if err != nil {
			t.Fatal(err)
		}
		return s, log
	}
	ctx := context.Background()
	req := agent.CheckpointRequest{Repository: stack.CheckpointRepo, Revision: stack.CheckpointRev, Files: files}
	var paths []string
	for _, f := range files {
		paths = append(paths, f.Path)
	}
	warm := func(log *pinLog) *WarmUnshownError {
		return warmStartShownByTheAgentsOwnLog(WarmObs{LogRead: true, ImageDigest: digest, Files: paths, Lines: ParseAgentStagingLines(log.Bytes())})
	}

	// Cold: the image is built and every file downloaded; then the agent's verification after the start, RehashOnly.
	cold, coldLog := newStager()
	if _, err := cold.Image(ctx, image, digest, agentapi.ImageFormatSandbox); err != nil {
		t.Fatal(err)
	}
	if _, err := cold.Checkpoint(ctx, req); err != nil {
		t.Fatal(err)
	}
	verify := req
	verify.RehashOnly = true
	if _, err := cold.Checkpoint(ctx, verify); err != nil {
		t.Fatal(err)
	}
	if lines := ParseAgentStagingLines(coldLog.Bytes()); len(lines) != 0 {
		t.Errorf("a cold staging and its verification logged %d lines with the reason words:\n%s", len(lines), coldLog.Bytes())
	}
	if e := warm(coldLog); e == nil || e.ImageShown || e.FilesShown != 0 {
		t.Errorf("a cold start reads as warm: %+v", e)
	}

	// Warm: a fresh stager over the same directory finds both staged.
	hot, hotLog := newStager()
	if _, err := hot.Image(ctx, image, digest, agentapi.ImageFormatSandbox); err != nil {
		t.Fatal(err)
	}
	if _, err := hot.Checkpoint(ctx, req); err != nil {
		t.Fatal(err)
	}
	if e := warm(hotLog); e != nil {
		t.Errorf("the warm staging is not shown by the stager's own lines: %+v\n%s", e, hotLog.Bytes())
	}
	text := string(hotLog.Bytes())
	for _, w := range []string{StagedAlreadyVerified, StagedPresentMatching} {
		if !strings.Contains(text, w) {
			t.Errorf("the stager's warm log lacks the reason words %q:\n%s", w, text)
		}
	}
	for _, l := range strings.Split(text, "\n") {
		if strings.Contains(l, staging) || strings.Contains(l, root) {
			t.Errorf("a line of the stager's log names a path: %q", l)
		}
	}
}

// Every format string internal/agent/stage logs (Stager.logf) with one of the reason words holds the constant, and each
// constant is in one: a change of #393's wording fails here, whichever line it is.
func TestTheWarmStartsReasonWordsAreInTheStagersFormats(t *testing.T) {
	dir := filepath.Join("..", "agent", "stage")
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var formats []string
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, e.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); !ok || sel.Sel.Name != "logf" {
				return true
			}
			if lit, ok := call.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					formats = append(formats, s)
				}
			}
			return true
		})
	}
	want := map[string]string{StagedAlreadyVerified: "image %s: " + StagedAlreadyVerified, StagedByAnother: "image %s: " + StagedByAnother,
		StagedPresentMatching: "checkpoint file %s: " + StagedPresentMatching}
	for reason, format := range want {
		if !slices.Contains(formats, format) {
			t.Errorf("the stager logs no %q (the reason %q); its formats: %q", format, reason, formats)
		}
	}
	for _, f := range formats {
		for _, loose := range []string{"already staged", "another Allocation", "present and"} {
			if strings.Contains(f, loose) && !slices.Contains([]string{want[StagedAlreadyVerified], want[StagedByAnother], want[StagedPresentMatching]}, f) {
				t.Errorf("the stager logs %q, near the reason words and not one of the pinned lines", f)
			}
		}
	}
}
