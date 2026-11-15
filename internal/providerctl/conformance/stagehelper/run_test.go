package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// okLine is whether a line is the success line, by the one parser of it that the pre-staging row and the stand-ins read it
// by too (internal/alpha, ParseStageResult), so the line this helper writes and theirs cannot drift.
func okLine(line string) bool {
	r, err := alpha.ParseStageResult([]byte(line + "\n"))
	return err == nil && r.OK
}

func TestSuccessLineHasTheFixedForm(t *testing.T) {
	hub := stack.StartHub(t)
	fx := newFixture(t, hub.URL)
	files := expectedFiles(t, hub.URL)

	code, errs := fx.run(t, goodInputs())
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errs)
	}
	line := fx.result(t)
	if !okLine(line) {
		t.Fatalf("result line %q is not the fixed form", line)
	}
	// The counts, against the double's own list (not against the staging).
	// A first, cold run rehashes nothing: no listed file was present before its downloads (issue #575).
	for _, w := range []string{"image_built=true", " files=" + strconv.Itoa(len(files)) + " ", "mismatched=0",
		" rehash_files=0 rehash_listed=" + strconv.Itoa(len(files)) + " rehash_bytes=0 "} {
		if !strings.Contains(line, w) {
			t.Errorf("result line %q lacks %q", line, w)
		}
	}
	if strings.Contains(line, "/") {
		t.Errorf("result line %q holds a slash: a path, a URL or a host", line)
	}
	if _, err := os.Stat(filepath.Join(fx.alloc, resultName+".part")); !os.IsNotExist(err) {
		t.Errorf("the temporary result file is left behind: %v", err)
	}
}

// A second run finds both warm: its line says no build, which the fake
// Apptainer's own log confirms (the first run's one build is the only one).
func TestSecondRunDoesNotBuildAgain(t *testing.T) {
	hub := stack.StartHub(t)
	fx := newFixture(t, hub.URL)
	for i := 0; i < 2; i++ {
		if code, errs := fx.run(t, goodInputs()); code != 0 {
			t.Fatalf("run %d exited %d: %s", i+1, code, errs)
		}
	}
	line := fx.result(t)
	if !strings.Contains(line, "image_built=false") {
		t.Errorf("the second run's line is %q, want image_built=false", line)
	}
	// The second run rehashes every listed file the first one staged (issue #575): as many as listed, some bytes.
	m := regexp.MustCompile(` rehash_files=(\d+) rehash_listed=(\d+) rehash_bytes=(\d+) `).FindStringSubmatch(line)
	if m == nil || m[1] != m[2] || m[1] == "0" || m[3] == "0" {
		t.Errorf("the second run's line is %q, want every listed file rehashed, with their bytes", line)
	}
	if calls := apptainerCalls(t, fx); !slices.Equal(calls, []string{"build"}) {
		t.Errorf("the fake Apptainer recorded %v over two runs, want one build", calls)
	}
}

// A file that is present and does not match is counted and left as it is: the
// agent's first start does not restage (CheckpointRequest.Restage is for a
// whole-Instance restart).
func TestMismatchedFileIsCountedAndNotReplaced(t *testing.T) {
	hub := stack.StartHub(t)
	fx := newFixture(t, hub.URL)
	dir := filepath.Join(fx.staging, agentapi.StagedCheckpointDir(stack.CheckpointRepo, stack.CheckpointRev))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	tampered := []byte("{\"tampered\":true}\n")
	if err := os.WriteFile(filepath.Join(dir, "config.json"), tampered, 0o640); err != nil {
		t.Fatal(err)
	}

	if code, errs := fx.run(t, goodInputs()); code != 0 {
		t.Fatalf("exit %d: %s", code, errs)
	}
	if line := fx.result(t); !strings.Contains(line, "outcome=ok") || !strings.Contains(line, " mismatched=1 ") {
		t.Errorf("result line %q, want outcome=ok and mismatched=1", line)
	}
	got, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil || !bytes.Equal(got, tampered) {
		t.Errorf("the mismatched file was replaced: %q, %v", got, err)
	}
}

// listingOnly is a Hub whose listing holds one weights file in LFS, so that the
// listing succeeds, and which answers no download.
func listingOnly(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/models/"+stack.CheckpointRepo+"/tree/"+stack.CheckpointRev {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"type":"file","oid":"` + strings.Repeat("c", 40) + `","size":1024,"path":"model.safetensors",` +
			`"lfs":{"oid":"` + strings.Repeat("d", 64) + `","size":1024,"pointerSize":135}}]`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// Every failure writes the step it came to and nothing else, exits 0, and the
// line holds no path.
func TestFailureLineNamesTheStep(t *testing.T) {
	failingApptainer := func(t *testing.T, fx *fixture) {
		p := filepath.Join(fx.bin, "failing-apptainer")
		if err := os.WriteFile(p, []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		fx.env["HPCBURST_APPTAINER"] = p
	}
	cases := []struct {
		name string
		hub  func(t *testing.T) string // the Hub agent.json names; the Hub double when nil
		set  func(t *testing.T, fx *fixture, in *inputs)
		step string
	}{
		{name: "no apptainer", step: "config", set: func(t *testing.T, fx *fixture, _ *inputs) {
			fx.env["HPCBURST_APPTAINER"] = ""
			t.Setenv("PATH", fx.bin+"/none") // a directory without one
		}},
		{name: "empty image", step: "config", set: func(_ *testing.T, _ *fixture, in *inputs) { in.image = "" }},
		{name: "empty image digest", step: "config", set: func(_ *testing.T, _ *fixture, in *inputs) { in.imageDigest = "" }},
		{name: "empty checkpoint repository", step: "config", set: func(_ *testing.T, _ *fixture, in *inputs) { in.checkpointRepo = "" }},
		{name: "empty checkpoint revision", step: "config", set: func(_ *testing.T, _ *fixture, in *inputs) { in.checkpointRev = "" }},
		{name: "empty load format", step: "config", set: func(_ *testing.T, _ *fixture, in *inputs) { in.loadFormat = "" }},
		{name: "no SLURM_JOB_ID", step: "config", set: func(_ *testing.T, fx *fixture, _ *inputs) { delete(fx.env, "SLURM_JOB_ID") }},
		{name: "an image build that fails", step: "image", set: func(t *testing.T, fx *fixture, _ *inputs) { failingApptainer(t, fx) }},
		{name: "a Hub that answers 404", step: "files",
			hub: func(t *testing.T) string {
				srv := httptest.NewServer(http.NotFoundHandler())
				t.Cleanup(srv.Close)
				return srv.URL
			}},
		{name: "a download the Hub refuses", step: "checkpoint", hub: listingOnly},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hubURL := stack.StartHub(t).URL
			if c.hub != nil {
				hubURL = c.hub(t)
			}
			fx := newFixture(t, hubURL)
			in := goodInputs()
			if c.set != nil {
				c.set(t, fx, &in)
			}
			code, errs := fx.run(t, in)
			if code != 0 {
				t.Fatalf("exit %d, want 0 once the line is written: %s", code, errs)
			}
			line := fx.result(t)
			if want := "outcome=error step=" + c.step; line != want {
				t.Errorf("result line %q, want %q (stderr: %s)", line, want, errs)
			}
			if strings.Contains(line, "/") {
				t.Errorf("result line %q holds a slash", line)
			}
			if errs == "" {
				t.Errorf("the error went nowhere: stderr is empty")
			}
		})
	}
}

// A call without exactly one argument is a usage error: it exits 2 and writes
// nothing.
func TestUsageErrorExits2(t *testing.T) {
	for _, args := range [][]string{nil, {"a", "b"}} {
		var errs syncBuf
		if code := runInputs(t.Context(), goodInputs(), args, func(string) string { return "" }, &errs); code != 2 {
			t.Errorf("%d arguments: exit %d, want 2", len(args), code)
		}
		if !strings.Contains(errs.String(), "usage") {
			t.Errorf("%d arguments: no usage on stderr: %q", len(args), errs.String())
		}
	}
}

// With no directory to write the line in, the helper exits 1: the one case in
// which the job's exit code says the line is missing.
func TestUnwritableResultExits1(t *testing.T) {
	fx := newFixture(t, "http://127.0.0.1:1")
	var errs syncBuf
	missing := filepath.Join(fx.root, "no-such-directory")
	if code := runInputs(t.Context(), goodInputs(), []string{missing}, fx.getenv, &errs); code != 1 {
		t.Errorf("exit %d, want 1: %s", code, errs.String())
	}
}

// run reads the compiled-in variables: with them set, as -ldflags -X sets them,
// it stages; with one empty, it says config.
func TestRunReadsTheCompiledInVariables(t *testing.T) {
	hub := stack.StartHub(t)
	fx := newFixture(t, hub.URL)
	g := goodInputs()
	for _, v := range []struct {
		p    *string
		want string
	}{{&image, g.image}, {&imageDigest, g.imageDigest}, {&checkpointRepo, g.checkpointRepo}, {&checkpointRev, g.checkpointRev}, {&loadFormat, g.loadFormat}} {
		old := *v.p
		*v.p = v.want
		t.Cleanup(func() { *v.p = old })
	}
	var errs syncBuf
	if code := run([]string{fx.alloc}, fx.getenv, &errs); code != 0 {
		t.Fatalf("exit %d: %s", code, errs.String())
	}
	if line := fx.result(t); !okLine(line) {
		t.Errorf("result line %q is not the fixed success form (stderr: %s)", line, errs.String())
	}
}
