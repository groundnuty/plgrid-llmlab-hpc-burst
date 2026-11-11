package stage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Issue 392 (`@hpc-burst-science-agent`, 11 Nov 2026, for #376's warm-start
// row): the stager says when an image or a checkpoint file was ALREADY staged,
// so that a row can prove a warm start from the agent's own log.
//
//   - "already staged and verified": Image's first check passed;
//   - "staged by another Allocation": Image's re-check after the lock passed;
//   - "present and matching": a checkpoint file found present with the right
//     hash before any repair.
//
// Each line names its digest or its checkpoint-relative file and the reason,
// and nothing else: no staging root, no absolute path. The lines are written
// out literally below, with the reason words as they are meant to be quoted,
// never built by the format the product uses. The tests that matter most are
// the ones where the evidence would lie: a fresh pull or build, a repaired or
// downloaded file and a verification's rehash must log none of the three.

// warmReasons are the three reasons, as literal words.
var warmReasons = []string{"already staged and verified", "staged by another Allocation", "present and matching"}

// logBuf is the stager's log, safe to read while Apptainer's output and a
// second stager write to it.
type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.Reset()
}

// warm returns the log lines that carry one of the three reasons.
func (l *logBuf) warm() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range strings.Split(l.b.String(), "\n") {
		for _, r := range warmReasons {
			if strings.Contains(line, r) {
				out = append(out, line)
				break
			}
		}
	}
	return out
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// withLog gives the stager a log it can read.
func withLog(s *Stager) *logBuf {
	l := &logBuf{}
	s.c.Log = l
	return l
}

// wantWarm fails the test unless the log's lines with a reason are exactly want.
func wantWarm(t *testing.T, step string, l *logBuf, want ...string) {
	t.Helper()
	got := l.warm()
	if len(got) != len(want) {
		t.Errorf("%s: %d lines with a reason, want %d:\n got  %q\n want %q", step, len(got), len(want), got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: line %d is %q, want %q", step, i+1, got[i], want[i])
		}
	}
}

// wantNoPath fails the test when a line names the staging root or holds a
// slash outside the checkpoint-relative file name it is given (empty for an
// image line, whose digest has no slash).
func wantNoPath(t *testing.T, root string, lines []string, name string) {
	t.Helper()
	if len(lines) == 0 {
		t.Error("no line to check for a path: the check would prove nothing")
	}
	for _, line := range lines {
		if strings.Contains(line, root) {
			t.Errorf("the line %q names the staging root %q", line, root)
		}
		rest := line
		if name != "" {
			if !strings.Contains(line, name) {
				t.Errorf("the line %q does not name %q", line, name)
			}
			rest = strings.Replace(line, name, "", 1)
		}
		if strings.Contains(rest, "/") {
			t.Errorf("the line %q holds a slash outside the file name %q", line, name)
		}
	}
}

// waitUntil waits for an observable state, with a generous deadline.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("gave up waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// An image whose first check passes says so, once per call, with its digest.
// The first staging of the same image says nothing of the kind.
func TestImageAlreadyStagedAndVerifiedIsLogged(t *testing.T) {
	for _, format := range []agentapi.ImageFormat{agentapi.ImageFormatSIF, agentapi.ImageFormatSandbox} {
		t.Run(string(format), func(t *testing.T) {
			s, _, root := newStager(t, nil)
			l := withLog(s)
			const line = "stage: image sha256:2222222222222222222222222222222222222222222222222222222222222222: already staged and verified"

			stageImage(t, s, format)
			wantWarm(t, "the first staging", l)

			stageImage(t, s, format)
			wantWarm(t, "the second call", l, line)
			wantNoPath(t, root, l.warm(), "")

			stageImage(t, s, format)
			wantWarm(t, "the third call", l, line, line)
		})
	}
}

// The case where the evidence would lie: an image pulled or built now, and one
// that failed its check and was pulled or built again, log no warm start, and
// not "staged by another Allocation" either.
func TestImagePulledOrBuiltNowLogsNoWarmStart(t *testing.T) {
	for _, tc := range []struct {
		format agentapi.ImageFormat
		damage func(t *testing.T, path string)
	}{
		{agentapi.ImageFormatSIF, func(t *testing.T, sif string) {
			if err := os.Truncate(sif, 5); err != nil {
				t.Fatal(err)
			}
		}},
		{agentapi.ImageFormatSandbox, func(t *testing.T, sb string) {
			if err := os.Remove(filepath.Join(sb, ".fake-sandbox")); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			s, _, root := newStager(t, nil)
			l := withLog(s)

			first := stageImage(t, s, tc.format)
			if n := len(apptainerCalls(t, root)); n != 1 {
				t.Fatalf("%d pulls or builds for a fresh image, want 1", n)
			}
			wantWarm(t, "a fresh pull or build", l)

			tc.damage(t, first.Path)
			l.reset()
			stageImage(t, s, tc.format)
			if n := len(apptainerCalls(t, root)); n != 2 {
				t.Fatalf("%d pulls or builds after the damage, want 2", n)
			}
			wantWarm(t, "a damaged image pulled or built again", l)
			if !strings.Contains(l.String(), "failed its check") {
				t.Errorf("the log does not show the damaged image's failed check, so the damage did not take: %q", l.String())
			}
		})
	}
}

// An Allocation that waits for the digest's lock while another pulls, then
// finds the image intact, says so with the digest. The one that pulled says
// nothing of the kind. Which Allocation pulls is fixed by starting the second
// only once the first's pull is running.
func TestImageStagedByAnotherAllocationIsLogged(t *testing.T) {
	s1, _, root := newStager(t, nil)
	l1 := withLog(s1)
	s2, err := New(s1.c)
	if err != nil {
		t.Fatal(err)
	}
	l2 := withLog(s2)
	busy, once := make(chan struct{}), sync.Once{}
	s2.hooks.lockBusy = func() { once.Do(func() { close(busy) }) }
	pulls, gate := filepath.Join(root, "pulls"), filepath.Join(root, "gate")
	scriptApptainer(t, root, fmt.Sprintf(`echo pull >> %s
while [ ! -e %s ]; do sleep 0.02; done
printf 'fake-sif\n' > "$2"
`, pulls, gate))

	var wg sync.WaitGroup
	errs := make([]error, 2)
	run := func(i int, s *Stager) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = s.Image(context.Background(), vllmImage, digest, agentapi.ImageFormatSIF)
		}()
	}
	run(0, s1)
	waitUntil(t, "the first Allocation's pull to start", func() bool { _, err := os.Stat(pulls); return err == nil })
	run(1, s2)
	select {
	case <-busy:
	case <-time.After(10 * time.Second):
		t.Error("the second Allocation never waited for the first's lock")
	}
	os.WriteFile(gate, nil, 0o600)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("Allocation %d: %v", i+1, err)
		}
	}
	if b, _ := os.ReadFile(pulls); strings.Count(string(b), "pull") != 1 {
		t.Fatalf("the pulls: %q, want exactly one, or the test does not show the wait", b)
	}

	wantWarm(t, "the Allocation that pulled", l1)
	const line = "stage: image sha256:2222222222222222222222222222222222222222222222222222222222222222: staged by another Allocation"
	wantWarm(t, "the Allocation that waited", l2, line)
	wantNoPath(t, root, l2.warm(), "")
}

// Files found present with the right hash, before any repair, are logged by
// their checkpoint-relative names, in the order listed. The same files
// downloaded by the first call are not.
func TestCheckpointFilesAlreadyPresentAndMatchingAreLogged(t *testing.T) {
	s, _, root := newStager(t, map[string]string{
		resolvePath("config.json"):        "hello weights\n",
		resolvePath("tok/tokenizer.json"): "hello weights\n",
		resolvePath("model.safetensors"):  "hello weights\n",
	})
	l := withLog(s)
	req := agent.CheckpointRequest{Repository: "org/model", Revision: rev, Files: []agentapi.FileDigest{
		{Path: "config.json", SHA256: weightsSum},
		{Path: "tok/tokenizer.json", SHA256: weightsSum},
		{Path: "model.safetensors", SHA256: weightsSum},
	}}

	res, err := s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range res.Files {
		if f.Outcome != agentapi.FileMatch {
			t.Fatalf("the first call: %+v", f)
		}
	}
	wantWarm(t, "the first call, which downloads", l)

	res, err = s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range res.Files {
		if f.Outcome != agentapi.FileMatch {
			t.Fatalf("the second call: %+v", f)
		}
	}
	wantWarm(t, "the second call", l,
		"stage: checkpoint file config.json: present and matching",
		"stage: checkpoint file tok/tokenizer.json: present and matching",
		"stage: checkpoint file model.safetensors: present and matching")
	lines := l.warm()
	names := []string{"config.json", "tok/tokenizer.json", "model.safetensors"}
	if len(lines) != len(names) {
		return // wantWarm has failed the test; nothing to take a path from
	}
	for i, name := range names {
		wantNoPath(t, root, lines[i:i+1], name)
	}
}

// The case where the evidence would lie: a file that was changed and is put
// right by a restage, a file that was missing and is downloaded, and a changed
// file left alone (no restage) are not "present and matching". Only the intact
// file is, and a verification's rehash (RehashOnly), which finds a cold
// start's downloads present and matching, logs nothing.
func TestCheckpointRepairedAndDownloadedFilesLogNoWarmStart(t *testing.T) {
	s, _, root := newStager(t, map[string]string{
		resolvePath("changed.json"):      "hello weights\n",
		resolvePath("model.safetensors"): "hello weights\n",
	})
	l := withLog(s)
	dir := ckptDir(s)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "intact.json"), []byte("hello weights\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "changed.json"), []byte("tampered\n"), 0o600)
	req := agent.CheckpointRequest{Repository: "org/model", Revision: rev, Files: []agentapi.FileDigest{
		{Path: "intact.json", SHA256: weightsSum},
		{Path: "changed.json", SHA256: weightsSum},
		{Path: "model.safetensors", SHA256: weightsSum},
	}}
	const intact = "stage: checkpoint file intact.json: present and matching"

	// No restage: the changed file stays a mismatch, the missing one is downloaded.
	res, err := s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	o := outcomes(res)
	if o["intact.json"].Outcome != agentapi.FileMatch || o["changed.json"].Outcome != agentapi.FileMismatch || o["model.safetensors"].Outcome != agentapi.FileMatch {
		t.Fatalf("outcomes without a restage: %+v", o)
	}
	wantWarm(t, "no restage: a mismatch and a download", l, intact)
	wantNoPath(t, root, l.warm(), "intact.json")

	// A restage puts the changed file right: a repair, not a warm start.
	l.reset()
	req.Restage = true
	res, err = s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	o = outcomes(res)
	if o["changed.json"].Outcome != agentapi.FileMatch {
		t.Fatalf("the restage did not repair changed.json: %+v", o["changed.json"])
	}
	wantWarm(t, "the restage that repairs", l, intact, "stage: checkpoint file model.safetensors: present and matching")

	// A cold start: everything is downloaded (nothing logged), then the
	// verification rehashes it (RehashOnly) and logs nothing either.
	cold, _, _ := newStager(t, map[string]string{resolvePath("model.safetensors"): "hello weights\n"})
	lc := withLog(cold)
	creq := agent.CheckpointRequest{Repository: "org/model", Revision: rev, Files: []agentapi.FileDigest{{Path: "model.safetensors", SHA256: weightsSum}}}
	if _, err := cold.Checkpoint(context.Background(), creq); err != nil {
		t.Fatal(err)
	}
	wantWarm(t, "a cold start's download", lc)
	creq.RehashOnly = true
	res, err = cold.Checkpoint(context.Background(), creq)
	if err != nil {
		t.Fatal(err)
	}
	if f := res.Files[0]; f.Outcome != agentapi.FileMatch {
		t.Fatalf("the rehash: %+v", f)
	}
	wantWarm(t, "the verification's rehash after a cold start", lc)
}

// The line a failed check leaves (it existed before issue 392, and carried the
// staged path; `@hpc-burst-science-agent` ruled on 11 Nov 2026 that it names
// the digest instead): the digest, the reason and the error, with the error's
// own paths hidden. An image path is <image> and its record's path <record>.
// The expected text is literal, down to the operating system's wording for a
// missing file.
func TestImageFailedCheckLogsItsDigestAndNoPath(t *testing.T) {
	const prefix = "stage: image sha256:2222222222222222222222222222222222222222222222222222222222222222: failed its check ("
	const suffix = "): staging it again"
	for _, tc := range []struct {
		name   string
		format agentapi.ImageFormat
		damage func(t *testing.T, path string)
		detail string // the error's text, which must start the parentheses
		exact  bool   // the parentheses hold nothing else
	}{
		{"SIF sidecar removed: an operating system error names its path", agentapi.ImageFormatSIF, func(t *testing.T, sif string) {
			if err := os.Remove(sif + ".sha256"); err != nil {
				t.Fatal(err)
			}
		}, "open <record>: no such file or directory", true},
		{"SIF sidecar garbled: our own message names its path", agentapi.ImageFormatSIF, func(t *testing.T, sif string) {
			os.WriteFile(sif+".sha256", []byte("not json"), 0o640)
		}, "record <record>: ", false},
		{"SIF sidecar names another image", agentapi.ImageFormatSIF, func(t *testing.T, sif string) {
			b, _ := os.ReadFile(sif + ".sha256")
			other := strings.Replace(string(b), digest, "sha256:"+strings.Repeat("3", 64), 1)
			if other == string(b) {
				t.Fatal("the sidecar does not hold the digest")
			}
			os.WriteFile(sif+".sha256", []byte(other), 0o640)
		}, "record <record> is not a hpcburst-sif/1 record for " + digest, true},
		{"sandbox manifest removed", agentapi.ImageFormatSandbox, func(t *testing.T, sb string) {
			if err := os.Remove(sb + ".manifest"); err != nil {
				t.Fatal(err)
			}
		}, "open <record>: no such file or directory", true},
		{"sandbox manifest truncated", agentapi.ImageFormatSandbox, func(t *testing.T, sb string) {
			b, _ := os.ReadFile(sb + ".manifest")
			i := strings.LastIndex(strings.TrimSuffix(string(b), "\n"), "\n")
			os.WriteFile(sb+".manifest", b[:i+1], 0o640)
		}, "manifest <record> ends without its count", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, root := newStager(t, nil)
			l := withLog(s)
			first := stageImage(t, s, tc.format)
			tc.damage(t, first.Path)
			l.reset()
			stageImage(t, s, tc.format) // pulled or built again

			var lines []string
			for _, line := range strings.Split(l.String(), "\n") {
				if strings.Contains(line, "failed its check") {
					lines = append(lines, line)
				}
			}
			if len(lines) != 1 {
				t.Fatalf("%d lines with the failed check, want 1: %q", len(lines), lines)
			}
			line := lines[0]
			want := prefix + tc.detail
			if tc.exact {
				want += suffix
			}
			if !strings.HasPrefix(line, want) || !strings.HasSuffix(line, suffix) {
				t.Errorf("the line is %q, want it to start %q and end %q", line, want, suffix)
			}
			if strings.Contains(line, root) {
				t.Errorf("the line %q names the staging root %q", line, root)
			}
			// The format's own name, hpcburst-sif/1, holds a slash; nothing else may.
			if strings.Contains(strings.ReplaceAll(line, "hpcburst-sif/1", ""), "/") {
				t.Errorf("the line %q holds a slash outside the format's name", line)
			}
		})
	}
}

// verifyImage's errors hide the staging paths wherever they come from, and
// stay reachable by errors.Is: an operating system error from lstat, and one
// from walking a sandbox whose directory cannot be read.
func TestVerifyImageErrorsNameNoStagingPath(t *testing.T) {
	t.Run("lstat in a path whose parent is a file", func(t *testing.T) {
		s, _, root := newStager(t, nil)
		if err := os.MkdirAll(s.c.StagingDir, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.c.StagingDir, "images"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		final := filepath.Join(s.c.StagingDir, agentapi.StagedImagePath(digest, agentapi.ImageFormatSIF))
		err := s.verifyImage(context.Background(), final, digest, agentapi.ImageFormatSIF)
		if err == nil {
			t.Fatal("no error for an image below a file")
		}
		if got, want := err.Error(), "lstat <image>: not a directory"; got != want {
			t.Errorf("the error says %q, want %q", got, want)
		}
		if strings.Contains(err.Error(), root) {
			t.Errorf("the error %q names the staging root", err)
		}
		if !errors.Is(err, syscall.ENOTDIR) {
			t.Errorf("the error %q no longer wraps the operating system's", err)
		}
	})
	t.Run("a sandbox directory that cannot be read", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads every directory")
		}
		s, _, root := newStager(t, nil)
		res := stageImage(t, s, agentapi.ImageFormatSandbox)
		sub := filepath.Join(res.Path, "sub")
		if err := os.Mkdir(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(sub, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(sub, 0o755) })
		err := s.verifyImage(context.Background(), res.Path, digest, agentapi.ImageFormatSandbox)
		if err == nil {
			t.Fatal("no error for a sandbox with an unreadable directory")
		}
		if got, want := err.Error(), "open <image>/sub: permission denied"; got != want {
			t.Errorf("the error says %q, want %q", got, want)
		}
		if strings.Contains(err.Error(), root) {
			t.Errorf("the error %q names the staging root", err)
		}
		if !errors.Is(err, fs.ErrPermission) {
			t.Errorf("the error %q no longer wraps the operating system's", err)
		}
	})
}
