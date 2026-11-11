package stage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Issue 394 (`@hpc-burst-science-agent`, 11 Nov 2026, a follow-up to issue 392):
// no line the stager logs names a staging path. An agent's log can reach
// places a post never does. The lines that still did, one test each:
//
//   - putting an image back, removing an image's copy moved aside, and
//     removing a stale temporary output: named by the digest;
//   - putting a checkpoint file back, removing its copy moved aside, and
//     removing a stale download: named by the file's place in the checkpoint;
//   - the flock notice: the lock file's name;
//   - the notice that no sample of the temporary directory succeeded: no name;
//   - the removal of a run's temporary tree: the tree's own name.
//
// Where an operating system error held a path, it reads <aside>, <image>,
// <file>, <temp>, <part> or <tree> instead. The expected lines are literal
// where the operating system's wording is fixed by the call, and otherwise
// checked by their start and their end; every one is checked for the staging
// root and for a slash outside the checkpoint-relative name.

// skipAsRoot skips a test that needs a directory root could still write to.
func skipAsRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root writes to every directory")
	}
}

// oneLine returns the log's single line that holds sub.
func oneLine(t *testing.T, l *logBuf, sub string) string {
	t.Helper()
	var found []string
	for _, line := range strings.Split(l.String(), "\n") {
		if strings.Contains(line, sub) {
			found = append(found, line)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d log lines hold %q, want 1: %q", len(found), sub, found)
	}
	return found[0]
}

// wantLineShape fails the test unless the line starts and ends as given, and
// names neither the staging root nor a slash outside name.
func wantLineShape(t *testing.T, root, line, start, end, name string) {
	t.Helper()
	if !strings.HasPrefix(line, start) || !strings.HasSuffix(line, end) {
		t.Errorf("the line is %q, want it to start %q and end %q", line, start, end)
	}
	wantNoPath(t, root, []string{line}, name)
}

// An image that failed its check and could not be put back after a failed pull:
// the line names the digest, and the rename's two paths read <aside> and
// <image>. The script takes the write permission off images/ before it fails,
// so the move back is refused.
func TestImagePutBackFailureIsLoggedWithoutPaths(t *testing.T) {
	skipAsRoot(t)
	s, _, root := newStager(t, nil)
	l := withLog(s)
	first := stageImage(t, s, agentapi.ImageFormatSIF)
	images := filepath.Dir(first.Path)
	os.Truncate(first.Path, 3)
	t.Cleanup(func() { os.Chmod(images, 0o700) })
	scriptApptainer(t, root, fmt.Sprintf("chmod 500 %s\nexit 1\n", images))
	l.reset()
	if _, err := s.Image(context.Background(), vllmImage, digest, agentapi.ImageFormatSIF); err == nil {
		t.Fatal("the pull failed but Image succeeded")
	}
	line := oneLine(t, l, "putting the image back")
	const want = "stage: image sha256:2222222222222222222222222222222222222222222222222222222222222222: putting the image back: rename <aside> <image>: permission denied"
	if line != want {
		t.Errorf("the line is %q, want %q", line, want)
	}
	wantNoPath(t, root, []string{line}, "")
}

// The copy of an image moved aside cannot be removed once its replacement is
// in place (no operating-system state fails that one removal, so the test sets
// hooks.removeAside): the line names the digest and the copy reads <aside>.
func TestImageRemoveAsideFailureIsLoggedWithoutPaths(t *testing.T) {
	s, _, root := newStager(t, nil)
	l := withLog(s)
	first := stageImage(t, s, agentapi.ImageFormatSIF)
	os.Truncate(first.Path, 3)
	var removed []string
	s.hooks.removeAside = func(p string) error {
		removed = append(removed, p)
		return &fs.PathError{Op: "unlinkat", Path: p, Err: syscall.EACCES}
	}
	l.reset()
	stageImage(t, s, agentapi.ImageFormatSIF) // pulled again; the repair succeeds
	if len(removed) != 1 || !strings.Contains(removed[0], ".mismatch-") {
		t.Fatalf("the copy moved aside was removed as %q, want one path with .mismatch-", removed)
	}
	line := oneLine(t, l, "removing the copy moved aside")
	const want = "stage: image sha256:2222222222222222222222222222222222222222222222222222222222222222: removing the copy moved aside: unlinkat <aside>: permission denied"
	if line != want {
		t.Errorf("the line is %q, want %q", line, want)
	}
	wantNoPath(t, root, []string{line}, "")
}

// A stale temporary output of the digest that cannot be removed: the line names
// the digest. The directory is read-only, so the removal is refused; the image
// itself is intact, so Image goes on.
func TestImageStaleTempFailureIsLoggedWithoutPaths(t *testing.T) {
	skipAsRoot(t)
	s, _, root := newStager(t, nil)
	l := withLog(s)
	first := stageImage(t, s, agentapi.ImageFormatSIF)
	images := filepath.Dir(first.Path)
	stale := filepath.Join(images, ".tmp-"+strings.TrimPrefix(digest, "sha256:")+"-old")
	if err := os.WriteFile(stale, []byte("left by a run that died"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(images, 0o700) })
	if err := os.Chmod(images, 0o500); err != nil {
		t.Fatal(err)
	}
	l.reset()
	stageImage(t, s, agentapi.ImageFormatSIF)
	if _, err := os.Lstat(stale); err != nil {
		t.Fatalf("the stale output was removed (%v), so the removal did not fail", err)
	}
	line := oneLine(t, l, "removing a stale temporary output")
	wantLineShape(t, root, line,
		"stage: image sha256:2222222222222222222222222222222222222222222222222222222222222222: removing a stale temporary output: ",
		": permission denied", "")
}

// A checkpoint file that was moved aside for a restage, whose download then
// failed and which cannot be put back: the line names the file, and the
// rename's two paths read <aside> and <file>. The Hub's answer takes the write
// permission off the checkpoint directory and says 404, which is not retried.
func TestCheckpointPutBackFailureIsLoggedWithoutPaths(t *testing.T) {
	skipAsRoot(t)
	s, hub, root := newStager(t, map[string]string{resolvePath("config.json"): "hello weights\n"})
	l := withLog(s)
	dir := ckptDir(s)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "config.json"), []byte("tampered\n"), 0o600)
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	hub.setIntercept(func(_ int, w http.ResponseWriter, r *http.Request) bool {
		os.Chmod(dir, 0o500)
		http.NotFound(w, r)
		return true
	})
	_, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev, Restage: true,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
	})
	if err == nil {
		t.Fatal("the download failed but Checkpoint succeeded")
	}
	line := oneLine(t, l, "putting the file back")
	const want = "stage: checkpoint file config.json: putting the file back: rename <aside> <file>: permission denied"
	if line != want {
		t.Errorf("the line is %q, want %q", line, want)
	}
	wantNoPath(t, root, []string{line}, "config.json")
}

// The copy of a checkpoint file moved aside, which is gone when the restage
// is done (the Hub's answer removes it): the line names the file, and the
// path in the operating system's error reads <aside>.
func TestCheckpointRemoveAsideFailureIsLoggedWithoutPaths(t *testing.T) {
	s, hub, root := newStager(t, map[string]string{resolvePath("config.json"): "hello weights\n"})
	l := withLog(s)
	dir := ckptDir(s)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "config.json"), []byte("tampered\n"), 0o600)
	hub.setIntercept(func(_ int, _ http.ResponseWriter, _ *http.Request) bool {
		aside, _ := filepath.Glob(filepath.Join(dir, "config.json.mismatch-*"))
		for _, a := range aside {
			os.Remove(a)
		}
		return false // the Hub answers as it would
	})
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev, Restage: true,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if f := res.Files[0]; f.Outcome != agentapi.FileMatch {
		t.Fatalf("the restage did not put config.json right: %+v", f)
	}
	line := oneLine(t, l, "removing the copy moved aside")
	const want = "stage: checkpoint file config.json: removing the copy moved aside: remove <aside>: no such file or directory"
	if line != want {
		t.Errorf("the line is %q, want %q", line, want)
	}
	wantNoPath(t, root, []string{line}, "config.json")
}

// A stale download part, in a subdirectory of the checkpoint, that cannot be
// removed: the line names it by its place in the checkpoint, with the slash
// that place has, and the path in the error reads <part>.
func TestCheckpointStaleDownloadFailureIsLoggedWithoutPaths(t *testing.T) {
	skipAsRoot(t)
	s, _, root := newStager(t, map[string]string{resolvePath("tok/tokenizer.json"): "hello weights\n"})
	l := withLog(s)
	req := agent.CheckpointRequest{Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "tok/tokenizer.json", SHA256: weightsSum}}}
	if _, err := s.Checkpoint(context.Background(), req); err != nil { // stages it, and makes the lock file
		t.Fatal(err)
	}
	sub := filepath.Join(ckptDir(s), "tok")
	part := filepath.Join(sub, ".tokenizer.json.part-old")
	if err := os.WriteFile(part, []byte("left by a download that died"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(part, old, old); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(sub, 0o700) })
	if err := os.Chmod(sub, 0o500); err != nil {
		t.Fatal(err)
	}
	l.reset()
	if _, err := s.Checkpoint(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(part); err != nil {
		t.Fatalf("the stale part was removed (%v), so the removal did not fail", err)
	}
	line := oneLine(t, l, "removing a stale download")
	const want = "stage: checkpoint file tok/.tokenizer.json.part-old: removing a stale download: remove <part>: permission denied"
	if line != want {
		t.Errorf("the line is %q, want %q", line, want)
	}
	wantNoPath(t, root, []string{line}, "tok/.tokenizer.json.part-old")
}

// The flock notice names the lock file, never its path: the checkpoint lock's
// own name, and ".<hex>.lock" beside an image.
func TestFlockNoticeNamesTheLockFileAndNoPath(t *testing.T) {
	const tail = " (function not implemented): staging goes on without exclusion between agents"
	t.Run("checkpoint", func(t *testing.T) {
		s, _, root := newStager(t, map[string]string{resolvePath("a.json"): "hello weights\n"})
		l := withLog(s)
		s.hooks.flock = func(int, int) error { return syscall.ENOSYS }
		if _, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{Repository: "org/model", Revision: rev,
			Files: []agentapi.FileDigest{{Path: "a.json", SHA256: weightsSum}}}); err != nil {
			t.Fatal(err)
		}
		line := oneLine(t, l, "flock is not supported")
		if want := "stage: flock is not supported at .hpcburst-stage.lock" + tail; line != want {
			t.Errorf("the line is %q, want %q", line, want)
		}
		wantNoPath(t, root, []string{line}, "")
	})
	t.Run("image", func(t *testing.T) {
		s, _, root := newStager(t, nil)
		l := withLog(s)
		s.hooks.flock = func(int, int) error { return syscall.ENOSYS }
		stageImage(t, s, agentapi.ImageFormatSIF)
		line := oneLine(t, l, "flock is not supported")
		if want := "stage: flock is not supported at .2222222222222222222222222222222222222222222222222222222222222222.lock" + tail; line != want {
			t.Errorf("the line is %q, want %q", line, want)
		}
		wantNoPath(t, root, []string{line}, "")
	})
}

// The notice that no sample of the use of the temporary directory succeeded
// names no directory.
func TestNoSampleNoticeNamesNoDirectory(t *testing.T) {
	s, _, root := newStager(t, nil)
	l := withLog(s)
	s.c.SampleEvery = 5 * time.Millisecond
	s.c.UsedSpace = func(string) (int64, error) { return 0, errors.New("statfs: input/output error") }
	scriptApptainer(t, root, `printf 'fake-sif\n' > "$2"`)
	stageImage(t, s, agentapi.ImageFormatSIF)
	line := oneLine(t, l, "no sample of the use of")
	const want = "stage: apptainer pull: no sample of the use of the temporary directory succeeded; the build's peak is unknown"
	if line != want {
		t.Errorf("the line is %q, want %q", line, want)
	}
	wantNoPath(t, root, []string{line}, "")
}

// hidePaths writes each path as its stand-in, longer first where one path
// starts another, keeps the error reachable by errors.Is and errors.As, and
// leaves a nil error nil. The expected texts are literal.
func TestHidePathsHidesEachPathAndKeepsTheError(t *testing.T) {
	t.Run("a rename's two paths, the longer first", func(t *testing.T) {
		final := "/s/images/x.sif"
		aside := final + ".mismatch-1"
		err := hidePaths(&os.LinkError{Op: "rename", Old: aside, New: final, Err: syscall.EACCES}, aside, "<aside>", final, "<image>")
		if got, want := err.Error(), "rename <aside> <image>: permission denied"; got != want {
			t.Errorf("the error says %q, want %q", got, want)
		}
		if !errors.Is(err, syscall.EACCES) {
			t.Error("the error no longer wraps the operating system's")
		}
		var le *os.LinkError
		if !errors.As(err, &le) || le.Old != aside {
			t.Errorf("the original *os.LinkError is not reachable: %v", le)
		}
	})
	t.Run("a path error", func(t *testing.T) {
		err := hidePaths(&fs.PathError{Op: "remove", Path: "/s/ckpt/a", Err: syscall.ENOENT}, "/s/ckpt/a", "<part>")
		if got, want := err.Error(), "remove <part>: no such file or directory"; got != want {
			t.Errorf("the error says %q, want %q", got, want)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Error("the error no longer wraps the operating system's")
		}
	})
	t.Run("nil stays nil", func(t *testing.T) {
		if err := hidePaths(nil, "/a", "<a>"); err != nil {
			t.Errorf("hidePaths(nil) = %v", err)
		}
	})
}
