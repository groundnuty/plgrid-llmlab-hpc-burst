package stage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// scriptApptainer replaces the stager's fake apptainer with a shell script.
func scriptApptainer(t *testing.T, root, body string) {
	t.Helper()
	p := filepath.Join(root, "bin", "apptainer")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
}

// waitForPID waits for a file holding a PID, written by a script, and returns
// the process's ID. The process is killed at cleanup if it still runs, so a
// failing test leaves nothing behind.
func waitForPID(t *testing.T, file string) procgroup.ID {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(file); err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err != nil {
				t.Fatalf("%s holds %q", file, b)
			}
			id, err := procgroup.IDOf(pid)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if id.Alive() {
					syscall.Kill(id.PID, syscall.SIGKILL)
				}
			})
			return id
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s was not written", file)
	return procgroup.ID{}
}

// A cancelled Image leaves no process of Apptainer's behind: the child that
// ignores TERM is killed after the grace, and Image returns only once it is
// gone. Under exec.CommandContext the child survived (Astra B3, Opus B4).
func TestCancelStopsEveryProcessOfApptainersGroup(t *testing.T) {
	for _, format := range []agentapi.ImageFormat{agentapi.ImageFormatSIF, agentapi.ImageFormatSandbox} {
		t.Run(string(format), func(t *testing.T) {
			s, _, root := newStager(t, nil)
			s.c.StopGrace = 300 * time.Millisecond
			pidFile := filepath.Join(root, "child.pid")
			scriptApptainer(t, root, fmt.Sprintf(`sh -c 'trap "" TERM; echo $$ > %[1]s.tmp && mv %[1]s.tmp %[1]s; exec sleep 31337' &
exec sleep 31337
`, pidFile))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := s.Image(ctx, "docker.io/vllm/vllm-openai", digest, format)
				done <- err
			}()
			child := waitForPID(t, pidFile)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("err %v, want context canceled", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("Image did not return after the cancel")
			}
			if child.Alive() {
				t.Fatalf("Apptainer's child %+v still runs after Image returned", child)
			}
			left, _ := filepath.Glob(filepath.Join(s.c.StagingDir, "images", ".tmp-*"))
			if len(left) != 0 {
				t.Errorf("temporary output left: %q", left)
			}
		})
	}
}

// A run that exits 0 and leaves a member of its group running does not leave it
// running either.
func TestSuccessfulRunLeavesNoMemberBehind(t *testing.T) {
	s, _, root := newStager(t, nil)
	s.c.StopGrace = 300 * time.Millisecond
	pidFile := filepath.Join(root, "child.pid")
	scriptApptainer(t, root, fmt.Sprintf(`sh -c 'echo $$ > %[1]s.tmp && mv %[1]s.tmp %[1]s; exec sleep 31337' &
while [ ! -e %[1]s ]; do sleep 0.01; done
printf 'fake-sif\n' > "$2"
`, pidFile))
	type result struct {
		res agent.ImageResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF)
		done <- result{res, err}
	}()
	child := waitForPID(t, pidFile)
	var r result
	select {
	case r = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Image did not return")
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	if _, err := os.Stat(r.res.Path); err != nil {
		t.Fatal(err)
	}
	if child.Alive() {
		t.Fatalf("a member of Apptainer's group still runs after Image returned: %+v", child)
	}
}

// A failing run is an error naming its exit code, and leaves no output.
func TestFailedApptainerRunLeavesNothing(t *testing.T) {
	s, _, root := newStager(t, nil)
	scriptApptainer(t, root, `echo boom >&2
printf partial > "$2"
exit 3
`)
	_, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF)
	if err == nil || !strings.Contains(err.Error(), "exit code 3") {
		t.Fatalf("err %v, want the exit code", err)
	}
	for _, e := range imageEntries(t, s) {
		t.Errorf("left after a failed run: %s", e)
	}
}

// Apptainer's layer cache is a directory of its own under TmpDir for the run,
// removed with it, and nothing piles up beside the images (Opus nit).
func TestApptainerCacheIsPerRunAndRemoved(t *testing.T) {
	s, _, root := newStager(t, nil)
	rec := filepath.Join(root, "cachedir")
	scriptApptainer(t, root, fmt.Sprintf(`echo "$APPTAINER_CACHEDIR" > %s
mkdir -p "$APPTAINER_CACHEDIR/blobs" && printf layer > "$APPTAINER_CACHEDIR/blobs/l1"
printf 'fake-sif\n' > "$2"
`, rec))
	if _, err := s.Image(context.Background(), "docker.io/vllm/vllm-openai", digest, agentapi.ImageFormatSIF); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(rec)
	if err != nil {
		t.Fatal(err)
	}
	cache := strings.TrimSpace(string(b))
	if filepath.Dir(cache) != root {
		t.Errorf("APPTAINER_CACHEDIR %q is not directly under TmpDir %q", cache, root)
	}
	if _, err := os.Stat(cache); !os.IsNotExist(err) {
		t.Errorf("the run's cache %q still exists (%v)", cache, err)
	}
	if _, err := os.Stat(filepath.Join(s.c.StagingDir, "images", ".cache")); !os.IsNotExist(err) {
		t.Errorf("images/.cache exists (%v)", err)
	}
}
