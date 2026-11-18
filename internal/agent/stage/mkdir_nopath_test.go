package stage

// A staging directory that cannot be made fails with a message that names the step and the kind of failure, never the path
// (#762, Astra's review; HBEP-4, Provider settings: the staging directory is account data). The error reaches the agent's log
// and, in a report's staging.message, the Allocation's status. The rows make the failure with a regular file where a directory
// is wanted, which fails the same way as root and as any user: the error the operating system gives is "not a directory", the
// kind a message keeps.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// unmakeableStaging gives the stager a staging directory under a regular file, whose parent holds a unique segment.
func unmakeableStaging(t *testing.T, s *Stager, root string) (staging, segment string) {
	t.Helper()
	segment = "zq7f3akx"
	blocker := filepath.Join(root, segment)
	if err := os.WriteFile(blocker, []byte("a file where a directory is wanted"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.c.StagingDir = filepath.Join(blocker, "team:alicexw8m4", "run")
	return s.c.StagingDir, segment
}

func wantPathFree(t *testing.T, what, text string, bad ...string) {
	t.Helper()
	for _, b := range bad {
		if strings.Contains(text, b) {
			t.Errorf("%s names %q: %q", what, b, text)
		}
	}
}

func TestAnImageDirectoryThatCannotBeMadeNamesNoPath(t *testing.T) {
	s, _, root := newStager(t, nil)
	l := withLog(s)
	staging, segment := unmakeableStaging(t, s, root)
	_, err := s.Image(context.Background(), vllmImage, digest, agentapi.ImageFormatSIF)
	if err == nil {
		t.Fatal("Image succeeded with no way to make its directory")
	}
	if !errors.Is(err, syscall.ENOTDIR) {
		t.Errorf("the error %v does not keep its kind (ENOTDIR) for errors.Is", err)
	}
	if want := "creating the image directory under the staging directory failed: not a directory"; err.Error() != want {
		t.Errorf("the error is %q, want %q", err.Error(), want)
	}
	wantPathFree(t, "the error", err.Error(), staging, root, segment, "alicexw8m4")
	wantPathFree(t, "the log", l.String(), staging, root, segment, "alicexw8m4")
}

func TestACheckpointDirectoryThatCannotBeMadeNamesNoPath(t *testing.T) {
	s, _, root := newStager(t, nil)
	l := withLog(s)
	staging, segment := unmakeableStaging(t, s, root)
	_, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: sumOf("{}")}},
	})
	if err == nil {
		t.Fatal("Checkpoint succeeded with no way to make its directory")
	}
	if !errors.Is(err, syscall.ENOTDIR) {
		t.Errorf("the error %v does not keep its kind (ENOTDIR) for errors.Is", err)
	}
	if want := "creating the checkpoint directory under the staging directory failed: not a directory"; err.Error() != want {
		t.Errorf("the error is %q, want %q", err.Error(), want)
	}
	wantPathFree(t, "the error", err.Error(), staging, root, segment, "alicexw8m4")
	wantPathFree(t, "the log", l.String(), staging, root, segment, "alicexw8m4")
}
