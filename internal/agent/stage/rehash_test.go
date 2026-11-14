package stage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The rehash's numbers (issue #575, HBEP-9's open rehash time): a cold call finds nothing present and rehashes nothing, a
// warm one rehashes every listed file it finds, by their sizes on disk, and one that finds part of the checkpoint counts
// that part alone. The duration is the rehash's own, set whenever the rehash ran. The files the cold call downloads are
// not counted: they were never rehashed before their download.
func TestCheckpointCountsTheRehashOfThePresentFilesAlone(t *testing.T) {
	s, _, root := newStager(t, map[string]string{
		resolvePath("config.json"):       "hello weights\n",
		resolvePath("model.safetensors"): "hello weights\n",
	})
	req := agent.CheckpointRequest{Repository: "org/model", Revision: rev, Files: []agentapi.FileDigest{
		{Path: "config.json", SHA256: weightsSum},
		{Path: "model.safetensors", SHA256: weightsSum},
	}}
	cold, err := s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if r := cold.Rehash; r.Files != 0 || r.Listed != 2 || r.Bytes != 0 {
		t.Errorf("a cold call: %+v, want 0 files of 2, 0 bytes", r)
	}
	warm, err := s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	size := int64(len("hello weights\n"))
	if r := warm.Rehash; r.Files != 2 || r.Listed != 2 || r.Bytes != 2*size || r.Duration <= 0 {
		t.Errorf("a warm call: %+v, want 2 files of 2, %d bytes, a duration", r, 2*size)
	}
	// One file removed: the next call rehashes the other alone, and downloads the removed one uncounted.
	var removed bool
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && !removed && d.Type().IsRegular() && d.Name() == "config.json" {
			removed = os.Remove(p) == nil
		}
		return nil
	})
	if !removed {
		t.Fatal("the staged config.json was not found to remove")
	}
	part, err := s.Checkpoint(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if r := part.Rehash; r.Files != 1 || r.Listed != 2 || r.Bytes != size {
		t.Errorf("a call that finds one of two: %+v, want 1 file of 2, %d bytes", r, size)
	}
}
