package agent

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// generationConfigFile says what is at the staged checkpoint's generation_config.json, for
// VerifyResult.GenerationConfigFile (issue #175):
//
//   - agentapi.GenerationConfigAbsent when the checkpoint directory has no entry of that name at all
//     (Lstat finds nothing);
//   - agentapi.GenerationConfigPresent when the entry resolves to a regular file that the agent can open
//     (a symlink to one counts), whatever it holds: an empty file or one that is not JSON is present, and
//     readGenerationConfig then records no keys for it;
//   - agentapi.GenerationConfigUnreadable when an entry is there that does not resolve to a readable
//     regular file: a directory, a dangling symlink, a file the agent may not open, or a checkpoint
//     directory that cannot be read.
//
// Empty when the agent has no checkpoint directory, which says nothing about the file and is not a claim
// that it is absent. An entry that cannot be read is never "absent": a dangling link is an entry, and the
// os.Open that follows it fails with the same ENOENT as a missing file.
func generationConfigFile(dir string) string {
	if dir == "" {
		return ""
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		return agentapi.GenerationConfigUnreadable // no checkpoint directory to look in
	}
	path := filepath.Join(dir, generationConfigName)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return agentapi.GenerationConfigAbsent
	} else if err != nil {
		return agentapi.GenerationConfigUnreadable
	}
	f, err := os.Open(path)
	if err != nil {
		return agentapi.GenerationConfigUnreadable
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil || !fi.Mode().IsRegular() {
		return agentapi.GenerationConfigUnreadable
	}
	return agentapi.GenerationConfigPresent
}
