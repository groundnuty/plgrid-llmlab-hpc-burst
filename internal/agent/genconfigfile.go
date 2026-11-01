package agent

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// generationConfigFile says what is at the staged checkpoint's generation_config.json, for
// VerifyResult.GenerationConfigFile (issue #175): agentapi.GenerationConfigAbsent when the checkpoint has
// no such file, GenerationConfigPresent when there is a readable regular file (readGenerationConfig then
// says which keys it sets, none or some), GenerationConfigUnreadable when something else is there (a
// directory, a file the agent may not open). Empty when the agent has no checkpoint directory, which
// says nothing about the file and is not a claim that it is absent.
func generationConfigFile(dir string) string {
	if dir == "" {
		return ""
	}
	f, err := os.Open(filepath.Join(dir, generationConfigName))
	if errors.Is(err, os.ErrNotExist) {
		return agentapi.GenerationConfigAbsent
	}
	if err != nil {
		return agentapi.GenerationConfigUnreadable
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil || !fi.Mode().IsRegular() {
		return agentapi.GenerationConfigUnreadable
	}
	return agentapi.GenerationConfigPresent
}
