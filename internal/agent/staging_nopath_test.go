package agent_test

// A staging directory that cannot be made reaches neither the agent's log nor the report's staging.message with its path
// (#762, Astra's review; HBEP-4, Provider settings: the staging directory is account data, and staging.message reaches the
// Allocation's status). The real agent runs over the real stager, whose staging directory is a regular file: the images
// directory under it cannot be made, so staging fails, and the failure is reported with its kind and no path.

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestAFailedImageDirectoryIsReportedAndLoggedWithoutItsPath(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	if err := os.RemoveAll(h.stagingDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.stagingDir, []byte("a file where the staging directory should be"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.set(h.runState("", 1, 0, nil))
	r := h.waitReport("staging failed", 30*time.Second, stagingPhase(agentapi.StagingFailed))
	st := r.Instance.Staging
	want := "creating the image directory under the staging directory failed: not a directory"
	if st.Message != want {
		t.Errorf("staging.message is %q, want %q", st.Message, want)
	}
	// The agent logs the failure too: the line is there, and it names no path either.
	if !strings.Contains(h.logs.String(), "staging failed") {
		t.Fatalf("the agent logged no staging failure: the log check would prove nothing:\n%s", h.logs.String())
	}
	// The staging directory is root/staging, so its path holds the root; the log also names the run directory under the root,
	// so the log is checked for the staging directory and the message for both.
	for what, c := range map[string]struct {
		text string
		bad  []string
	}{
		"staging.message": {st.Message, []string{h.stagingDir, h.root}},
		"the agent's log": {h.logs.String(), []string{h.stagingDir}},
	} {
		for _, bad := range c.bad {
			if strings.Contains(c.text, bad) {
				t.Errorf("%s names %q: %q", what, bad, c.text)
			}
		}
	}
}
