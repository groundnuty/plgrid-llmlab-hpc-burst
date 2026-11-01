package agent_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// As an image build starts, the agent reports it: while Apptainer's pull is
// held, the last report already says "building image <image>@<digest> in the
// job's temporary directory ($TMPDIR)", so a build that runs out of the job's memory
// (OUT_OF_MEMORY, AllocationFailed) leaves that report last. Once the build is
// done, the report carries its peak TMPDIR use (statfs, the production
// sampler) and no build tree is left under TMPDIR (science-agent rulings,
// 1 Nov 2026, issue #125; HBEP-4). Waits are on observable state: a file the
// held pull writes, and the reports.
func TestBuildIsReportedAsItStarts(t *testing.T) {
	dir := t.TempDir()
	if err := enginedouble.Install(dir); err != nil {
		t.Fatal(err)
	}
	started, release := filepath.Join(dir, "started"), filepath.Join(dir, "release")
	wrap := filepath.Join(dir, "apptainer-held")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = pull ]; then
  touch %[1]s
  i=0; while [ ! -e %[2]s ] && [ $i -lt 3000 ]; do sleep 0.01; i=$((i+1)); done
fi
exec %[3]s "$@"
`, started, release, filepath.Join(dir, "apptainer"))
	if err := os.WriteFile(wrap, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, harnessOptions{apptainer: wrap})
	h.set(h.runState("", 1, 0, nil))
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(started); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the pull did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	want := "building image docker.io/vllm/vllm-openai@" + hDigest + " in the job's temporary directory ($TMPDIR)"
	h.waitReport("the build reported while held", 20*time.Second, func(r agentapi.Report) bool {
		return r.Instance != nil && r.Instance.Staging != nil && r.Instance.Staging.Message == want
	})
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r := h.waitReport("staging done", 30*time.Second, func(r agentapi.Report) bool {
		return r.Instance != nil && r.Instance.Staging != nil && r.Instance.Staging.Phase == agentapi.StagingDone
	})
	if st := r.Instance.Staging; st.BuildPeakTmpBytes <= 0 || st.Message != "" {
		t.Errorf("staging %+v, want a peak and no build message", *st)
	}
	if left, _ := filepath.Glob(filepath.Join(h.runDir, "hb-apptainer-*")); len(left) != 0 {
		t.Errorf("left under TMPDIR after the build: %v", left)
	}
}
