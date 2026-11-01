package agent_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// announcing is true for a report that announces an image build.
func announcing(r agentapi.Report) bool {
	return r.Instance != nil && r.Instance.Staging != nil && strings.HasPrefix(r.Instance.Staging.Message, "building image ")
}

// announceBackend records, in a file, the build message of the first report
// that announces a build, once the Backend has taken it; delay holds such a
// report before the Backend takes it, and fail refuses it.
type announceBackend struct {
	agentapi.Backend
	marker string
	delay  time.Duration
	fail   bool
}

func (b *announceBackend) Report(ctx context.Context, c agentapi.Caller, r agentapi.Report) error {
	if !announcing(r) {
		return b.Backend.Report(ctx, c, r)
	}
	if b.fail {
		return &agentapi.Error{Code: agentapi.CodeUnavailable, Message: "the test refuses build announcements"}
	}
	select {
	case <-time.After(b.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := b.Backend.Report(ctx, c, r); err != nil {
		return err
	}
	if _, err := os.Stat(b.marker); os.IsNotExist(err) {
		if err := os.WriteFile(b.marker, []byte(r.Instance.Staging.Message), 0o600); err != nil {
			return err
		}
	}
	return nil
}

// buildApptainer writes a fake Apptainer that, on a pull, first runs check
// (shell), then hands over to the engine double's Apptainer.
func buildApptainer(t *testing.T, check string) string {
	t.Helper()
	dir := t.TempDir()
	if err := enginedouble.Install(dir); err != nil {
		t.Fatal(err)
	}
	wrap := filepath.Join(dir, "apptainer-checked")
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = pull ]; then\n%s\nfi\nexec %s \"$@\"\n", check, filepath.Join(dir, "apptainer"))
	if err := os.WriteFile(wrap, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return wrap
}

func stagingPhase(p agentapi.StagingPhase) func(agentapi.Report) bool {
	return func(r agentapi.Report) bool {
		return r.Instance != nil && r.Instance.Staging != nil && r.Instance.Staging.Phase == p
	}
}

// The report that announces an image build reaches the endpoint before
// Apptainer starts the build (PR #138 review, Astra B3; HBEP-7, Staging: the
// last report before a build that runs out of the job's memory names it). The
// check is made at the process-launch boundary: the fake Apptainer's first
// act on a pull is to look for the file the Backend writes once it has taken
// the announcement, and it fails the pull when there is none. The Backend
// holds the announcement 300 ms before taking it, so an Apptainer started
// without waiting for its delivery finds no file; with the wait, the order
// holds whatever the delay. The message names the directory by its variable,
// not the resolved run directory (Opus 9).
func TestBuildAnnouncementIsDeliveredBeforeApptainerStarts(t *testing.T) {
	marks := t.TempDir()
	marker, early := filepath.Join(marks, "announced"), filepath.Join(marks, "started-unannounced")
	wrap := buildApptainer(t, fmt.Sprintf("[ -e %s ] || { touch %s; exit 1; }", marker, early))
	h := newHarness(t, harnessOptions{apptainer: wrap, backendWrap: func(b agentapi.Backend) agentapi.Backend {
		return &announceBackend{Backend: b, marker: marker, delay: 300 * time.Millisecond}
	}})
	h.set(h.runState("", 1, 0, nil))
	r := h.waitReport("staging ended", 30*time.Second, func(r agentapi.Report) bool {
		return stagingPhase(agentapi.StagingDone)(r) || stagingPhase(agentapi.StagingFailed)(r)
	})
	if _, err := os.Stat(early); err == nil {
		t.Fatal("Apptainer started the pull before the Backend had the build's announcement")
	}
	if st := r.Instance.Staging; st.Phase != agentapi.StagingDone {
		t.Fatalf("staging %+v, want done", *st)
	}
	b, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("the Backend never took an announcement: %v", err)
	}
	want := "building image docker.io/vllm/vllm-openai@" + hDigest + " in the job's temporary directory ($TMPDIR)"
	if msg := string(b); msg != want || strings.Contains(msg, h.runDir) {
		t.Errorf("announced %q, want %q", msg, want)
	}
}

// With the endpoint refusing the announcement, the build waits at most
// BuildReportTimeout for its delivery, then starts anyway: an unreachable
// endpoint changes nothing the agent runs (HBEP-7, Unreachable).
func TestBuildStartsWhenItsAnnouncementCannotBeDelivered(t *testing.T) {
	h := newHarness(t, harnessOptions{buildReportTimeout: 300 * time.Millisecond, backendWrap: func(b agentapi.Backend) agentapi.Backend {
		return &announceBackend{Backend: b, fail: true}
	}})
	h.set(h.runState("", 1, 0, nil))
	h.waitReport("staging done", 30*time.Second, stagingPhase(agentapi.StagingDone))
	if !strings.Contains(h.logs.String(), "was not delivered in time; building anyway") {
		t.Errorf("the agent did not log the undelivered announcement")
	}
}

// A build that fails still reports its peak TMPDIR use, in the failed
// staging report (PR #138 review, Astra B4, Opus 6). The production sampler
// (statfs) samples the run directory's file system, whose use is above 0.
func TestFailedBuildReportsItsPeak(t *testing.T) {
	wrap := buildApptainer(t, "echo 'pull failed' >&2; exit 1")
	h := newHarness(t, harnessOptions{apptainer: wrap})
	h.set(h.runState("", 1, 0, nil))
	r := h.waitReport("staging failed", 30*time.Second, stagingPhase(agentapi.StagingFailed))
	if st := r.Instance.Staging; st.BuildPeakTmpBytes <= 0 || !strings.Contains(st.Message, "exit code 1") {
		t.Errorf("failed staging %+v, want the build's peak and its failure", *st)
	}
}

// A good build followed by a checkpoint step that fails keeps the build's
// peak in the failed staging report: the image is now cached, so the next
// staging builds nothing and this is the one measurement (Astra B4, Opus 6).
// The Hub has no such file, so the download fails.
func TestCheckpointFailureAfterABuildKeepsThePeak(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	missing := agentapi.FileDigest{Path: "missing.json", SHA256: strings.Repeat("0", 64)}
	h.set(h.runState("", 1, 0, verifyChecks(1, []agentapi.FileDigest{defaultFile, missing})))
	r := h.waitReport("staging failed", 30*time.Second, stagingPhase(agentapi.StagingFailed))
	if st := r.Instance.Staging; st.BuildPeakTmpBytes <= 0 || !strings.Contains(st.Message, "404") {
		t.Errorf("failed staging %+v, want the build's peak and the download's failure", *st)
	}
	if cmds := h.appCommands(); len(cmds) == 0 || cmds[0] != "pull" {
		t.Errorf("apptainer ran %q, want the pull first", cmds)
	}
}

// Once the build ends, and before the checkpoint is staged, the report drops
// the build message and carries the peak (Opus 3, Astra B4): while the Hub
// holds the checkpoint's file, the staging is in progress with no message
// and a peak. The waits are on observable state: the Hub's hold, the report.
func TestBuildPeakIsReportedWhileTheCheckpointDownloads(t *testing.T) {
	holding, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h := newHarness(t, harnessOptions{hubIntercept: func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/"+defaultFile.Path) {
			return false
		}
		once.Do(func() { close(holding) })
		select {
		case <-release:
		case <-r.Context().Done():
			return true
		}
		io.WriteString(w, defaultContent)
		return true
	}})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	h.set(h.runState("", 1, 0, nil))
	select {
	case <-holding:
	case <-time.After(30 * time.Second):
		t.Fatal("the checkpoint download never started")
	}
	h.waitReport("the peak without the build message", 20*time.Second, func(r agentapi.Report) bool {
		if r.Instance == nil || r.Instance.Staging == nil {
			return false
		}
		st := r.Instance.Staging
		return st.Phase == agentapi.StagingInProgress && st.Message == "" && st.BuildPeakTmpBytes > 0
	})
	released = true
	close(release)
	r := h.waitReport("staging done", 30*time.Second, stagingPhase(agentapi.StagingDone))
	if st := r.Instance.Staging; st.BuildPeakTmpBytes <= 0 || st.Message != "" {
		t.Errorf("staging %+v, want the peak and no message", *st)
	}
}
