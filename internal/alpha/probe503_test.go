package alpha

// PROBE for #503, a throwaway that is never merged: row A's exact sequence over the stand-in (the real stack and the fake Slurm), with the
// retention the LIVE world has instead of the stand-in's. The live world is built from examples/modeldeployment-qwen25-instruct.yaml, whose
// retentionPolicy is ReleaseWhenIdle with an idle limit of 30 minutes; the stand-in's world sets 10 seconds, which is why
// TestStandInNowServeRequeueRefused is green. The sequence is the live one: the Instance ready, the worker fault, the deployment's end set to
// now with a stream in flight, the refused requeue (the fake's login refuses scontrol requeuehold), then the deployment deleted and the wait
// for the job's end. Expected: that wait times out, and its failure names the state it saw last (EndWaitState); the evidence a failed run keeps
// is listed after it.

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestProbe503RowAWithTheLiveRetention(t *testing.T) {
	if os.Getenv(envStandIn) != "1" {
		t.Skipf("the probe runs only with %s=1", envStandIn)
	}
	row := standInRow(t, "alpha-athena-now-serve")
	id, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out")
	cfg := Config{Row: row, StandIn: true, Out: out, PendingMax: 2 * time.Minute, RunID: id, FrontLabel: standInFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	w, cl := standInWorldShaped(t, row, nil)
	w.md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(30 * time.Minute)}
	t.Logf("the probe's retention: ReleaseWhenIdle, idle limit 30m (the live world's, from the example)")
	lr := newLiveRun(t, cfg, w)
	lr.stop = &stackStop{}
	lr.agentLogFn = func(path string) string { return "tail -c 65536 " + path }
	// Last, after Close has kept what a failed run keeps: what the fake cluster's record of the run's jobs says, and the evidence.
	t.Cleanup(func() {
		for _, j := range cl.Plat.Jobs() {
			if strings.HasPrefix(j.Name, TagPrefix(id)) {
				term := "never"
				if !j.TermAt.IsZero() {
					term = j.TermAt.Sub(j.Start).Round(time.Second).String() + " after its start"
				}
				t.Logf("PROBE: the run's job: state %s, started %s, ended %s after its start, sent TERM %s, time limit %d min, restart count %d",
					j.State, j.Start.Format(time.TimeOnly), j.End.Sub(j.Start).Round(time.Second), term, j.TimeLimit, j.RestartCount)
			}
		}
		dir := filepath.Join(out, EvidenceDir)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Logf("PROBE: no evidence directory: %v", err)
			return
		}
		var names []string
		for _, e := range entries {
			fi, _ := e.Info()
			names = append(names, e.Name()+" ("+sizeOf(fi)+")")
		}
		sort.Strings(names)
		t.Logf("PROBE: the evidence kept: %s", strings.Join(names, ", "))
		if b, err := os.ReadFile(filepath.Join(dir, "controller.log")); err == nil {
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			var keep []string
			for _, l := range lines {
				low := strings.ToLower(l)
				if strings.Contains(low, "retention") || strings.Contains(low, "releas") || strings.Contains(low, "drain") || strings.Contains(low, "idle") || strings.Contains(low, "cancel") || strings.Contains(low, "delet") {
					keep = append(keep, l)
				}
			}
			if len(keep) > 80 {
				keep = keep[len(keep)-80:]
			}
			t.Logf("PROBE: the controller's log, the lines about release, drain, idle and deletion (the last %d):\n%s", len(keep), strings.Join(keep, "\n"))
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), cfg.PendingMax+25*time.Minute)
	defer cancel()
	failures, err := Run(ctx, cfg, lr, rec, t.Logf)
	t.Logf("PROBE: the run ended without the wait timing out: failures %v, err %v", failures, err)
}

func sizeOf(fi os.FileInfo) string {
	if fi == nil {
		return "?"
	}
	return strconv.FormatInt(fi.Size(), 10) + " bytes"
}
