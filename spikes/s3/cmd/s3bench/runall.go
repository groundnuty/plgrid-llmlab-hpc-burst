package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// fileSHA256 is the full hex digest of a file; an unreadable file yields an "unreadable" marker
// rather than an empty string, so a phase record never looks complete when it is not.
func fileSHA256(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return "unreadable: " + err.Error()
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "unreadable: " + err.Error()
	}
	return hex.EncodeToString(h.Sum(nil))
}

// beginPhase records the start time and the identity of every binary the phase will run. A
// phase that is run again replaces its earlier record, never inherits its time.
func (b *bench) beginPhase(name string, versions ...string) {
	self, _ := os.Executable()
	ph := phaseRecord{Name: name, StartedUTC: time.Now().UTC().Format(time.RFC3339),
		E2ESHA256: fileSHA256(b.e2eBin), HarnessSHA: fileSHA256(self), HostBinary: map[string]string{}}
	for _, v := range versions {
		for label, path := range b.h.binaries(v) {
			ph.HostBinary[label+" "+v] = fileSHA256(path)
		}
	}
	var keep []phaseRecord
	for _, old := range b.res.Phases {
		if old.Name != name {
			keep = append(keep, old)
		}
	}
	b.res.Phases = append(keep, ph)
	sort.Slice(b.res.Phases, func(i, j int) bool { return b.res.Phases[i].Name < b.res.Phases[j].Name })
}

func (b *bench) endPhase(name string) {
	for i := range b.res.Phases {
		if b.res.Phases[i].Name == name {
			b.res.Phases[i].FinishedUTC = time.Now().UTC().Format(time.RFC3339)
		}
	}
}

// afterStop audits what a stopped host left behind: its logs must not mention the system paths
// of the root-owned k3s on the shared VM.
func (b *bench) afterStop(r *running) {
	if _, ok := b.h.(k3sHost); !ok {
		return
	}
	if err := auditLogs(r.logs, k3sForbidden); err != nil {
		b.runtimeProblems = append(b.runtimeProblems, "system path in a k3s log: "+err.Error())
		b.logf("PROBLEM: %v", err)
	}
}

// stop stops a host by PID and audits it.
func (b *bench) stop(r *running) (time.Duration, error) {
	d, err := r.stopAll(60 * time.Second)
	b.live = nil
	b.afterStop(r)
	return d, err
}

func (b *bench) dataDir(name string) string {
	d := filepath.Join(b.scratch, "data", b.h.name()+"-"+name)
	_ = os.RemoveAll(d)
	_ = os.MkdirAll(d, 0o700)
	return d
}

func (b *bench) runAll(ctx context.Context, only string) error {
	if only == "all" || only == "main" {
		if err := b.mainRun(ctx); err != nil {
			return fmt.Errorf("main run: %w", err)
		}
	}
	if only == "all" || only == "upgrade" {
		if err := b.upgradeRun(ctx); err != nil {
			return fmt.Errorf("upgrade run: %w", err)
		}
	}
	return nil
}

// mainRun: cold start, idle RSS, load phase and its RSS, the e2e suite, then warm and cold restarts.
// Its data directories are deleted as soon as they are no longer needed (the shared disk is tight).
func (b *bench) mainRun(ctx context.Context) (rerr error) {
	_, ver := b.h.versions()
	b.res.Cold, b.res.Warm = nil, nil
	b.beginPhase("main", ver)
	defer b.endPhase("main")
	dir := b.dataDir("main")
	defer os.RemoveAll(dir)
	b.logf("cold start %s", ver)
	r, t, err := b.startReady(ctx, ver, dir, "main-cold1")
	if err != nil {
		return err
	}
	b.logf("ready in %d ms (readyz %d ms); settling %s for idle RSS", t.ReadyMS, t.ReadyzMS, settle)
	if b.res.Idle, err = b.sample(ctx, r, nil, settle); err != nil {
		return err
	}
	u0 := time.Now()
	kc, err := b.usable(ctx, r)
	if err != nil {
		return err
	}
	t.UsableMS = t.ReadyMS + time.Since(u0).Milliseconds()
	b.res.Cold = append(b.res.Cold, t)
	b.logf("usable (workspace+CRDs) after %d ms; load phase", t.UsableMS)

	if err := b.loadPhase(ctx, r, kc, "main", false); err != nil {
		return err
	}
	b.logf("full e2e suite")
	e, err := b.runE2E(kc, b.h.name()+"-main", "", nil, nil)
	if err != nil {
		return err
	}
	b.res.E2E = &e
	b.saveScrubbed(e.Log, b.h.name()+"-e2e.txt")
	b.logf("e2e: %d passed, %d failed %v", len(e.Passed), len(e.Failed), e.Failed)
	b.res.DataDirMiB = dirMiB(dir)
	b.res.StateMiB = dirMiB(b.h.stateDir(dir))
	d, err := b.stop(r)
	b.res.StopMS = d.Milliseconds()
	if err != nil {
		return err
	}

	for i := 1; i <= 3; i++ { // warm starts on the loaded data directory
		r, t, err := b.startReady(ctx, ver, dir, fmt.Sprintf("main-warm%d", i))
		if err != nil {
			return err
		}
		u0 := time.Now()
		if _, err := b.usable(ctx, r); err != nil {
			return err
		}
		t.UsableMS = t.ReadyMS + time.Since(u0).Milliseconds()
		b.res.Warm = append(b.res.Warm, t)
		b.logf("warm start %d: ready %d ms usable %d ms", i, t.ReadyMS, t.UsableMS)
		if _, err := b.stop(r); err != nil {
			return err
		}
	}
	_ = os.RemoveAll(dir)     // the loaded directory is no longer needed
	for i := 2; i <= 3; i++ { // two more cold starts on fresh directories
		d2 := b.dataDir(fmt.Sprintf("cold%d", i))
		r, t, err := b.startReady(ctx, ver, d2, fmt.Sprintf("main-cold%d", i))
		if err != nil {
			_ = os.RemoveAll(d2)
			return err
		}
		u0 := time.Now()
		if _, err := b.usable(ctx, r); err != nil {
			_ = os.RemoveAll(d2)
			return err
		}
		t.UsableMS = t.ReadyMS + time.Since(u0).Milliseconds()
		b.res.Cold = append(b.res.Cold, t)
		b.logf("cold start %d: ready %d ms usable %d ms", i, t.ReadyMS, t.UsableMS)
		_, err = b.stop(r)
		_ = os.RemoveAll(d2)
		if err != nil {
			return err
		}
	}
	return nil
}

// loadPhase runs the controller and creates ~100 objects through the e2e binary. When
// measure is true it samples RSS while the controller is still connected.
func (b *bench) loadPhase(ctx context.Context, r *running, kc, tag string, quick bool) error {
	ready := filepath.Join(b.scratch, "work", b.h.name()+"-"+tag+".ready")
	done := filepath.Join(b.scratch, "work", b.h.name()+"-"+tag+".done")
	_ = os.Remove(ready)
	_ = os.Remove(done)
	if quick { // upgrade run: no sampling, release the controller at once
		if err := os.WriteFile(done, []byte("done\n"), 0o600); err != nil {
			return err
		}
	}
	type sampled struct {
		s   *rssSample
		err error
	}
	sampledCh := make(chan sampled, 1)
	env := []string{"S3_PHASE=load", "S3_MARKER_READY=" + ready, "S3_MARKER_DONE=" + done}
	res, err := b.runE2E(kc, b.h.name()+"-"+tag+"-load", "^TestPhaseLoad$", env, func(p *proc) {
		go func() {
			defer func() { _ = os.WriteFile(done, []byte("done\n"), 0o600) }()
			for end := time.Now().Add(5 * time.Minute); time.Now().Before(end) && p.alive(); time.Sleep(100 * time.Millisecond) {
				if _, err := os.Stat(ready); err == nil {
					if quick {
						sampledCh <- sampled{}
						return
					}
					s, err := b.sample(ctx, r, p, settle)
					sampledCh <- sampled{s, err}
					return
				}
			}
			sampledCh <- sampled{err: fmt.Errorf("the load phase never signalled that its objects exist")}
		}()
	})
	if err != nil {
		return err
	}
	sm := <-sampledCh
	if probs := expectLoad().check("load phase", res); len(probs) > 0 {
		return fmt.Errorf("load phase failed: %v (log %s)", probs, res.Log)
	}
	if !quick {
		if sm.err != nil {
			return fmt.Errorf("load phase RSS sample: %w", sm.err)
		}
		b.res.Loaded = sm.s
	}
	return nil
}

var scrub = regexp.MustCompile(`^(=== |--- |\s{4}\S+_test\.go|PASS|FAIL|ok\s|panic)`)

// saveScrubbed keeps only test-framework lines of an e2e log in the repo, with scratch paths removed.
func (b *bench) saveScrubbed(src, name string) {
	bs, err := os.ReadFile(src)
	if err != nil {
		return
	}
	var out []string
	for _, l := range strings.Split(string(bs), "\n") {
		if scrub.MatchString(l) {
			out = append(out, strings.ReplaceAll(l, b.scratch, "$SCRATCH"))
		}
	}
	_ = os.WriteFile(filepath.Join(b.outDir, "logs", name), []byte(strings.Join(out, "\n")+"\n"), 0o644)
}

// upgradeRun: old version with the 100 objects, controller stopped, new version on the same
// data directory; every object must come back with the same UID and creation time. The outcome
// is stored in the result; the returned error is the first thing that went wrong, if anything.
func (b *bench) upgradeRun(ctx context.Context) error {
	oldV, newV := b.h.versions()
	u := &upgradeResult{From: oldV, To: newV}
	b.res.Upgrade = u
	b.beginPhase("upgrade", oldV, newV)
	defer b.endPhase("upgrade")
	fail := func(err error) error { u.Error = err.Error(); b.logf("upgrade failed: %v", err); return err }

	dir := b.dataDir("upgrade")
	defer os.RemoveAll(dir)
	b.logf("upgrade: start %s, load objects", oldV)
	r, _, err := b.startReady(ctx, oldV, dir, "upgrade-old")
	if err != nil {
		return fail(err)
	}
	kc, err := b.usable(ctx, r)
	if err != nil {
		return fail(err)
	}
	if err := b.loadPhase(ctx, r, kc, "upgrade", true); err != nil {
		return fail(err)
	}
	c, err := newClient(kc, r.ctxName)
	if err != nil {
		return fail(err)
	}
	before, err := snapshot(ctx, c)
	if err != nil {
		return fail(err)
	}
	u.ObjectsBefore, u.ServerBefore = len(before), serverVersion(kc, r.ctxName)
	b.logf("snapshot of %d objects on %s; stopping the old host (no controller running)", len(before), u.ServerBefore)
	// Stop to ready = how long the old host takes to stop plus how long the new one takes to be
	// ready. The wait for a quiet VM between the two (b.gate) is not part of it.
	stopDur, err := b.stop(r)
	if err != nil {
		return fail(err)
	}

	r2, tNew, err := b.startReady(ctx, newV, dir, "upgrade-new")
	if err != nil {
		return fail(err)
	}
	u.StopToReadyMS = stopDur.Milliseconds() + tNew.ReadyMS
	kc2, err := b.usable(ctx, r2)
	if err != nil {
		return fail(err)
	}
	u.ServerAfter = serverVersion(kc2, r2.ctxName)
	c2, err := newClient(kc2, r2.ctxName)
	if err != nil {
		return fail(err)
	}
	after, err := snapshot(ctx, c2)
	if err != nil {
		return fail(err)
	}
	u.ObjectsKept, u.Diffs = compareSnapshots(before, after)
	b.logf("after upgrade to %s: %d/%d objects identical, stop to ready %d ms", u.ServerAfter, u.ObjectsKept, u.ObjectsBefore, u.StopToReadyMS)

	if u.E2EAfter, err = b.runE2E(kc2, b.h.name()+"-upgrade", "", nil, nil); err != nil {
		return fail(err)
	}
	b.saveScrubbed(u.E2EAfter.Log, b.h.name()+"-e2e-after-upgrade.txt")
	if u.PostUpgrade, err = b.runE2E(kc2, b.h.name()+"-postupgrade", "^TestPhasePostUpgrade$", []string{"S3_PHASE=postupgrade"}, nil); err != nil {
		return fail(err)
	}
	b.saveScrubbed(u.PostUpgrade.Log, b.h.name()+"-post-upgrade.txt")
	if _, err := b.stop(r2); err != nil {
		return fail(err)
	}
	return nil
}
