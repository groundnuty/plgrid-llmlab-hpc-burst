package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

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
		b.upgradeRun(ctx)
	}
	return nil
}

// mainRun: cold start, idle RSS, load phase and its RSS, the e2e suite, then warm and cold restarts.
func (b *bench) mainRun(ctx context.Context) error {
	_, ver := b.h.versions()
	dir := b.dataDir("main")
	b.logf("cold start %s", ver)
	r, t, err := b.startReady(ctx, ver, dir, "main-cold1")
	if err != nil {
		return err
	}
	if ls, err := b.checkListeners(r); err != nil {
		b.res.Notes = append(b.res.Notes, err.Error())
		return err
	} else {
		b.res.Listeners = ls
	}
	b.logf("ready in %d ms (readyz %d ms); settling %s for idle RSS", t.ReadyMS, t.ReadyzMS, settle)
	b.res.Idle = b.sample(ctx, r, nil, settle)
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
	d, err := r.stopAll(60 * time.Second)
	b.res.StopMS = d.Milliseconds()
	if err != nil {
		return err
	}
	b.live = nil

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
		if _, err := r.stopAll(60 * time.Second); err != nil {
			return err
		}
		b.live = nil
	}
	for i := 2; i <= 3; i++ { // two more cold starts on fresh directories
		d2 := b.dataDir(fmt.Sprintf("cold%d", i))
		r, t, err := b.startReady(ctx, ver, d2, fmt.Sprintf("main-cold%d", i))
		if err != nil {
			return err
		}
		u0 := time.Now()
		if _, err := b.usable(ctx, r); err != nil {
			return err
		}
		t.UsableMS = t.ReadyMS + time.Since(u0).Milliseconds()
		b.res.Cold = append(b.res.Cold, t)
		b.logf("cold start %d: ready %d ms usable %d ms", i, t.ReadyMS, t.UsableMS)
		if _, err := r.stopAll(60 * time.Second); err != nil {
			return err
		}
		b.live = nil
		_ = os.RemoveAll(d2)
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
	sampled := make(chan *rssSample, 1)
	env := []string{"S3_PHASE=load", "S3_MARKER_READY=" + ready, "S3_MARKER_DONE=" + done}
	res, err := b.runE2E(kc, b.h.name()+"-"+tag+"-load", "^TestPhaseLoad$", env, func(p *proc) {
		go func() {
			defer func() { _ = os.WriteFile(done, []byte("done\n"), 0o600) }()
			for end := time.Now().Add(5 * time.Minute); time.Now().Before(end) && p.alive(); time.Sleep(100 * time.Millisecond) {
				if _, err := os.Stat(ready); err == nil {
					if quick {
						sampled <- nil
						return
					}
					sampled <- b.sample(ctx, r, p, settle)
					return
				}
			}
			sampled <- nil
		}()
	})
	if err != nil {
		return err
	}
	s := <-sampled
	if len(res.Failed) > 0 || res.Exit != 0 || len(res.Passed) != 1 {
		return fmt.Errorf("load phase failed (exit %d, passed %v, failed %v; log %s)", res.Exit, res.Passed, res.Failed, res.Log)
	}
	if !quick {
		if s == nil {
			return fmt.Errorf("load phase produced no RSS sample")
		}
		b.res.Loaded = s
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
// data directory; every object must come back with the same UID and creation time.
func (b *bench) upgradeRun(ctx context.Context) {
	oldV, newV := b.h.versions()
	u := &upgradeResult{From: oldV, To: newV}
	b.res.Upgrade = u
	fail := func(err error) { u.Error = err.Error(); b.logf("upgrade failed: %v", err) }

	dir := b.dataDir("upgrade")
	b.logf("upgrade: start %s, load objects", oldV)
	r, _, err := b.startReady(ctx, oldV, dir, "upgrade-old")
	if err != nil {
		fail(err)
		return
	}
	kc, err := b.usable(ctx, r)
	if err != nil {
		fail(err)
		return
	}
	if err := b.loadPhase(ctx, r, kc, "upgrade", true); err != nil {
		fail(err)
		return
	}
	c, err := newClient(kc, r.ctxName)
	if err != nil {
		fail(err)
		return
	}
	before, err := snapshot(ctx, c)
	if err != nil {
		fail(err)
		return
	}
	u.ObjectsBefore, u.ServerBefore = len(before), serverVersion(kc, r.ctxName)
	b.logf("snapshot of %d objects on %s; stopping the old host (no controller running)", len(before), u.ServerBefore)
	// Stop to ready = how long the old host takes to stop plus how long the new one takes to be
	// ready. The wait for a quiet VM between the two (b.gate) is not part of it.
	stopDur, err := r.stopAll(60 * time.Second)
	if err != nil {
		fail(err)
		return
	}
	b.live = nil

	r2, tNew, err := b.startReady(ctx, newV, dir, "upgrade-new")
	if err != nil {
		fail(err)
		return
	}
	u.StopToReadyMS = stopDur.Milliseconds() + tNew.ReadyMS
	kc2, err := b.usable(ctx, r2)
	if err != nil {
		fail(err)
		return
	}
	u.ServerAfter = serverVersion(kc2, r2.ctxName)
	c2, err := newClient(kc2, r2.ctxName)
	if err != nil {
		fail(err)
		return
	}
	after, err := snapshot(ctx, c2)
	if err != nil {
		fail(err)
		return
	}
	u.ObjectsKept, u.Diffs = compareSnapshots(before, after)
	b.logf("after upgrade to %s: %d/%d objects identical, stop to ready %d ms", u.ServerAfter, u.ObjectsKept, u.ObjectsBefore, u.StopToReadyMS)

	if u.E2EAfter, err = b.runE2E(kc2, b.h.name()+"-upgrade", "", nil, nil); err != nil {
		fail(err)
		return
	}
	b.saveScrubbed(u.E2EAfter.Log, b.h.name()+"-e2e-after-upgrade.txt")
	if u.PostUpgrade, err = b.runE2E(kc2, b.h.name()+"-postupgrade", "^TestPhasePostUpgrade$", []string{"S3_PHASE=postupgrade"}, nil); err != nil {
		fail(err)
		return
	}
	b.saveScrubbed(u.PostUpgrade.Log, b.h.name()+"-post-upgrade.txt")
	if _, err := r2.stopAll(60 * time.Second); err != nil {
		fail(err)
	}
	b.live = nil
}
