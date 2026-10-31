package stage

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// pair builds two Stagers on one staging directory, each with its own open of
// the lock files, as two Allocations have. flock locks conflict between two
// opens of one file even in one process; an fcntl lock would not, and this
// test would then pass without any lock.
func pair(t *testing.T, files map[string]string) (*Stager, *Stager, *fakeHub) {
	t.Helper()
	s1, hub, _ := newStager(t, files)
	s2, err := New(s1.c)
	if err != nil {
		t.Fatal(err)
	}
	return s1, s2, hub
}

// twoPartyBarrier returns a hook that holds each of two callers until both
// have arrived, or five seconds have passed.
func twoPartyBarrier() func() {
	arrive, release := make(chan struct{}, 2), make(chan struct{})
	go func() {
		<-arrive
		<-arrive
		close(release)
	}()
	return func() {
		arrive <- struct{}{}
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
	}
}

// Two stagers repair one checkpoint file at once. Both rehash it before either
// changes it, so both hold the same stale result. The first to take the lock
// downloads, and the Hub holds its download until the other is seen waiting for
// the lock. The other then finds the file good under the lock: it moves nothing
// aside and downloads nothing (Astra B6: it moved the good replacement aside
// on its stale result, or failed on the file the first had moved).
func TestConcurrentStagersRepairAFileOnce(t *testing.T) {
	for _, tc := range []struct {
		name    string
		onDisk  string // "" for a missing file
		restage bool
	}{
		{"corrupt file, restage", "tampered\n", true},
		{"missing file, first start", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s1, s2, hub := pair(t, map[string]string{resolvePath("config.json"): "hello weights\n"})
			dir := ckptDir(s1)
			os.MkdirAll(dir, 0o700)
			p := filepath.Join(dir, "config.json")
			if tc.onDisk != "" {
				os.WriteFile(p, []byte(tc.onDisk), 0o600)
			}
			busy, once := make(chan struct{}), sync.Once{}
			barrier := twoPartyBarrier()
			for _, s := range []*Stager{s1, s2} {
				s.hooks.afterRehash = barrier
				s.hooks.lockBusy = func() { once.Do(func() { close(busy) }) }
			}
			var mu sync.Mutex
			var asides []int // the .mismatch-* copies there are while the download is held
			hub.setIntercept(func(n int, w http.ResponseWriter, r *http.Request) bool {
				select {
				case <-busy:
				case <-time.After(3 * time.Second): // nobody waits: the lock does not work
				}
				m, _ := filepath.Glob(filepath.Join(dir, "*.mismatch-*"))
				mu.Lock()
				asides = append(asides, len(m))
				mu.Unlock()
				return false
			})

			req := agent.CheckpointRequest{
				Repository: "org/model", Revision: rev, Restage: tc.restage,
				Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
			}
			results := make([]agent.CheckpointResult, 2)
			errs := make([]error, 2)
			var wg sync.WaitGroup
			for i, s := range []*Stager{s1, s2} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[i], errs[i] = s.Checkpoint(context.Background(), req)
				}()
			}
			wg.Wait()

			for i := range results {
				if errs[i] != nil {
					t.Errorf("stager %d: %v", i, errs[i])
				} else if f := results[i].Files[0]; f.Outcome != agentapi.FileMatch {
					t.Errorf("stager %d: %+v", i, f)
				}
			}
			select {
			case <-busy:
			default:
				t.Error("neither stager ever waited for the other's lock")
			}
			if gets, _ := hub.requests(); len(gets) != 1 {
				t.Errorf("downloads %q, want exactly one", gets)
			}
			if b, _ := os.ReadFile(p); string(b) != "hello weights\n" {
				t.Errorf("the file holds %q", b)
			}
			wantAside := 0
			if tc.restage {
				wantAside = 1
			}
			mu.Lock()
			defer mu.Unlock()
			if len(asides) != 1 || asides[0] != wantAside {
				t.Errorf(".mismatch-* copies seen during the download: %v, want [%d]", asides, wantAside)
			}
			if m, _ := filepath.Glob(filepath.Join(dir, "*.mismatch-*")); len(m) != 0 {
				t.Errorf("copies moved aside and left: %q", m)
			}
		})
	}
}

// Waiting for the lock honours the context.
func TestLockWaitHonoursTheContext(t *testing.T) {
	s, hub, _ := newStager(t, map[string]string{resolvePath("config.json"): "hello weights\n"})
	dir := ckptDir(s)
	os.MkdirAll(dir, 0o700)
	held, ok, err := s.tryLock(filepath.Join(dir, lockName))
	if err != nil || !ok {
		t.Fatalf("tryLock: %v %v", ok, err)
	}
	defer held.release()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = s.Checkpoint(ctx, agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "config.json", SHA256: weightsSum}},
	})
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("err %v, want the context's deadline", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("returned after %v", d)
	}
	if gets, _ := hub.requests(); len(gets) != 0 {
		t.Errorf("downloaded %q without the lock", gets)
	}
}

// Where the filesystem has no flock, staging goes on, says so once, and does
// not fail.
func TestStagingGoesOnWhereFlockIsUnsupported(t *testing.T) {
	s, _, _ := newStager(t, map[string]string{
		resolvePath("a.json"): "hello weights\n",
		resolvePath("b.json"): "hello weights\n",
	})
	var log strings.Builder
	s.c.Log = &log
	s.hooks.flock = func(int, int) error { return syscall.ENOSYS }
	res, err := s.Checkpoint(context.Background(), agent.CheckpointRequest{
		Repository: "org/model", Revision: rev,
		Files: []agentapi.FileDigest{{Path: "a.json", SHA256: weightsSum}, {Path: "b.json", SHA256: weightsSum}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range res.Files {
		if f.Outcome != agentapi.FileMatch {
			t.Errorf("%+v", f)
		}
	}
	if n := strings.Count(log.String(), "flock is not supported"); n != 1 {
		t.Errorf("the notice was logged %d times: %q", n, log.String())
	}
}
