package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestCapVolume checks the test file system itself, so that the store-full test below stands on
// something that was shown to behave like a full disk: writes up to the limit succeed, the next
// ones fail with ENOSPC, statfs reports the limit, and freeing space makes writes work again.
func TestCapVolume(t *testing.T) {
	const limit = 2 << 20
	v := newCapVolume(t, limit)
	f, err := os.Create(filepath.Join(v.mount, "a"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	chunk := make([]byte, 64<<10)
	var written int64
	var werr error
	for i := 0; i < 200 && werr == nil; i++ {
		var n int
		n, werr = f.Write(chunk)
		written += int64(n)
	}
	if werr == nil || !strings.Contains(werr.Error(), "no space left") {
		t.Fatalf("after %d bytes the write did not fail with ENOSPC: %v", written, werr)
	}
	if written < limit/2 || written > limit {
		t.Errorf("wrote %d bytes before ENOSPC, limit %d", written, limit)
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(v.mount, &st); err != nil {
		t.Fatal(err)
	}
	if got := int64(st.Blocks) * int64(st.Bsize); got != limit {
		t.Errorf("statfs reports %d bytes, want %d", got, limit)
	}
	if int64(st.Bavail)*int64(st.Bsize) > limit/4 {
		t.Errorf("statfs reports %d bytes free on a full volume", int64(st.Bavail)*int64(st.Bsize))
	}
	if v.enospc.Load() == 0 {
		t.Error("the volume did not count a refusal")
	}
	f.Close()
	if err := os.Remove(filepath.Join(v.mount, "a")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(v.mount, "b"), chunk, 0o600); err != nil {
		t.Errorf("a write after freeing space failed: %v", err)
	}
}

// TestCapVolumeFrozen checks the frozen mode the store-full test stands on (#180): before the freeze, a
// rewrite inside a file that already has its blocks is accepted, even on a volume at its limit; once frozen,
// that rewrite, a write that grows the file and a truncation are all refused with ENOSPC, reads still work,
// and the refusals are counted as the freeze's; after the volume is unfrozen the rewrite is accepted again.
func TestCapVolumeFrozen(t *testing.T) {
	const limit = 2 << 20
	v := newCapVolume(t, limit)
	path := filepath.Join(v.mount, "wal")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	block := make([]byte, 64<<10)
	for i := 0; i < 64 && v.enospc.Load() == 0; i++ { // fill the volume with this one file
		if _, err := f.Write(block); err != nil {
			break
		}
	}
	if v.enospc.Load() == 0 {
		t.Fatal("the volume never reached its limit")
	}
	inPlace := func() error { _, err := f.WriteAt([]byte("rewritten"), 0); return err }
	if err := inPlace(); err != nil {
		t.Fatalf("a rewrite inside the file was refused on a volume that is only full: %v", err)
	}
	v.freeze()
	refusedBefore := v.frozenRefusals.Load()
	for name, op := range map[string]func() error{
		"a rewrite inside the file":  inPlace,
		"a write that grows it":      func() error { _, err := f.WriteAt(block, 1<<30); return err },
		"a truncation to its length": func() error { fi, _ := f.Stat(); return f.Truncate(fi.Size()) },
	} {
		if err := op(); err == nil || !strings.Contains(err.Error(), "no space left") {
			t.Errorf("%s on a frozen volume: %v, want ENOSPC", name, err)
		}
	}
	if got := v.frozenRefusals.Load() - refusedBefore; got < 3 {
		t.Errorf("the freeze refused %d requests, want at least the 3 above", got)
	}
	if b, err := os.ReadFile(path); err != nil || len(b) == 0 {
		t.Errorf("reading a frozen volume: %d bytes, %v", len(b), err)
	}
	v.unfreeze()
	if err := inPlace(); err != nil {
		t.Errorf("a rewrite inside the file after the unfreeze: %v", err)
	}
}

// TestStoreFull is HBEP-11's store-full case on k3s: the SQLite store sits on a size-limited file
// system, the file system fills up, writes are refused, and `hpcburst status` reports the write
// failures. Here the store directory (data/server/db) is a link to a 32 MiB volume; the rest of the
// data directory, with k3s's unpacked binaries, is on the ordinary disk.
func TestStoreFull(t *testing.T) { storeFull(t, false) }

// TestStoreFullWholeDataDir is the same case with the whole data directory on the volume, as the
// HBEP words it: data/ itself is a link to a volume that holds k3s's unpacked binaries (about
// 260 MiB) and the store, with 40 MiB to spare.
func TestStoreFullWholeDataDir(t *testing.T) { storeFull(t, true) }

const (
	// diskFull is what the API server says when SQLite refuses a write for lack of space: the text of
	// SQLITE_FULL, which the first refusal of the fill shows as "rpc error: code = Unknown desc = database or
	// disk is full". Only a refusal that says so counts as evidence that the store is full (#180).
	diskFull = "database or disk is full"
	// followingRefused: tiny writes that must be refused once the volume is frozen.
	followingRefused = 3
	// requestTimeout bounds one API request of the test. Each request has a context of its own, so the work
	// of the whole test (about two thousand requests at client-go's default five a second would take
	// minutes) cannot expire a shared context mid-test.
	requestTimeout = 60 * time.Second
)

// requireDiskFull fails the test unless err is the refusal of a full store. A context deadline, a
// cancellation or a transport error also ends a request, and must never be taken for evidence of fullness.
func requireDiskFull(t *testing.T, what string, err error) {
	t.Helper()
	switch {
	case err == nil:
		t.Fatalf("%s succeeded, but the store is full", what)
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "Client.Timeout"):
		t.Fatalf("%s ended by a timeout or a cancellation, which says nothing about the store: %v", what, err)
	case !strings.Contains(err.Error(), diskFull):
		t.Fatalf("%s failed for another reason than a full store (want %q): %v", what, diskFull, err)
	}
}

func storeFull(t *testing.T, wholeDataDir bool) {
	binDirOrSkip(t)
	limit := int64(32 << 20)
	if wholeDataDir {
		limit = 300 << 20
	}
	vol := newCapVolume(t, limit) // registered first, so it is unmounted last
	h := newHost(t)
	// Unfreeze before the host is stopped (clean-ups run last-in first-out), so that its shutdown meets a
	// volume that is only full, as it always did.
	t.Cleanup(vol.unfreeze)
	if wholeDataDir {
		if err := os.MkdirAll(h.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(vol.mount, filepath.Join(h.dir, "data")); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := os.MkdirAll(filepath.Join(h.dir, "data", "server"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(vol.mount, filepath.Join(h.dir, "data", "server", "db")); err != nil {
			t.Fatal(err)
		}
	}

	if r := cli(t, h.upArgs(29220)...); r.code != exitOK {
		t.Fatalf("up exited %d", r.code)
	}
	var rep struct {
		Health  string   `json:"health"`
		Reasons []string `json:"reasons"`
		Readyz  *bool    `json:"readyz_ok"`
		KCM     *bool    `json:"controller_manager_ok"`
		Writes  *bool    `json:"writes_ok"`
	}
	r := cli(t, h.args("status", "--json")...)
	parseJSON(t, r.out, &rep)
	if r.code != exitOK || rep.Health != "healthy" || rep.Writes == nil || !*rep.Writes {
		t.Fatalf("a fresh host on the volume is not healthy: exit %d %+v", r.code, rep)
	}
	if got := vol.used(); got == 0 {
		t.Fatal("the store is not on the size-limited volume: it holds no data")
	}
	t.Logf("the volume holds %d KiB of a %d KiB limit before the fill; refusals so far: %d", vol.used()>>10, vol.limit>>10, vol.enospc.Load())

	// Fill it with large objects, then smaller and smaller ones, until even tiny writes fail. Every refusal
	// must be the full store's (requireDiskFull); a request of its own context each.
	c := h.clients()
	ctx := func() (context.Context, context.CancelFunc) { return context.WithTimeout(t.Context(), requestTimeout) }
	nsCtx, nsCancel := ctx()
	_, err := c.cs.CoreV1().Namespaces().Create(nsCtx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "fill"}}, metav1.CreateOptions{})
	nsCancel()
	if err != nil {
		t.Fatal(err)
	}
	create := func(name, payload string) error {
		rctx, cancel := ctx()
		defer cancel()
		_, err := c.cs.CoreV1().ConfigMaps("fill").Create(rctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: map[string]string{"d": payload}}, metav1.CreateOptions{})
		return err
	}
	created, failed := 0, 0
	var firstErr error
	for _, size := range []int{700 << 10, 100 << 10, 10 << 10, 1 << 10, 100} {
		payload := strings.Repeat("x", size)
		for i := 0; i < 400; i++ {
			err := create(fmt.Sprintf("f-%d-%03d", size, i), payload)
			if err != nil {
				requireDiskFull(t, fmt.Sprintf("the fill's %d-byte write number %d", size, i), err)
				failed++
				if firstErr == nil {
					firstErr = err
					t.Logf("first refused write, at %d bytes after %d objects: %v", size, created, err)
				}
				break
			}
			created++
		}
	}
	t.Logf("created %d objects; the volume refused %d requests; volume used %d of %d KiB", created, vol.enospc.Load(), vol.used()>>10, vol.limit>>10)
	if firstErr == nil || vol.enospc.Load() == 0 {
		t.Fatalf("the fill never hit the limit (first error %v, refusals %d)", firstErr, vol.enospc.Load())
	}

	// The store is full, but a volume that is merely at its limit still accepts a write that fits in space
	// its files already hold (SQLite reusing pages, its WAL restarting after a checkpoint), at any moment, so
	// "the store is full" is not a state that counting refusals can establish. The volume is frozen instead:
	// from here every write is refused, which models the steady state of a full store. A real full disk may
	// accept such rewrites now and then; status must report failing writes whenever its own probe is
	// refused, and the assertions below are about the state in which every write is.
	vol.freeze()
	t.Logf("the volume is frozen at %d of %d KiB", vol.used()>>10, vol.limit>>10)

	// Writes are refused, even a tiny one, and by the full store; reads are not.
	for i := 0; i < followingRefused; i++ {
		requireDiskFull(t, fmt.Sprintf("tiny write %d on the frozen volume", i+1), create(fmt.Sprintf("one-more-%d", i), "x"))
	}
	if vol.frozenRefusals.Load() == 0 {
		t.Fatal("the frozen volume refused no request: the freeze did not take effect, and the refusals above came from elsewhere")
	}
	lctx, lcancel := ctx()
	list, err := c.cs.CoreV1().ConfigMaps("fill").List(lctx, metav1.ListOptions{})
	lcancel()
	if err != nil {
		t.Errorf("reads should still work: %v", err)
	} else if len(list.Items) < created {
		t.Errorf("reads lost objects: %d listed, %d created", len(list.Items), created)
	} else if len(list.Items) > created {
		// HBEP-11, Operational readiness 3: a write can return an error and still take effect.
		// Here a refused create left an object behind, which is why controllers re-read after
		// every failed write.
		t.Logf("%d object(s) exist although their create returned an error: a refused write that took effect", len(list.Items)-created)
	}

	// status reports the write failure, with its cause. The process view alone cannot see it. Its own write
	// probe is refused by the frozen volume, so what it reports is judged against storage: the reason must say
	// the store is full, not only that writes are failing, which a write that merely timed out would also say.
	r = cli(t, h.args("status", "--json")...)
	parseJSON(t, r.out, &rep)
	t.Logf("NUMBERS store full: status exit %d, health %s, readyz_ok=%v, controller_manager_ok=%v, writes_ok=%v, reasons=%v",
		r.code, rep.Health, ptrStr(rep.Readyz), ptrStr(rep.KCM), ptrStr(rep.Writes), rep.Reasons)
	if r.code != exitFail || rep.Health != "unhealthy" || rep.Writes == nil || *rep.Writes {
		t.Fatalf("status did not report the failing writes: exit %d %+v", r.code, rep)
	}
	reasons := strings.Join(rep.Reasons, ";")
	if !strings.Contains(reasons, "writes are failing") || !strings.Contains(reasons, diskFull) {
		t.Errorf("reasons %v do not say that writes are failing because the store is full (%q)", rep.Reasons, diskFull)
	}
	if r := cli(t, h.args("status", "--no-api")...); r.code != exitOK {
		t.Errorf("the process-only view should still be healthy (that is why the write probe exists): exit %d", r.code)
	}
	if r := cli(t, h.args("status")...); r.code != exitFail || !strings.Contains(r.out, "writes are failing") || !strings.Contains(r.out, diskFull) {
		t.Errorf("text status: exit %d: %q", r.code, r.out)
	}
}

func ptrStr(b *bool) string {
	if b == nil {
		return "unset"
	}
	return fmt.Sprint(*b)
}
