package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

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

// TestStoreFull is HBEP-11's store-full case on k3s: the SQLite store sits on a size-limited file
// system, the file system fills up, writes are refused, and `hpcburst status` reports the write
// failures. Here the store directory (data/server/db) is a link to a 32 MiB volume; the rest of the
// data directory, with k3s's unpacked binaries, is on the ordinary disk.
func TestStoreFull(t *testing.T) { storeFull(t, false) }

// TestStoreFullWholeDataDir is the same case with the whole data directory on the volume, as the
// HBEP words it: data/ itself is a link to a volume that holds k3s's unpacked binaries (about
// 260 MiB) and the store, with 40 MiB to spare.
func TestStoreFullWholeDataDir(t *testing.T) { storeFull(t, true) }

func storeFull(t *testing.T, wholeDataDir bool) {
	binDirOrSkip(t)
	limit := int64(32 << 20)
	if wholeDataDir {
		limit = 300 << 20
	}
	vol := newCapVolume(t, limit) // registered first, so it is unmounted last
	h := newHost(t)
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

	// Fill it with large objects, then smaller and smaller ones, until even tiny writes fail.
	c := h.clients()
	ctx, cancel := testCtx()
	defer cancel()
	if _, err := c.cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "fill"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	created, failed := 0, 0
	var firstErr error
	for _, size := range []int{700 << 10, 100 << 10, 10 << 10, 1 << 10, 100} {
		payload := strings.Repeat("x", size)
		for i := 0; i < 400; i++ {
			_, err := c.cs.CoreV1().ConfigMaps("fill").Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("f-%d-%03d", size, i)}, Data: map[string]string{"d": payload}}, metav1.CreateOptions{})
			if err != nil {
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
	// The refusal must be the full disk, not a timeout or a connection error that would also end the fill.
	if !strings.Contains(firstErr.Error(), "database or disk is full") {
		t.Fatalf("the first refused write failed for another reason than a full store: %v", firstErr)
	}
	// Writes are refused, even a tiny one; reads are not.
	if _, err := c.cs.CoreV1().ConfigMaps("fill").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "one-more"}, Data: map[string]string{"d": "x"}}, metav1.CreateOptions{}); err == nil {
		t.Fatal("a tiny write still succeeds: the store is not full")
	}
	list, err := c.cs.CoreV1().ConfigMaps("fill").List(ctx, metav1.ListOptions{})
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

	// status reports the write failure. The process view alone cannot see it.
	r = cli(t, h.args("status", "--json")...)
	parseJSON(t, r.out, &rep)
	t.Logf("NUMBERS store full: status exit %d, health %s, readyz_ok=%v, controller_manager_ok=%v, writes_ok=%v, reasons=%v",
		r.code, rep.Health, ptrStr(rep.Readyz), ptrStr(rep.KCM), ptrStr(rep.Writes), rep.Reasons)
	if r.code != exitFail || rep.Health != "unhealthy" || rep.Writes == nil || *rep.Writes {
		t.Fatalf("status did not report the failing writes: exit %d %+v", r.code, rep)
	}
	if !strings.Contains(strings.Join(rep.Reasons, ";"), "writes are failing") {
		t.Errorf("reasons %v do not say that writes are failing", rep.Reasons)
	}
	if r := cli(t, h.args("status", "--no-api")...); r.code != exitOK {
		t.Errorf("the process-only view should still be healthy (that is why the write probe exists): exit %d", r.code)
	}
	if r := cli(t, h.args("status")...); r.code != exitFail || !strings.Contains(r.out, "writes are failing") {
		t.Errorf("text status: exit %d: %q", r.code, r.out)
	}
}

func ptrStr(b *bool) string {
	if b == nil {
		return "unset"
	}
	return fmt.Sprint(*b)
}
