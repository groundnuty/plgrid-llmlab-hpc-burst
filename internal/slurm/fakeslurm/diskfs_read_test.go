package fakeslurm

// New. B4 of the PR 153 review: the run-mode reads of the disk are bounded.
// The expected values are the limits set by the test itself and the SHA-256
// of content the test made, computed with crypto/sha256 directly; the files
// that go over a limit are sparse, so a read that ignored the limit would cost
// the memory of the claimed size, and the cases run well under the cap with
// the limits lowered. A reader that serves bytes without end fails its test
// with an error of its own after a few times the limit, so a mutant that
// reads without bound fails cleanly instead of eating the VM.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// endlessReader serves zero bytes, counting them, and fails with errRunaway
// once it has served runaway of them.
type endlessReader struct {
	served, runaway int64
}

var errRunaway = errors.New("the reader was read without bound")

func (r *endlessReader) Read(b []byte) (int, error) {
	if r.served >= r.runaway {
		return 0, errRunaway
	}
	n := int64(len(b))
	if left := r.runaway - r.served; n > left {
		n = left
	}
	clear(b[:n])
	r.served += n
	return int(n), nil
}

// A file that grows while it is read, which is a reader that does not end, is
// read for the limit and one byte more, and no further.
func TestReadCappedStopsAtTheLimit(t *testing.T) {
	const limit = 1000
	for name, read := range map[string]func(*endlessReader) error{
		"readCapped": func(r *endlessReader) error { _, err := readCapped(r, limit); return err },
		"copyCapped": func(r *endlessReader) error { return copyCapped(sha256.New(), r, limit) },
	} {
		t.Run(name, func(t *testing.T) {
			r := &endlessReader{runaway: 100 * limit}
			if err := read(r); !errors.Is(err, errTooLarge) {
				t.Errorf("error %v, want errTooLarge", err)
			}
			if r.served != limit+1 {
				t.Errorf("served %d bytes, want the limit and one more (%d)", r.served, limit+1)
			}
		})
	}
	// The boundary: a reader of exactly the limit is fine, one byte more is not.
	for _, c := range []struct {
		size    int
		tooLong bool
	}{{limit, false}, {limit + 1, true}} {
		b, err := readCapped(strings.NewReader(strings.Repeat("x", c.size)), limit)
		if (err != nil) != c.tooLong || (err == nil && len(b) != c.size) {
			t.Errorf("readCapped of %d bytes: %d bytes, %v; want an error: %v", c.size, len(b), err, c.tooLong)
		}
	}
}

// writeSized makes a file of n bytes; sparse, it costs no disk and no memory.
func writeSized(t *testing.T, name string, n int64) {
	t.Helper()
	f, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(n); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// The snapshot has a limit per file, a limit in all and a limit on the number
// of files, each an error that names what broke it; a tree inside the limits,
// with a file of exactly the limit, is read whole.
func TestDiskFSSnapshotLimits(t *testing.T) {
	for _, c := range []struct {
		name    string
		sizes   []int64 // the files, f0, f1, ...
		wantErr string  // "" for a snapshot that works
	}{
		{"inside the limits, one at the per-file limit", []int64{64, 60, 10}, ""},
		{"one byte over the per-file limit", []int64{65}, "f0: larger than the run-mode limit (65 bytes, the limit per file is 64)"},
		{"a sparse file far over the per-file limit", []int64{1 << 36}, "the limit per file is 64"},
		{"over the limit in all", []int64{60, 60, 60}, "the limit in all is 150"},
		{"over the limit on the files", []int64{0, 0, 0, 0}, "more than 3 files"},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			for i, n := range c.sizes {
				writeSized(t, fmt.Sprintf("%s/f%d", root, i), n)
			}
			d := newTestDiskFS(t, root)
			d.fileLimit, d.totalLimit, d.maxFiles = 64, 150, 3
			files, err := d.snapshotFiles()
			if c.wantErr == "" {
				if err != nil || len(files) != len(c.sizes) {
					t.Fatalf("snapshot: %d files, %v; want %d", len(files), err, len(c.sizes))
				}
				return
			}
			if !errors.Is(err, errTooLarge) || !strings.Contains(err.Error(), c.wantErr) || files != nil {
				t.Errorf("snapshot: %d files, error %v; want no map and an errTooLarge holding %q", len(files), err, c.wantErr)
			}
		})
	}
}

// Files has the in-memory signature, so a snapshot over a limit is a panic
// that names the file, and a small tree is read as before.
func TestRunModeFilesPanicsOverTheLimit(t *testing.T) {
	r := newRunRig(t, RunMode{})
	if err := os.WriteFile(r.home+"/small", []byte("small"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := r.p.Files()[r.home+"/small"]; string(got.Content) != "small" || got.Mode != 0o644 {
		t.Fatalf("Files()[small] = %q, mode %v", got.Content, got.Mode)
	}
	writeSized(t, r.home+"/huge", maxFileBytes+1)
	defer func() {
		err, _ := recover().(error)
		if !errors.Is(err, errTooLarge) || !strings.Contains(err.Error(), "huge") {
			t.Errorf("Files over the limit panicked with %v, want errTooLarge naming the file", err)
		}
	}()
	r.p.Files()
	t.Errorf("Files returned for a file over %d bytes", maxFileBytes)
}

// sha256sum streams the file, and a file over the limit is an error, not a
// hash of a prefix.
func TestRunModeSha256sumStreamsWithinTheLimit(t *testing.T) {
	r := newRunRig(t, RunMode{})
	big := strings.Repeat("0123456789abcdef", 3<<16) // 3 MiB, many chunks
	sum := sha256.Sum256([]byte(big))
	if err := os.WriteFile(r.home+"/big", []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	if out := r.must("sha256sum " + r.home + "/big"); out != hex.EncodeToString(sum[:])+"  "+r.home+"/big\n" {
		t.Errorf("sha256sum of 3 MiB: %q", out)
	}
	r.p.fs.(*diskFS).hashLimit = int64(len(big))
	if out := r.must("sha256sum " + r.home + "/big"); !strings.HasPrefix(out, hex.EncodeToString(sum[:])) {
		t.Errorf("sha256sum of a file of exactly the limit: %q", out)
	}
	r.p.fs.(*diskFS).hashLimit = int64(len(big)) - 1
	writeSized(t, r.home+"/sparse", 1<<36)
	for _, name := range []string{"big", "sparse"} {
		if out, errOut, code := r.run("sha256sum " + r.home + "/" + name); code != 1 || out != "" || !strings.Contains(errOut, "larger than the run-mode limit") {
			t.Errorf("sha256sum of %s over the limit: exit %d, stdout %q, stderr %q; want exit 1 and the limit named", name, code, out, errOut)
		}
	}
}

// Files and Dirs read the disk without Platform.mu, so a long read blocks no
// reader of the platform and no scancel.
func TestRunModeSnapshotsHoldNoLock(t *testing.T) {
	r := newRunRig(t, RunMode{})
	if err := os.WriteFile(r.home+"/f", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	r.p.mu.Lock()
	held := true
	unlock := func() {
		if held {
			held = false
			r.p.mu.Unlock()
		}
	}
	defer unlock()
	returnsAtOnce(t, "Files with Platform.mu held", unlock, func() {
		if _, ok := r.p.Files()[r.home+"/f"]; !ok {
			t.Errorf("Files lacks %s/f", r.home)
		}
	})
	returnsAtOnce(t, "Dirs with Platform.mu held", unlock, func() {
		if _, ok := r.p.Dirs()[r.home]; !ok {
			t.Errorf("Dirs lacks %s", r.home)
		}
	})
}
