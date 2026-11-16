package main

// A size-limited file system for the store-full test, built without root or any system change.
// An ordinary user cannot mount a loop device or a tmpfs here (user namespaces are restricted),
// but /dev/fuse is writable and fusermount3 is setuid, so a FUSE file system in this process can
// serve a small volume. It passes everything through to a directory (go-fuse's loopback file
// system, BSD-3-Clause) and answers ENOSPC to any write, allocation or truncation that would take
// the directory above its limit, as a full disk does. It also reports the limit in statfs.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const capBlock = 4096

type capVolume struct {
	backing string
	limit   atomic.Int64 // bytes; grow raises it, as freeing space on a full disk would
	enospc  atomic.Int64 // how many requests were refused, so a test can show the volume really was full
	server  *fuse.Server
	watch   *capWatcher // unmounts the mount if this process dies while it is mounted; nil when the volume has none
	mount   string

	// frozen is the steady state of a full store (#180): once set, every write, allocation and
	// truncation is refused with ENOSPC, including one that fits in space the files already hold, which
	// a volume that is merely at its limit accepts (SQLite reusing pages, or its WAL restarting after a
	// checkpoint, writes inside the files). A real full disk can accept such rewrites now and then; the
	// frozen volume models the state in which none is, so that "the store is full" holds until the test
	// unfreezes it, and a test can make assertions about that state without racing the store's own
	// housekeeping. frozenRefusals counts the requests that only the freeze refused.
	frozen         atomic.Bool
	frozenRefusals atomic.Int64
}

// grow adds bytes to the volume's limit, as freeing space on a full disk does.
func (v *capVolume) grow(bytes int64) { v.limit.Add(bytes) }

// freeze makes the volume refuse every write from now on; unfreeze lifts it.
func (v *capVolume) freeze()   { v.frozen.Store(true) }
func (v *capVolume) unfreeze() { v.frozen.Store(false) }

// used is the space the files in the backing directory take, in bytes of allocated blocks.
func (v *capVolume) used() int64 {
	var n int64
	_ = filepath.Walk(v.backing, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && fi.Mode().IsRegular() {
			if st, ok := fi.Sys().(*syscall.Stat_t); ok {
				n += st.Blocks * 512
			} else {
				n += fi.Size()
			}
		}
		return nil
	})
	return n
}

// admit decides whether growing the file at path to newSize fits. A frozen volume refuses every
// request, whatever it would take.
func (v *capVolume) admit(path string, newSize int64) syscall.Errno {
	if v.frozen.Load() {
		v.frozenRefusals.Add(1)
		v.enospc.Add(1)
		return syscall.ENOSPC
	}
	var cur int64
	if fi, err := os.Stat(path); err == nil {
		cur = fi.Size()
	}
	growth := newSize - cur
	if growth <= 0 {
		return 0
	}
	if v.used()+growth > v.limit.Load() {
		v.enospc.Add(1)
		return syscall.ENOSPC
	}
	return 0
}

type capNode struct {
	fs.LoopbackNode
	vol *capVolume
}

func (n *capNode) backingPath() string {
	return filepath.Join(n.RootData.Path, n.Path(n.RootData.RootNode.EmbeddedInode()))
}

var (
	_ fs.NodeWriter    = (*capNode)(nil)
	_ fs.NodeWritever  = (*capNode)(nil)
	_ fs.NodeAllocater = (*capNode)(nil)
	_ fs.NodeSetattrer = (*capNode)(nil)
	_ fs.NodeStatfser  = (*capNode)(nil)
)

func (n *capNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	if e := n.vol.admit(n.backingPath(), off+int64(len(data))); e != 0 {
		return 0, e
	}
	w, ok := f.(fs.FileWriter)
	if !ok {
		return 0, syscall.ENOSYS
	}
	return w.Write(ctx, data, off)
}

func (n *capNode) Writev(ctx context.Context, f fs.FileHandle, data [][]byte, off int64) (uint32, syscall.Errno) {
	var all []byte
	for _, d := range data {
		all = append(all, d...)
	}
	return n.Write(ctx, f, all, off)
}

func (n *capNode) Allocate(ctx context.Context, f fs.FileHandle, off, size uint64, mode uint32) syscall.Errno {
	if e := n.vol.admit(n.backingPath(), int64(off+size)); e != 0 {
		return e
	}
	a, ok := f.(fs.FileAllocater)
	if !ok {
		return syscall.ENOSYS
	}
	return a.Allocate(ctx, off, size, mode)
}

func (n *capNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		if e := n.vol.admit(n.backingPath(), int64(sz)); e != 0 {
			return e
		}
	}
	return n.LoopbackNode.Setattr(ctx, f, in, out)
}

func (n *capNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	used := n.vol.used()
	free := n.vol.limit.Load() - used
	if free < 0 {
		free = 0
	}
	out.Bsize, out.Frsize, out.NameLen = capBlock, capBlock, 255
	out.Blocks = uint64(n.vol.limit.Load() / capBlock)
	out.Bfree, out.Bavail = uint64(free/capBlock), uint64(free/capBlock)
	out.Files, out.Ffree = 1<<16, 1<<16
	return 0
}

// capFsName is the file system name a volume's mount shows in /proc/self/mountinfo (its source), and capFsType its type. The sweep of
// dead mounts (capfs_sweep_test.go) finds a mount by them, so the mount below and the sweep must agree. capFsWedgeName is the source of
// the one mount a test wedges on purpose (TestTheSweepLeavesAWedgedMountAloneAndReturnsPromptly): a newCapVolume of another run on the
// same machine sweeps it once it is dead, but does not fail on it while that test holds it wedged.
const (
	capFsName      = "hpcburst-capfs"
	capFsWedgeName = "hpcburst-capfs-wedge"
	capFsType      = "fuse.capfs"
)

// capMountError is the failure of the FUSE mount itself, as against a failure to prepare for it.
type capMountError struct{ err error }

func (e capMountError) Error() string { return e.err.Error() }
func (e capMountError) Unwrap() error { return e.err }

// mountCapVolume mounts a volume of the given size under dir, which it fills with a backing directory and a mount point, and returns it
// with its server running in this process. The mount carries source as its name. newCapVolume mounts through it, and so does the child
// process of the sweep's tests, so that both mount alike. watch starts a watcher first, which unmounts the mount if this process dies
// while it is mounted, and logs to watch.log in dir (capfs_mount_test.go says what that covers).
func mountCapVolume(dir string, limit int64, source string, watch bool) (*capVolume, error) {
	if real, err := filepath.EvalSymlinks(dir); err == nil {
		dir = real // mountinfo and fusermount3 name a mount point by its real path
	}
	v := &capVolume{backing: filepath.Join(dir, "backing"), mount: filepath.Join(dir, "mnt")}
	v.limit.Store(limit)
	for _, d := range []string{v.backing, v.mount} {
		if err := os.Mkdir(d, 0o700); err != nil {
			return nil, err
		}
	}
	rootData := &fs.LoopbackRoot{Path: v.backing}
	rootData.NewNode = func(r *fs.LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
		return &capNode{LoopbackNode: fs.LoopbackNode{RootData: r}, vol: v}
	}
	var st syscall.Stat_t
	if err := syscall.Stat(v.backing, &st); err != nil {
		return nil, err
	}
	rootData.Dev = uint64(st.Dev)
	root := rootData.NewNode(rootData, nil, "", &st)
	rootData.RootNode = root
	if watch {
		w, err := startCapWatcher(v.mount, filepath.Join(dir, "watch.log"))
		if err != nil {
			return nil, err
		}
		v.watch = w
	}
	conn, err := fusermountCapfs(v.mount, source)
	if err != nil {
		v.releaseWatcher()
		return nil, capMountError{err}
	}
	// go-fuse serves the descriptor through its /dev/fd/N mount point, and so never runs fusermount3 itself.
	srv, err := fs.Mount(fmt.Sprintf("/dev/fd/%d", conn.fd), root, &fs.Options{MountOptions: fuse.MountOptions{FsName: source, Name: "capfs", MaxWrite: capMaxWrite}})
	if err != nil {
		uerr := capUnmount(v.mount, capUnmountTimeout)
		if uerr == nil {
			v.releaseWatcher()
		}
		return nil, capMountError{fmt.Errorf("serving the mount at %s: %w (its unmount: %v)", v.mount, err, uerr)}
	}
	v.server = srv
	return v, nil
}

// releaseWatcher releases the volume's watcher, if it has one, once the mount is gone.
func (v *capVolume) releaseWatcher() error {
	if v.watch == nil {
		return nil
	}
	w := v.watch
	v.watch = nil
	return w.release(capWatchTimeout)
}

// unmount unmounts the volume and stops its server, each step bounded by d: fusermount3 -u, retried while the mount is busy, then the
// end of the server, then the release of its watcher, which finds the mount gone and exits. A mount that will not unmount keeps its
// watcher, which would only leave a live mount alone.
func (v *capVolume) unmount(d time.Duration) error {
	deadline := time.Now().Add(d)
	var err error
	for {
		if err = capUnmount(v.mount, capUnmountTimeout); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	served := make(chan struct{})
	go func() { v.server.Wait(); close(served) }()
	select {
	case <-served:
	case <-time.After(d):
		return fmt.Errorf("the server of %s did not stop within %s of its unmount", v.mount, d)
	}
	return v.releaseWatcher()
}

// newCapVolume mounts a volume of the given size and registers its unmount with the test.
// Register it before the host's clean-up, so that the unmount runs after the host is stopped
// (clean-ups run last-in first-out). If FUSE cannot be mounted it skips with the exact reason,
// unless HPCBURST_REQUIRE_INTEGRATION=1, which turns that skip into a failure.
//
// The server runs in the test's own process, so a test that is killed never runs the clean-up
// (issue #648). Its mount has a watcher, which unmounts it once the process is gone; and before
// it mounts, newCapVolume sweeps this user's capfs mounts whose endpoint is dead, which a SIGKILL
// of the whole scope leaves (sweepDeadCapMounts). A mount found wedged (open, but its stat does
// not return) fails the test here, since any later stat of it, or a removal of its directory,
// would block.
func newCapVolume(t *testing.T, limit int64) *capVolume {
	t.Helper()
	if res := sweepDeadCapMounts(realSweepEnv(t.Logf)); res.failure() != "" {
		t.Fatal(res.failure())
	}
	dir, err := os.MkdirTemp("", "cv")
	if err != nil {
		t.Fatal(err)
	}
	v, err := mountCapVolume(dir, limit, capFsName, true)
	if err != nil {
		if rerr := capRemoveDir(dir); rerr != nil {
			t.Logf("%v", rerr)
		}
		var me capMountError
		if !errors.As(err, &me) {
			t.Fatal(err)
		}
		msg := "the store-full test needs a size-limited file system, and a user-level FUSE mount failed: " + err.Error()
		if os.Getenv("HPCBURST_REQUIRE_INTEGRATION") == "1" {
			t.Fatal(msg)
		}
		t.Skip("SKIPPED, NOT TESTED: " + msg)
	}
	t.Cleanup(func() {
		if err := v.unmount(30 * time.Second); err != nil {
			t.Errorf("unmount %s: %v", v.mount, err)
		}
		if err := capRemoveDir(dir); err != nil {
			t.Errorf("%v", err)
		}
	})
	return v
}

// capRemoveDir removes dir, unless /proc/self/mountinfo lists a mount at or under it: a removal would stat that mount, and block for
// good on a wedged one. Such a directory is left in place, and the error says why.
func capRemoveDir(dir string) error {
	if real, err := filepath.EvalSymlinks(dir); err == nil {
		dir = real
	}
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return fmt.Errorf("%s is left in place: /proc/self/mountinfo cannot be read to show that nothing is mounted under it: %w", dir, err)
	}
	if p, ok := mountedUnder(string(data), dir); ok {
		return fmt.Errorf("%s is left in place: %s is still mounted", dir, p)
	}
	return os.RemoveAll(dir)
}
