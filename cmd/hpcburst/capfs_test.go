package main

// A size-limited file system for the store-full test, built without root or any system change.
// An ordinary user cannot mount a loop device or a tmpfs here (user namespaces are restricted),
// but /dev/fuse is writable and fusermount3 is setuid, so a FUSE file system can serve a small
// volume. It passes everything through to a directory (go-fuse's loopback file system,
// BSD-3-Clause) and answers ENOSPC to any write, allocation or truncation that would take the
// directory above its limit, as a full disk does. It also reports the limit in statfs.
//
// The server of a volume runs in a CHILD process (capfs_volume_test.go), never in the process of
// the test that uses the volume (issue #658). A test process whose own thread waits on its own
// mount for a request that its own server has read cannot finish dying when it is killed, and then
// holds /dev/fuse for good: the mount is wedged. With the server in another process, a killed test
// leaves the server to die with it (Pdeathsig) and the connection ends: the mount is dead, and the
// next newCapVolume sweeps it. This file is the server side. A test changes a running volume's
// settings, and reads its counts, through files in a control directory beside the mount
// (capfs_volume_test.go), so that nothing a test asks of a volume goes through the mount.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	capBlock = 4096
	// capStallMax bounds how long the server holds a write that a test asked it to hold (capCtlStall). It is longer than any wait of the
	// tests, so that only the end of the server, or of the hold's request, releases a held write inside a test's bounds.
	capStallMax = 2 * time.Minute
)

// capVolume is the server side of a volume: it runs where mountCapVolume was called, which is the child process of newCapVolume.
type capVolume struct {
	backing string
	ctl     capCtl // the limit, the freeze and the counts, in files (capfs_volume_test.go)
	server  *fuse.Server
	conn    *capConn // the mount's connection, and fusermount3 with auto_unmount, which stays until its socket closes
	mount   string

	stop     chan struct{} // closed by end: a write held for a test (capCtlStall) is released by it
	stopOnce sync.Once
}

// end releases every write the volume is holding, and is called before an unmount, so that a held request does not keep the mount busy.
func (v *capVolume) end() { v.stopOnce.Do(func() { close(v.stop) }) }

// capUsed is the space the files in a backing directory take, in bytes of allocated blocks.
func capUsed(backing string) int64 {
	var n int64
	_ = filepath.Walk(backing, func(_ string, fi os.FileInfo, err error) error {
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

// fault is the answer to a control directory that cannot be read or written: the server says so on its stderr and fails the request with
// EIO, so that a test sees a failure of the volume, and never a number or a state that the server made up.
func (v *capVolume) fault(err error) syscall.Errno {
	fmt.Fprintf(os.Stderr, "capfs: the control directory %s: %v\n", v.ctl.dir, err)
	return syscall.EIO
}

// refuse counts a refused request in the control directory, and then answers ENOSPC. The count is made before the answer, so that a test
// that has seen the error reads it. A request that cannot be counted fails with EIO instead (fault). frozen says the freeze alone refused it.
func (v *capVolume) refuse(frozen bool) syscall.Errno {
	names := []string{capCtlRefused}
	if frozen {
		names = append(names, capCtlFrozenRefused)
	}
	for _, name := range names {
		if err := v.ctl.bump(name); err != nil {
			return v.fault(err)
		}
	}
	return syscall.ENOSPC
}

// holdIfAsked holds a write while the control directory says so (capCtlStall): it records that it holds one (capCtlStalled), outside the
// mount, and waits until the hold is lifted, the volume ends, or capStallMax has passed. A test shows with it a request that the server has
// already read, and has not answered.
func (v *capVolume) holdIfAsked() {
	on, err := v.ctl.flag(capCtlStall)
	if err != nil {
		v.fault(err)
		return
	}
	if !on {
		return
	}
	if err := v.ctl.setFlag(capCtlStalled, true); err != nil {
		v.fault(err)
		return
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	limit := time.NewTimer(capStallMax)
	defer limit.Stop()
	for {
		select {
		case <-v.stop:
			return
		case <-limit.C:
			return
		case <-tick.C:
		}
		if still, err := v.ctl.flag(capCtlStall); err != nil || !still {
			return
		}
	}
}

// admit decides whether growing the file at path to newSize fits. A frozen volume refuses every
// request, whatever it would take.
func (v *capVolume) admit(path string, newSize int64) syscall.Errno {
	frozen, err := v.ctl.flag(capCtlFrozen)
	if err != nil {
		return v.fault(err)
	}
	if frozen {
		return v.refuse(true)
	}
	var cur int64
	if fi, err := os.Stat(path); err == nil {
		cur = fi.Size()
	}
	growth := newSize - cur
	if growth <= 0 {
		return 0
	}
	limit, err := v.ctl.limit()
	if err != nil {
		return v.fault(err)
	}
	if capUsed(v.backing)+growth > limit {
		return v.refuse(false)
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
	n.vol.holdIfAsked()
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
	limit, err := n.vol.ctl.limit()
	if err != nil {
		return n.vol.fault(err)
	}
	free := limit - capUsed(n.vol.backing)
	if free < 0 {
		free = 0
	}
	out.Bsize, out.Frsize, out.NameLen = capBlock, capBlock, 255
	out.Blocks = uint64(limit / capBlock)
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

// mountCapVolume mounts a volume of the given size under dir, which it fills with a backing directory, a mount point and a control
// directory, and returns it with its server running in this process. The mount carries source as its name. Only the child process of
// capfs_volume_test.go calls it, for every volume of the tests, so that all mount alike. autoUnmount passes fusermount3's auto_unmount
// (capfs_mount_test.go says what it does, and does not do, on each version). opts change go-fuse's mount options (the child's debug log).
func mountCapVolume(dir string, limit int64, source string, autoUnmount bool, opts ...func(*fuse.MountOptions)) (*capVolume, error) {
	if real, err := filepath.EvalSymlinks(dir); err == nil {
		dir = real // mountinfo and fusermount3 name a mount point by its real path
	}
	v := &capVolume{backing: filepath.Join(dir, "backing"), mount: filepath.Join(dir, "mnt"), ctl: capCtl{filepath.Join(dir, "ctl")}, stop: make(chan struct{})}
	for _, d := range []string{v.backing, v.mount, v.ctl.dir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			return nil, err
		}
	}
	if err := v.ctl.setLimit(limit); err != nil {
		return nil, err
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
	conn, err := fusermountCapfs(v.mount, source, autoUnmount)
	if err != nil {
		return nil, capMountError{err}
	}
	v.conn = conn
	// go-fuse serves the descriptor through its /dev/fd/N mount point, and so never runs fusermount3 itself.
	mo := fuse.MountOptions{FsName: source, Name: "capfs", MaxWrite: capMaxWrite}
	for _, o := range opts {
		o(&mo)
	}
	srv, err := fs.Mount(fmt.Sprintf("/dev/fd/%d", conn.fd), root, &fs.Options{MountOptions: mo})
	if err != nil {
		uerr := capUnmount(v.mount, capUnmountTimeout)
		if uerr == nil {
			_ = conn.closeComm(capUnmountTimeout)
		}
		return nil, capMountError{fmt.Errorf("serving the mount at %s: %w (its unmount: %v)", v.mount, err, uerr)}
	}
	v.server = srv
	if err := capPollHack(v.mount, capMountTimeout); err != nil {
		uerr := v.unmount(capUnmountTimeout)
		return nil, capMountError{fmt.Errorf("the poll hack on %s: %w (its unmount: %v)", v.mount, err, uerr)}
	}
	return v, nil
}

// unmount unmounts the volume and stops its server, each step bounded by d: fusermount3 -u, retried while the mount is busy, then the
// end of the server, then the close of the socket, on which an auto_unmount fusermount3 finds the mount gone and exits. A mount that
// will not unmount keeps its socket open.
func (v *capVolume) unmount(d time.Duration) error {
	v.end()
	deadline := time.Now().Add(d)
	var err error
	for {
		if err = capUnmount(v.mount, capUnmountTimeout); err == nil || time.Now().After(deadline) {
			break
		}
		if !capListed(v.mount) {
			err = nil // unmounted meanwhile, by someone else
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
	return v.conn.closeComm(d)
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
