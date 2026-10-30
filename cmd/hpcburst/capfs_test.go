package main

// A size-limited file system for the store-full test, built without root or any system change.
// An ordinary user cannot mount a loop device or a tmpfs here (user namespaces are restricted),
// but /dev/fuse is writable and fusermount3 is setuid, so a FUSE file system in this process can
// serve a small volume. It passes everything through to a directory (go-fuse's loopback file
// system, BSD-3-Clause) and answers ENOSPC to any write, allocation or truncation that would take
// the directory above its limit, as a full disk does. It also reports the limit in statfs.

import (
	"context"
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
	limit   int64
	enospc  atomic.Int64 // how many requests were refused, so a test can show the volume really was full
	server  *fuse.Server
	mount   string
}

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

// admit decides whether growing the file at path to newSize fits.
func (v *capVolume) admit(path string, newSize int64) syscall.Errno {
	var cur int64
	if fi, err := os.Stat(path); err == nil {
		cur = fi.Size()
	}
	growth := newSize - cur
	if growth <= 0 {
		return 0
	}
	if v.used()+growth > v.limit {
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
	free := n.vol.limit - used
	if free < 0 {
		free = 0
	}
	out.Bsize, out.Frsize, out.NameLen = capBlock, capBlock, 255
	out.Blocks = uint64(n.vol.limit / capBlock)
	out.Bfree, out.Bavail = uint64(free/capBlock), uint64(free/capBlock)
	out.Files, out.Ffree = 1<<16, 1<<16
	return 0
}

// newCapVolume mounts a volume of the given size and registers its unmount with the test.
// Register it before the host's clean-up, so that the unmount runs after the host is stopped
// (clean-ups run last-in first-out). If FUSE cannot be mounted it skips with the exact reason,
// unless HPCBURST_REQUIRE_INTEGRATION=1, which turns that skip into a failure.
func newCapVolume(t *testing.T, limit int64) *capVolume {
	t.Helper()
	dir, err := os.MkdirTemp("", "cv")
	if err != nil {
		t.Fatal(err)
	}
	v := &capVolume{backing: filepath.Join(dir, "backing"), mount: filepath.Join(dir, "mnt"), limit: limit}
	for _, d := range []string{v.backing, v.mount} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	rootData := &fs.LoopbackRoot{Path: v.backing}
	rootData.NewNode = func(r *fs.LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
		return &capNode{LoopbackNode: fs.LoopbackNode{RootData: r}, vol: v}
	}
	var st syscall.Stat_t
	if err := syscall.Stat(v.backing, &st); err != nil {
		t.Fatal(err)
	}
	rootData.Dev = uint64(st.Dev)
	root := rootData.NewNode(rootData, nil, "", &st)
	rootData.RootNode = root
	srv, err := fs.Mount(v.mount, root, &fs.Options{MountOptions: fuse.MountOptions{FsName: "hpcburst-capfs", Name: "capfs"}})
	if err != nil {
		os.RemoveAll(dir)
		msg := "the store-full test needs a size-limited file system, and a user-level FUSE mount failed: " + err.Error()
		if os.Getenv("HPCBURST_REQUIRE_INTEGRATION") == "1" {
			t.Fatal(msg)
		}
		t.Skip("SKIPPED, NOT TESTED: " + msg)
	}
	v.server = srv
	t.Cleanup(func() {
		deadline := time.Now().Add(30 * time.Second)
		for {
			err := srv.Unmount()
			if err == nil || time.Now().After(deadline) {
				if err != nil {
					t.Errorf("unmount %s: %v", v.mount, err)
				}
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		srv.Wait()
		os.RemoveAll(dir)
	})
	return v
}
