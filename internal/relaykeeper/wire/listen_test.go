package wire_test

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Listen

func TestListenMakesAMode0600Socket(t *testing.T) {
	// A permissive umask would leave the socket open: the mode is set, not
	// left to the umask.
	old := syscall.Umask(0)
	defer syscall.Umask(old)

	path := filepath.Join(shortDir(t), "s")
	ln, err := wire.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Type() != os.ModeSocket {
		t.Errorf("the file at path has type %v; want a socket", fi.Mode().Type())
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("the socket's mode is %o; want 600", perm)
	}
	if got := ln.Addr().String(); got != path {
		t.Errorf("the listener's address is %q; want the path it was given", got)
	}
}

func TestListenRefusesAPathOverTheLimit(t *testing.T) {
	dir := shortDir(t)
	named := func(n int) string { // a path of n bytes
		return filepath.Join(dir, strings.Repeat("a", n-len(dir)-1))
	}
	for _, n := range []int{agentapi.MaxRelaySocketPath + 1, agentapi.MaxRelaySocketPath + 50} {
		path := named(n)
		if len(path) != n {
			t.Fatalf("test setup: path of %d bytes, want %d", len(path), n)
		}
		if ln, err := wire.Listen(path); err == nil {
			ln.Close()
			t.Errorf("Listen accepted a path of %d bytes; the limit is %d", n, agentapi.MaxRelaySocketPath)
		}
		if _, err := os.Lstat(path); err == nil {
			t.Errorf("a file was made at the path of %d bytes that Listen refused", n)
		}
	}
	// And a path of exactly the limit is accepted.
	path := named(agentapi.MaxRelaySocketPath)
	ln, err := wire.Listen(path)
	if err != nil {
		t.Fatalf("Listen of a path of exactly %d bytes: %v", agentapi.MaxRelaySocketPath, err)
	}
	ln.Close()
}

func TestListenRefusesARelativePath(t *testing.T) {
	dir := shortDir(t)
	t.Chdir(dir)
	for _, path := range []string{"s", "./s", "sub/s", "../s", ""} {
		if ln, err := wire.Listen(path); err == nil {
			ln.Close()
			t.Errorf("Listen(%q) accepted a path that is not absolute", path)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Errorf("the refused relative paths left %v (err %v) in the working directory", entries, err)
	}
}

func TestListenRefusesADirectoryThatDoesNotExist(t *testing.T) {
	path := filepath.Join(shortDir(t), "missing", "s")
	if ln, err := wire.Listen(path); err == nil {
		ln.Close()
		t.Error("Listen made a socket in a directory that does not exist")
	}
}

func TestListenRefusesWhatIsNotASocketAndLeavesItUntouched(t *testing.T) {
	const content = "keep me\n"
	cases := []struct {
		name  string
		make  func(t *testing.T, path string)
		check func(t *testing.T, path string)
	}{
		{
			name: "a regular file",
			make: func(t *testing.T, path string) {
				if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, path string) {
				b, err := os.ReadFile(path)
				if err != nil || string(b) != content {
					t.Errorf("the file reads %q, %v; want %q", b, err, content)
				}
				if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o640 {
					t.Errorf("the file's mode changed: %v, %v", fi, err)
				}
			},
		},
		{
			name: "an empty regular file",
			make: func(t *testing.T, path string) {
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, path string) {
				if fi, err := os.Stat(path); err != nil || !fi.Mode().IsRegular() || fi.Size() != 0 {
					t.Errorf("the empty file is now %v, %v", fi, err)
				}
			},
		},
		{
			name: "a directory with a file in it",
			make: func(t *testing.T, path string) {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "f"), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, path string) {
				b, err := os.ReadFile(filepath.Join(path, "f"))
				if err != nil || string(b) != content {
					t.Errorf("the file in the directory reads %q, %v; want %q", b, err, content)
				}
			},
		},
		{
			name: "a symbolic link to a file",
			make: func(t *testing.T, path string) {
				target := path + "-target"
				if err := os.WriteFile(target, []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, path string) {
				if fi, err := os.Lstat(path); err != nil || fi.Mode().Type() != os.ModeSymlink {
					t.Errorf("the link is now %v, %v", fi, err)
				}
				b, err := os.ReadFile(path + "-target")
				if err != nil || string(b) != content {
					t.Errorf("the link's target reads %q, %v; want %q", b, err, content)
				}
			},
		},
		{
			name: "a dangling symbolic link",
			make: func(t *testing.T, path string) {
				if err := os.Symlink(path+"-nowhere", path); err != nil {
					t.Fatal(err)
				}
			},
			check: func(t *testing.T, path string) {
				if fi, err := os.Lstat(path); err != nil || fi.Mode().Type() != os.ModeSymlink {
					t.Errorf("the link is now %v, %v", fi, err)
				}
				if _, err := os.Lstat(path + "-nowhere"); err == nil {
					t.Error("Listen made a file at the dangling link's target")
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(shortDir(t), "s")
			c.make(t, path)
			ln, err := wire.Listen(path)
			if err == nil {
				ln.Close()
				t.Fatal("Listen accepted a path that holds something that is not a socket")
			}
			c.check(t, path)
		})
	}
}

func TestListenRefusesWhileAnotherKeeperAnswersAndLeavesItServing(t *testing.T) {
	s := startServer(t, sampleReply)

	if ln, err := wire.Listen(s.path); err == nil {
		ln.Close()
		t.Fatal("Listen made a second listener at a path where a keeper answers")
	}
	// Nothing was removed: the first keeper still answers at its path.
	if fi, err := os.Lstat(s.path); err != nil || fi.Mode().Type() != os.ModeSocket {
		t.Fatalf("after the refused Listen, the path is %v, %v; want the first keeper's socket", fi, err)
	}
	var pong wire.Pong
	if err := exchange(t, s.path, wire.KindPing, struct{}{}, &pong); err != nil || pong.KeeperEpoch != 9 {
		t.Errorf("the first keeper after the refused Listen: %+v, %v; want its pong", pong, err)
	}
}

func TestListenReplacesASocketNobodyListensAt(t *testing.T) {
	path := filepath.Join(shortDir(t), "s")
	// A keeper that is gone: its socket file stays, nobody answers.
	old, err := wire.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	old.SetUnlinkOnClose(false)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Lstat(path); err != nil || fi.Mode().Type() != os.ModeSocket {
		t.Fatalf("test setup: the stale socket is %v, %v", fi, err)
	}
	if c, err := net.Dial("unix", path); err == nil {
		c.Close()
		t.Fatal("test setup: something answers at the stale socket")
	} else if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("test setup: dialing the stale socket: %v; want a refusal", err)
	}

	s := serve(t, path, sampleReply)
	if fi, err := os.Lstat(path); err != nil || fi.Mode().Type() != os.ModeSocket || fi.Mode().Perm() != 0o600 {
		t.Errorf("the replacement is %v, %v; want a socket of mode 600", fi, err)
	}
	var pong wire.Pong
	if err := exchange(t, s.path, wire.KindPing, struct{}{}, &pong); err != nil || pong.KeeperEpoch != 9 {
		t.Errorf("the replacing keeper: %+v, %v; want its pong", pong, err)
	}
}
