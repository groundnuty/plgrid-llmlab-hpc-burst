package main

// A capfs volume as a test uses it (issue #658): served by a child process, and driven through files.
//
// Why a child. A volume served in the test's own process leaves one path to a WEDGED mount (issue #648, and #656's review). If the test
// is killed while one of its own threads waits on its own volume for a request the server has already read, that thread cannot finish
// dying, the process keeps /dev/fuse, and every later stat of the mount blocks for good. With the server in another process the test
// process holds no /dev/fuse: the server is the child of newCapVolume's test, started so that the kernel kills it when the test binary
// ends however it ends (testenv.StartDyingWithParent, Pdeathsig), and its connection then ends, the waiting thread returns ENOTCONN, and
// the mount is DEAD, which the next newCapVolume sweeps (capfs_sweep_test.go). The server's stdin is not what ends it then: a process
// stuck on its mount never closes the pipe.
//
// Why files. The limit, the freeze, a hold on writes and the counts of refusals live in the server, and a test changes and reads them
// while it runs. They are kept as small files in a control directory beside the mount (capCtl), in the directory the server and the test
// both reach by its path, outside the mount. So nothing a test asks of a volume goes through the mount or waits for the server: a read is
// a read of a file, bounded by the host's own file system, and a change takes effect for every request the server reads after it. The
// server counts a refusal before it answers, so a test that has seen the ENOSPC finds the count.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// The files of a volume's control directory.
const (
	capCtlLimit         = "limit"          // the size in bytes, decimal; replaced whole, by a rename
	capCtlFrozen        = "frozen"         // exists while the volume is frozen (#180)
	capCtlStall         = "stall"          // exists while the server is to hold every write it reads
	capCtlStalled       = "stalled"        // created by the server when it holds a write: it has read the request, and not answered it
	capCtlRefused       = "refused"        // one byte for each refused request
	capCtlFrozenRefused = "frozen-refused" // one byte for each request that only the freeze refused
)

// capCtl is a control directory: the server reads the settings from it and writes the counts to it, a test does the reverse.
type capCtl struct{ dir string }

func (c capCtl) path(name string) string { return filepath.Join(c.dir, name) }

// limit is the volume's size in bytes.
func (c capCtl) limit() (int64, error) {
	b, err := os.ReadFile(c.path(capCtlLimit))
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("the limit file holds %q, not a size", b)
	}
	return n, nil
}

// setLimit replaces the limit by a temporary file and a rename, so that the server never reads half of one.
func (c capCtl) setLimit(n int64) error {
	tmp := c.path(capCtlLimit + ".tmp")
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(n, 10)), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.path(capCtlLimit))
}

// flag says whether the named flag file exists.
func (c capCtl) flag(name string) (bool, error) {
	_, err := os.Stat(c.path(name))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	}
	return false, err
}

// setFlag creates the named flag file, or removes it.
func (c capCtl) setFlag(name string, on bool) error {
	if on {
		f, err := os.OpenFile(c.path(name), os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		return f.Close()
	}
	if err := os.Remove(c.path(name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// count is the number of bytes in the named counter file, which is the number of bumps: none, if the file does not exist yet.
func (c capCtl) count(name string) (int64, error) {
	fi, err := os.Stat(c.path(name))
	switch {
	case err == nil:
		return fi.Size(), nil
	case errors.Is(err, fs.ErrNotExist):
		return 0, nil
	}
	return 0, err
}

// bump appends one byte to the named counter file.
func (c capCtl) bump(name string) error {
	f, err := os.OpenFile(c.path(name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte{'x'}); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// capHandle is a test's volume: a capfs mount served by a child process (newCapVolume).
type capHandle struct {
	t         *testing.T
	mount     string // where the volume is mounted
	backing   string // the directory the volume passes everything through to, which a test reads for what the volume holds
	ctl       capCtl
	serverPID int // the server's process; the kill test records its identity, to show that the server ends with its test
}

// grow adds bytes to the volume's limit, as freeing space on a full disk does.
func (v *capHandle) grow(bytes int64) {
	v.t.Helper()
	cur, err := v.ctl.limit()
	if err == nil {
		err = v.ctl.setLimit(cur + bytes)
	}
	if err != nil {
		v.t.Errorf("raising the limit of %s by %d: %v", v.mount, bytes, err)
	}
}

// freeze makes the volume refuse every write from now on; unfreeze lifts it (#180). The refusals are counted (frozenRefused).
func (v *capHandle) freeze()   { v.t.Helper(); v.set(capCtlFrozen, true) }
func (v *capHandle) unfreeze() { v.t.Helper(); v.set(capCtlFrozen, false) }

// stall makes the volume's server hold every write it reads from now on, without answering it, until release or the end of the server.
func (v *capHandle) stall()   { v.t.Helper(); v.set(capCtlStall, true) }
func (v *capHandle) release() { v.t.Helper(); v.set(capCtlStall, false) }

func (v *capHandle) set(name string, on bool) {
	v.t.Helper()
	if err := v.ctl.setFlag(name, on); err != nil {
		v.t.Errorf("setting %s to %t for %s: %v", name, on, v.mount, err)
	}
}

// used is the space the files in the backing directory take, in bytes of allocated blocks.
func (v *capHandle) used() int64 { return capUsed(v.backing) }

// limitBytes is the volume's limit now.
func (v *capHandle) limitBytes() int64 {
	v.t.Helper()
	n, err := v.ctl.limit()
	if err != nil {
		v.t.Fatalf("reading the limit of %s: %v", v.mount, err)
	}
	return n
}

// refused is how many requests the volume has refused with ENOSPC, so that a test can show the volume really was full.
func (v *capHandle) refused() int64 { v.t.Helper(); return v.counted(capCtlRefused) }

// frozenRefused is how many requests only the freeze refused: one that fits in space the files already hold is not refused by a volume
// that is merely at its limit.
func (v *capHandle) frozenRefused() int64 { v.t.Helper(); return v.counted(capCtlFrozenRefused) }

func (v *capHandle) counted(name string) int64 {
	v.t.Helper()
	n, err := v.ctl.count(name)
	if err != nil {
		v.t.Fatalf("reading the count %s of %s: %v", name, v.mount, err)
	}
	return n
}

// newCapVolume mounts a volume of the given size, served by a child process, and registers the end of the child and the unmount with the
// test. Register it before the host's clean-up, so that the unmount runs after the host is stopped (clean-ups run last-in first-out). If
// FUSE cannot be mounted it skips with the exact reason, unless HPCBURST_REQUIRE_INTEGRATION=1, which turns that skip into a failure.
//
// The server is the test binary run again (TestMain), started by launchCapServer so that it ends with the test process, however that
// ends. The clean-up closes its stdin, on which it unmounts the volume (fusermount3 -u) and exits; one that does not exit is killed
// through its pidfd and reaped, and a mount it leaves is unmounted by the clean-up. The volume's counts and settings are files
// (capCtl). Before it mounts, newCapVolume sweeps this user's capfs mounts whose endpoint is dead (sweepDeadCapMounts): a killed test's
// volume is left dead, since its server ends with it. A mount found wedged (open, but its stat does not return) fails the test here,
// since any later stat of it, or a removal of its directory, would block.
func newCapVolume(t *testing.T, limit int64) *capHandle {
	t.Helper()
	if res := sweepDeadCapMounts(realSweepEnv(t.Logf)); res.failure() != "" {
		t.Fatal(res.failure())
	}
	c := launchCapServer(t, capChildMode{auto: true, limit: limit})
	waitCapServerMounted(t, c)
	if errno, answered := capStatErrno(c.mount, capChildBound); !answered || errno != 0 {
		t.Fatalf("the new volume %s does not answer a stat: answered %t, errno %v. Its server's output: %s", c.mount, answered, errno, c.output())
	}
	return &capHandle{
		t:         t,
		mount:     c.mount,
		backing:   filepath.Join(c.dir, "backing"),
		ctl:       capCtl{filepath.Join(c.dir, "ctl")},
		serverPID: c.cmd.Process.Pid,
	}
}

// capSkipOrFailOnMount ends a test whose volume's server could not mount (it exited with capfsExitMount): the test skips, with the exact
// reason, unless HPCBURST_REQUIRE_INTEGRATION=1, which makes it a failure. It reports false when the exit was another failure.
func capSkipOrFailOnMount(t *testing.T, c *capChild) bool {
	t.Helper()
	ws, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !ws.Exited() || ws.ExitStatus() != capfsExitMount {
		return false
	}
	msg := "the store-full test needs a size-limited file system, and a user-level FUSE mount failed: " + strings.TrimSpace(c.output())
	if os.Getenv("HPCBURST_REQUIRE_INTEGRATION") == "1" {
		t.Fatal(msg)
	}
	t.Skip("SKIPPED, NOT TESTED: " + msg)
	return true
}
