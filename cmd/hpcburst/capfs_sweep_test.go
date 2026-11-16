package main

// The sweep of dead capfs mounts (issue #648). The size-limited file system of the store-full tests (capfs_test.go) is served by the test's own
// process. When that process is killed (SIGKILL, a stopped systemd scope, a -test.timeout panic) its clean-up never runs: the kernel's FUSE
// connection breaks, and the mount stays, with no one to serve it. Its endpoint is dead, a stat of it fails with ENOTCONN ("transport endpoint is
// not connected"), an ordinary rm cannot remove it, and a later run or a removal of the scratch directory trips on it.
//
// newCapVolume sweeps those before it mounts: it reads /proc/self/mountinfo for THIS user's capfs mounts and unmounts, with fusermount3 -u, only the
// ones whose endpoint is dead. A mount that answers belongs to someone's running test and is never touched, and neither is another user's, or any
// mount that is not capfs, or one whose state cannot be told (a stat that does not return in time, or fails for another reason): the sweep leaves it
// and says so. It does not unmount lazily (-z), which would take a live mount away from its user, and it removes no directory.
//
// What each of the two ways of cleaning up covers: fusermount3's auto_unmount would unmount when the server dies, at the source, with no later run
// needed; it does not work through go-fuse's mount, as TestAutoUnmountThroughGoFuse in capfs_child_test.go measures, so the sweep, which runs at the
// next newCapVolume, is what removes a mount left by a killed test. Until that run, the dead mount stays.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// capMount is one capfs mount of /proc/self/mountinfo: its mount point, decoded, and the user it was mounted for (the user_id of its options, -1
// when it has none).
type capMount struct {
	point string
	uid   int
}

// unescapeMountinfo decodes the octal escapes the kernel writes in a mount point (a space is \040, a newline \012, a backslash \134).
func unescapeMountinfo(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// parseCapMounts returns the capfs mounts of a /proc/self/mountinfo text: the lines whose file system type is capFsType and whose source is capFsName.
// A line is "id parent major:minor root mountpoint options [optional fields] - fstype source superoptions".
func parseCapMounts(mountinfo string) []capMount {
	var out []capMount
	for _, line := range strings.Split(mountinfo, "\n") {
		left, right, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		lf, rf := strings.Fields(left), strings.Fields(right)
		if len(lf) < 6 || len(rf) < 2 || rf[0] != capFsType || rf[1] != capFsName {
			continue
		}
		m := capMount{point: unescapeMountinfo(lf[4]), uid: -1}
		if len(rf) >= 3 {
			for _, o := range strings.Split(rf[2], ",") {
				if v, ok := strings.CutPrefix(o, "user_id="); ok {
					if n, err := strconv.Atoi(v); err == nil {
						m.uid = n
					}
				}
			}
		}
		out = append(out, m)
	}
	return out
}

// capState is what a stat of a capfs mount says of it.
type capState int

const (
	capLive    capState = iota // the stat answered: someone's server is running
	capDead                    // the stat failed with ENOTCONN: no server holds the connection
	capUnknown                 // anything else: the stat did not return in time, or failed for another reason
)

// capStatTimeout bounds the stat of a mount: a server that does not answer (a stopped test) must not hang the sweep, and it is not dead.
const capStatTimeout = 5 * time.Second

// statCapMount stats the mount point and says what that shows. Only ENOTCONN is dead.
func statCapMount(point string) capState {
	done := make(chan error, 1)
	go func() {
		var st syscall.Stat_t
		done <- syscall.Stat(point, &st)
	}()
	select {
	case err := <-done:
		switch {
		case err == nil:
			return capLive
		case errors.Is(err, syscall.ENOTCONN):
			return capDead
		}
		return capUnknown
	case <-time.After(capStatTimeout):
		return capUnknown
	}
}

// capUnmountArgs are the arguments of the unmount: -u, a plain unmount that fails with EBUSY for a mount in use. Never -z, a lazy unmount, which
// detaches a mount that is live too.
func capUnmountArgs(point string) []string { return []string{"-u", point} }

// capUnmount unmounts a mount point with fusermount3.
func capUnmount(point string) error {
	bin, err := exec.LookPath("fusermount3")
	if err != nil {
		bin = "/bin/fusermount3"
	}
	if out, err := exec.Command(bin, capUnmountArgs(point)...).CombinedOutput(); err != nil {
		return fmt.Errorf("fusermount3 -u %s: %w: %s", point, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// sweepEnv is what the sweep reads and does, so that a test can stand in for each: the mountinfo text, the user, the stat of a mount, the unmount
// and the log.
type sweepEnv struct {
	mountinfo func() ([]byte, error)
	uid       int
	state     func(point string) capState
	unmount   func(point string) error
	logf      func(format string, args ...any)
}

// realSweepEnv is the sweep's environment on this machine, logging through logf.
func realSweepEnv(logf func(format string, args ...any)) sweepEnv {
	return sweepEnv{
		mountinfo: func() ([]byte, error) { return os.ReadFile("/proc/self/mountinfo") },
		uid:       os.Getuid(),
		state:     statCapMount,
		unmount:   capUnmount,
		logf:      logf,
	}
}

// sweepDeadCapMounts unmounts this user's capfs mounts whose endpoint is dead, logging each, and returns their mount points. It leaves every other
// mount alone: a live one, another user's, one whose state is unknown, and anything that is not capfs.
func sweepDeadCapMounts(env sweepEnv) []string {
	data, err := env.mountinfo()
	if err != nil {
		env.logf("capfs sweep: /proc/self/mountinfo cannot be read, so nothing is swept: %v", err)
		return nil
	}
	var swept []string
	for _, m := range parseCapMounts(string(data)) {
		if m.uid != env.uid {
			env.logf("capfs sweep: %s is not this user's (user_id %d, not %d): left alone", m.point, m.uid, env.uid)
			continue
		}
		switch env.state(m.point) {
		case capDead:
			if err := env.unmount(m.point); err != nil {
				env.logf("capfs sweep: %s has a dead endpoint and could not be unmounted: %v", m.point, err)
				continue
			}
			env.logf("capfs sweep: unmounted %s, a capfs mount with a dead endpoint (its test was killed)", m.point)
			swept = append(swept, m.point)
		case capLive:
			env.logf("capfs sweep: %s answers, so a test is using it: left alone", m.point)
		default:
			env.logf("capfs sweep: %s could not be told dead or alive: left alone", m.point)
		}
	}
	return swept
}

// A mountinfo text the sweep reads: each line is one kind of mount, and the sweep takes only the dead capfs ones of this user's, decoding the
// escaped mount point.
func TestSweepTakesOnlyThisUsersDeadCapMounts(t *testing.T) {
	const uid = 4242
	line := func(id int, point, fstype, source, opts string) string {
		return fmt.Sprintf("%d 40 0:%d / %s rw,nosuid,nodev,relatime shared:%d - %s %s %s", id, id, point, id, fstype, source, opts)
	}
	mountinfo := strings.Join([]string{
		line(101, "/scratch/dead/mnt", capFsType, capFsName, "rw,user_id=4242,group_id=4242"),
		line(102, "/scratch/live/mnt", capFsType, capFsName, "rw,user_id=4242,group_id=4242"),
		line(103, "/scratch/foreign/mnt", capFsType, capFsName, "rw,user_id=1000,group_id=1000"),
		line(104, "/scratch/other-fs/mnt", "fuse.sshfs", "remote:/data", "rw,user_id=4242,group_id=4242"),
		line(105, "/scratch/other-name/mnt", capFsType, "another-capfs", "rw,user_id=4242,group_id=4242"),
		line(106, `/scratch/with\040space/mnt`, capFsType, capFsName, "rw,user_id=4242,group_id=4242"),
		line(107, "/scratch/unknown/mnt", capFsType, capFsName, "rw,user_id=4242,group_id=4242"),
		line(108, "/scratch/no-uid/mnt", capFsType, capFsName, "rw"),
		line(109, "/scratch/ext4", "ext4", "/dev/sda1", "rw"),
		"not a mountinfo line",
	}, "\n")
	state := map[string]capState{
		"/scratch/dead/mnt": capDead, "/scratch/live/mnt": capLive, "/scratch/foreign/mnt": capDead, "/scratch/other-fs/mnt": capDead,
		"/scratch/other-name/mnt": capDead, "/scratch/with space/mnt": capDead, "/scratch/unknown/mnt": capUnknown, "/scratch/no-uid/mnt": capDead,
		"/scratch/ext4": capDead,
	}
	var unmounted, logged []string
	env := sweepEnv{
		mountinfo: func() ([]byte, error) { return []byte(mountinfo), nil },
		uid:       uid,
		state: func(p string) capState {
			s, ok := state[p]
			if !ok {
				t.Errorf("the sweep asked about %q, which is not a mount point of the text", p)
			}
			return s
		},
		unmount: func(p string) error { unmounted = append(unmounted, p); return nil },
		logf:    func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) },
	}
	swept := sweepDeadCapMounts(env)
	want := []string{"/scratch/dead/mnt", "/scratch/with space/mnt"}
	if fmt.Sprint(unmounted) != fmt.Sprint(want) || fmt.Sprint(swept) != fmt.Sprint(want) {
		t.Errorf("unmounted %v and reported %v, want exactly the two dead capfs mounts of this user's, %v", unmounted, swept, want)
	}
	for _, p := range want {
		if !strings.Contains(strings.Join(logged, "\n"), p) {
			t.Errorf("the sweep did not log %s: %q", p, logged)
		}
	}
	for _, left := range []string{"/scratch/live/mnt", "/scratch/foreign/mnt", "/scratch/unknown/mnt", "/scratch/no-uid/mnt"} {
		if !strings.Contains(strings.Join(logged, "\n"), left) {
			t.Errorf("the sweep did not say that it left %s alone: %q", left, logged)
		}
	}
}

// The unmount is -u and nothing else: a lazy unmount (-z) of a mount that turns out to be live would take it from its user.
func TestSweepUnmountsPlainlyAndNeverLazily(t *testing.T) {
	args := capUnmountArgs("/scratch/dead/mnt")
	if fmt.Sprint(args) != "[-u /scratch/dead/mnt]" {
		t.Errorf("the unmount arguments are %v, want [-u /scratch/dead/mnt]", args)
	}
}

// A mountinfo that cannot be read sweeps nothing, and says so.
func TestSweepWithNoMountinfoSweepsNothing(t *testing.T) {
	var logged []string
	env := sweepEnv{
		mountinfo: func() ([]byte, error) { return nil, errors.New("no such file") },
		uid:       1,
		state:     func(string) capState { t.Error("a mount was looked at"); return capDead },
		unmount:   func(string) error { t.Error("a mount was unmounted"); return nil },
		logf:      func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) },
	}
	if got := sweepDeadCapMounts(env); len(got) != 0 || len(logged) != 1 {
		t.Errorf("swept %v and logged %q, want nothing swept and one line", got, logged)
	}
}

// The octal escapes of a mount point decode as the kernel writes them; text with no escape is unchanged, and a backslash that is not an escape stays.
func TestUnescapeMountinfo(t *testing.T) {
	for in, want := range map[string]string{
		`/a/b`:           "/a/b",
		`/a\040b`:        "/a b",
		`/a\012b\134c`:   "/a\nb\\c",
		`/a\04`:          `/a\04`,
		`/a\zzzb`:        `/a\zzzb`,
		`/trailing\`:     `/trailing\`,
		`\040\040/start`: "  /start",
	} {
		if got := unescapeMountinfo(in); got != want {
			t.Errorf("unescapeMountinfo(%q) = %q, want %q", in, got, want)
		}
	}
}
