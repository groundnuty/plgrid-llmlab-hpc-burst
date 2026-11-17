package main

// The sweep of dead capfs mounts (issue #648). The size-limited file system of the store-full tests (capfs_test.go) is served by the
// test's own process. When that process is killed, its clean-up never runs, and since nothing else holds the connection
// (capfs_mount_test.go), the kernel ends it: the mount stays with no one to serve it. Its endpoint is dead, a stat of it fails with
// ENOTCONN ("transport endpoint is not connected"), an ordinary rm cannot remove it, and a later run or a removal of the scratch
// directory trips on it. fusermount3's auto_unmount removes such a mount only from version 3.16 on.
//
// newCapVolume sweeps those before it mounts. It reads /proc/self/mountinfo for THIS user's capfs mounts, and unmounts, with fusermount3
// -u, only the ones whose endpoint is dead. It never touches a mount that answers, which belongs to someone's running test, nor another
// user's, nor one that is not capfs. It never unmounts lazily (-z), which would take a live mount from its user, and it removes no
// directory.
//
// A mount can also be WEDGED: its connection is open, because some process still holds its /dev/fuse descriptor, but nothing answers, so
// a stat of it blocks for good (a server that is stopped or stuck, or a descriptor that leaked into another process). That is worse than
// dead, and it is what wedged two sessions on 16 Nov 2026. So the sweep never stats a mount without a bound: the stat runs in a goroutine,
// and one that has not returned in capStatTimeout is abandoned, and the mount reported wedged, with its FUSE connection. A wedged mount is
// not touched, and newCapVolume fails the test on one, rather than run beside it. The abandoned stat waits in the kernel until the
// connection ends; when this process exits it gives up if no server has read its request yet, and otherwise holds the exit until the
// connection ends. The sweep does not start a second one on the same mount while it waits.

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// capStatTimeout bounds the stat of a mount. A live server answers a stat of its root at once, even busy, so this is generous: past it,
// the mount is reported wedged, and the newCapVolume that met it fails.
const capStatTimeout = 10 * time.Second

// capMount is one capfs mount of /proc/self/mountinfo.
type capMount struct {
	point  string // the mount point, decoded
	source string // the file system name
	uid    int    // the user it was mounted for (user_id in its options), -1 when it names none
	dev    string // its device number, major:minor, which names its FUSE connection
}

// connDir is the mount's FUSE connection directory: the kernel names it after the device number, which for FUSE has major 0, so the
// minor alone. Empty when the device number is not of that form.
func (m capMount) connDir() string {
	major, minor, ok := strings.Cut(m.dev, ":")
	if !ok || major != "0" || minor == "" {
		return ""
	}
	return "/sys/fs/fuse/connections/" + minor
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

// mountinfoLines splits a /proc/self/mountinfo text into its lines' fields before and after the " - " separator. A line is
// "id parent major:minor root mountpoint options [optional fields] - fstype source superoptions".
func mountinfoLines(mountinfo string) (out [][2][]string) {
	for _, line := range strings.Split(mountinfo, "\n") {
		left, right, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		lf, rf := strings.Fields(left), strings.Fields(right)
		if len(lf) < 6 || len(rf) < 2 {
			continue
		}
		out = append(out, [2][]string{lf, rf})
	}
	return out
}

// mountPoints returns every mount point of a /proc/self/mountinfo text, decoded.
func mountPoints(mountinfo string) []string {
	var out []string
	for _, l := range mountinfoLines(mountinfo) {
		out = append(out, unescapeMountinfo(l[0][4]))
	}
	return out
}

// mountedUnder returns a mount point of the mountinfo text that is dir or lies under it.
func mountedUnder(mountinfo, dir string) (string, bool) {
	for _, p := range mountPoints(mountinfo) {
		if p == dir || strings.HasPrefix(p, dir+"/") {
			return p, true
		}
	}
	return "", false
}

// parseCapMounts returns the capfs mounts of a /proc/self/mountinfo text: the lines whose file system type is capFsType and whose source
// is one of sources.
func parseCapMounts(mountinfo string, sources ...string) []capMount {
	var out []capMount
	for _, l := range mountinfoLines(mountinfo) {
		lf, rf := l[0], l[1]
		if rf[0] != capFsType || !containsString(sources, rf[1]) {
			continue
		}
		m := capMount{point: unescapeMountinfo(lf[4]), source: rf[1], uid: -1, dev: lf[2]}
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

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// capState is what a bounded stat of a capfs mount says of it.
type capState int

const (
	capLive    capState = iota // the stat answered: someone's server is running
	capDead                    // the stat failed with ENOTCONN: the connection has ended, nobody holds it
	capWedged                  // the stat did not return in time: the connection is open, and nobody answers
	capUnknown                 // the stat failed for another reason (the mount went away meanwhile, say)
)

func (s capState) String() string {
	return [...]string{"live", "dead", "wedged", "unknown"}[s]
}

// capProbes records the stats of mounts that this process started and that have not returned, by mount point, so that a mount whose
// stat is still stuck is reported wedged again without a second stat piling up behind the first.
var capProbes = struct {
	sync.Mutex
	inflight map[string]chan struct{}
}{inflight: map[string]chan struct{}{}}

// probeCapMount stats point in a goroutine, waiting at most bound. Only ENOTCONN is dead. On a timeout the stat is abandoned, still
// waiting in the kernel, and the mount is wedged.
func probeCapMount(point string, bound time.Duration) (capState, error) {
	capProbes.Lock()
	if ch, ok := capProbes.inflight[point]; ok {
		capProbes.Unlock()
		select {
		case <-ch:
		default:
			return capWedged, errors.New("a stat of it that this process started earlier has not returned")
		}
		capProbes.Lock()
	}
	ch := make(chan struct{})
	capProbes.inflight[point] = ch
	capProbes.Unlock()
	res := make(chan error, 1)
	go func() {
		var st syscall.Stat_t
		res <- syscall.Stat(point, &st)
		capProbes.Lock()
		if capProbes.inflight[point] == ch {
			delete(capProbes.inflight, point)
		}
		capProbes.Unlock()
		close(ch)
	}()
	select {
	case err := <-res:
		switch {
		case err == nil:
			return capLive, nil
		case errors.Is(err, syscall.ENOTCONN):
			return capDead, err
		}
		return capUnknown, err
	case <-time.After(bound):
		return capWedged, fmt.Errorf("a stat of it did not return within %s", bound)
	}
}

// capConnWaiting is the number of requests waiting on the mount's FUSE connection, from its connection directory, for a report; it is
// not used to decide anything.
func capConnWaiting(m capMount) string {
	dir := m.connDir()
	if dir == "" {
		return "unknown (no connection directory for device " + m.dev + ")"
	}
	b, err := os.ReadFile(dir + "/waiting")
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	return strings.TrimSpace(string(b))
}

// sweepEnv is what the sweep reads and does, so that a test can stand in for each: the mountinfo text, the user, the mount names it looks
// at, which wedged mounts fail its caller, the probe of a mount, the unmount, the connection's waiting count and the log.
type sweepEnv struct {
	mountinfo     func() ([]byte, error)
	uid           int
	sources       []string
	failsOnWedged func(capMount) bool
	probe         func(point string) (capState, error)
	unmount       func(point string) error
	waiting       func(capMount) string
	logf          func(format string, args ...any)
}

// realSweepEnv is the sweep's environment on this machine, logging through logf. It looks at the volumes' mounts and at the wedge test's,
// and fails on a wedged mount of a volume; the wedge test's mount is wedged on purpose while that test runs, so it is only swept once dead.
func realSweepEnv(logf func(format string, args ...any)) sweepEnv {
	return sweepEnv{
		mountinfo:     func() ([]byte, error) { return os.ReadFile("/proc/self/mountinfo") },
		uid:           os.Getuid(),
		sources:       []string{capFsName, capFsWedgeName},
		failsOnWedged: func(m capMount) bool { return m.source == capFsName },
		probe:         func(p string) (capState, error) { return probeCapMount(p, capStatTimeout) },
		unmount:       func(p string) error { return capUnmount(p, capUnmountTimeout) },
		waiting:       capConnWaiting,
		logf:          logf,
	}
}

// sweepResult is what a sweep did: the mount points it unmounted, and the wedged mounts that fail its caller, each with a line that names
// its connection.
type sweepResult struct {
	swept  []string
	wedged []capMount
	report []string
}

// failure is the message on which the caller fails, empty when no mount is wedged.
func (r sweepResult) failure() string {
	if len(r.report) == 0 {
		return ""
	}
	return "a capfs mount of this user's is wedged, and is left alone: " + strings.Join(r.report, "; ") +
		". A store-full test does not run beside one: a stat of it, or a removal of the directory it is in, blocks for good." +
		" Its server must be stopped, or its connection aborted, by whoever runs it."
}

// sweepDeadCapMounts unmounts this user's capfs mounts whose endpoint is dead, logging each, and reports the wedged ones. It leaves every
// other mount alone: a live one, a wedged one, another user's, one whose state is unknown, and anything that is not capfs.
func sweepDeadCapMounts(env sweepEnv) sweepResult {
	var res sweepResult
	data, err := env.mountinfo()
	if err != nil {
		env.logf("capfs sweep: /proc/self/mountinfo cannot be read, so nothing is swept: %v", err)
		return res
	}
	for _, m := range parseCapMounts(string(data), env.sources...) {
		if m.uid != env.uid {
			env.logf("capfs sweep: %s is not this user's (user_id %d, not %d): left alone", m.point, m.uid, env.uid)
			continue
		}
		state, why := env.probe(m.point)
		switch state {
		case capDead:
			if err := env.unmount(m.point); err != nil {
				env.logf("capfs sweep: %s has a dead endpoint and could not be unmounted: %v", m.point, err)
				continue
			}
			env.logf("capfs sweep: unmounted %s, a capfs mount with a dead endpoint (its test was killed)", m.point)
			res.swept = append(res.swept, m.point)
		case capLive:
			env.logf("capfs sweep: %s answers, so a test is using it: left alone", m.point)
		case capWedged:
			line := fmt.Sprintf("%s (FUSE connection %s, %s, waiting %s): %v", m.point, m.dev, m.connDir(), env.waiting(m), why)
			if env.failsOnWedged(m) {
				res.wedged = append(res.wedged, m)
				res.report = append(res.report, line)
				env.logf("capfs sweep: wedged, so left alone: %s", line)
			} else {
				env.logf("capfs sweep: wedged, as the wedge test holds it, so left alone: %s", line)
			}
		default:
			env.logf("capfs sweep: %s could not be told dead or alive (%v): left alone", m.point, why)
		}
	}
	return res
}

// A mountinfo text the sweep reads: each line is one kind of mount, and the sweep takes only the dead capfs ones of this user's, decoding
// the escaped mount point; it fails on a wedged volume, naming its connection, but not on the wedge test's own.
func TestSweepTakesOnlyThisUsersDeadCapMounts(t *testing.T) {
	const uid = 4242
	line := func(id int, point, fstype, source, opts string) string {
		return fmt.Sprintf("%d 40 0:%d / %s rw,nosuid,nodev,relatime shared:%d - %s %s %s", id, id, point, id, fstype, source, opts)
	}
	mine := "rw,user_id=4242,group_id=4242"
	mountinfo := strings.Join([]string{
		line(101, "/scratch/dead/mnt", capFsType, capFsName, mine),
		line(102, "/scratch/live/mnt", capFsType, capFsName, mine),
		line(103, "/scratch/foreign/mnt", capFsType, capFsName, "rw,user_id=1000,group_id=1000"),
		line(104, "/scratch/other-fs/mnt", "fuse.sshfs", "remote:/data", mine),
		line(105, "/scratch/other-name/mnt", capFsType, "another-capfs", mine),
		line(106, `/scratch/with\040space/mnt`, capFsType, capFsName, mine),
		line(107, "/scratch/unknown/mnt", capFsType, capFsName, mine),
		line(108, "/scratch/no-uid/mnt", capFsType, capFsName, "rw"),
		line(109, "/scratch/ext4", "ext4", "/dev/sda1", "rw"),
		line(110, "/scratch/wedged/mnt", capFsType, capFsName, mine),
		line(111, "/scratch/wedge-test/mnt", capFsType, capFsWedgeName, mine),
		line(112, "/scratch/wedge-test-dead/mnt", capFsType, capFsWedgeName, mine),
		line(113, "/scratch/unmount-fails/mnt", capFsType, capFsName, mine),
		"not a mountinfo line",
	}, "\n")
	state := map[string]capState{
		"/scratch/dead/mnt": capDead, "/scratch/live/mnt": capLive, "/scratch/foreign/mnt": capDead, "/scratch/other-fs/mnt": capDead,
		"/scratch/other-name/mnt": capDead, "/scratch/with space/mnt": capDead, "/scratch/unknown/mnt": capUnknown, "/scratch/no-uid/mnt": capDead,
		"/scratch/ext4": capDead, "/scratch/wedged/mnt": capWedged, "/scratch/wedge-test/mnt": capWedged, "/scratch/wedge-test-dead/mnt": capDead,
		"/scratch/unmount-fails/mnt": capDead,
	}
	var unmounted, logged []string
	env := sweepEnv{
		mountinfo:     func() ([]byte, error) { return []byte(mountinfo), nil },
		uid:           uid,
		sources:       []string{capFsName, capFsWedgeName},
		failsOnWedged: func(m capMount) bool { return m.source == capFsName },
		probe: func(p string) (capState, error) {
			s, ok := state[p]
			if !ok {
				t.Errorf("the sweep asked about %q, which is not a mount point of the text", p)
			}
			return s, errors.New("the probe's reason")
		},
		unmount: func(p string) error {
			if p == "/scratch/unmount-fails/mnt" {
				return errors.New("device busy")
			}
			unmounted = append(unmounted, p)
			return nil
		},
		waiting: func(m capMount) string { return "7" },
		logf:    func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) },
	}
	res := sweepDeadCapMounts(env)
	want := []string{"/scratch/dead/mnt", "/scratch/with space/mnt", "/scratch/wedge-test-dead/mnt"}
	if fmt.Sprint(unmounted) != fmt.Sprint(want) || fmt.Sprint(res.swept) != fmt.Sprint(want) {
		t.Errorf("unmounted %v and reported %v, want exactly the three dead capfs mounts of this user's, %v", unmounted, res.swept, want)
	}
	all := strings.Join(logged, "\n")
	for _, p := range append(want, "/scratch/live/mnt", "/scratch/foreign/mnt", "/scratch/unknown/mnt", "/scratch/no-uid/mnt",
		"/scratch/wedged/mnt", "/scratch/wedge-test/mnt", "/scratch/unmount-fails/mnt") {
		if !strings.Contains(all, p) {
			t.Errorf("the sweep did not log what it did with %s: %q", p, logged)
		}
	}
	if len(res.wedged) != 1 || res.wedged[0].point != "/scratch/wedged/mnt" {
		t.Fatalf("wedged %v, want only the volume /scratch/wedged/mnt (the wedge test's own does not fail the caller)", res.wedged)
	}
	f := res.failure()
	for _, s := range []string{"/scratch/wedged/mnt", "FUSE connection 0:110", "/sys/fs/fuse/connections/110", "waiting 7", "the probe's reason"} {
		if !strings.Contains(f, s) {
			t.Errorf("the failure does not name %q: %s", s, f)
		}
	}
	if strings.Contains(f, "wedge-test") {
		t.Errorf("the failure names the wedge test's mount, which does not fail the caller: %s", f)
	}
}

// A sweep that finds nothing wedged fails nothing.
func TestASweepWithNothingWedgedHasNoFailure(t *testing.T) {
	if f := (sweepResult{swept: []string{"/scratch/dead/mnt"}}).failure(); f != "" {
		t.Errorf("a sweep that only swept has the failure %q", f)
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
		sources:   []string{capFsName},
		probe:     func(string) (capState, error) { t.Error("a mount was looked at"); return capDead, nil },
		unmount:   func(string) error { t.Error("a mount was unmounted"); return nil },
		logf:      func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) },
	}
	if res := sweepDeadCapMounts(env); len(res.swept) != 0 || res.failure() != "" || len(logged) != 1 {
		t.Errorf("swept %v, failure %q and logged %q, want nothing swept, no failure and one line", res.swept, res.failure(), logged)
	}
}

// The device number of a mountinfo line names the FUSE connection: the kernel's directory for it is the minor, under major 0.
func TestACapMountsConnectionIsNamedFromItsDeviceNumber(t *testing.T) {
	text := "71 40 0:52 / /scratch/a/mnt rw - fuse.capfs hpcburst-capfs rw,user_id=1,group_id=1\n" +
		"72 40 8:3 / /scratch/b/mnt rw - fuse.capfs hpcburst-capfs rw,user_id=1,group_id=1"
	ms := parseCapMounts(text, capFsName)
	if len(ms) != 2 {
		t.Fatalf("parsed %v, want two mounts", ms)
	}
	if ms[0].dev != "0:52" || ms[0].connDir() != "/sys/fs/fuse/connections/52" {
		t.Errorf("the first mount's device %q and connection %q, want 0:52 and /sys/fs/fuse/connections/52", ms[0].dev, ms[0].connDir())
	}
	if ms[1].connDir() != "" {
		t.Errorf("a device with major 8 names the connection %q, want none", ms[1].connDir())
	}
}

// A directory is removed only when nothing is mounted at or under it: mountedUnder finds the mount point itself and one below it, and not
// a sibling whose name only starts the same.
func TestMountedUnderFindsAMountInTheDirectory(t *testing.T) {
	text := "1 0 0:1 / / rw - ext4 /dev/sda1 rw\n" +
		"2 1 0:2 / /scratch/cv1/mnt rw - fuse.capfs hpcburst-capfs rw\n" +
		`3 1 0:3 / /scratch/with\040space rw - tmpfs tmpfs rw`
	for dir, want := range map[string]string{
		"/scratch/cv1":         "/scratch/cv1/mnt",
		"/scratch/cv1/mnt":     "/scratch/cv1/mnt",
		"/scratch/cv":          "",
		"/scratch/cv1/backing": "",
		"/scratch/with space":  "/scratch/with space",
	} {
		got, ok := mountedUnder(text, dir)
		if got != want || ok != (want != "") {
			t.Errorf("mountedUnder(%q) = %q, %t, want %q", dir, got, ok, want)
		}
	}
}

// The probe of an ordinary directory answers at once, and a path that does not exist is neither live nor dead.
func TestTheProbeOfAnOrdinaryPath(t *testing.T) {
	if s, err := probeCapMount(t.TempDir(), capStatTimeout); s != capLive {
		t.Errorf("an ordinary directory is %v (%v), want live", s, err)
	}
	if s, err := probeCapMount(t.TempDir()+"/absent", capStatTimeout); s != capUnknown {
		t.Errorf("a path that does not exist is %v (%v), want unknown", s, err)
	}
}

// The octal escapes of a mount point decode as the kernel writes them; text with no escape is unchanged, and a backslash that is not an
// escape stays.
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
