package alpha

// The worker-rung fault of the stack rows (issue #245; the science agent's
// ruling of 2 Nov 2026): HBEP-7:637 asks for "one restart at the worker rung"
// on each alpha platform, so the run kills the engine in its own job and the
// stack must restart the worker. The run reaches the node the way an operator
// would, from the login node with srun --overlap into its own job's
// allocation, and:
//
//  1. the job is ours: its ID comes from our Allocation's status, and right
//     before each srun it is read again from the queue's --me listing under
//     our tag (the steps check it; a mismatch fails the row);
//  2. one srun step lists the processes this user owns on the node, and
//     PickEngine finds EXACTLY ONE engine: a child of a process of our job
//     that is Apptainer, whose command is the launch's engine command, its
//     program included (MatchesCommand). Zero or several fail the row, with
//     no retry;
//  3. a second step kills that process alone, with SIGKILL, through a pidfd
//     (pidfd_open(2), pidfd_send_signal(2)): the pidfd is opened first, the
//     process at the PID is then checked to be the one listed (its owner,
//     start time and command line) and the pidfd to name it still, and the
//     signal goes through the pidfd, which names that process and never a
//     later one that reused its PID (review of PR 255, B1). A node where no
//     pidfd can be opened kills nothing. Never by name, and nothing outside
//     our job's step.
//
// The listing and the choice are split so that the choice is a pure function
// of what the node showed, and the kill script re-checks the process it is
// given rather than searching again.

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// SrunLine is the srun command line of a step in the run's own job: --overlap
// to run beside the job's batch step, one task, a one-minute limit, the
// script on standard input. jobID must be digits only.
func SrunLine(jobID string) (string, error) {
	if _, err := strconv.ParseUint(jobID, 10, 64); err != nil || strings.TrimLeft(jobID, "0123456789") != "" {
		return "", fmt.Errorf("a job ID must be digits only")
	}
	return "srun --overlap --jobid=" + jobID + " --ntasks=1 --time=1 bash -s", nil
}

// ListScript lists, on the job's node, every process this user owns: its PID,
// its parent's, its start time (field 22 of /proc/<pid>/stat), whether it is
// of the job, and its command line, base64. A process is of the job when its
// environment carries the job's SLURM_JOB_ID, or when it is in the job's
// cgroup (a path segment job_<id> in /proc/<pid>/cgroup: Slurm's cgroup
// plugins put every task of a job there, under v1 and v2), which holds also
// for a process that did not keep the environment, as Apptainer's starter may
// not (review of PR 255, nit; unverified on the clusters' nodes). Processes of
// other users are skipped unread.
func ListScript(jobID string) string { return listScript("/proc", jobID) }

// listScript is ListScript over a process tree at proc, so that a test can
// give it one of its own.
func listScript(proc, jobID string) string {
	// No set -f: the loop's glob needs globbing, and the stat fields after the
	// command name hold no glob character.
	return `set -u
job=` + jobID + `
proc='` + proc + `'
for d in "$proc"/[0-9]*; do
  [ -O "$d" ] || continue
  st=$(cat "$d/stat" 2>/dev/null) || continue
  rest=${st##*) }
  set -- $rest
  [ $# -ge 20 ] || continue
  ppid=$2 start=${20}
  injob=0
  if tr '\0' '\n' < "$d/environ" 2>/dev/null | grep -qx "SLURM_JOB_ID=$job"; then injob=1; fi
  if grep -q "/job_$job/" "$d/cgroup" 2>/dev/null; then injob=1; fi
  cmd=$(base64 -w0 < "$d/cmdline" 2>/dev/null) || continue
  [ -n "$cmd" ] || continue
  echo "proc ${d#"$proc"/} $ppid $start $injob $cmd"
done
echo "listed"
`
}

// NodeProc is one process the listing showed.
type NodeProc struct {
	PID, PPID int
	// Start is the process's start time in clock ticks since boot: with the
	// PID, it names one process, not a later one that reused the PID.
	Start string
	// InJob is true when its environment carries the job's SLURM_JOB_ID.
	InJob bool
	Argv  []string
	// cmdline is the raw command line, base64, as the kill script reads it again.
	cmdline string
}

// ErrListing is a listing that did not come back whole.
var ErrListing = errors.New("the node's process listing is incomplete or malformed")

// ParseListing reads ListScript's output. It fails unless the listing ended
// (its last line is "listed") and every process line parses.
func ParseListing(out []byte) ([]NodeProc, error) {
	var procs []NodeProc
	ended := false
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 1<<20), 4<<20)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" {
			continue
		}
		if ended {
			return nil, ErrListing
		}
		if l == "listed" {
			ended = true
			continue
		}
		f := strings.Fields(l)
		if len(f) != 6 || f[0] != "proc" || (f[4] != "0" && f[4] != "1") {
			return nil, ErrListing
		}
		pid, err1 := strconv.Atoi(f[1])
		ppid, err2 := strconv.Atoi(f[2])
		_, err3 := strconv.ParseUint(f[3], 10, 64)
		raw, err4 := base64.StdEncoding.DecodeString(f[5])
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || pid <= 0 {
			return nil, ErrListing
		}
		procs = append(procs, NodeProc{PID: pid, PPID: ppid, Start: f[3], InJob: f[4] == "1", Argv: splitArgv(raw), cmdline: f[5]})
	}
	if err := sc.Err(); err != nil || !ended {
		return nil, ErrListing
	}
	return procs, nil
}

// splitArgv is a /proc cmdline's arguments: NUL-separated, with a final NUL.
func splitArgv(raw []byte) []string {
	s := strings.TrimSuffix(string(raw), "\x00")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\x00")
}

// isApptainer reports whether a process is Apptainer's: its program's base
// name names apptainer or its starter, or it has renamed itself to the
// runtime parent's title.
func isApptainer(argv []string) bool {
	if len(argv) == 0 {
		return false
	}
	base := strings.ToLower(filepath.Base(argv[0]))
	return strings.Contains(base, "apptainer") || strings.HasPrefix(base, "starter") || strings.HasPrefix(strings.ToLower(argv[0]), "apptainer runtime parent")
}

// MatchesCommand reports whether a process runs the launch's command (review
// of PR 255, B2): its program, then exactly the launch's arguments, each equal
// (inside the container a path is the launch's own). The program is the
// launch's first word: a bare name is any absolute path whose base name it is
// (the PATH lookup), or the name itself; an absolute path is that path. It
// runs directly, or in the one interpreter form the engine's console script
// gives, a Python interpreter (python, python3 or python3.N, by any path)
// with flags of no value (as a shebang line can carry: -s, -u, -E, -I, -B, -O,
// -S) before the program's path: python3 /usr/local/bin/vllm serve .... Any
// other program, a model, a port or a socket of another value, and an
// argument more or less, is another process.
func MatchesCommand(argv, command []string) bool {
	if len(command) < 2 || len(argv) == 0 {
		return false
	}
	rest := argv
	if !isProgram(rest[0], command[0]) {
		if !pythonRE.MatchString(filepath.Base(rest[0])) {
			return false
		}
		rest = rest[1:]
		for len(rest) > 0 && pyFlagsRE.MatchString(rest[0]) {
			rest = rest[1:]
		}
		if len(rest) == 0 || !strings.HasPrefix(rest[0], "/") || !isProgram(rest[0], command[0]) {
			return false
		}
	}
	if len(rest) != len(command) {
		return false
	}
	for i := 1; i < len(command); i++ {
		if rest[i] != command[i] {
			return false
		}
	}
	return true
}

// pythonRE and pyFlagsRE are the interpreter form MatchesCommand accepts.
var (
	pythonRE  = regexp.MustCompile(`^python(3(\.[0-9]+)?)?$`)
	pyFlagsRE = regexp.MustCompile(`^-[sSuEIBO]+$`)
)

// isProgram reports whether a process's argument names the launch's program.
func isProgram(arg, program string) bool {
	if strings.Contains(program, "/") {
		return arg == program
	}
	return arg == program || (strings.HasPrefix(arg, "/") && filepath.Base(arg) == program)
}

// ErrEngineCount is a listing in which the run did not find exactly one engine.
type ErrEngineCount struct{ N int }

func (e ErrEngineCount) Error() string {
	return fmt.Sprintf("the node shows %d engine processes of the run's job, want exactly 1: nothing was killed", e.N)
}

// PickEngine finds the one engine of the run's job in a listing: a process
// whose parent is a process of the job (InJob) that is Apptainer's, and whose
// command is the launch's (match: MatchesCommand on a cluster; a stand-in
// gives its own, for the fake Apptainer's translated paths). Anything but
// exactly one is an error.
func PickEngine(procs []NodeProc, command []string, match func(argv, command []string) bool) (NodeProc, error) {
	byPID := make(map[int]NodeProc, len(procs))
	for _, p := range procs {
		byPID[p.PID] = p
	}
	var found []NodeProc
	for _, p := range procs {
		parent, ok := byPID[p.PPID]
		if ok && parent.InJob && isApptainer(parent.Argv) && match(p.Argv, command) {
			found = append(found, p)
		}
	}
	if len(found) != 1 {
		return NodeProc{}, ErrEngineCount{N: len(found)}
	}
	return found[0], nil
}

// KillScript kills the process p with SIGKILL through a pidfd, if the
// process at its PID is still the one listed: owned by this user, with the
// same start time and the same command line (KillHelper). It prints
// "killed <pid>" when it sent the signal; "changed" or "gone" when the process
// is not the one listed or has ended; and "no-pidfd" when the node has no
// python3 or cannot open a pidfd, so that nothing can be killed safely: the
// row then fails, killing nothing.
func KillScript(p NodeProc) string {
	return `set -u
set -f
command -v python3 >/dev/null 2>&1 || { echo no-pidfd; exit 0; }
exec python3 -I - ` + strconv.Itoa(p.PID) + ` ` + p.Start + ` ` + p.cmdline + ` <<'HB_KILL_HELPER'
` + KillHelper + `HB_KILL_HELPER
`
}

// KillHelper is the kill's Python program: pidfd_open(2) on the PID, then the
// identity of the process at the PID in /proc (owner, start time, command
// line), then the pidfd's own PID in /proc/self/fdinfo (it is -1 once the
// process it names has been reaped, so a PID now used by another process does
// not pass), then pidfd_send_signal(2). The pidfd names the process that had
// the PID when it was opened: a process that passed the check and exited
// before the signal is not replaced by a later one with its PID, and the
// signal then fails with ESRCH. The syscall numbers, 434 and 424, are the same
// on x86_64 and aarch64 (the generic table, Linux 5.3 and later); Python 3.9
// and later have the calls themselves. It prints one word and exits 0.
// main's after_check is the seam a test uses to run code between the check
// and the signal.
const KillHelper = `import base64, ctypes, errno, os, signal, sys

PIDFD_OPEN, PIDFD_SEND_SIGNAL = 434, 424


def pidfd_open(pid):
    if hasattr(os, "pidfd_open"):
        return os.pidfd_open(pid)
    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.syscall(PIDFD_OPEN, ctypes.c_int(pid), ctypes.c_uint(0))
    if fd < 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))
    return fd


def pidfd_send_signal(fd, sig):
    if hasattr(signal, "pidfd_send_signal"):
        return signal.pidfd_send_signal(fd, sig)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.syscall(PIDFD_SEND_SIGNAL, ctypes.c_int(fd), ctypes.c_int(sig), None, ctypes.c_uint(0)) != 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))


def same(pid, start, cmdline):
    d = "/proc/%d" % pid
    if os.stat(d).st_uid != os.getuid():
        return False
    with open(d + "/stat") as f:
        st = f.read()
    rest = st[st.rindex(")") + 2:].split()
    if len(rest) < 20 or rest[19] != start:
        return False
    with open(d + "/cmdline", "rb") as f:
        return base64.b64encode(f.read()).decode() == cmdline


def fd_pid(fd):
    with open("/proc/self/fdinfo/%d" % fd) as f:
        for line in f:
            if line.startswith("Pid:"):
                return int(line.split()[1])
    return None


def main(argv, after_check=None):
    pid, start, cmdline = int(argv[0]), argv[1], argv[2]
    try:
        fd = pidfd_open(pid)
    except OSError as e:
        return "gone" if e.errno == errno.ESRCH else "no-pidfd"
    try:
        if not same(pid, start, cmdline):
            return "changed"
    except (OSError, ValueError):
        return "gone"
    got = fd_pid(fd)
    if got is None:
        return "no-pidfd"
    if got != pid:
        return "gone"
    if after_check is not None:
        after_check()
    try:
        pidfd_send_signal(fd, signal.SIGKILL)
    except OSError as e:
        return "gone" if e.errno == errno.ESRCH else "no-pidfd"
    return "killed %d" % pid


if __name__ == "__main__":
    print(main(sys.argv[1:]))
`

// Killed reports whether KillScript's output says it sent the signal to p.
func Killed(out []byte, p NodeProc) bool {
	return strings.TrimSpace(string(out)) == "killed "+strconv.Itoa(p.PID)
}

// CommandDigest is a short digest of a command, for the log in place of the
// command itself.
func CommandDigest(argv []string) string {
	s := sha256.Sum256([]byte(strings.Join(argv, "\x00")))
	return fmt.Sprintf("%x", s[:6])
}

// RestartTimes are the times of the worker-rung fault, posted with the row
// (the science agent's condition 5): the kill; the controller first showing
// the exit the agent reported (Healthy False with PartExited); the restart it
// decided (status.restarts); and the Instance ready again.
type RestartTimes struct {
	Killed       time.Time `json:"killed"`
	AgentNoticed time.Time `json:"agentNoticed"`
	Restarted    time.Time `json:"restarted"`
	ReadyAgain   time.Time `json:"readyAgain"`
}

// Line is the times as one line to post: each step's delay from the kill.
func (r RestartTimes) Line() string {
	since := func(t time.Time) string {
		if t.IsZero() || r.Killed.IsZero() {
			return "not seen"
		}
		return t.Sub(r.Killed).Round(time.Second).String()
	}
	return fmt.Sprintf("worker-rung restart, from the engine's kill: the exit shown %s, the restart decided %s, ready again %s",
		since(r.AgentNoticed), since(r.Restarted), since(r.ReadyAgain))
}
