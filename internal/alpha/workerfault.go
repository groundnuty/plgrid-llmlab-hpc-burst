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
//     that is Apptainer, whose command matches the launch's engine command.
//     Zero or several fail the row, with no retry;
//  3. a second step kills that PID alone, with SIGKILL, and only if the
//     process at that PID is still the one listed (its start time and its
//     command line): never by name, and nothing outside our job's step.
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
// its parent's, its start time (field 22 of /proc/<pid>/stat), whether its
// environment carries the job's SLURM_JOB_ID, and its command line, base64.
// Processes of other users are skipped unread.
func ListScript(jobID string) string {
	// No set -f: the loop's /proc glob needs globbing, and the stat fields
	// after the command name hold no glob character.
	return `set -u
job=` + jobID + `
for d in /proc/[0-9]*; do
  [ -O "$d" ] || continue
  st=$(cat "$d/stat" 2>/dev/null) || continue
  rest=${st##*) }
  set -- $rest
  [ $# -ge 20 ] || continue
  ppid=$2 start=${20}
  injob=0
  if tr '\0' '\n' < "$d/environ" 2>/dev/null | grep -qx "SLURM_JOB_ID=$job"; then injob=1; fi
  cmd=$(base64 -w0 < "$d/cmdline" 2>/dev/null) || continue
  [ -n "$cmd" ] || continue
  echo "proc ${d#/proc/} $ppid $start $injob $cmd"
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

// MatchesCommand reports whether a process's arguments end with the launch's
// command after its program (the program itself may run through an
// interpreter, python3 /usr/local/bin/vllm). An argument matches when it is
// equal, or when both name a path (hold a "/") with the same text before the
// first "/": inside the container a path is the launch's own, and a fake
// Apptainer rewrites a bound path to the host's.
func MatchesCommand(argv, command []string) bool {
	n := len(command) - 1
	if n < 1 || len(argv) < n+1 {
		return false
	}
	tail := argv[len(argv)-n:]
	for i, want := range command[1:] {
		got := tail[i]
		if got == want {
			continue
		}
		gi, wi := strings.Index(got, "/"), strings.Index(want, "/")
		if gi < 0 || wi < 0 || got[:gi] != want[:wi] {
			return false
		}
	}
	return true
}

// ErrEngineCount is a listing in which the run did not find exactly one engine.
type ErrEngineCount struct{ N int }

func (e ErrEngineCount) Error() string {
	return fmt.Sprintf("the node shows %d engine processes of the run's job, want exactly 1: nothing was killed", e.N)
}

// PickEngine finds the one engine of the run's job in a listing: a process
// whose parent is a process of the job (InJob) that is Apptainer's, and whose
// command matches the launch's command. Anything but exactly one is an error.
func PickEngine(procs []NodeProc, command []string) (NodeProc, error) {
	byPID := make(map[int]NodeProc, len(procs))
	for _, p := range procs {
		byPID[p.PID] = p
	}
	var found []NodeProc
	for _, p := range procs {
		parent, ok := byPID[p.PPID]
		if ok && parent.InJob && isApptainer(parent.Argv) && MatchesCommand(p.Argv, command) {
			found = append(found, p)
		}
	}
	if len(found) != 1 {
		return NodeProc{}, ErrEngineCount{N: len(found)}
	}
	return found[0], nil
}

// KillScript kills the process p with SIGKILL if the process at its PID is
// still the one listed: owned by this user, with the same start time and the
// same command line. It prints "killed <pid>" when it sent the signal, and
// "changed" or "gone" when it did not.
func KillScript(p NodeProc) string {
	return `set -u
set -f
d=/proc/` + strconv.Itoa(p.PID) + `
[ -O "$d" ] || { echo gone; exit 0; }
st=$(cat "$d/stat" 2>/dev/null) || { echo gone; exit 0; }
rest=${st##*) }
set -- $rest
[ $# -ge 20 ] && [ "${20}" = "` + p.Start + `" ] || { echo changed; exit 0; }
[ "$(base64 -w0 < "$d/cmdline" 2>/dev/null)" = "` + p.cmdline + `" ] || { echo changed; exit 0; }
kill -KILL ` + strconv.Itoa(p.PID) + ` && echo "killed ` + strconv.Itoa(p.PID) + `"
`
}

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
