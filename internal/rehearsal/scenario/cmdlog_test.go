package scenario_test

// The login node's record of the commands the controller ran on the fake
// cluster (issue #193, review of PR #247). The relay runs each command through
// cmdLog.around, in the test process: just before the command runs, it reads
// which jobs hold a slot of each partition; for an sbatch it keeps the
// attempt's own outcome. A check then reads the cluster's state at each
// command the controller sent, never at a sample of its own.

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// cmdRecord is one command the controller ran on the fake cluster.
type cmdRecord struct {
	at   time.Time // just before it ran, when holders was read
	word string    // its first word
	// holders maps each partition to the jobs that held its slot just
	// before the command ran (holdsSlot).
	holders map[string][]int
}

// submitAttempt is one sbatch the controller ran: what it asked for, the
// partition's slot holders just before it ran, and its own outcome.
type submitAttempt struct {
	at              time.Time
	name, partition string // --job-name and --partition
	holders         []int  // the partition's slot holders just before it ran
	exit            int
	jobID           int    // the job ID it printed (--parsable); 0 for none
	err             string // the runner's error, "" for none
}

// cmdLog is the record. sub holds every sbatch from its read of the holders
// until it has registered its job, so no two submissions interleave there.
type cmdLog struct {
	plat     *fakeslurm.Platform
	sub      sync.Mutex
	mu       sync.Mutex
	cmds     []cmdRecord
	attempts []submitAttempt
}

// around is the login node's LoginConfig.Around.
func (l *cmdLog) around(line string, run func() (slurm.Result, error)) (slurm.Result, error) {
	words := strings.Fields(line)
	rec := cmdRecord{}
	if len(words) > 0 {
		rec.word = words[0]
	}
	submit := rec.word == "sbatch"
	if submit {
		l.sub.Lock()
		defer l.sub.Unlock()
	}
	rec.holders = slotHolders(l.plat.Jobs())
	rec.at = time.Now()
	res, err := run()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cmds = append(l.cmds, rec)
	if submit {
		a := submitAttempt{at: rec.at, exit: res.ExitCode}
		for _, w := range words {
			if v, ok := strings.CutPrefix(w, "--job-name="); ok {
				a.name = v
			}
			if v, ok := strings.CutPrefix(w, "--partition="); ok {
				a.partition = v
			}
		}
		a.holders = rec.holders[a.partition]
		if err != nil {
			a.err = err.Error()
		} else {
			a.jobID = parsableJobID(res.Stdout)
		}
		l.attempts = append(l.attempts, a)
	}
	return res, err
}

// parsableJobID reads sbatch --parsable's reply, "<id>" or "<id>;<cluster>";
// 0 when it holds none.
func parsableJobID(out []byte) int {
	s := strings.TrimSpace(string(out))
	if i := strings.IndexByte(s, ';'); i >= 0 {
		s = s[:i]
	}
	id, err := strconv.Atoi(s)
	if err != nil || id <= 0 {
		return 0
	}
	return id
}

// submissions returns the sbatch attempts to the partition, in order.
func (l *cmdLog) submissions(partition string) []submitAttempt {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []submitAttempt
	for _, a := range l.attempts {
		if a.partition == partition {
			out = append(out, a)
		}
	}
	return out
}

// lastHeld is the time of the last command before which the job held its
// slot, and how many commands were recorded: a lower bound on when the job
// left its slot, read at the controller's own commands.
func (l *cmdLog) lastHeld(partition string, job int) (time.Time, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var last time.Time
	for _, c := range l.cmds {
		for _, id := range c.holders[partition] {
			if id == job {
				last = c.at
			}
		}
	}
	return last, len(l.cmds)
}

// slotHolders maps each partition to its jobs that hold a slot (holdsSlot).
func slotHolders(jobs []fakeslurm.Job) map[string][]int {
	out := map[string][]int{}
	for _, j := range jobs {
		if holdsSlot(j) {
			out[j.Partition] = append(out[j.Partition], j.ID)
		}
	}
	return out
}

// holdsSlot reports whether a job holds its partition's submit slot as HBEP-7
// counts it for the one-slot path: pending (held or not), running, or ended
// and still completing (its processes or its epilog), until the flag clears.
func holdsSlot(j fakeslurm.Job) bool {
	switch j.State {
	case "PENDING", "RUNNING":
		return true
	}
	return j.Completing || !j.Finished
}
