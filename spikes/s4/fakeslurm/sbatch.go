package fakeslurm

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// sbatchDefs lists the options sbatch reads, plus the value-taking ones it
// ignores, so that their values are not taken for the script path.
var sbatchDefs = []optDef{
	{name: "parsable"}, {name: "requeue"}, {name: "no-requeue"},
	{name: "job-name", short: 'J', value: valRequired},
	{name: "comment", value: valRequired},
	{name: "time", short: 't', value: valRequired},
	{name: "partition", short: 'p', value: valRequired},
	{name: "account", short: 'A', value: valRequired},
	{name: "wrap", value: valRequired},
	{name: "output", short: 'o', value: valRequired},
	{name: "error", short: 'e', value: valRequired},
	{name: "chdir", short: 'D', value: valRequired},
	{name: "nodes", short: 'N', value: valRequired},
	{name: "ntasks", short: 'n', value: valRequired},
	{name: "cpus-per-task", short: 'c', value: valRequired},
	{name: "mem", value: valRequired},
	{name: "gres", value: valRequired},
	{name: "gpus", short: 'G', value: valRequired},
	{name: "qos", short: 'q', value: valRequired},
	{name: "constraint", short: 'C', value: valRequired},
	{name: "nodelist", short: 'w', value: valRequired},
	{name: "exclude", short: 'x', value: valRequired},
	{name: "dependency", short: 'd', value: valRequired},
	{name: "array", short: 'a', value: valRequired},
	{name: "begin", short: 'b', value: valRequired},
	{name: "export", value: valRequired},
	{name: "mail-type", value: valRequired},
	{name: "mail-user", value: valRequired},
	{name: "reservation", value: valRequired},
	{name: "signal", value: valRequired},
	{name: "clusters", short: 'M', value: valRequired},
	{name: "licenses", short: 'L', value: valRequired},
	{name: "mem-per-cpu", value: valRequired},
	{name: "ntasks-per-node", value: valRequired},
	{name: "cpus-per-gpu", value: valRequired},
	{name: "mem-per-gpu", value: valRequired},
	{name: "gpus-per-node", value: valRequired},
}

// sbatchSpec is a parsed sbatch command line.
type sbatchSpec struct {
	name, comment, partition, account, script string
	timeLimit                                 int
	requeue, parsable                         bool
}

func parseSbatch(args []string) (sbatchSpec, error) {
	p, err := parseOpts(args, sbatchDefs)
	if err != nil {
		return sbatchSpec{}, err
	}
	s := sbatchSpec{requeue: p.last("requeue", "no-requeue") != "no-requeue", parsable: p.has("parsable")}
	s.name, _ = p.get("job-name")
	s.comment, _ = p.get("comment")
	s.partition, _ = p.get("partition")
	s.account, _ = p.get("account")
	if v, ok := p.get("time"); ok {
		if s.timeLimit, err = parseTimeLimit(v); err != nil {
			return s, err
		}
	}
	if len(p.rest) > 0 {
		s.script = p.rest[0]
	} else if w, ok := p.get("wrap"); ok {
		s.script = "--wrap=" + w
	}
	return s, nil
}

// sbatch registers a job. The locked section ends before any sleep or block,
// so a hung sbatch never holds the lock.
func (a *app) sbatch(args []string) int {
	if err := a.locked(func(t *tx) error {
		t.emit("sbatch_start", "sbatch", 0, nil)
		return nil
	}); err != nil {
		return a.internalError(err)
	}
	a.pause("delay_before", a.cfg.DelayBefore)

	var spec sbatchSpec
	var perr error
	if spec, perr = parseSbatch(args); perr != nil || a.cfg.Fail {
		msg := "sbatch: error: Batch job submission failed: fake failure\n"
		if perr != nil {
			msg = fmt.Sprintf("sbatch: error: %v\n", perr)
		}
		if err := a.locked(func(t *tx) error {
			t.emit("sbatch_failed", "sbatch", 0, intp(1))
			return nil
		}); err != nil {
			return a.internalError(err)
		}
		fmt.Fprint(a.env.Stderr, msg)
		return 1
	}

	var id int
	if err := a.locked(func(t *tx) error {
		now := a.env.Now()
		id = t.st.NextID
		t.st.NextID++
		t.st.Jobs = append(t.st.Jobs, a.newJob(id, spec, now))
		t.dirty = true
		t.emit("sbatch_submitted", "sbatch", id, intp(0))
		return nil
	}); err != nil {
		return a.internalError(err)
	}

	if a.cfg.BlockMarker != "" {
		b, _ := json.Marshal(map[string]int{"pid": a.env.Pid, "job_id": id})
		if err := writeFileAtomic(a.cfg.BlockMarker, b); err != nil {
			return a.internalError(err)
		}
	}
	a.pause("delay_after", a.cfg.DelayAfter)
	if a.cfg.BlockMarker != "" {
		a.hook("block")
		a.env.Block()
		return 0 // only reached when a test's Block hook returns
	}
	if spec.parsable {
		fmt.Fprintf(a.env.Stdout, "%d\n", id)
	} else {
		fmt.Fprintf(a.env.Stdout, "Submitted batch job %d\n", id)
	}
	return 0
}

func (a *app) newJob(id int, s sbatchSpec, now time.Time) *Job {
	j := &Job{
		ID: id, Name: s.name, Comment: s.comment, User: a.cfg.User,
		Partition: s.partition, Account: s.account, TimeLimit: s.timeLimit,
		Requeue: s.requeue, Argv: a.argv, Script: s.script,
		Execs: []*Exec{newExec(now, "None")},
	}
	if j.Name == "" {
		switch {
		case strings.HasPrefix(s.script, "--wrap="):
			j.Name = "wrap"
		case s.script != "":
			j.Name = filepath.Base(s.script)
		default:
			j.Name = "sbatch"
		}
	}
	if j.Partition == "" {
		j.Partition = "batch"
	}
	if j.Account == "" {
		j.Account = "default"
	}
	return j
}

// pause sleeps outside the lock (a zero delay does nothing).
func (a *app) pause(point string, d time.Duration) {
	if d <= 0 {
		return
	}
	a.hook(point)
	a.env.Sleep(d)
}

func (a *app) hook(point string) {
	if a.env.OutsideLock != nil {
		a.env.OutsideLock(point)
	}
}
