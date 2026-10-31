package fakeslurm

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

// defaultPriority is the priority of every job that is not held.
const defaultPriority = 1000

var squeueDefs = []optDef{
	{name: "json"}, {name: "yaml"}, {name: "me"},
	{name: "name", short: 'n', value: valRequired},
	{name: "jobs", short: 'j', value: valRequired},
	{name: "user", short: 'u', value: valRequired},
	{name: "format", short: 'o', value: valRequired},
	{name: "Format", short: 'O', value: valRequired},
	{name: "states", short: 't', value: valRequired},
	{name: "partition", short: 'p', value: valRequired},
	{name: "account", short: 'A', value: valRequired},
	{name: "sort", short: 'S', value: valRequired},
	{name: "clusters", short: 'M', value: valRequired},
	{name: "iterate", short: 'i', value: valRequired},
	{name: "nodelist", short: 'w', value: valRequired},
	{name: "noheader", short: 'h'},
}

type squeueJob struct {
	JobID       int      `json:"job_id"`
	Name        string   `json:"name"`
	Comment     string   `json:"comment"`
	JobState    []string `json:"job_state"`
	RestartCnt  int      `json:"restart_cnt"`
	Requeue     bool     `json:"requeue"`
	SubmitTime  NV       `json:"submit_time"`
	StartTime   NV       `json:"start_time"`
	EndTime     NV       `json:"end_time"`
	UserName    string   `json:"user_name"`
	Partition   string   `json:"partition"`
	Account     string   `json:"account"`
	StateReason string   `json:"state_reason"`
	Hold        bool     `json:"hold"`
	Priority    NV       `json:"priority"`
	TimeLimit   NV       `json:"time_limit"`
	ExitCode    exitCode `json:"exit_code"`
}

type squeueResp struct {
	Jobs         []squeueJob `json:"jobs"`
	LastBackfill NV          `json:"last_backfill"`
	LastUpdate   NV          `json:"last_update"`
	Meta         meta        `json:"meta"`
	Errors       []any       `json:"errors"`
	Warnings     []any       `json:"warnings"`
}

// jobFilter is the intersection of the --me, --user, --name and --jobs
// filters shared by squeue and sacct. An empty list means no filter.
type jobFilter struct {
	users, names []string
	ids          []int
}

func newJobFilter(p parsed, cfg Config) (jobFilter, error) {
	var f jobFilter
	if p.has("me") {
		f.users = append(f.users, cfg.User)
	}
	if v, ok := p.get("user"); ok {
		f.users = append(f.users, splitList(v)...)
	}
	if v, ok := p.get("name"); ok {
		f.names = splitList(v)
	}
	if v, ok := p.get("jobs"); ok {
		for _, s := range splitList(v) {
			id, err := strconv.Atoi(s)
			if err != nil {
				return f, fmt.Errorf("invalid job id: %s", s)
			}
			f.ids = append(f.ids, id)
		}
	}
	return f, nil
}

func (f jobFilter) match(j *Job) bool {
	if len(f.users) > 0 && !contains(f.users, j.User) {
		return false
	}
	if len(f.names) > 0 && !contains(f.names, j.Name) {
		return false
	}
	if len(f.ids) > 0 {
		for _, id := range f.ids {
			if id == j.ID {
				return true
			}
		}
		return false
	}
	return true
}

func contains[T comparable](list []T, v T) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func (a *app) squeue(args []string) int {
	return a.run(func(t *tx) result {
		fail := func(msg string) result {
			t.emit("squeue", "squeue", 0, intp(1))
			return result{errOut: "squeue: error: " + msg + "\n", exit: 1}
		}
		p, err := parseOpts(args, squeueDefs)
		if err != nil {
			return fail(err.Error())
		}
		if p.has("yaml") || !p.has("json") {
			return fail("the fake squeue supports only --json")
		}
		f, err := newJobFilter(p, a.cfg)
		if err != nil {
			return fail(err.Error())
		}
		now := a.env.Now()
		resp := squeueResp{
			Jobs:         []squeueJob{},
			LastBackfill: nv(now.Unix()), LastUpdate: nv(now.Unix()),
			Meta: newMeta(a.cfg.DataParser), Errors: []any{}, Warnings: []any{},
		}
		for _, j := range t.st.Jobs {
			if j.purged(now, a.cfg.MinJobAge) || !f.match(j) {
				continue
			}
			resp.Jobs = append(resp.Jobs, squeueJobOf(j, now))
		}
		sort.SliceStable(resp.Jobs, func(i, k int) bool {
			if a.cfg.SqueueDesc {
				return resp.Jobs[i].JobID > resp.Jobs[k].JobID
			}
			return resp.Jobs[i].JobID < resp.Jobs[k].JobID
		})
		out, err := marshalOut(resp)
		if err != nil {
			return fail(err.Error())
		}
		t.emit("squeue", "squeue", 0, intp(0))
		return result{out: out}
	})
}

func squeueJobOf(j *Job, now time.Time) squeueJob {
	e := j.Cur()
	state := e.StateAt(now)
	// A requeued job is plain PENDING in Slurm 23.11 and 24.11: no REQUEUED
	// flag; only restart_cnt tells.
	js := []string{state}
	// Slurm reports hold = (priority == 0); derive it the same way.
	prio := nv(defaultPriority)
	if state == Pending && j.Held {
		prio = nv(0)
	}
	reason := "None"
	if state == Pending {
		reason = e.Reason
		if reason == "" {
			reason = "None"
		}
	}
	end := time.Time{}
	if IsTerminal(state) {
		end = e.End
	}
	return squeueJob{
		JobID: j.ID, Name: j.Name, Comment: j.Comment, JobState: js,
		RestartCnt: j.RestartCnt, Requeue: j.Requeue,
		SubmitTime: nvTime(e.Submit), StartTime: nvTime(e.Start), EndTime: nvTime(end),
		UserName: j.User, Partition: j.Partition, Account: j.Account,
		StateReason: reason, Hold: prio.Number == 0, Priority: prio, TimeLimit: limitNV(j.TimeLimit), ExitCode: exitCodeFor(state),
	}
}

// limitNV is a time limit in minutes; 0 means none was given.
func limitNV(minutes int) NV {
	if minutes <= 0 {
		return NV{}
	}
	return nv(int64(minutes))
}
