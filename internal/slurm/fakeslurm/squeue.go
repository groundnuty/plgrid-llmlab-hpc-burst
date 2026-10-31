package fakeslurm

// Ported from spikes/s4/fakeslurm/squeue.go. Added: the fuller job object of
// S7's running-job capture, the per-user --me view, the pending start
// estimate, the running job's projected end_time, the two "not listed" shapes
// (S7: Helios and Athena) and the text table.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// defaultPriority is the priority of every job that is not held.
const defaultPriority = 1000

// invalidJob is squeue's answer for a job ID slurmctld does not know (S7).
const invalidJob = "slurm_load_jobs error: Invalid job id specified"

var squeueDefs = []optDef{
	{name: "json"}, {name: "me"},
	{name: "jobs", short: 'j', value: valRequired},
}

// squeueJob is a job object of squeue --json: S7's v0.0.42 running-job shape,
// cut to the fields a reader could use, with the same names and types. The
// v0.0.40 shape is the same, as in S4.
type squeueJob struct {
	Account          string   `json:"account"`
	AccrueTime       NV       `json:"accrue_time"`
	BatchFlag        bool     `json:"batch_flag"`
	Comment          string   `json:"comment"`
	CPUs             NV       `json:"cpus"`
	CPUsPerTask      NV       `json:"cpus_per_task"`
	DerivedExitCode  exitCode `json:"derived_exit_code"`
	EligibleTime     NV       `json:"eligible_time"`
	EndTime          NV       `json:"end_time"`
	ExitCode         exitCode `json:"exit_code"`
	Flags            []string `json:"flags"`
	Hold             bool     `json:"hold"`
	JobID            int      `json:"job_id"`
	JobState         []string `json:"job_state"`
	MemoryPerCPU     NV       `json:"memory_per_cpu"`
	MemoryPerNode    NV       `json:"memory_per_node"`
	Name             string   `json:"name"`
	NodeCount        NV       `json:"node_count"`
	Partition        string   `json:"partition"`
	Priority         NV       `json:"priority"`
	QOS              string   `json:"qos"`
	Requeue          bool     `json:"requeue"`
	RestartCnt       int      `json:"restart_cnt"`
	StandardError    string   `json:"standard_error"`
	StandardOutput   string   `json:"standard_output"`
	StartTime        NV       `json:"start_time"`
	StateDescription string   `json:"state_description"`
	StateReason      string   `json:"state_reason"`
	SubmitTime       NV       `json:"submit_time"`
	Tasks            NV       `json:"tasks"`
	TimeLimit        NV       `json:"time_limit"`
	TresAllocStr     string   `json:"tres_alloc_str"`
	TresReqStr       string   `json:"tres_req_str"`
	UserName         string   `json:"user_name"`
}

type squeueResp struct {
	Jobs         []squeueJob `json:"jobs"`
	LastBackfill NV          `json:"last_backfill"`
	LastUpdate   NV          `json:"last_update"`
	Meta         meta        `json:"meta"`
	Errors       []any       `json:"errors"`
	Warnings     []any       `json:"warnings"`
}

// squeue lists jobs as JSON, or as a text table without --json.
func (sh *shell) squeue(args []string) int {
	p, err := parseOpts(args, squeueDefs)
	if err == nil && len(p.rest) > 0 {
		err = fmt.Errorf("unexpected argument %q", p.rest[0])
	}
	var ids []int
	if err == nil {
		if v, ok := p.get("jobs"); ok {
			ids, err = parseJobIDs(v)
		}
	}
	if err != nil {
		return sh.fail(1, "squeue: error: %v", err)
	}
	rows, ok := sh.p.squeueRows(sh.s.user, p.has("me"), ids, sh.now)
	if !ok {
		return sh.fail(1, "%s", invalidJob)
	}
	if !p.has("json") {
		sh.out("%s", sh.p.squeueText(rows, sh.now))
		return 0
	}
	resp := squeueResp{
		Jobs:         []squeueJob{},
		LastBackfill: nv(sh.now.Unix()), LastUpdate: nv(sh.now.Unix()),
		Meta: sh.p.newMeta(sh.s.user, "squeue"), Errors: []any{}, Warnings: []any{},
	}
	for _, j := range rows {
		resp.Jobs = append(resp.Jobs, squeueJobOf(j))
	}
	out, err := marshalOut(resp)
	if err != nil {
		return sh.fail(1, "squeue: error: %v", err)
	}
	sh.out("%s", out)
	return 0
}

func parseJobIDs(v string) ([]int, error) {
	var ids []int
	for _, s := range splitList(v) {
		id, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("invalid job id: %s", s)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// squeueRows selects the jobs squeue lists and orders them by ID. ok is false
// when a requested job ID is unknown or purged: squeue then fails with exit 1.
//
// Whether an ended job is listed is the NotListed mode: Helios never lists it
// (an empty answer for it by --jobs until it is purged), Athena lists it in its
// end state until it is purged. A purged job is never listed. A job another
// user owns is known to slurmctld, so --me --jobs=<id> for it is an empty list,
// not the error.
func (p *Platform) squeueRows(user string, me bool, ids []int, now time.Time) ([]*job, bool) {
	var rows []*job
	listed := func(j *job) bool {
		if j.purged(now, p.cfg.MinJobAge) || (me && j.user != user) {
			return false
		}
		return !IsTerminal(j.state()) || p.cfg.NotListed == NotListedAthena
	}
	if len(ids) == 0 {
		for _, j := range p.jobs {
			if listed(j) {
				rows = append(rows, j)
			}
		}
	}
	for _, id := range ids {
		j := p.find(id)
		if j == nil || j.purged(now, p.cfg.MinJobAge) {
			return nil, false
		}
		if listed(j) {
			rows = append(rows, j)
		}
	}
	sort.SliceStable(rows, func(a, b int) bool {
		if p.cfg.SqueueDesc {
			return rows[a].id > rows[b].id
		}
		return rows[a].id < rows[b].id
	})
	return rows, true
}

// find returns the job with the id, or nil.
func (p *Platform) find(id int) *job {
	for _, j := range p.jobs {
		if j.id == id {
			return j
		}
	}
	return nil
}

func squeueJobOf(j *job) squeueJob {
	e := j.cur()
	state := e.state
	// Slurm reports hold = (priority == 0); derive it the same way. A requeued
	// job is plain PENDING in Slurm 23.11 and 24.11: no REQUEUED flag, only
	// restart_cnt tells.
	prio := nv(defaultPriority)
	if state == Pending && j.held {
		prio = nv(0)
	}
	limit := time.Duration(j.timeLimit) * time.Minute
	start, end := nvTime(e.start), NV{}
	switch {
	case state == Pending:
		// A pending job's start_time is the scheduler's estimate; a held job
		// is not scheduled and has none.
		start = NV{}
		if j.estimate != nil && !j.held {
			start = nvTime(*j.estimate)
		}
	case state == Running && j.timeLimit > 0:
		// While a job runs, end_time is its projected end (S7).
		end = nvTime(e.start.Add(limit))
	case IsTerminal(state):
		end = nvTime(e.end)
	}
	alloc := ""
	if !e.start.IsZero() {
		alloc = tresStr(j)
	}
	return squeueJob{
		Account: j.account, AccrueTime: nvTime(e.submit), BatchFlag: true, Comment: "",
		CPUs: nv(int64(j.cpus)), CPUsPerTask: nv(int64(j.cpus)),
		DerivedExitCode: exitCodeFor(state), EligibleTime: nvTime(e.submit), EndTime: end,
		ExitCode: exitCodeFor(state), Flags: []string{}, Hold: prio.Number == 0 && state == Pending,
		JobID: j.id, JobState: []string{state},
		MemoryPerCPU: NV{}, MemoryPerNode: nv(j.memMiB), Name: j.name, NodeCount: nv(int64(j.nodes)),
		Partition: j.partition, Priority: prio, QOS: "normal", Requeue: j.requeue, RestartCnt: j.restartCnt,
		StandardError: j.errPath, StandardOutput: j.output, StartTime: start,
		StateDescription: "", StateReason: j.stateReason(), SubmitTime: nvTime(e.submit),
		Tasks: nv(1), TimeLimit: limitNV(j.timeLimit), TresAllocStr: alloc, TresReqStr: tresStr(j),
		UserName: j.user,
	}
}

// stateCodes are squeue's short state names.
var stateCodes = map[string]string{
	Pending: "PD", Running: "R", Completed: "CD", Failed: "F",
	Cancelled: "CA", Timeout: "TO", NodeFail: "NF",
}

const textTime = "2006-01-02T15:04:05"

// squeueText is the table of squeue without --json (see doc.go for the
// columns). The two time columns are local time in Config.TimeZone.
func (p *Platform) squeueText(rows []*job, now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%8s %-9s %-12s %-10s %-2s %9s %5s %-18s %-19s %-19s\n",
		"JOBID", "PARTITION", "NAME", "USER", "ST", "TIME", "NODES", "NODELIST(REASON)", "SUBMIT_TIME", "START_TIME")
	for _, j := range rows {
		e := j.cur()
		nodes, start := "", "N/A"
		switch {
		case e.state == Pending:
			nodes = "(" + j.stateReason() + ")"
			if j.estimate != nil && !j.held {
				start = j.estimate.In(p.zone).Format(textTime)
			}
		case !e.start.IsZero():
			start = e.start.In(p.zone).Format(textTime)
			nodes = "example-node-1"
		}
		fmt.Fprintf(&b, "%8d %-9s %-12s %-10s %-2s %9s %5d %-18s %-19s %-19s\n",
			j.id, j.partition, j.name, j.user, stateCodes[e.state], runTime(e, now), j.nodes, nodes,
			e.submit.In(p.zone).Format(textTime), start)
	}
	return b.String()
}

// runTime is the time the execution has run, as squeue writes it, H:MM:SS.
func runTime(e *execution, now time.Time) string {
	if e.start.IsZero() {
		return "0:00"
	}
	end := e.end
	if end.IsZero() {
		end = now
	}
	d := int64(end.Sub(e.start) / time.Second)
	return fmt.Sprintf("%d:%02d:%02d", d/3600, d%3600/60, d%60)
}
