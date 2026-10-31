package fakeslurm

// Ported from spikes/s4/fakeslurm/sacct.go. Added: the fuller job object of
// S7's sacct capture (hold, priority, time.planned, tres requested and
// allocated, qos), the calendar zone from Config.TimeZone, and the per-user
// default. S4's accounting lag (FAKE_SLURM_ACCT_DELAY) is not ported; AcctHide
// models a job slurmdbd does not have yet.

import (
	"fmt"
	"time"
)

var sacctDefs = []optDef{
	{name: "json"},
	{name: "allocations", short: 'X'},
	{name: "duplicates", short: 'D'},
	{name: "name", value: valRequired},
	{name: "jobs", short: 'j', value: valRequired},
	{name: "starttime", short: 'S', value: valRequired},
	{name: "endtime", short: 'E', value: valRequired},
}

type sacctComment struct {
	Administrator string `json:"administrator"`
	Job           string `json:"job"`
	System        string `json:"system"`
}

type sacctState struct {
	Current []string `json:"current"`
	Reason  string   `json:"reason"`
}

// sacctTime is S7's time object: epoch seconds, elapsed and suspended in
// seconds, planned (the wait) in seconds and limit in minutes as triples.
type sacctTime struct {
	Elapsed    int64 `json:"elapsed"`
	Eligible   int64 `json:"eligible"`
	End        int64 `json:"end"`
	Planned    NV    `json:"planned"`
	Start      int64 `json:"start"`
	Submission int64 `json:"submission"`
	Suspended  int64 `json:"suspended"`
	Limit      NV    `json:"limit"`
}

type sacctTres struct {
	Allocated []tres `json:"allocated"`
	Requested []tres `json:"requested"`
}

type sacctJob struct {
	Account         string       `json:"account"`
	Comment         sacctComment `json:"comment"`
	DerivedExitCode exitCode     `json:"derived_exit_code"`
	Time            sacctTime    `json:"time"`
	ExitCode        exitCode     `json:"exit_code"`
	Flags           []string     `json:"flags"`
	JobID           int          `json:"job_id"`
	Name            string       `json:"name"`
	Partition       string       `json:"partition"`
	Hold            bool         `json:"hold"`
	Priority        NV           `json:"priority"`
	QOS             string       `json:"qos"`
	RestartCnt      *int         `json:"restart_cnt,omitempty"` // v0.0.42 only
	State           sacctState   `json:"state"`
	Tres            sacctTres    `json:"tres"`
	User            string       `json:"user"`
}

type sacctResp struct {
	Jobs     []sacctJob `json:"jobs"`
	Meta     meta       `json:"meta"`
	Errors   []any      `json:"errors"`
	Warnings []any      `json:"warnings"`
}

// sacct lists the calling user's jobs as JSON, in the window -S to -E.
func (sh *shell) sacct(args []string) int {
	p, err := parseOpts(args, sacctDefs)
	if err == nil && len(p.rest) > 0 {
		err = fmt.Errorf("unexpected argument %q", p.rest[0])
	}
	if err == nil && !p.has("json") {
		err = fmt.Errorf("the fake supports only --json")
	}
	var ids []int
	if v, ok := p.get("jobs"); err == nil && ok {
		ids, err = parseJobIDs(v)
	}
	var from, to time.Time
	if err == nil {
		from, to, err = sh.p.sacctWindow(p, sh.now)
	}
	if err != nil {
		return sh.fail(1, "sacct: error: %v", err)
	}
	name, byName := p.get("name")
	resp := sacctResp{
		Jobs: []sacctJob{}, Meta: sh.p.newMeta(sh.s.user, "sacct"),
		Errors: []any{}, Warnings: []any{},
	}
	for _, j := range sh.p.jobs {
		if j.acctHidden || j.user != sh.s.user || (byName && j.name != name) || !wantID(ids, j.id) {
			continue
		}
		for _, rec := range sh.p.accountingRecords(j, sh.now, p.has("duplicates")) {
			// Window: submitted by E and not ended before S.
			if rec.submit.After(to) || (!rec.end.IsZero() && rec.end.Before(from)) {
				continue
			}
			resp.Jobs = append(resp.Jobs, rec.job)
		}
	}
	out, err := marshalOut(resp)
	if err != nil {
		return sh.fail(1, "sacct: error: %v", err)
	}
	sh.out("%s", out)
	return 0
}

func wantID(ids []int, id int) bool {
	if len(ids) == 0 {
		return true
	}
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// sacctWindow resolves -S and -E. The default start is midnight today in the
// configured zone, or the epoch when -j is given (sacct man page); the default
// end is now.
func (p *Platform) sacctWindow(o parsed, now time.Time) (from, to time.Time, err error) {
	from = midnight(now, p.zone)
	if o.has("jobs") {
		from = time.Unix(0, 0)
	}
	to = now
	if v, ok := o.get("starttime"); ok {
		if from, err = parseSlurmTime(v, now, p.zone); err != nil {
			return
		}
	}
	if v, ok := o.get("endtime"); ok {
		to, err = parseSlurmTime(v, now, p.zone)
	}
	return
}

type acctRecord struct {
	job         sacctJob
	submit, end time.Time
}

// accountingRecords is slurmdbd's view of j at now: the latest execution in its
// current state and, with duplicates, the earlier ones as PENDING plus the
// REQUEUED flag (Slurm sets a requeued job PENDING, and slurmdbd keeps the flag
// on the closed record).
func (p *Platform) accountingRecords(j *job, now time.Time, duplicates bool) []acctRecord {
	var recs []acctRecord
	for idx, e := range j.execs {
		latest := idx == len(j.execs)-1
		if !latest && !duplicates {
			continue
		}
		recs = append(recs, acctRecord{job: p.sacctJobOf(j, idx, now), submit: e.submit, end: e.end})
	}
	return recs
}

// sacctJobOf is the record of execution idx of j.
func (p *Platform) sacctJobOf(j *job, idx int, now time.Time) sacctJob {
	e := j.execs[idx]
	latest := idx == len(j.execs)-1
	current, state := []string{e.state}, e.state
	if !latest {
		current, state = []string{Pending, Requeued}, Requeued
	}
	var elapsed int64
	var planned NV
	if !e.start.IsZero() {
		stop := e.end
		if stop.IsZero() {
			stop = now
		}
		elapsed = int64(stop.Sub(e.start) / time.Second)
		planned = nv(int64(e.start.Sub(e.submit) / time.Second))
	}
	prio, held := nv(defaultPriority), false
	if latest && e.state == Pending && j.held {
		prio, held = nv(0), true
	}
	out := sacctJob{
		Account: j.account, DerivedExitCode: exitCodeFor(state), ExitCode: exitCodeFor(state),
		Flags: []string{}, JobID: j.id, Name: j.name, Partition: j.partition, Hold: held,
		Priority: prio, QOS: "normal", User: j.user,
		State: sacctState{Current: current, Reason: "None"},
		Time: sacctTime{
			Elapsed: elapsed, Eligible: epoch(e.submit), End: epoch(e.end), Planned: planned,
			Start: epoch(e.start), Submission: epoch(e.submit), Limit: limitNV(j.timeLimit),
		},
		Tres: sacctTres{Allocated: []tres{}, Requested: tresList(j)},
	}
	if !e.start.IsZero() {
		out.Tres.Allocated = tresList(j)
	}
	if p.cfg.DataParser == "v0.0.42" {
		out.RestartCnt = &idx
	}
	return out
}
