package fakeslurm

import (
	"time"
)

var sacctDefs = []optDef{
	{name: "json"}, {name: "yaml"}, {name: "me"},
	{name: "allocations", short: 'X'},
	{name: "duplicates", short: 'D'},
	{name: "allusers", short: 'a'},
	{name: "name", short: 'n', value: valSoft},
	{name: "jobs", short: 'j', value: valRequired},
	{name: "user", short: 'u', value: valRequired},
	{name: "starttime", short: 'S', value: valRequired},
	{name: "endtime", short: 'E', value: valRequired},
	{name: "format", short: 'o', value: valRequired},
	{name: "state", short: 's', value: valRequired},
	{name: "accounts", short: 'A', value: valRequired},
	{name: "partition", short: 'r', value: valRequired},
	{name: "nodelist", short: 'N', value: valRequired},
	{name: "clusters", short: 'M', value: valRequired},
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

type sacctTime struct {
	Submission int64 `json:"submission"`
	Start      int64 `json:"start"`
	End        int64 `json:"end"`
	Elapsed    int64 `json:"elapsed"`
	Eligible   int64 `json:"eligible"`
	Limit      NV    `json:"limit"`
}

type sacctJob struct {
	JobID      int          `json:"job_id"`
	Name       string       `json:"name"`
	User       string       `json:"user"`
	Partition  string       `json:"partition"`
	Account    string       `json:"account"`
	Comment    sacctComment `json:"comment"`
	State      sacctState   `json:"state"`
	Time       sacctTime    `json:"time"`
	ExitCode   exitCode     `json:"exit_code"`
	Flags      []string     `json:"flags"`
	RestartCnt *int         `json:"restart_cnt,omitempty"` // v0.0.42 only
}

type sacctResp struct {
	Jobs     []sacctJob `json:"jobs"`
	Meta     meta       `json:"meta"`
	Errors   []any      `json:"errors"`
	Warnings []any      `json:"warnings"`
}

func (a *app) sacct(args []string) int {
	return a.run(func(t *tx) result {
		fail := func(msg string) result {
			t.emit("sacct", "sacct", 0, intp(1))
			return result{errOut: "sacct: error: " + msg + "\n", exit: 1}
		}
		p, err := parseOpts(args, sacctDefs)
		if err != nil {
			return fail(err.Error())
		}
		if p.has("yaml") || !p.has("json") {
			return fail("the fake sacct supports only --json")
		}
		f, err := newJobFilter(p, a.cfg)
		if err != nil {
			return fail(err.Error())
		}
		now := a.env.Now()
		from, to, err := a.sacctWindow(p, now)
		if err != nil {
			return fail(err.Error())
		}
		asOf := now.Add(-a.cfg.AcctDelay)
		resp := sacctResp{Jobs: []sacctJob{}, Meta: newMeta(a.cfg.DataParser), Errors: []any{}, Warnings: []any{}}
		for _, j := range t.st.Jobs {
			if !f.match(j) {
				continue
			}
			for _, rec := range a.accountingRecords(j, asOf, p.has("duplicates")) {
				// Window: submitted by E and not ended before S.
				if rec.submit.After(to) || (!rec.end.IsZero() && rec.end.Before(from)) {
					continue
				}
				resp.Jobs = append(resp.Jobs, rec.job)
			}
		}
		out, err := marshalOut(resp)
		if err != nil {
			return fail(err.Error())
		}
		t.emit("sacct", "sacct", 0, intp(0))
		return result{out: out}
	})
}

// sacctWindow resolves -S and -E. The default start is midnight today in the
// TZ zone, or the epoch when -j is given (sacct man page); the default end
// is now.
func (a *app) sacctWindow(p parsed, now time.Time) (from, to time.Time, err error) {
	from = midnight(now, a.cfg.Zone)
	if p.has("jobs") {
		from = time.Unix(0, 0)
	}
	to = now
	if v, ok := p.get("starttime"); ok {
		if from, err = parseSlurmTime(v, now, a.cfg.Zone); err != nil {
			return
		}
	}
	if v, ok := p.get("endtime"); ok {
		if to, err = parseSlurmTime(v, now, a.cfg.Zone); err != nil {
			return
		}
	}
	return
}

type acctRecord struct {
	job         sacctJob
	submit, end time.Time
}

// accountingRecords is slurmdbd's view of j as of asOf: the executions
// submitted by then, the latest one in the state it had at asOf and, with
// duplicates, the earlier ones as REQUEUED.
func (a *app) accountingRecords(j *Job, asOf time.Time, duplicates bool) []acctRecord {
	var visible []*Exec
	for _, e := range j.Execs {
		if !e.Submit.After(asOf) {
			visible = append(visible, e)
		}
	}
	var recs []acctRecord
	for idx, e := range visible {
		latest := idx == len(visible)-1
		if !latest && !duplicates {
			continue
		}
		state := Requeued
		if latest {
			state = e.StateAt(asOf)
		}
		start, end := e.StartAt(asOf), e.EndAt(asOf)
		var elapsed int64
		switch {
		case start.IsZero():
		case !end.IsZero():
			elapsed = int64(end.Sub(start) / time.Second)
		default:
			elapsed = int64(asOf.Sub(start) / time.Second)
		}
		job := sacctJob{
			JobID: j.ID, Name: j.Name, User: j.User, Partition: j.Partition, Account: j.Account,
			Comment: sacctComment{},
			State:   sacctState{Current: []string{state}, Reason: "None"},
			Time: sacctTime{
				Submission: epoch(e.Submit), Start: epoch(start), End: epoch(end),
				Elapsed: elapsed, Eligible: epoch(e.Submit), Limit: limitNV(j.TimeLimit),
			},
			ExitCode: exitCodeFor(state), Flags: []string{},
		}
		if a.cfg.StoreComment {
			job.Comment.Job = j.Comment
		}
		if a.cfg.DataParser == "v0.0.42" {
			job.RestartCnt = intp(idx)
		}
		recs = append(recs, acctRecord{job: job, submit: e.Submit, end: end})
	}
	return recs
}
