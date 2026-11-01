package slurm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The JSON of squeue and sacct, data_parser v0.0.40 (Slurm 23.11: Athena,
// Sano) and v0.0.42 (Slurm 24.11: Helios), HBEP-4's field tables:
//
//   - queue listing: job_id, name, job_state (a list: base state plus flags),
//     state_reason, hold, priority, restart_cnt, partition, time_limit,
//     tres_req_str, and submit_time, start_time and end_time as {set,
//     infinite, number} triples in epoch seconds;
//   - accounting: job_id, name, partition, state.current (a list) and
//     state.reason, time.submission, time.start and time.end in epoch
//     seconds, time.elapsed in seconds (HBEP-8), time.limit, tres.requested
//     and tres.allocated; restart_cnt only in v0.0.42, where v0.0.40 shows an
//     earlier execution as a record with the REQUEUED state.
//
// The v0.0.42 shapes are S7's real Helios captures (spikes/s7/
// slurm-json-real.md). The v0.0.40 shapes follow Slurm's source, as S4's fake
// does; to be safe the readers accept a number where a triple is expected and
// a string where a list is expected. Every reply's meta.plugin.data_parser,
// without its "data_parser/" prefix, must equal the profile's jsonDataParser,
// or the reply is not interpreted (provider.ErrDialectMismatch). Every epoch
// value becomes UTC; no text time is parsed.

// KnownDataParsers are the data_parser versions this plugin reads.
var KnownDataParsers = []string{"v0.0.40", "v0.0.42"}

// knownDataParser reports whether v is one of KnownDataParsers.
func knownDataParser(v string) bool {
	for _, k := range KnownDataParsers {
		if k == v {
			return true
		}
	}
	return false
}

// noVal is Slurm's {set, infinite, number} triple. A bare number reads as a set
// value.
type noVal struct {
	Set      bool
	Infinite bool
	Number   float64
}

func (n *noVal) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if bytes.Equal(b, []byte("null")) {
		*n = noVal{}
		return nil
	}
	if len(b) > 0 && b[0] != '{' {
		var f float64
		if err := json.Unmarshal(b, &f); err != nil {
			return fmt.Errorf("neither a number nor a {set, infinite, number} object")
		}
		*n = noVal{Set: true, Number: f}
		return nil
	}
	var o struct {
		Set      bool    `json:"set"`
		Infinite bool    `json:"infinite"`
		Number   float64 `json:"number"`
	}
	if err := json.Unmarshal(b, &o); err != nil {
		return err
	}
	*n = noVal{Set: o.Set, Infinite: o.Infinite, Number: o.Number}
	return nil
}

// value returns the number when it is set and finite.
func (n noVal) value() (float64, bool) {
	if !n.Set || n.Infinite {
		return 0, false
	}
	return n.Number, true
}

// epoch returns the triple as a UTC time; unset, infinite and zero are none.
func (n noVal) epoch() *time.Time {
	v, ok := n.value()
	if !ok || v <= 0 {
		return nil
	}
	t := time.Unix(int64(v), 0).UTC()
	return &t
}

// minutes returns a time limit in minutes as a duration; 0 when unset.
func (n noVal) minutes() time.Duration {
	v, ok := n.value()
	if !ok || v <= 0 {
		return 0
	}
	return time.Duration(v) * time.Minute
}

// stateList is a list of state words; a single string reads as a list of one.
type stateList []string

func (s *stateList) UnmarshalJSON(b []byte) error {
	var l []string
	if err := json.Unmarshal(b, &l); err == nil {
		*s = l
		return nil
	}
	var one string
	if err := json.Unmarshal(b, &one); err != nil {
		return fmt.Errorf("state: neither a list nor a string")
	}
	*s = []string{one}
	return nil
}

func (s stateList) upper() []string {
	out := make([]string, len(s))
	for i, w := range s {
		out[i] = strings.ToUpper(w)
	}
	return out
}

func (s stateList) has(word string) bool {
	for _, w := range s {
		if strings.EqualFold(w, word) {
			return true
		}
	}
	return false
}

type replyMeta struct {
	Plugin struct {
		DataParser string `json:"data_parser"`
	} `json:"plugin"`
}

// queueRecord is one job object of squeue --json.
type queueRecord struct {
	// RawID is job_id as read; JobID is set from it once parseQueue has
	// checked that it is there and positive.
	RawID       *int64    `json:"job_id"`
	JobID       int64     `json:"-"`
	Name        string    `json:"name"`
	JobState    stateList `json:"job_state"`
	StateReason string    `json:"state_reason"`
	Hold        bool      `json:"hold"`
	Priority    noVal     `json:"priority"`
	RestartCnt  int32     `json:"restart_cnt"`
	Partition   string    `json:"partition"`
	TimeLimit   noVal     `json:"time_limit"`
	TresReqStr  string    `json:"tres_req_str"`
	SubmitTime  noVal     `json:"submit_time"`
	StartTime   noVal     `json:"start_time"`
	EndTime     noVal     `json:"end_time"`
}

type queueReply struct {
	Jobs   *[]queueRecord    `json:"jobs"`
	Meta   replyMeta         `json:"meta"`
	Errors []json.RawMessage `json:"errors"`
}

type tresEntry struct {
	Type string `json:"type"`
	Name string `json:"name"`
	// Count is nil when the entry has no count or a null one: an unknown count,
	// which the allocated readers never take for a number. Some entries carry a
	// negative sentinel (v0.0.42's energy entry has -2).
	Count *int64 `json:"count"`
}

// accountingRecord is one record of sacct --json; with -D a job has one record
// per execution.
type accountingRecord struct {
	RawID     *int64 `json:"job_id"`
	JobID     int64  `json:"-"`
	Name      string `json:"name"`
	Partition string `json:"partition"`
	State     struct {
		Current stateList `json:"current"`
		Reason  string    `json:"reason"`
	} `json:"state"`
	Hold       bool   `json:"hold"`
	Priority   noVal  `json:"priority"`
	RestartCnt *int32 `json:"restart_cnt"`
	Time       struct {
		Submission noVal `json:"submission"`
		Start      noVal `json:"start"`
		End        noVal `json:"end"`
		Elapsed    noVal `json:"elapsed"`
		Limit      noVal `json:"limit"`
	} `json:"time"`
	Tres struct {
		Requested []tresEntry `json:"requested"`
		Allocated []tresEntry `json:"allocated"`
	} `json:"tres"`
}

type accountingReply struct {
	Jobs   *[]accountingRecord `json:"jobs"`
	Meta   replyMeta           `json:"meta"`
	Errors []json.RawMessage   `json:"errors"`
}

// DataParserOf returns a reply's data_parser version without its
// "data_parser/" prefix.
func DataParserOf(raw string) string { return strings.TrimPrefix(raw, "data_parser/") }

// checkReply checks the parts every reply shares: a jobs key, no errors, and
// the data_parser version. It returns the reply's version.
func checkReply(cmd string, hasJobs bool, meta replyMeta, errs []json.RawMessage, want string) (string, error) {
	got := DataParserOf(meta.Plugin.DataParser)
	if got != want {
		return got, fmt.Errorf("%s: %w: the reply's is %q, the profile's %q", cmd, provider.ErrDialectMismatch, got, want)
	}
	if !hasJobs {
		return got, fmt.Errorf("%s --json: the reply has no jobs key", cmd)
	}
	if len(errs) > 0 {
		return got, fmt.Errorf("%s --json: the reply lists %d error(s)", cmd, len(errs))
	}
	return got, nil
}

// parseQueue reads squeue --json. want is the profile's jsonDataParser.
func parseQueue(b []byte, want string) ([]queueRecord, string, error) {
	var r queueReply
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, "", fmt.Errorf("squeue --json: the reply is not the expected JSON: %w", err)
	}
	v, err := checkReply("squeue", r.Jobs != nil, r.Meta, r.Errors, want)
	if err != nil {
		return nil, v, err
	}
	recs := *r.Jobs
	for i := range recs {
		if recs[i].RawID == nil || *recs[i].RawID <= 0 {
			return nil, v, fmt.Errorf("squeue --json: record %d has no positive job_id", i)
		}
		recs[i].JobID = *recs[i].RawID
	}
	return recs, v, nil
}

// parseAccounting reads sacct --json.
func parseAccounting(b []byte, want string) ([]accountingRecord, string, error) {
	var r accountingReply
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, "", fmt.Errorf("sacct --json: the reply is not the expected JSON: %w", err)
	}
	v, err := checkReply("sacct", r.Jobs != nil, r.Meta, r.Errors, want)
	if err != nil {
		return nil, v, err
	}
	recs := *r.Jobs
	for i := range recs {
		if recs[i].RawID == nil || *recs[i].RawID <= 0 {
			return nil, v, fmt.Errorf("sacct --json: record %d has no positive job_id", i)
		}
		recs[i].JobID = *recs[i].RawID
	}
	return recs, v, nil
}

// heldBy reports the hold signals of HBEP-4 (The Slurm dialect, Held): hold
// true, a set priority of 0, or a state reason that starts with JobHeld.
func heldBy(hold bool, prio noVal, reason string) bool {
	p, ok := prio.value()
	return hold || (ok && p == 0) || strings.HasPrefix(reason, "JobHeld")
}

// gpusOfTres reads the GPU count from a TRES string such as
// "cpu=8,mem=16G,node=1,billing=8,gres/gpu=1" or "...,gres/gpu:a100=1". The
// untyped entry wins; without one, typed entries are summed.
func gpusOfTres(s string) int32 {
	var untyped, typed int64
	seen := false
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			continue
		}
		switch {
		case k == "gres/gpu":
			untyped, seen = n, true
		case strings.HasPrefix(k, "gres/gpu:"):
			typed += n
		}
	}
	if seen {
		return int32(untyped)
	}
	return int32(typed)
}

// gpusOfTresList reads the GPU count from accounting's tres lists, for the
// read-back of a job's GPU count: it compares a count that is reported, and a
// count that is missing or null adds 0, as it always did. The allocated count
// of a job's accounting is read with allocatedGPUsOfTresList instead.
func gpusOfTresList(l []tresEntry) (int32, bool) {
	var untyped, typed int64
	seen, any := false, false
	for _, e := range l {
		if e.Type != "gres" {
			continue
		}
		var n int64
		if e.Count != nil {
			n = *e.Count
		}
		switch {
		case e.Name == "gpu":
			untyped, seen, any = n, true, true
		case strings.HasPrefix(e.Name, "gpu:"):
			typed, any = typed+n, true
		}
	}
	if seen {
		return int32(untyped), true
	}
	return int32(typed), any
}

// tresCount returns the count of an allocated entry as an int32. ok is false
// when the count is missing, null, negative or above math.MaxInt32: a negative
// number is a sentinel (v0.0.42's energy entry has -2), not a count.
func tresCount(c *int64) (n int32, ok bool) {
	if c == nil || *c < 0 || *c > math.MaxInt32 {
		return 0, false
	}
	return int32(*c), true
}

// allocatedGPUsOfTresList reads the GPUs of a tres.allocated list: the gres
// entries named gpu (untyped) and gpu:<type> (typed). Only those entries are
// read; the others (energy, mem, billing) never make the count unknown.
//
// listed is true when the list has at least one such entry, readable or not.
// gpus is nil when the list has none, and when a count that the result depends
// on cannot be read (tresCount), never a total made of the counts that can:
//
//   - An untyped entry takes precedence, the typed entries being aliases of it.
//     If an untyped count cannot be read, or two untyped entries disagree, the
//     result is unknown: it does not fall back to the typed entries.
//   - Without an untyped entry, the typed counts are summed. If one cannot be
//     read, or the sum is above math.MaxInt32, the result is unknown.
func allocatedGPUsOfTresList(l []tresEntry) (gpus *int32, listed bool) {
	var (
		untyped, typedSum    int64
		untypedSeen          bool
		untypedBad, typedBad bool
	)
	for _, e := range l {
		if e.Type != "gres" {
			continue
		}
		switch {
		case e.Name == "gpu":
			listed = true
			n, ok := tresCount(e.Count)
			switch {
			case !ok:
				untypedBad = true
			case untypedSeen && int64(n) != untyped:
				untypedBad = true
			default:
				untyped = int64(n)
			}
			untypedSeen = true
		case strings.HasPrefix(e.Name, "gpu:"):
			listed = true
			if n, ok := tresCount(e.Count); ok {
				typedSum += int64(n)
			} else {
				typedBad = true
			}
		}
	}
	switch {
	case !listed:
		return nil, false
	case untypedSeen:
		if untypedBad {
			return nil, true
		}
		g := int32(untyped)
		return &g, true
	case typedBad || typedSum > math.MaxInt32:
		return nil, true
	}
	g := int32(typedSum)
	return &g, true
}

// isTerminalBase reports whether a base state is an end state.
func isTerminalBase(base string) bool {
	switch base {
	case "PENDING", "RUNNING", "SUSPENDED", "REQUEUED", "":
		return false
	}
	return true
}

// job turns a queue record into a job record. It fails when the phase cannot
// be told (an unknown base state): the read is then unknown.
func (q queueRecord) job(parser string) (provider.Job, error) {
	state := q.JobState.upper()
	requeued := q.RestartCnt > 0 || q.JobState.has("REQUEUED")
	base := ""
	if len(state) > 0 {
		base = state[0]
	}
	held := base == "PENDING" && heldBy(q.Hold, q.Priority, q.StateReason)
	phase, err := provider.SlurmPhase(state, held, requeued)
	if err != nil {
		return provider.Job{}, fmt.Errorf("squeue: job %d: %w", q.JobID, err)
	}
	j := provider.Job{
		ID:           strconv.FormatInt(q.JobID, 10),
		Name:         q.Name,
		State:        state,
		Phase:        phase,
		Hold:         provider.HoldNone,
		Reason:       q.StateReason,
		RestartCount: q.RestartCnt,
		Requeued:     requeued,
		Partition:    q.Partition,
		TimeLimit:    q.TimeLimit.minutes(),
		GPUs:         gpusOfTres(q.TresReqStr),
		Source:       provider.SourceQueue,
		DataParser:   parser,
	}
	if held {
		j.Hold = provider.HoldOf(true, q.StateReason)
	}
	if t := q.SubmitTime.epoch(); t != nil {
		j.SubmitTime = *t
	}
	start := q.StartTime.epoch()
	switch {
	case base == "PENDING" || base == "REQUEUED":
		// A pending job's start_time is the scheduler's estimate: advisory,
		// never planned on. A held job has none.
		if !held {
			j.StartEstimate = start
		}
	case base == "RUNNING" || base == "SUSPENDED":
		// While a job runs, end_time is its projected end, start plus the
		// time limit (S7's capture): the hard end, re-read on every observe.
		j.StartTime = start
		j.HardEnd = q.EndTime.epoch()
		if j.HardEnd == nil && start != nil && j.TimeLimit > 0 {
			h := start.Add(j.TimeLimit)
			j.HardEnd = &h
		}
	default:
		j.StartTime = start
		j.EndTime = q.EndTime.epoch()
		if start != nil && j.TimeLimit > 0 {
			h := start.Add(j.TimeLimit)
			j.HardEnd = &h
		}
		// An end state listed without an end time keeps EndTime nil, so the
		// job still counts as live for cancelling; scancel then reports it
		// already ended.
	}
	return j, nil
}

// mergeAccounting folds the -D records of each job ID into one job record,
// sorted by job ID. The latest execution (by submission time, then output
// order) gives the state; an earlier execution, a REQUEUED state or a restart
// count above 0 marks the job requeued.
func mergeAccounting(recs []accountingRecord, parser string) ([]provider.Job, error) {
	byID := map[int64][]accountingRecord{}
	var ids []int64
	for _, r := range recs {
		if _, ok := byID[r.JobID]; !ok {
			ids = append(ids, r.JobID)
		}
		byID[r.JobID] = append(byID[r.JobID], r)
	}
	sort.Slice(ids, func(i, k int) bool { return ids[i] < ids[k] })
	out := make([]provider.Job, 0, len(ids))
	var errs []error
	for _, id := range ids {
		rs := byID[id]
		sort.SliceStable(rs, func(i, k int) bool {
			a, _ := rs[i].Time.Submission.value()
			b, _ := rs[k].Time.Submission.value()
			return a < b
		})
		requeued := len(rs) > 1
		for _, r := range rs {
			if r.State.Current.has("REQUEUED") || (r.RestartCnt != nil && *r.RestartCnt > 0) {
				requeued = true
			}
		}
		j, err := rs[len(rs)-1].job(parser, requeued)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		j.Accounting = accountingOf(rs)
		out = append(out, j)
	}
	return out, errors.Join(errs...)
}

// allocatedCPUsOfTresList reads the CPUs of a tres.allocated list: the count of
// its first entry of type cpu. It is nil when the list has none, and when that
// count cannot be read (tresCount); a later cpu entry is not looked at then.
func allocatedCPUsOfTresList(l []tresEntry) *int32 {
	for _, e := range l {
		if e.Type != "cpu" {
			continue
		}
		if n, ok := tresCount(e.Count); ok {
			return &n
		}
		return nil
	}
	return nil
}

// accountingOf reads the allocated time of a job from its records, oldest
// first, one per execution (HBEP-8, Provider accounting). The allocated counts
// are the latest execution's; the elapsed time is time.elapsed summed over all
// the executions. It is unknown when an execution has none, and when the records
// are fewer than the executions the latest record's restart count says there
// were (v0.0.42): an earlier execution that the read did not return would
// otherwise be left out of the sum without a sign.
func accountingOf(rs []accountingRecord) *provider.JobAccounting {
	a := &provider.JobAccounting{Executions: len(rs)}
	latest := rs[len(rs)-1]
	a.AllocatedCPUs = allocatedCPUsOfTresList(latest.Tres.Allocated)
	a.AllocatedGPUs, a.GPUsListed = allocatedGPUsOfTresList(latest.Tres.Allocated)
	var total time.Duration
	for _, r := range rs {
		v, ok := r.Time.Elapsed.value()
		if !ok || v < 0 {
			return a
		}
		total += time.Duration(v) * time.Second
	}
	if latest.RestartCnt != nil && int(*latest.RestartCnt) >= len(rs) {
		return a
	}
	a.Elapsed = &total
	return a
}

// job turns the latest accounting record of a job into a job record.
func (r accountingRecord) job(parser string, requeued bool) (provider.Job, error) {
	state := r.State.Current.upper()
	base := ""
	if len(state) > 0 {
		base = state[0]
	}
	held := base == "PENDING" && heldBy(r.Hold, r.Priority, r.State.Reason)
	phase, err := provider.SlurmPhase(state, held, requeued)
	if err != nil {
		return provider.Job{}, fmt.Errorf("sacct: job %d: %w", r.JobID, err)
	}
	var restarts int32
	if r.RestartCnt != nil {
		restarts = *r.RestartCnt
	}
	j := provider.Job{
		ID:           strconv.FormatInt(r.JobID, 10),
		Name:         r.Name,
		State:        state,
		Phase:        phase,
		Hold:         provider.HoldNone,
		Reason:       r.State.Reason,
		RestartCount: restarts,
		Requeued:     requeued,
		Partition:    r.Partition,
		TimeLimit:    r.Time.Limit.minutes(),
		Source:       provider.SourceAccounting,
		DataParser:   parser,
	}
	if g, ok := gpusOfTresList(r.Tres.Requested); ok {
		j.GPUs = g
	} else if g, ok := gpusOfTresList(r.Tres.Allocated); ok {
		j.GPUs = g
	}
	if held {
		j.Hold = provider.HoldOf(true, r.State.Reason)
	}
	if t := r.Time.Submission.epoch(); t != nil {
		j.SubmitTime = *t
	}
	j.StartTime = r.Time.Start.epoch()
	if j.StartTime != nil && j.TimeLimit > 0 {
		h := j.StartTime.Add(j.TimeLimit)
		j.HardEnd = &h
	}
	if isTerminalBase(base) {
		j.EndTime = r.Time.End.epoch()
	}
	return j, nil
}

// sortJobs sorts by job ID, numerically, never by output order.
func sortJobs(jobs []provider.Job) {
	sort.SliceStable(jobs, func(i, k int) bool {
		a, errA := strconv.ParseInt(jobs[i].ID, 10, 64)
		b, errB := strconv.ParseInt(jobs[k].ID, 10, 64)
		if errA != nil || errB != nil {
			return jobs[i].ID < jobs[k].ID
		}
		return a < b
	})
}

// notListed reports whether a failed squeue for one job ID is Slurm's "not
// listed" answer: exit 1 with "Invalid job id specified" (S7's capture). An
// empty jobs list with exit 0 is the other shape, read by the caller.
func notListed(res Result) bool {
	return res.ExitCode == 1 && strings.Contains(string(res.Stderr), "Invalid job id specified")
}
