package slurm

import (
	"encoding/json"
	"fmt"
	"time"
)

// The JSON shapes follow Slurm's data_parser v0.0.40 (Slurm 23.11: Athena,
// Sano) and v0.0.42 (Slurm 24.11: Helios), read from
// src/plugins/data_parser/v0.0.4{0,2}/parsers.c:
//   - squeue (job_info): times are {set, infinite, number} objects, job_state
//     is a list of a base state plus flags, restart_cnt is an int.
//   - sacct (slurmdb job): times are plain epoch ints under "time",
//     the state is {current: [...], reason}, restart_cnt exists only in
//     v0.0.42, and comment is an object.
// Unverified against real output on our clusters (see the README).

// noVal is Slurm's {set, infinite, number} wrapper.
type noVal struct {
	Set      bool  `json:"set"`
	Infinite bool  `json:"infinite"`
	Number   int64 `json:"number"`
}

func (n *noVal) time() time.Time {
	if n == nil || !n.Set || n.Infinite || n.Number == 0 {
		return time.Time{}
	}
	return time.Unix(n.Number, 0).UTC()
}

// stateList accepts a list of strings or a single string.
type stateList []string

func (s *stateList) UnmarshalJSON(b []byte) error {
	var l []string
	if err := json.Unmarshal(b, &l); err == nil {
		*s = l
		return nil
	}
	var one string
	if err := json.Unmarshal(b, &one); err != nil {
		return fmt.Errorf("state: neither a list nor a string: %s", b)
	}
	*s = []string{one}
	return nil
}

type squeueResp struct {
	Jobs []struct {
		JobID      int64     `json:"job_id"`
		Name       string    `json:"name"`
		JobState   stateList `json:"job_state"`
		RestartCnt int       `json:"restart_cnt"`
		SubmitTime *noVal    `json:"submit_time"`
		StartTime  *noVal    `json:"start_time"`
		EndTime    *noVal    `json:"end_time"`
	} `json:"jobs"`
	Errors []json.RawMessage `json:"errors"`
}

// ParseSqueue parses squeue --json output.
func ParseSqueue(b []byte) ([]Job, error) {
	var r squeueResp
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("squeue --json: %w", err)
	}
	if r.Jobs == nil {
		return nil, fmt.Errorf("squeue --json: no jobs key in output")
	}
	if len(r.Errors) > 0 {
		return nil, fmt.Errorf("squeue --json: errors: %s", r.Errors[0])
	}
	out := make([]Job, 0, len(r.Jobs))
	for _, j := range r.Jobs {
		job := Job{
			ID: j.JobID, Name: j.Name, State: j.JobState, RestartCount: j.RestartCnt,
			Source: "squeue", Records: 1,
			Submit: j.SubmitTime.time(), Start: j.StartTime.time(), End: j.EndTime.time(),
		}
		job.Requeued = job.RestartCount > 0 || job.HasState("REQUEUED")
		out = append(out, job)
	}
	return out, nil
}

type sacctResp struct {
	Jobs []struct {
		JobID int64  `json:"job_id"`
		Name  string `json:"name"`
		State struct {
			Current stateList `json:"current"`
		} `json:"state"`
		RestartCnt *int `json:"restart_cnt"` // absent in v0.0.40
		Time       struct {
			Submission int64 `json:"submission"`
			Start      int64 `json:"start"`
			End        int64 `json:"end"`
		} `json:"time"`
	} `json:"jobs"`
	Errors []json.RawMessage `json:"errors"`
}

func epoch(s int64) time.Time {
	if s <= 0 {
		return time.Time{}
	}
	return time.Unix(s, 0).UTC()
}

// ParseSacct parses sacct --json output into one Job per record.
func ParseSacct(b []byte) ([]Job, error) {
	var r sacctResp
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("sacct --json: %w", err)
	}
	if r.Jobs == nil {
		return nil, fmt.Errorf("sacct --json: no jobs key in output")
	}
	if len(r.Errors) > 0 {
		return nil, fmt.Errorf("sacct --json: errors: %s", r.Errors[0])
	}
	out := make([]Job, 0, len(r.Jobs))
	for _, j := range r.Jobs {
		job := Job{
			ID: j.JobID, Name: j.Name, State: j.State.Current, Source: "sacct", Records: 1,
			Submit: epoch(j.Time.Submission), Start: epoch(j.Time.Start), End: epoch(j.Time.End),
		}
		if j.RestartCnt != nil {
			job.RestartCount = *j.RestartCnt
		}
		job.Requeued = job.RestartCount > 0 || job.HasState("REQUEUED")
		out = append(out, job)
	}
	return out, nil
}
