// Package slurm is the spike's Slurm command-line client: sbatch, squeue
// --json, sacct --json and scancel, run through a Runner. The spike runs
// them locally against a fake on PATH; a real provider would run the same
// commands over one multiplexed ssh connection (HBEP-4).
package slurm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Runner runs one command and returns its stdout and stderr.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) (stdout, stderr []byte, err error)
}

// ExecRunner runs commands as local processes. Env, when non-nil, replaces the
// child's environment.
type ExecRunner struct {
	Env []string
}

// Run implements Runner.
func (r ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if r.Env != nil {
		cmd.Env = r.Env
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	return out.Bytes(), errb.Bytes(), err
}

// Client issues Slurm commands.
type Client struct {
	R Runner
}

// SubmitRequest is one sbatch submission.
type SubmitRequest struct {
	Tag       string // goes into --job-name; the only lookup key
	Partition string
	Account   string
	TimeLimit string
	Wrap      string
	Comment   string // informational only: slurmdbd keeps it only with AccountingStoreFlags=job_comment
}

// SubmitArgs returns the sbatch arguments for r. Always --no-requeue (HBEP-7
// candidate; S5 found JobRequeue=1 on every platform).
func SubmitArgs(r SubmitRequest) []string {
	args := []string{"--parsable", "--job-name=" + r.Tag, "--no-requeue"}
	if r.Partition != "" {
		args = append(args, "--partition="+r.Partition)
	}
	if r.Account != "" {
		args = append(args, "--account="+r.Account)
	}
	if r.TimeLimit != "" {
		args = append(args, "--time="+r.TimeLimit)
	}
	if r.Comment != "" {
		args = append(args, "--comment="+r.Comment)
	}
	wrap := r.Wrap
	if wrap == "" {
		wrap = "sleep infinity"
	}
	return append(args, "--wrap="+wrap)
}

// Submit runs sbatch and returns the job ID.
func (c *Client) Submit(ctx context.Context, r SubmitRequest) (int64, error) {
	if r.Tag == "" {
		return 0, errors.New("sbatch: refusing to submit without a tag")
	}
	out, errb, err := c.R.Run(ctx, "sbatch", SubmitArgs(r)...)
	if err != nil {
		return 0, fmt.Errorf("sbatch: %w: %s", err, strings.TrimSpace(string(errb)))
	}
	// --parsable prints "<jobid>[;<cluster>]".
	first := strings.TrimSpace(strings.SplitN(string(out), ";", 2)[0])
	id, perr := strconv.ParseInt(first, 10, 64)
	if perr != nil || id <= 0 {
		return 0, fmt.Errorf("sbatch: cannot parse job id from %q", strings.TrimSpace(string(out)))
	}
	return id, nil
}

// Cancel runs scancel. A job that already ended or is unknown counts as
// cancelled, so Delete is idempotent.
func (c *Client) Cancel(ctx context.Context, id int64) error {
	_, errb, err := c.R.Run(ctx, "scancel", strconv.FormatInt(id, 10))
	if err == nil {
		return nil
	}
	msg := string(errb)
	if strings.Contains(msg, "already completing or completed") || strings.Contains(msg, "Invalid job id") {
		return nil
	}
	return fmt.Errorf("scancel %d: %w: %s", id, err, strings.TrimSpace(msg))
}

// Job is one Slurm job as squeue or sacct reports it.
type Job struct {
	ID           int64
	Name         string
	State        []string // job_state (squeue) or state.current (sacct) of the latest record
	RestartCount int
	Requeued     bool // restart count > 0, a REQUEUED state flag, or an earlier REQUEUED record
	Records      int  // sacct records for this job ID (with -D)
	Source       string
	Submit       time.Time
	Start        time.Time
	End          time.Time
}

// HasState reports whether the job's state list contains s.
func (j Job) HasState(s string) bool {
	for _, x := range j.State {
		if x == s {
			return true
		}
	}
	return false
}

// SqueueByTag lists the caller's jobs whose name equals tag. The name filter
// is also applied client-side, because squeue --json documents only that
// "most filtering arguments are still used".
func (c *Client) SqueueByTag(ctx context.Context, tag string) ([]Job, error) {
	return c.squeue(ctx, func(j Job) bool { return j.Name == tag }, "--me", "--json", "--name="+tag)
}

// SqueueByID lists one job by ID.
func (c *Client) SqueueByID(ctx context.Context, id int64) ([]Job, error) {
	return c.squeue(ctx, func(j Job) bool { return j.ID == id }, "--me", "--json", "--jobs="+strconv.FormatInt(id, 10))
}

func (c *Client) squeue(ctx context.Context, keep func(Job) bool, args ...string) ([]Job, error) {
	out, errb, err := c.R.Run(ctx, "squeue", args...)
	if err != nil {
		// An unknown job ID makes real squeue exit non-zero; callers then
		// fall back to sacct, so report "none" only for that message.
		if strings.Contains(string(errb), "Invalid job id") {
			return nil, nil
		}
		return nil, fmt.Errorf("squeue: %w: %s", err, strings.TrimSpace(string(errb)))
	}
	jobs, err := ParseSqueue(out)
	if err != nil {
		return nil, err
	}
	return filter(jobs, keep), nil
}

// SacctByTag lists accounting records whose name equals tag, starting
// `since` ago. It always passes -S: sacct's default start is midnight of the
// current day, which would miss a job submitted the day before. The relative
// form avoids the login session's time zone.
func (c *Client) SacctByTag(ctx context.Context, tag string, since time.Duration) ([]Job, error) {
	return c.sacct(ctx, func(j Job) bool { return j.Name == tag },
		"--json", "-X", "-D", "--name="+tag, "-S", relStart(since), "-E", "now")
}

// SacctByID lists accounting records of one job ID (-j makes the default
// start the epoch).
func (c *Client) SacctByID(ctx context.Context, id int64) ([]Job, error) {
	return c.sacct(ctx, func(j Job) bool { return j.ID == id },
		"--json", "-X", "-D", "-j", strconv.FormatInt(id, 10))
}

func relStart(since time.Duration) string {
	s := int64(since / time.Second)
	if s < 60 {
		s = 60
	}
	return "now-" + strconv.FormatInt(s, 10)
}

func (c *Client) sacct(ctx context.Context, keep func(Job) bool, args ...string) ([]Job, error) {
	out, errb, err := c.R.Run(ctx, "sacct", args...)
	if err != nil {
		return nil, fmt.Errorf("sacct: %w: %s", err, strings.TrimSpace(string(errb)))
	}
	recs, err := ParseSacct(out)
	if err != nil {
		return nil, err
	}
	return mergeRecords(filter(recs, keep)), nil
}

// mergeRecords folds the -D records of each job ID into one Job: the latest
// record's state, with Requeued set when an earlier record exists.
func mergeRecords(recs []Job) []Job {
	byID := map[int64][]Job{}
	var ids []int64
	for _, r := range recs {
		if _, ok := byID[r.ID]; !ok {
			ids = append(ids, r.ID)
		}
		byID[r.ID] = append(byID[r.ID], r)
	}
	sort.Slice(ids, func(i, k int) bool { return ids[i] < ids[k] })
	out := make([]Job, 0, len(ids))
	for _, id := range ids {
		rs := byID[id]
		sort.SliceStable(rs, func(i, k int) bool { return rs[i].Submit.Before(rs[k].Submit) })
		j := rs[len(rs)-1]
		j.Records = len(rs)
		for _, r := range rs {
			if r.Requeued || r.HasState("REQUEUED") {
				j.Requeued = true
			}
		}
		if len(rs) > 1 {
			j.Requeued = true
		}
		out = append(out, j)
	}
	return out
}

func filter(jobs []Job, keep func(Job) bool) []Job {
	var out []Job
	for _, j := range jobs {
		if keep(j) {
			out = append(out, j)
		}
	}
	return out
}
