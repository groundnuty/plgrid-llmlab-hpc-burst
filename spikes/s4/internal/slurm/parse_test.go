package slurm

import (
	"testing"
	"time"
)

// Fixtures follow Slurm's data_parser field tables (parsers.c, v0.0.40 at
// slurm-23-11-7-1 and v0.0.42 at slurm-24-11-5-1), written by hand, not
// produced by this package or by the fake.

const squeue42 = `{"jobs":[{"job_id":1001,"name":"hb-a-1","comment":"","job_state":["PENDING","REQUEUED"],
"restart_cnt":1,"requeue":false,"submit_time":{"set":true,"infinite":false,"number":1761800000},
"start_time":{"set":false,"infinite":false,"number":0},"end_time":{"set":true,"infinite":false,"number":0}}],
"last_backfill":{"set":true,"infinite":false,"number":1761800000},"last_update":{"set":true,"infinite":false,"number":1761800000},
"meta":{},"errors":[],"warnings":[]}`

// v0.0.40 sacct: no restart_cnt; -D gives an earlier REQUEUED record.
const sacct40 = `{"jobs":[
{"job_id":1001,"name":"hb-a-1","comment":{"administrator":"","job":"","system":""},"state":{"current":["REQUEUED"],"reason":"None"},
 "time":{"submission":1761800000,"start":1761800010,"end":1761800050,"elapsed":40,"eligible":1761800000,"limit":{"set":true,"infinite":false,"number":60}}},
{"job_id":1001,"name":"hb-a-1","comment":{"administrator":"","job":"","system":""},"state":{"current":["COMPLETED"],"reason":"None"},
 "time":{"submission":1761800050,"start":1761800100,"end":1761800200,"elapsed":100,"eligible":1761800050,"limit":{"set":true,"infinite":false,"number":60}}},
{"job_id":1002,"name":"hb-b-2","comment":{"administrator":"","job":"","system":""},"state":{"current":["COMPLETED"],"reason":"None"},
 "time":{"submission":1761800000,"start":1761800001,"end":1761800002,"elapsed":1,"eligible":1761800000,"limit":{"set":true,"infinite":false,"number":60}}}],
"meta":{},"errors":[],"warnings":[]}`

func TestParseSqueueRequeued(t *testing.T) {
	jobs, err := ParseSqueue([]byte(squeue42))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs", len(jobs))
	}
	j := jobs[0]
	if j.ID != 1001 || !j.HasState("PENDING") || !j.Requeued || j.RestartCount != 1 {
		t.Errorf("got %+v", j)
	}
	if !j.Submit.Equal(time.Unix(1761800000, 0)) || !j.Start.IsZero() || !j.End.IsZero() {
		t.Errorf("times: submit %v start %v end %v", j.Submit, j.Start, j.End)
	}
}

func TestParseSacctV40MergesDuplicates(t *testing.T) {
	recs, err := ParseSacct([]byte(sacct40))
	if err != nil {
		t.Fatal(err)
	}
	jobs := mergeRecords(filter(recs, func(j Job) bool { return j.Name == "hb-a-1" }))
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs", len(jobs))
	}
	j := jobs[0]
	if j.ID != 1001 || !j.HasState("COMPLETED") || !j.Requeued || j.Records != 2 || j.RestartCount != 0 {
		t.Errorf("got %+v", j)
	}
	if !j.End.Equal(time.Unix(1761800200, 0)) {
		t.Errorf("end %v", j.End)
	}
}

func TestParseRejectsMissingJobs(t *testing.T) {
	if _, err := ParseSqueue([]byte(`{"errors":[]}`)); err == nil {
		t.Error("squeue output without jobs accepted")
	}
	if _, err := ParseSacct([]byte(`{"jobs":[],"errors":[{"error":"x"}]}`)); err == nil {
		t.Error("sacct output with errors accepted")
	}
}

func TestSubmitArgsNoRequeue(t *testing.T) {
	args := SubmitArgs(SubmitRequest{Tag: "hb-x"})
	want := map[string]bool{"--job-name=hb-x": true, "--no-requeue": true, "--parsable": true}
	for _, a := range args {
		delete(want, a)
	}
	if len(want) != 0 {
		t.Errorf("missing %v in %v", want, args)
	}
}

func TestRelStartIsRelative(t *testing.T) {
	if got := relStart(26 * time.Hour); got != "now-93600" {
		t.Errorf("got %q", got)
	}
}
