package alpha

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The run's own job ID in these tests; the #385 cases name another number, 1234567, so that what strips those is the
// content scan's form and not the run's own ID.
const testOwnJobID = "7654321"

// refusal is the posted reason of a refusal whose standard error is text.
func refusal(text, jobID string) string { return RequeueReplyOf(1, nil, []byte(text), jobID).Refusal }

// The scheduler's reply: exit 0 is an accepted requeue whatever it printed; any other is a refusal that keeps the
// scheduler's words with every job ID in them replaced: the forms PR #385's content scan finds (its positive cases,
// internal/slurm/shipped_profiles_content_test.go:82-91), each with only its number replaced, so that the words stay.
func TestARequeueRefusalKeepsTheSchedulersWordsWithNoJobID(t *testing.T) {
	if r := RequeueReplyOf(0, []byte("JobId=1234567 requeued"), nil, testOwnJobID); !r.Accepted || r.Refusal != "" {
		t.Errorf("exit 0 read as %+v", r)
	}
	for _, c := range []struct{ text, want string }{
		{"from job 1234567 of the run", "from job [job] of the run"},
		{"Submitted batch job 1234567", "Submitted batch job [job]"},
		{"JobId=1234567 JobName=x", "JobId=[job] JobName=x"},
		{"job_id: 1234567", "job_id: [job]"},
		{"jobid 1234567", "jobid [job]"},
		{"sacct -j 1234567", "sacct -j [job]"},
		{"sacct -j1234567", "sacct -j[job]"},
		{"squeue --jobs=1234567", "squeue --jobs=[job]"},
		{"SLURM_JOB_ID=1234567", "SLURM_JOB_ID=[job]"},
		// Slurm's own refusals of a requeue, as scontrol words them.
		{"scontrol: error: Requested operation is presently disabled for job 1234567", "scontrol: error: Requested operation is presently disabled for job [job]"},
		{"scontrol: error: Access/permission denied for job 1234567", "scontrol: error: Access/permission denied for job [job]"},
		{"slurm_requeue error for JobId=1234567: Job is pending execution", "slurm_requeue error for JobId=[job]: Job is pending execution"},
	} {
		r := RequeueReplyOf(1, nil, []byte(c.text), testOwnJobID)
		if r.Accepted || r.Refusal != "the scheduler's words: "+c.want {
			t.Errorf("%q posted as %+v, want the words %q", c.text, r, c.want)
		}
		if strings.Contains(r.Refusal, "1234567") {
			t.Errorf("%q kept its job ID: %q", c.text, r.Refusal)
		}
	}
}

// The content scan's negative cases (shipped_profiles_content_test.go:95-99) are no job ID of its form: short numbers,
// words that hold "job", a job-name rule and ssh's -J are posted as they are.
func TestARequeueRefusalLeavesWhatIsNoJobIDAsItIs(t *testing.T) {
	for _, text := range []string{
		"job 1234 is not an ID",
		"JobId=1234 job_id: 12 jobid 99 -j 5 SLURM_JOB_ID=4321",
		"subjob 1234567 and jobs 1234567",
		"jobName: maxLength 56 job_name_56_characters_unchanged",
		"ssh -J gateway 1234567",
	} {
		if got := refusal(text, testOwnJobID); got != "the scheduler's words: "+text {
			t.Errorf("%q posted as %q, want it unchanged", text, got)
		}
	}
}

// The run's own job ID goes wherever it stands, bare or in a word, whatever the words around it: the job requeued is the
// one the line named.
func TestARequeueRefusalNamesNotTheRunsOwnJob(t *testing.T) {
	for _, c := range []struct{ text, want string }{
		{"Requeue of " + testOwnJobID + " refused", "Requeue of [job] refused"},
		{"array task " + testOwnJobID + "_0 is not requeueable", "array task [job]_0 is not requeueable"},
		{"error " + testOwnJobID + ": Invalid job id specified", "error [job]: Invalid job id specified"},
	} {
		if got := refusal(c.text, testOwnJobID); got != "the scheduler's words: "+c.want {
			t.Errorf("%q posted as %q, want %q", c.text, got, c.want)
		}
	}
}

// Every absolute path goes, as a post carries none; a slash inside a word is no path. The words are one line, of standard
// error or else standard output, with brackets for parentheses (hack/live-checks.sh's row_a_reason reads the note's reason
// up to the first parenthesis), cut to refusalMaxBytes; a refusal with no words says so.
func TestARequeueRefusalIsOneLineWithNoPath(t *testing.T) {
	for _, c := range []struct{ stdout, stderr, want string }{
		{"", "scontrol: error: cannot open /scratch/people/example-user/hpc-burst/alloc/hb-x/job.out: Permission denied", "scontrol: error: cannot open [path]: Permission denied"},
		{"", "StdOut=/net/home/example-user/job.out StdErr='/tmp/x y'", "StdOut=[path] StdErr='[path] y'"},
		{"", "Access/permission denied", "Access/permission denied"},
		{"", "slurm_requeue error: Job is not requeueable (job 1234567)\n  second line\tend", "slurm_requeue error: Job is not requeueable [job [job]] second line end"},
		{"Requested operation is presently disabled for job 1234567\n", "", "Requested operation is presently disabled for job [job]"},
		{"stdout words", "stderr words", "stderr words"},
	} {
		got := RequeueReplyOf(1, []byte(c.stdout), []byte(c.stderr), testOwnJobID).Refusal
		if got != "the scheduler's words: "+c.want {
			t.Errorf("%q / %q posted as %q, want %q", c.stdout, c.stderr, got, c.want)
		}
		if strings.ContainsAny(got, "()\n") {
			t.Errorf("%q holds a parenthesis or a newline", got)
		}
	}
	if got := RequeueReplyOf(2, []byte(" \n"), nil, testOwnJobID).Refusal; got != "scontrol exited 2 with no words" {
		t.Errorf("a refusal with no words posted as %q", got)
	}
	long := refusal(strings.Repeat("word ", 100), testOwnJobID)
	if words := strings.TrimPrefix(long, "the scheduler's words: "); len(words) > refusalMaxBytes+len("...") || !strings.HasSuffix(words, "...") {
		t.Errorf("a long refusal is not cut to %d bytes: %d bytes, %q", refusalMaxBytes, len(words), long)
	}
}

// The job-ID form is PR #385's: jobIDPattern is byte for byte the literal of profileJobID at
// internal/slurm/shipped_profiles_content_test.go:31, read from that file as Go source, so the two cannot drift and the
// comment's cite holds.
func TestTheRefusalsJobIDFormIsTheContentScans(t *testing.T) {
	name := filepath.Join("..", "slurm", "shipped_profiles_content_test.go")
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var lit *ast.BasicLit
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok || len(vs.Names) != 1 || vs.Names[0].Name != "profileJobID" || len(vs.Values) != 1 {
			return true
		}
		if call, ok := vs.Values[0].(*ast.CallExpr); ok && len(call.Args) == 1 {
			lit, _ = call.Args[0].(*ast.BasicLit)
		}
		return false
	})
	if lit == nil || lit.Kind != token.STRING {
		t.Fatalf("%s holds no profileJobID = regexp.MustCompile(<string literal>)", name)
	}
	src, err := strconv.Unquote(lit.Value)
	if err != nil {
		t.Fatal(err)
	}
	if src != jobIDPattern || jobIDForm.String() != src {
		t.Errorf("the refusal's job-ID form is not the content scan's:\n got %s\nwant %s", jobIDPattern, src)
	}
	if line := fset.Position(lit.Pos()).Line; line != 31 {
		t.Errorf("profileJobID is at line %d of %s, not the :31 that jobIDPattern's comment cites", line, name)
	}
}
