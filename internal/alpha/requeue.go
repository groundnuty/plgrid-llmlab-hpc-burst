package alpha

// Row A's requeue (alpha-athena-now-serve, issue #376, Q5): the scontrol line that requeues the run's own job, and what
// the scheduler's answer to it is. A refusal is NOT RUN, never a pass, and the post says why in the scheduler's own words,
// with every job ID and path in them replaced (k343's ruling of 11 Nov 2026, on the review of PR #400): the post carries
// no job ID and no path, the private log keeps the raw text.

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ErrRequeueRefused is what row A's scenario returns, beside what it saw, when the scheduler refused the requeue of the
// run's own job (Q5): the requeue was never put to the test. Run judges the rest of the run, and a failure there wins;
// with none, the run is NOT RUN (NotRun), its record and text to post written with the reason, never a pass.
var ErrRequeueRefused = errors.New("the scheduler refused the requeue of the run's own job")

// RequeueRefusedError is ErrRequeueRefused with the reason, the scheduler's words as RequeueReplyOf makes them postable.
type RequeueRefusedError struct{ Reason string }

func (e *RequeueRefusedError) Error() string {
	return fmt.Sprintf("%v (%s)", ErrRequeueRefused, e.Reason)
}

// Is makes the error ErrRequeueRefused.
func (e *RequeueRefusedError) Is(target error) bool { return target == ErrRequeueRefused }

// refusalUnknown is the reason of a refusal that carries none, the bare ErrRequeueRefused.
const refusalUnknown = "no words of the scheduler's were recorded; its text, if any, is in the private log"

// refusalOf is the reason a refusal gives.
func refusalOf(err error) string {
	var r *RequeueRefusedError
	if errors.As(err, &r) {
		return r.Reason
	}
	return refusalUnknown
}

// RequeueHoldLine is the scontrol line that requeues the run's own job and holds it (Q5: requeuehold, not a plain
// requeue, which would leave the job pending without a hold, free to run a second time in the slot). jobID must be
// digits only: it is checked to be the run's by its ID and tag first (ourJob).
func RequeueHoldLine(jobID string) (string, error) {
	if _, err := strconv.ParseUint(jobID, 10, 64); err != nil || strings.TrimLeft(jobID, "0123456789") != "" {
		return "", errors.New("a job ID must be digits only")
	}
	return "scontrol requeuehold " + jobID, nil
}

// RequeueReply is what the scontrol call of the requeue said: accepted (exit 0), or refused, with the scheduler's words
// made postable (refusalWords).
type RequeueReply struct {
	Accepted bool
	Refusal  string
}

// RequeueReplyOf reads the call's exit code and its output; jobID is the run's own job, the one the line requeued. A
// transport failure is not a reply at all (the caller's error, a failure of the run): only an answer of the scheduler's is
// a refusal.
func RequeueReplyOf(exit int, stdout, stderr []byte, jobID string) RequeueReply {
	if exit == 0 {
		return RequeueReply{Accepted: true}
	}
	return RequeueReply{Refusal: refusalWords(exit, stdout, stderr, jobID)}
}

// The tokens that stand where a job ID or a path was.
const (
	jobToken  = "[job]"
	pathToken = "[path]"
)

// refusalMaxBytes caps the scheduler's words in a post.
const refusalMaxBytes = 200

// jobIDPattern is the job-ID form of PR #385's content scan, copied byte for byte from profileJobID in
// internal/slurm/shipped_profiles_content_test.go:31, which is in a test file of package slurm and cannot be imported
// (TestTheRefusalsJobIDFormIsTheContentScans compares the two). It takes "job", "job id", "jobid", "job_id" and "JobId"
// (also after "SLURM_") and a number of five or more digits after a space, a colon, an equals sign or a number sign, and
// "-j", "--job", "--jobs" or "--jobid" and such a number: scontrol's JobId=, the JSON's job_id, sacct's -j, and the prose
// "for job 1234567".
const jobIDPattern = `(?i:(?:\b|_)job(?:[ _-]?id)?\b[ :=#]+[0-9]{5,}\b)|(?:^|\s)-j[ =]*[0-9]{5,}\b|(?:^|\s)--job(?:id|s)?[ =]+[0-9]{5,}\b`

var (
	jobIDForm = regexp.MustCompile(jobIDPattern)
	// jobIDDigits is the number inside a match of jobIDForm, the only part replaced, so that "JobId=" and "job" stay.
	jobIDDigits = regexp.MustCompile(`[0-9]{5,}`)
	// absPath is an absolute path: a slash at the start or after a space or a punctuation mark, and what follows up to a
	// space, a quote, a bracket or a separator (a colon too, as in "cannot open <path>: Permission denied"). A slash inside
	// a word ("Access/permission denied") is no path.
	absPath = regexp.MustCompile(`(^|[\s=:'"\[<,])/[^\s'"()\[\]<>{},;:]*`)
	// spaces are the runs of white space the words are collapsed to one line at.
	spaces = regexp.MustCompile(`\s+`)
)

// refusalWords is the reason a refusal posts: the scheduler's own words, its standard error, else its standard output,
// as one line, with every job ID in them replaced by [job] (jobIDForm's number, and the run's own job ID wherever it
// appears, a digit run of any length) and every absolute path by [path], then cut to refusalMaxBytes. The marker's note
// writes the reason in parentheses, which hack/live-checks.sh reads up to the first one (row_a_reason), so parentheses in
// the words become brackets. With no words at all, it says so, with the exit code. The raw text is the private log's.
func refusalWords(exit int, stdout, stderr []byte, jobID string) string {
	text := strings.TrimSpace(string(stderr))
	if text == "" {
		text = strings.TrimSpace(string(stdout))
	}
	text = strings.Map(func(r rune) rune {
		switch {
		case r == '(':
			return '['
		case r == ')':
			return ']'
		case unicode.IsControl(r) || r == unicode.ReplacementChar:
			return ' '
		}
		return r
	}, text)
	text = strings.TrimSpace(spaces.ReplaceAllString(text, " "))
	if text == "" {
		return fmt.Sprintf("scontrol exited %d with no words", exit)
	}
	text = jobIDForm.ReplaceAllStringFunc(text, func(m string) string { return jobIDDigits.ReplaceAllString(m, jobToken) })
	if jobID != "" {
		text = strings.ReplaceAll(text, jobID, jobToken)
	}
	text = absPath.ReplaceAllString(text, "${1}"+pathToken)
	if len(text) > refusalMaxBytes {
		cut := refusalMaxBytes
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		text = text[:cut] + "..."
	}
	return "the scheduler's words: " + text
}
