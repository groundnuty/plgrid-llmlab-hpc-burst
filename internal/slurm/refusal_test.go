package slurm

// Reading sbatch's accounting-policy refusal (HBEP-4, QOS limits, A limit
// refusal). The texts are written out by hand from the HBEP: the reason line
// comes first, then the accounting-policy line. A fixture that carries only the
// policy line proves nothing about the classification, so every kind of
// refusal is in one table.

import (
	"errors"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

const policyText = "sbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n"

func TestClassifySubmitPolicyRefusals(t *testing.T) {
	for _, c := range []struct {
		name   string
		stderr string
		reason string // "" when the refusal is no PolicyRefusal
		limit  bool
		text   string // wanted in the error's text
	}{
		{"a full per-user QOS limit", "sbatch: error: QOSMaxSubmitJobPerUserLimit\n" + policyText, "QOSMaxSubmitJobPerUserLimit", true, "sbatch: error: QOSMaxSubmitJobPerUserLimit"},
		{"a full association limit", "sbatch: error: AssocMaxSubmitJobLimit\n" + policyText, "AssocMaxSubmitJobLimit", true, "sbatch: error: AssocMaxSubmitJobLimit"},
		{"a full account limit", "sbatch: error: MaxSubmitJobsPerAccount\n" + policyText, "MaxSubmitJobsPerAccount", true, "sbatch: error: MaxSubmitJobsPerAccount"},
		// The group limits are full slots too, shared by the grant's users
		// (`@hpc-burst-science-agent`, 1 Nov 2026, N1): the code is the same,
		// and the error's text says whose limit it is.
		{"a full QOS group limit", "sbatch: error: QOSGrpSubmitJobsLimit\n" + policyText, "QOSGrpSubmitJobsLimit", true, "sbatch: error: QOSGrpSubmitJobsLimit (group limit, shared by the grant's users)"},
		{"a full association group limit", "sbatch: error: AssocGrpSubmitJobsLimit\n" + policyText, "AssocGrpSubmitJobsLimit", true, "sbatch: error: AssocGrpSubmitJobsLimit (group limit, shared by the grant's users)"},
		{"a stand-alone breach is another reason", "sbatch: error: QOSMaxWallDurationPerJobLimit\n" + policyText, "QOSMaxWallDurationPerJobLimit", false, "sbatch: error: QOSMaxWallDurationPerJobLimit"},
		{"another reason that is no submit limit", "sbatch: error: QOSMaxGRESPerUser\n" + policyText, "QOSMaxGRESPerUser", false, "sbatch: error: QOSMaxGRESPerUser"},
		{"carriage returns, as an ssh pty may add", "sbatch: error: QOSMaxSubmitJobPerUserLimit\r\n" + strings.ReplaceAll(policyText, "\n", "\r\n"), "QOSMaxSubmitJobPerUserLimit", true, "sbatch: error: QOSMaxSubmitJobPerUserLimit"},
		{"a banner line before the two", "Welcome to example\nsbatch: error: QOSMaxSubmitJobPerUserLimit\n" + policyText, "QOSMaxSubmitJobPerUserLimit", true, ""},
		// Not policy refusals: ordinary Rejected outcomes with Slurm's text.
		{"the policy line alone has no reason line", policyText, "", false, "Job violates accounting/QOS policy"},
		{"the policy line without the sbatch prefix", "Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)\n", "", false, "Job violates accounting/QOS policy"},
		{"a reason line with other words", "sbatch: error: user has too many jobs\n" + policyText, "", false, "Job violates accounting/QOS policy"},
		{"a reason line with a path", "sbatch: error: /scratch/example-user/QOSMaxSubmitJobPerUserLimit\n" + policyText, "", false, ""},
		{"the reason line after the policy line", policyText + "sbatch: error: QOSMaxSubmitJobPerUserLimit\n", "", false, ""},
		{"a blank line between", "sbatch: error: QOSMaxSubmitJobPerUserLimit\n\n" + policyText, "", false, ""},
		{"not a policy refusal", "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified\n", "", false, "Invalid account"},
		{"an account refusal with a reason-shaped line before", "sbatch: error: QOSMaxSubmitJobPerUserLimit\nsbatch: error: Batch job submission failed: Invalid account or account/partition combination specified\n", "", false, "Invalid account"},
	} {
		t.Run(c.name, func(t *testing.T) {
			id, outcome, err := classifySubmit(Result{ExitCode: 1, Stderr: []byte(c.stderr)}, nil)
			if id != "" || outcome != provider.Rejected || err == nil {
				t.Fatalf("classifySubmit = %q, %s, %v; want no ID, Rejected and an error", id, outcome, err)
			}
			var pr *provider.PolicyRefusal
			isPolicy := errors.As(err, &pr)
			if isPolicy != (c.reason != "") {
				t.Fatalf("errors.As(PolicyRefusal) = %v for %v, want %v", isPolicy, err, c.reason != "")
			}
			if isPolicy {
				if pr.Reason != c.reason || pr.SubmitLimit() != c.limit {
					t.Errorf("refusal %+v: reason %q limit %v, want %q and %v", pr, pr.Reason, pr.SubmitLimit(), c.reason, c.limit)
				}
				// Line is the reason line as printed; the error's text is that
				// line, with the group-limit note for the two group names.
				if want := "sbatch: error: " + c.reason; pr.Line != want || !strings.HasPrefix(err.Error(), want) {
					t.Errorf("Line %q and text %q, want the reason line %q first", pr.Line, err.Error(), want)
				}
				if c.text != "" && err.Error() != c.text {
					t.Errorf("text %q, want exactly %q", err.Error(), c.text)
				}
			}
			if c.text != "" && !strings.Contains(err.Error(), c.text) {
				t.Errorf("the error %q lacks %q", err, c.text)
			}
		})
	}
}

// A transient reason stays Unknown, whatever else the text holds: Slurm may
// have registered the job.
func TestClassifySubmitTransientStaysUnknownWithAReasonLine(t *testing.T) {
	stderr := "sbatch: error: QOSMaxSubmitJobPerUserLimit\n" +
		"sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n"
	_, outcome, err := classifySubmit(Result{ExitCode: 1, Stderr: []byte(stderr)}, nil)
	var pr *provider.PolicyRefusal
	if outcome != provider.Unknown || errors.As(err, &pr) {
		t.Errorf("outcome %s, error %v; want Unknown and no policy refusal", outcome, err)
	}
	// A policy refusal followed by a transient text elsewhere in stderr is
	// still Unknown: the transient check comes first.
	_, outcome, _ = classifySubmit(Result{ExitCode: 1, Stderr: []byte("sbatch: error: QOSMaxSubmitJobPerUserLimit\n" + policyText + "sbatch: error: Connection reset\n")}, nil)
	if outcome != provider.Unknown {
		t.Errorf("outcome %s, want Unknown when a transient text is on stderr", outcome)
	}
}

// The reason line holds no host or path: only a reason name passes the strict
// form, and the length is capped.
func TestPolicyRefusalReasonIsBounded(t *testing.T) {
	long := strings.Repeat("A", 80)
	_, _, err := classifySubmit(Result{ExitCode: 1, Stderr: []byte("sbatch: error: " + long + "\n" + policyText)}, nil)
	var pr *provider.PolicyRefusal
	if errors.As(err, &pr) {
		t.Errorf("an 80-character name was read as a reason: %+v", pr)
	}
}
