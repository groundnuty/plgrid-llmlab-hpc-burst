package slurm

// Reading sbatch's accounting-policy refusal (HBEP-4, QOS limits, A limit
// refusal).

import (
	"regexp"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// policyPhrase is the part of the accounting-policy line that never changes:
// Slurm's text for ESLURM_ACCOUNTING_POLICY, after sbatch's "error:" and
// "Batch job submission failed:". The tail in parentheses is Slurm's, and
// provider.PolicyLine holds the whole line.
const policyPhrase = "Job violates accounting/QOS policy"

// reasonLine is the line Slurm's client prints before the accounting-policy
// line: "sbatch: error: " and the reason's name, a Slurm job reason string
// such as QOSMaxSubmitJobPerUserLimit. Nothing else may be on the line, so a
// line that carries other words is no reason line.
var reasonLine = regexp.MustCompile(`^sbatch: error: ([A-Za-z][A-Za-z0-9]{0,63})$`)

// policyRefusal reads the refusal in stderr: the accounting-policy line, and
// the line right before it naming a reason. It returns nil when stderr holds no
// accounting-policy line, or when the line before it is not a reason line, so
// that such a refusal stays an ordinary Rejected outcome with its text. The
// reason line is kept as Slurm printed it, with absolute paths and host names
// removed (redactPaths).
func policyRefusal(stderr string) *provider.PolicyRefusal {
	lines := strings.Split(stderr, "\n")
	for i := len(lines) - 1; i >= 1; i-- {
		if !strings.Contains(lines[i], policyPhrase) {
			continue
		}
		m := reasonLine.FindStringSubmatch(strings.TrimSpace(lines[i-1]))
		if m == nil {
			return nil
		}
		return &provider.PolicyRefusal{Reason: m[1], Line: redactPaths(strings.TrimSpace(lines[i-1]))}
	}
	return nil
}
