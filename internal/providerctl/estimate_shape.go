package providerctl

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// estimateShapeForm is the canonical form of the submission parameters an
// estimate runs with, and estimateShape its shape (OperationEstimate.Shape):
// 16 hex characters of the SHA-256 of that text. Two targets of one shape get
// one estimate (HBEP-10, The release cost; `@hpc-burst-science-agent`, 17 Nov
// 2026, #661, Q3).
//
// The form is one "key=value" line per parameter that Slurm's start estimate can
// depend on, in this order, after the version line, each line ending in a
// newline:
//
//	hpcburst-estimate-shape/1
//	partition=<partition>
//	qos=<the Provider's qos, empty for none>
//	gpus=<gpus>
//	cpus=<cpus per task>
//	memMiB=<memory in whole MiB, rounded up, as the submission sends it>
//	minutes=<the time limit in whole minutes>
//	nodes=<nodes, 1 when the parameters leave it unset>
//	constraint=<the partition's tmpDirFeature, empty for none>
//	account=<the Provider's account>
//
// It leaves out what the estimate does not depend on: the job tag, the hold, and
// the script (the agent's digest and the allocation directory, which only the
// batch script holds). A time limit that is not a duration keeps its text in
// place of the minutes, so a request that is refused has a shape of its own.
// Bump the version line when the form changes.
func estimateShapeForm(req provider.SubmitRequest, constraint string) string {
	p := req.Params
	minutes := string(p.TimeLimit)
	if d, err := p.TimeLimit.AsDuration(); err == nil {
		minutes = strconv.FormatInt(int64(d.Minutes()), 10)
	}
	nodes := p.Nodes
	if nodes < 1 {
		nodes = 1
	}
	memMiB := (p.Memory.Value() + (1<<20 - 1)) >> 20
	var b strings.Builder
	for _, kv := range [][2]string{
		{"partition", p.Partition},
		{"qos", req.QOS},
		{"gpus", strconv.Itoa(int(p.GPUs))},
		{"cpus", strconv.Itoa(int(p.CPUsPerTask))},
		{"memMiB", strconv.FormatInt(memMiB, 10)},
		{"minutes", minutes},
		{"nodes", strconv.Itoa(int(nodes))},
		{"constraint", constraint},
		{"account", req.Account},
	} {
		b.WriteString(kv[0] + "=" + kv[1] + "\n")
	}
	return "hpcburst-estimate-shape/1\n" + b.String()
}

// estimateShape is the shape of a request: see estimateShapeForm.
func estimateShape(req provider.SubmitRequest, constraint string) string {
	sum := sha256.Sum256([]byte(estimateShapeForm(req, constraint)))
	return hex.EncodeToString(sum[:])[:16]
}
