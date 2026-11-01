package fakeslurm

// Ported from spikes/s4/fakeslurm/sbatch.go and sbatch_faults.go, with the
// option set cut to what slurm.submitLine builds and the S4 hooks (delay, gate,
// block, lost reply) replaced by Script and Gate. The partition policies are
// new.

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// sbatchDefs are the options slurm.submitLine builds. Every one takes its value
// after "=" or as the next word.
var sbatchDefs = []optDef{
	{name: "parsable"}, {name: "no-requeue"}, {name: "requeue"}, {name: "hold"},
	{name: "job-name", value: valRequired},
	{name: "partition", value: valRequired},
	{name: "account", value: valRequired},
	{name: "time", value: valRequired},
	{name: "nodes", value: valRequired},
	{name: "gres", value: valRequired},
	{name: "cpus-per-task", value: valRequired},
	{name: "mem", value: valRequired},
	{name: "output", value: valRequired},
	{name: "error", value: valRequired},
	{name: "wrap", value: valRequired},
	{name: "constraint", value: valRequired},
}

// Definite refusals, each "Batch job submission failed: <reason>". None holds a
// text that slurm.transientReasons reads as a lost reply.
const (
	reasonOneJob     = "Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"
	reasonGPUOnly    = "Job violates partition policy: this partition accepts only jobs that request a GPU"
	reasonPartition  = "Invalid partition name specified"
	reasonGRES       = "Invalid generic resource (gres) specification"
	transientTimeout = "Socket timed out on send/recv operation"
)

type sbatchSpec struct {
	name, partition, account, output, errPath, wrap string
	constraint                                      string
	minutes, cpus, nodes, gpus                      int
	memMiB                                          int64
	hold, requeue, parsable                         bool
}

var gresGPU = regexp.MustCompile(`^gpu(?::([0-9]+))?$`)

// parseSbatch reads the options; the error text is what sbatch prints after
// "sbatch: error: ".
func parseSbatch(args []string) (sbatchSpec, error) {
	p, err := parseOpts(args, sbatchDefs)
	if err != nil {
		return sbatchSpec{}, err
	}
	if len(p.rest) > 0 {
		return sbatchSpec{}, fmt.Errorf("the fake takes no script file, only --wrap")
	}
	s := sbatchSpec{nodes: 1, cpus: 1, parsable: p.has("parsable"), hold: p.has("hold")}
	s.requeue = !p.has("no-requeue") // sbatch's default is the cluster's; the plugin always says
	s.name, _ = p.get("job-name")
	s.partition, _ = p.get("partition")
	s.account, _ = p.get("account")
	s.output, _ = p.get("output")
	s.errPath, _ = p.get("error")
	s.constraint, _ = p.get("constraint")
	var ok bool
	if s.wrap, ok = p.get("wrap"); !ok {
		return s, fmt.Errorf("This does not look like a batch script")
	}
	return s, s.parseNumbers(p)
}

// parseNumbers reads --time, --nodes, --gres, --cpus-per-task and --mem.
func (s *sbatchSpec) parseNumbers(p parsed) error {
	var err error
	if v, ok := p.get("time"); ok {
		if s.minutes, err = parseTimeLimit(v); err != nil {
			return err
		}
	}
	if v, ok := p.get("nodes"); ok {
		if s.nodes, err = strconv.Atoi(v); err != nil || s.nodes < 1 {
			return fmt.Errorf("invalid node count %q", v)
		}
	}
	if v, ok := p.get("cpus-per-task"); ok {
		if s.cpus, err = strconv.Atoi(v); err != nil || s.cpus < 1 {
			return fmt.Errorf("invalid cpus-per-task %q", v)
		}
	}
	if v, ok := p.get("mem"); ok {
		if s.memMiB, err = parseMem(v); err != nil {
			return err
		}
	}
	if v, ok := p.get("gres"); ok {
		m := gresGPU.FindStringSubmatch(v)
		if m == nil {
			return errGres
		}
		s.gpus = 1
		if m[1] != "" {
			s.gpus, _ = strconv.Atoi(m[1])
		}
	}
	return nil
}

var errGres = errors.New(reasonGRES)

// parseMem reads --mem: a number of MiB with an optional M or G suffix.
func parseMem(v string) (int64, error) {
	unit := int64(1)
	switch {
	case strings.HasSuffix(v, "G"):
		unit, v = 1024, strings.TrimSuffix(v, "G")
	case strings.HasSuffix(v, "M"):
		v = strings.TrimSuffix(v, "M")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid memory specification %q", v)
	}
	return n * unit, nil
}

// sbatch registers a job. The scripted step (Script) decides the outcome after
// the partition policies have had their say.
func (sh *shell) sbatch(args []string) int {
	spec, err := parseSbatch(args)
	if err != nil {
		if errors.Is(err, errGres) {
			return sh.submitFailed(reasonGRES)
		}
		return sh.fail(1, "sbatch: error: %v", err)
	}
	if spec.partition == "" {
		spec.partition = sh.p.defaultPartition()
	}
	step := sh.p.nextStep()
	if reason := sh.p.policyRefusal(sh.s.user, spec); reason != "" {
		return sh.submitFailed(reason)
	}
	switch {
	case step.kind == stepReject:
		return sh.submitFailed(step.reason)
	case step.kind == stepTransient && !step.registered:
		return sh.submitFailed(transientTimeout)
	}
	j := sh.p.register(sh.s.user, spec, sh.now, step)
	switch step.kind {
	case stepTransient:
		return sh.submitFailed(transientTimeout)
	case stepLostReply:
		sh.runErr = step.err
		return 0
	case stepUnparseable:
		sh.out("sbatch: submitted, but the reply is garbled\n")
	default:
		if spec.parsable {
			sh.out("%d\n", j.id)
		} else {
			sh.out("Submitted batch job %d\n", j.id)
		}
	}
	return 0
}

// submitFailed prints sbatch's definite-failure message and exits 1.
func (sh *shell) submitFailed(reason string) int {
	return sh.fail(1, "sbatch: error: Batch job submission failed: %s", reason)
}

// defaultPartition is the partition of a job given none: the first listed, or
// "batch" when the platform lists none.
func (p *Platform) defaultPartition() string {
	if len(p.cfg.Partitions) > 0 {
		return p.cfg.Partitions[0].Name
	}
	return "batch"
}

// partition returns the configured partition of that name.
func (p *Platform) partition(name string) (Partition, bool) {
	for _, pt := range p.cfg.Partitions {
		if pt.Name == name {
			return pt, true
		}
	}
	return Partition{}, false
}

// policyRefusal is the reason a partition refuses the job, or "".
func (p *Platform) policyRefusal(user string, s sbatchSpec) string {
	if len(p.cfg.Partitions) == 0 {
		return ""
	}
	pt, ok := p.partition(s.partition)
	switch {
	case !ok:
		return reasonPartition
	case pt.GPUOnly && s.gpus == 0:
		return reasonGPUOnly
	case pt.OneJobPerUser && p.liveJobs(user, pt.Name) > 0:
		return reasonOneJob
	}
	return ""
}

// liveJobs counts the user's pending and running jobs in a partition.
func (p *Platform) liveJobs(user, partition string) int {
	n := 0
	for _, j := range p.jobs {
		if j.user == user && j.partition == partition && j.live() {
			n++
		}
	}
	return n
}

// register adds the job, applying the scripted step's changes.
func (p *Platform) register(user string, s sbatchSpec, now time.Time, step SubmitStep) *job {
	j := &job{
		id: p.nextID, name: s.name, user: user, partition: s.partition, account: s.account,
		timeLimit: s.minutes, cpus: s.cpus, nodes: s.nodes, gpus: s.gpus, memMiB: s.memMiB,
		requeue: s.requeue, output: s.output, errPath: s.errPath, wrap: s.wrap,
		constraint: s.constraint,
		execs:      []*execution{newExecution(now, "None")},
	}
	p.nextID++
	if j.name == "" {
		j.name = "wrap"
	}
	if j.account == "" {
		j.account = "default"
	}
	if s.hold {
		j.hold(HeldUser)
	}
	switch step.kind {
	case stepRename:
		if step.n >= 0 && len(j.name) > step.n {
			j.name = j.name[:step.n]
		}
	case stepAsTimeLimit:
		j.timeLimit = step.n
	case stepAdminHold:
		j.hold(HeldAdmin)
	}
	p.jobs = append(p.jobs, j)
	return j
}
