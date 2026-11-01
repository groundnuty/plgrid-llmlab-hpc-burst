package fakeslurm

// New. The QOS model (HBEP-4, QOS limits): a per-user submit limit that
// counts pending and running jobs, a per-user GPU cap that makes a job pend,
// the two-line policy refusal, and the late registration of a job whose reply
// was lost.
//
// The precedence is written here on its own, not taken from the API package
// whose helpers the plugin uses: this fake is the oracle the plugin's tests
// check against. For each limit the first QOS that sets it wins, and the
// partition's QOS comes before the job's (no OverPartQOS is modelled).

import (
	"fmt"
	"time"
)

// QOS is one quality of service of the fake cluster. A limit that is nil is
// not set; a limit of 0 is set, and allows nothing, as in Slurm (a QOS with
// MaxSubmitJobsPerUser 0 refuses every submission, one with a GPU cap of 0
// never starts a job with a GPU), so Cap(0) and nil differ. For each limit the
// first QOS in the order that sets it applies, whatever its value.
type QOS struct {
	Name string
	// MaxSubmitPerUser is the most jobs one user may have pending or running
	// under the QOS (MaxSubmitJobsPerUser). A job counts against the QOS it
	// was submitted under and against its partition's QOS. A held job is a
	// pending one.
	MaxSubmitPerUser *int
	// MaxGPUsPerUser bounds the GPUs of one user's running jobs under the QOS
	// (MaxTRESPerUser gres/gpu). A job that fits alone but not beside the
	// user's running jobs is accepted and pends; Start refuses it.
	MaxGPUsPerUser *int
}

// Cap is a limit that is set to n, 0 included, for QOS's fields.
func Cap(n int) *int { return &n }

// Slurm's text for a policy refusal: sbatch prints the reason's name, then the
// accounting-policy line (HBEP-4, QOS limits, A limit refusal; Slurm's
// job_mgr.c, slurm_protocol_pack.c, sbatch.c and slurm_errno.c).
const (
	policyLine = "Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"

	reasonSubmitPerUser = "QOSMaxSubmitJobPerUserLimit"
	reasonGRESPerUser   = "QOSMaxGRESPerUser"
	reasonInvalidQOS    = "Invalid qos specification"
)

// policyRefusalLines is the standard error of a policy refusal with the
// reason's name: the reason line, then the accounting-policy line.
func policyRefusalLines(reason string) string {
	return "sbatch: error: " + reason + "\nsbatch: error: " + policyLine + "\n"
}

// RejectPolicy refuses the submission for the accounting policy with the
// reason's name, such as AssocMaxSubmitJobLimit, MaxSubmitJobsPerAccount or
// QOSMaxWallDurationPerJobLimit: stderr holds the reason line and then the
// accounting-policy line, exit 1, nothing registered. Reject, in contrast,
// prints the one line "Batch job submission failed: <reason>", a refusal with
// no reason line.
func RejectPolicy(reason string) SubmitStep { return SubmitStep{kind: stepPolicy, reason: reason} }

// LostReplyLate loses the reply, as LostReply does, and the job is not
// registered yet: slurmctld registers it, held when --hold was sent, when the
// next sbatch arrives from anyone, before that sbatch is checked. So the job
// takes its user's slot, and the next attempt is refused for the limit, which
// is how a reply lost under a one-job limit plays out (HBEP-4, QOS limits).
// Registration at the next sbatch is deterministic; it needs no goroutine.
func LostReplyLate(err error) SubmitStep { return SubmitStep{kind: stepLostReplyLate, err: err} }

type lateJob struct {
	sess *Session
	spec sbatchSpec
}

// registerLate registers the jobs whose reply was lost and which slurmctld had
// not registered yet (LostReplyLate). The caller holds mu.
func (p *Platform) registerLate(now time.Time) {
	late := p.late
	p.late = nil
	for _, l := range late {
		p.registerFor(l.sess, l.spec, now, Accept())
	}
}

// LateJobs is the number of jobs a LostReplyLate step is still to register.
func (p *Platform) LateJobs() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.late)
}

// registerFor registers the job of a session. It is the one place the QOS code
// reaches register, so a change to register's signature is a change here.
func (p *Platform) registerFor(sess *Session, s sbatchSpec, now time.Time, step SubmitStep) *job {
	return p.register(sess, s, now, step)
}

// jobQOS is the QOS a submission runs under: the one --qos names, else the
// association's default (Config.DefaultQOS), "" when there is none.
func (p *Platform) jobQOS(s sbatchSpec) string {
	if s.qos != "" {
		return s.qos
	}
	return p.cfg.DefaultQOS
}

func (p *Platform) findQOS(name string) (QOS, bool) {
	for _, q := range p.cfg.QOS {
		if q.Name == name {
			return q, true
		}
	}
	return QOS{}, false
}

// qosOrder is the QOS list a job on partition under jobQOS is held to, in
// Slurm's order: the partition's QOS first, then the job's.
func (p *Platform) qosOrder(partition, jobQOS string) []QOS {
	var out []QOS
	if pt, ok := p.partition(partition); ok && pt.QOS != "" {
		if q, ok := p.findQOS(pt.QOS); ok {
			out = append(out, q)
		}
	}
	if q, ok := p.findQOS(jobQOS); ok && jobQOS != "" {
		out = append(out, q)
	}
	return out
}

// firstQOS returns the first QOS in the order that sets the limit pick reads
// (a nil limit is not set; 0 is), and that limit.
func firstQOS(order []QOS, pick func(QOS) *int) (QOS, int, bool) {
	for _, q := range order {
		if n := pick(q); n != nil {
			return q, *n, true
		}
	}
	return QOS{}, 0, false
}

// underQOS reports whether the job counts against the QOS: it was submitted
// under it, or its partition has it as the partition's QOS.
func (p *Platform) underQOS(j *job, name string) bool {
	if j.qos == name {
		return true
	}
	pt, ok := p.partition(j.partition)
	return ok && pt.QOS == name
}

// submitLimitRefusal returns QOSMaxSubmitJobPerUserLimit when the user is at
// the submit limit that applies to a job with this partition and QOS, else "".
func (p *Platform) submitLimitRefusal(user, partition, jobQOS string) string {
	q, limit, ok := firstQOS(p.qosOrder(partition, jobQOS), func(q QOS) *int { return q.MaxSubmitPerUser })
	if !ok {
		return ""
	}
	n := 0
	for _, j := range p.jobs {
		if j.user == user && j.live() && p.underQOS(j, q.Name) {
			n++
		}
	}
	if n >= limit {
		return reasonSubmitPerUser
	}
	return ""
}

// canStart is the per-user GPU cap: the job may start only if its GPUs and
// those of the user's running jobs under the QOS that sets the cap are within
// it. A job that does not fit stays pending. Every way of starting a job goes
// through it (Start, and the run mode's scheduler).
func (p *Platform) canStart(j *job) error {
	q, limit, ok := firstQOS(p.qosOrder(j.partition, j.qos), func(q QOS) *int { return q.MaxGPUsPerUser })
	if !ok {
		return nil
	}
	used := 0
	for _, o := range p.jobs {
		if o != j && o.user == j.user && o.state() == Running && p.underQOS(o, q.Name) {
			used += o.gpus
		}
	}
	if used+j.gpus > limit {
		return fmt.Errorf("job %d cannot start: its %d GPUs and the %d that user %s already runs exceed the %d of QOS %s (%s)",
			j.id, j.gpus, used, j.user, limit, q.Name, reasonGRESPerUser)
	}
	return nil
}

// qosRefusal is the standard error of the QOS refusal of a submission, or "".
// An unknown QOS name is refused when the cluster lists QOSes at all.
func (p *Platform) qosRefusal(user string, s sbatchSpec) string {
	if s.qos != "" && len(p.cfg.QOS) > 0 {
		if _, ok := p.findQOS(s.qos); !ok {
			return failedLine(reasonInvalidQOS)
		}
	}
	if len(p.cfg.QOS) == 0 {
		return ""
	}
	if r := p.submitLimitRefusal(user, s.partition, p.jobQOS(s)); r != "" {
		return policyRefusalLines(r)
	}
	return ""
}

// OutsideJob registers a pending job of user that the framework did not
// submit, as the owner's other projects would through the same login: named
// name, in the partition, under the QOS (none: the default), with gpus GPUs.
// It takes a slot of the user's submit limit and, once started, GPUs of the
// user's cap. It is no call: it is not in Calls, so counts of sbatch calls
// stay the framework's own. It returns the job ID.
func (p *Platform) OutsideJob(user, name, partition, qos string, gpus int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := sbatchSpec{name: name, partition: partition, qos: qos, gpus: gpus, nodes: 1, cpus: 1}
	j := p.registerFor(&Session{p: p, user: user, env: map[string]string{}}, s, p.clock.Now(), Accept())
	return j.id
}
