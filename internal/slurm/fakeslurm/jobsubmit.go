package fakeslurm

// New. A site's job_submit plugin, as Sano's (#735, spike S5 and the probes of
// 18 Nov 2026): on its partitions it refuses a submission that names no --qos,
// no --mem or no --cpus-per-task, or whose job name is generic, and one above
// its QOS's per-job caps, with the plugin's message shapes. The numbers a test
// uses are its own literals (SanoConfig), written from the probes, never read
// from the profile the plugin is given.
//
// Message shapes, from #735's probe 3 and the S5 draft (the real lines are
// paraphrased: "..." in the probe stands for text the probe did not keep):
//
//	--qos=<class> e.g. --qos=gpu1
//	Missing required parameters ... --job-name=<name> 'bash' says nothing about the computation
//	Job exceeds the limits of QOS 'gpu8': cores: requested 96, limit 92 ... no QOS fits this request; reduce it
//	Job exceeds the limits of QOS 'gpu1': cores: requested 13, limit 12 ... use --qos=gpu2 instead
//
// A plugin refusal prints the message as an error line of sbatch, then sbatch's
// own failure line with Slurm's text for an unspecified error; the real second
// line of a plugin refusal was not captured, so a test must not read it.

import (
	"fmt"
	"slices"
	"strings"
)

// JobSubmit models a job_submit plugin that holds a site's partitions to its
// rules (Config.JobSubmit).
type JobSubmit struct {
	// Partitions are the partitions the plugin applies to: on each, a job must
	// name --qos, --mem and --cpus-per-task, and a job name not in
	// GenericNames, and must fit its QOS's per-job caps (QOS.MaxGPUsPerJob,
	// MaxCPUsPerJob, MaxMemMiBPerJob and MaxWallMinutes).
	Partitions []string
	// GenericNames are the job names the plugin refuses as saying nothing about
	// the computation, such as sbatch's default of "wrap" for --wrap.
	GenericNames []string
}

// SanoConfig is a Config in Sano's shape: one partition, gpu, whose QOS classes
// gpu1, gpu2, gpu4 and gpu8 carry the per-job caps of #735's probe 3 and a
// 3-day wall, and Sano's job_submit rules. The caps are hand-written here: 12,
// 24, 48 and 92 CPUs, and 140 GiB, 280 GiB, 560 GiB and 1,087,232 MiB (1,061.75
// GiB) of memory, for 1, 2, 4 and 8 GPUs.
func SanoConfig() Config {
	wall := 3 * 24 * 60
	return Config{
		Partitions: []Partition{{Name: "gpu"}},
		QOS: []QOS{
			{Name: "gpu1", MaxGPUsPerJob: Cap(1), MaxCPUsPerJob: Cap(12), MaxMemMiBPerJob: Cap(143360), MaxWallMinutes: Cap(wall)},
			{Name: "gpu2", MaxGPUsPerJob: Cap(2), MaxCPUsPerJob: Cap(24), MaxMemMiBPerJob: Cap(286720), MaxWallMinutes: Cap(wall)},
			{Name: "gpu4", MaxGPUsPerJob: Cap(4), MaxCPUsPerJob: Cap(48), MaxMemMiBPerJob: Cap(573440), MaxWallMinutes: Cap(wall)},
			{Name: "gpu8", MaxGPUsPerJob: Cap(8), MaxCPUsPerJob: Cap(92), MaxMemMiBPerJob: Cap(1087232), MaxWallMinutes: Cap(wall)},
		},
		JobSubmit: &JobSubmit{Partitions: []string{"gpu"}, GenericNames: []string{"wrap", "job", "bash"}},
	}
}

// jobSubmitRefusal is the standard error of the job_submit plugin's refusal of
// a submission, or "". The plugin runs before the accounting policy, as in
// slurmctld, and does not judge an unknown QOS (qosRefusal does).
func (p *Platform) jobSubmitRefusal(s sbatchSpec) string {
	js := p.cfg.JobSubmit
	if js == nil || !slices.Contains(js.Partitions, s.partition) {
		return ""
	}
	var missing []string
	if s.qos == "" {
		missing = append(missing, "--qos=<class> e.g. --qos="+p.exampleQOS())
	}
	if !s.memGiven {
		missing = append(missing, "--mem=<size>")
	}
	if !s.cpusGiven {
		missing = append(missing, "--cpus-per-task=<n>")
	}
	if slices.Contains(js.GenericNames, s.name) {
		missing = append(missing, fmt.Sprintf("--job-name=<name> '%s' says nothing about the computation", s.name))
	}
	if len(missing) > 0 {
		return pluginRefusal("Missing required parameters: " + strings.Join(missing, "; "))
	}
	q, ok := p.findQOS(s.qos)
	if !ok {
		return ""
	}
	if over := overJobCaps(q, s); len(over) > 0 {
		hint := "no QOS fits this request; reduce it"
		for _, o := range p.cfg.QOS {
			if o.Name != q.Name && len(overJobCaps(o, s)) == 0 && hasJobCaps(o) {
				hint = "use --qos=" + o.Name + " instead"
				break
			}
		}
		return pluginRefusal(fmt.Sprintf("Job exceeds the limits of QOS '%s': %s; %s", q.Name, strings.Join(over, "; "), hint))
	}
	return ""
}

// exampleQOS is the QOS the plugin's hint names: the first listed with a
// per-job cap.
func (p *Platform) exampleQOS() string {
	for _, q := range p.cfg.QOS {
		if hasJobCaps(q) {
			return q.Name
		}
	}
	return "class"
}

func hasJobCaps(q QOS) bool {
	return q.MaxGPUsPerJob != nil || q.MaxCPUsPerJob != nil || q.MaxMemMiBPerJob != nil || q.MaxWallMinutes != nil
}

// overJobCaps lists the per-job caps of the QOS that the submission is above,
// each as the plugin words it: the cores, the GPUs, the memory in MB, the time
// in minutes.
func overJobCaps(q QOS, s sbatchSpec) []string {
	var over []string
	if q.MaxCPUsPerJob != nil && s.cpus > *q.MaxCPUsPerJob {
		over = append(over, fmt.Sprintf("cores: requested %d, limit %d", s.cpus, *q.MaxCPUsPerJob))
	}
	if q.MaxGPUsPerJob != nil && s.gpus > *q.MaxGPUsPerJob {
		over = append(over, fmt.Sprintf("GPUs: requested %d, limit %d", s.gpus, *q.MaxGPUsPerJob))
	}
	if q.MaxMemMiBPerJob != nil && s.memMiB > int64(*q.MaxMemMiBPerJob) {
		over = append(over, fmt.Sprintf("memory: requested %d MB, limit %d MB", s.memMiB, *q.MaxMemMiBPerJob))
	}
	if q.MaxWallMinutes != nil && s.minutes > *q.MaxWallMinutes {
		over = append(over, fmt.Sprintf("time: requested %d minutes, limit %d minutes", s.minutes, *q.MaxWallMinutes))
	}
	return over
}

// pluginRefusal is sbatch's standard error for a job_submit plugin's message.
func pluginRefusal(msg string) string {
	return "sbatch: error: " + msg + "\nsbatch: error: Batch job submission failed: Unspecified error\n"
}
