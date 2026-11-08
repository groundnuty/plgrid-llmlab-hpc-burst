package quickstart

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The guide's commands on a login node (blocks fenced as cluster) cannot run here: there is no
// cluster, and the fake Slurm (internal/slurm/fakeslurm) implements only the provider's own lines.
// Each flag the guide gives is checked against the man pages of both alpha clusters' releases,
// Slurm 24.11.5 (Helios) and 23.11.7 (Athena): squeue, sacct, scontrol and scancel at
// https://slurm.schedmd.com/archive/slurm-24.11.5/<command>.html and the same under
// slurm-23.11.7, accessed 2026-11-02. A command, a flag or a value not listed here fails.

// slurmStates are the job state codes the guide names, from squeue's JOB STATE CODES section in
// both releases. COMPLETING is the flag the provider reads (HBEP-4, Phases, A completing job).
var slurmStates = []string{
	"PENDING", "RUNNING", "CANCELLED", "COMPLETED", "FAILED", "TIMEOUT", "NODE_FAIL", provider.FlagCompleting,
}

// squeueFields are the -o/--format field specifications the guide uses: %i, "Job or job step id",
// and %j, "Job or job step name" (squeue, -o).
var squeueFields = map[string]bool{"%i": true, "%j": true}

func slurmStateList(v string) error {
	for _, s := range strings.Split(v, ",") {
		if !slices.Contains(slurmStates, s) {
			return fmt.Errorf("%s is not a job state code the guide knows", s)
		}
	}
	return nil
}

func squeueFormat(v string) error {
	for _, f := range strings.Fields(v) {
		if !squeueFields[f] {
			return fmt.Errorf("%s is not a field the guide knows", f)
		}
	}
	return nil
}

// slurmRelativeTime is sacct's -S form "now[{+|-}count[seconds(default)|minutes|hours|days|weeks]]"
// (sacct, -S), as the guide uses it: a time in the past.
var slurmRelativeTime = regexp.MustCompile(`^now-[0-9]+(seconds|minutes|hours|days|weeks)?$`)

func nonEmpty(v string) error {
	if v == "" {
		return fmt.Errorf("want a value")
	}
	return nil
}

// clusterFlags are each command's flags as the guide may use them, short and long forms, with
// their value checks (nil for a flag that takes no value). squeue: --me, -n/--name, -t/--states,
// -o/--format, -h/--noheader. sacct: --name, -X/--allocations, -S/--starttime.
var clusterFlags = map[string]map[string]valueCheck{
	"squeue": {
		"--me": nil, "-h": nil, "--noheader": nil,
		"-n": nonEmpty, "--name": nonEmpty,
		"-t": slurmStateList, "--states": slurmStateList,
		"-o": squeueFormat, "--format": squeueFormat,
	},
	"sacct": {
		"-X": nil, "--allocations": nil,
		"--name": nonEmpty,
		"-S": func(v string) error {
			if !slurmRelativeTime.MatchString(v) {
				return fmt.Errorf("want now-<count>[unit]")
			}
			return nil
		},
	},
	"scontrol": {},
	"scancel":  {},
}

// checkCluster checks one login-node line of the guide, its words as the shell gives them.
// scontrol is used as "show job <id>" ("show <ENTITY>[=<ID>] or <ENTITY> [<ID>]", scontrol), and
// scancel with job IDs alone ("scancel [OPTIONS...] [job_id...]", scancel); squeue and sacct take
// flags only.
func checkCluster(t *testing.T, line string, words []string) {
	t.Helper()
	flags, ok := clusterFlags[words[0]]
	if !ok {
		t.Errorf("%q runs %s on a login node, which the test does not know", line, words[0])
		return
	}
	var pos []string
	for i := 1; i < len(words); i++ {
		w := words[i]
		if !strings.HasPrefix(w, "-") {
			pos = append(pos, w)
			continue
		}
		name, val, inline := strings.Cut(w, "=")
		check, known := flags[name]
		switch {
		case !known:
			t.Errorf("%q: %s has no flag %s in the test's list from Slurm's man pages", line, words[0], name)
			continue
		case check == nil && inline:
			t.Errorf("%q: %s takes no value", line, name)
			continue
		case check != nil && !inline:
			if i+1 == len(words) {
				t.Errorf("%q: %s has no value", line, name)
				continue
			}
			i++
			val = words[i]
		}
		if check != nil {
			if err := check(val); err != nil {
				t.Errorf("%q: %s %q: %v", line, name, val, err)
			}
		}
	}
	jobID := regexp.MustCompile(`^[0-9]+$`)
	switch words[0] {
	case "scontrol":
		if len(pos) != 3 || pos[0] != "show" || pos[1] != "job" || !jobID.MatchString(pos[2]) {
			t.Errorf("%q: the guide uses scontrol only as show job <job-id>", line)
		}
	case "scancel":
		if len(pos) == 0 {
			t.Errorf("%q names no job", line)
		}
		for _, p := range pos {
			if !jobID.MatchString(p) {
				t.Errorf("%q: %s is not a job ID", line, p)
			}
		}
	default:
		if len(pos) != 0 {
			t.Errorf("%q: %s takes no argument here, got %q", line, words[0], pos)
		}
	}
}
