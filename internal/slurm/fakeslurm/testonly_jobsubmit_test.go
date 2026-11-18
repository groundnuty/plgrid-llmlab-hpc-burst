package fakeslurm

// A --test-only call goes through the site's job_submit plugin, as a real submission does (#735's checklist: the fake's
// estimate must apply Sano's plugin rules, since a rejected estimate stops the first live row). The messages are the
// plugin's shapes written by hand in jobsubmit.go; the literals here are the test's own, never read back from the code.

import (
	"strings"
	"testing"
)

// sanoTestOnly is an estimate request in Sano's shape: partition gpu, QOS gpu1, 11 CPUs, one GPU and a job name that is not
// generic. %s is where --mem goes, or nothing.
const sanoTestOnly = "sbatch --test-only --parsable --job-name=hb-0123456789ab --no-requeue --partition=gpu --account=SEEDACCT --qos=gpu1 " +
	"--time=10 --nodes=1 --gres=gpu:1 --cpus-per-task=11 %s--output=/s/job.out --error=/s/job.err --wrap=true"

func TestATestOnlyCallMissingWhatSanosPluginRequiresIsRefusedWithThePluginsMessage(t *testing.T) {
	line := func(mem string) string { return strings.Replace(sanoTestOnly, "%s", mem, 1) }
	r := newRig(t, SanoConfig())

	// Without --mem: refused, with the plugin's own message naming it, and no start estimate.
	out, errOut, code := r.run(line(""))
	if code != 1 || out != "" || !strings.Contains(errOut, "Missing required parameters") || !strings.Contains(errOut, "--mem=<size>") || strings.Contains(errOut, "to start at") {
		t.Fatalf("a --test-only call with no --mem: exit %d, stdout %q, stderr %q, want exit 1 and the plugin's message naming --mem", code, out, errOut)
	}
	// The same call with it passes: the refusal is the missing piece, not the shape or the fixture.
	if _, errOut, code := r.run(line("--mem=135904M ")); code != 0 || !strings.Contains(errOut, "to start at") {
		t.Fatalf("the same call with --mem=135904M: exit %d, stderr %q, want exit 0 and a start estimate", code, errOut)
	}
	// The real submission is refused the same way, so the two paths agree (the order of sbatch.go: policy, plugin, QOS).
	real := strings.Replace(line(""), "--test-only ", "", 1)
	if _, errOut, code := r.run(real); code != 1 || !strings.Contains(errOut, "--mem=<size>") {
		t.Fatalf("a real submission with no --mem: exit %d, stderr %q, want the same refusal", code, errOut)
	}
	// Each other piece the plugin requires is refused on a test too: the QOS, the CPUs and a generic job name.
	for name, c := range map[string]struct{ line, want string }{
		"no --qos":              {strings.Replace(line("--mem=135904M "), "--qos=gpu1 ", "", 1), "--qos=<class>"},
		"no --cpus-per-task":    {strings.Replace(line("--mem=135904M "), "--cpus-per-task=11 ", "", 1), "--cpus-per-task=<n>"},
		"the generic name wrap": {strings.Replace(line("--mem=135904M "), "--job-name=hb-0123456789ab", "--job-name=wrap", 1), "'wrap' says nothing about the computation"},
	} {
		if _, errOut, code := r.run(c.line); code != 1 || !strings.Contains(errOut, c.want) {
			t.Errorf("%s: exit %d, stderr %q, want a refusal naming %q", name, code, errOut, c.want)
		}
	}
}

// A platform with no plugin (every fixture but Sano's) estimates a call with no --mem as before: the new check is the plugin's
// alone, and no partition is refused for a rule it does not have.
func TestATestOnlyCallOnAPlatformWithNoPluginIsNotRefusedForAMissingMem(t *testing.T) {
	r := newRig(t, Config{})
	if _, errOut, code := r.run(strings.Replace(textLine, "--mem=8192M ", "", 1)); code != 0 || !strings.Contains(errOut, "to start at") {
		t.Fatalf("exit %d, stderr %q, want an estimate: no plugin, no rule", code, errOut)
	}
}
