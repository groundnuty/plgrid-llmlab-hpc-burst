package quickstart

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The operator's guide (issue #240) is checked like the quickstart: its hpcburst commands against
// the binary's -h, its hack/ scripts by running them up to their arguments, and the one status
// output it shows against the binary's own. Its kubectl lines (hbk) cannot ask a kubectl, since CI's
// go job has no k3s binary; they are checked against the CRDs, the label and annotation constants
// and a fixed list of the verbs and flags the guide uses. Every condition, reason, phase and mode
// it names is checked against the constant that holds it. A line or a name the test cannot place
// fails, so nothing in the guide goes unchecked by default.

const opsGuide = "docs/operations.md"

// opsFence matches the guide's fenced blocks, also those indented under a list item.
var opsFence = regexp.MustCompile("(?ms)^( *)```([a-z-]*)\n(.*?)^ *```$")

// opsSegment is a run of the guide: prose, or a fenced block with its info string and its text
// with the fence's indentation removed.
type opsSegment struct {
	fenced bool
	lang   string
	text   string
}

// opsSegments splits the guide into prose and fenced blocks, in order. Every fence line must
// belong to a block it found, so a fence the pattern misses fails here instead of reading as prose.
func opsSegments(t *testing.T) []opsSegment {
	t.Helper()
	doc := readRepoFile(t, opsGuide)
	var out []opsSegment
	last := 0
	for _, m := range opsFence.FindAllStringSubmatchIndex(doc, -1) {
		out = append(out, opsSegment{text: doc[last:m[0]]})
		indent := doc[m[2]:m[3]]
		var lines []string
		for _, l := range strings.Split(doc[m[6]:m[7]], "\n") {
			lines = append(lines, strings.TrimPrefix(l, indent))
		}
		out = append(out, opsSegment{fenced: true, lang: doc[m[4]:m[5]], text: strings.Join(lines, "\n")})
		last = m[1]
	}
	out = append(out, opsSegment{text: doc[last:]})
	fences, blocks := 0, 0
	for _, s := range out {
		if s.fenced {
			blocks++
		} else {
			fences += len(regexp.MustCompile("(?m)^ *```").FindAllString(s.text, -1))
		}
	}
	if blocks == 0 || fences != 0 {
		t.Fatalf("%s: %d fenced blocks found, and %d fence lines outside them", opsGuide, blocks, fences)
	}
	return out
}

// fillOps replaces the guide's placeholders with values of the right shape; the quickstart's own
// placeholders are filled by fill. An unknown placeholder stays, and the shell then refuses it.
func fillOps(s string) string {
	return fill(strings.NewReplacer(
		"<name>", "example-deployment",
		"<instance>", "example-instance",
		"<allocation>", "example-allocation",
		"<provider>", "example-provider",
		"<record>", "acct-00000000-0000-4000-8000-000000000000",
		"<attempt>", "2",
		"<version>", "1.36.6",
		"<backup>", "/nonexistent/backup",
		"<tag>", "example-tag",
		"<job-id>", "1",
	).Replace(s))
}

// The fences the guide may use: plain for commands on the control-plane host, cluster for commands
// on a login node, which nothing here can run, and text for output.
var opsFenceLangs = map[string]bool{"": true, "cluster": true, "text": true}

// clusterCommands are the programs the guide runs on a login node: Slurm's, run as the user.
var clusterCommands = map[string]bool{"squeue": true, "sacct": true, "scontrol": true}

// Every command in the guide's blocks and prose is accepted by what runs it: an hpcburst command by
// the binary's -h, a hack/ script by the script, an hbk line by the CRDs and constants. A bare flag
// in the prose belongs to the hpcburst subcommand named last before it.
func TestOperationsCommands(t *testing.T) {
	d, home := testDirs(t)
	crds := readCRDNames(t)
	sub := "" // the hpcburst subcommand named last, which a bare flag in the prose belongs to
	var hpcburstLines, hbkLines, scripts int
	for _, s := range opsSegments(t) {
		if !s.fenced {
			for _, m := range regexp.MustCompile("`([^`\n]+)`").FindAllStringSubmatch(s.text, -1) {
				f := strings.Fields(m[1])
				switch {
				case f[0] == "hpcburst" || f[0] == "bin/hpcburst":
					checkHpcburst(t, home, f[1:])
					if len(f) > 1 {
						sub = f[1]
					}
				case f[0] == "hbk":
					checkHbk(t, m[1], shellWords(t, fillOps(m[1]), d, home), crds)
				case strings.HasPrefix(f[0], "--"):
					if sub == "" {
						t.Errorf("the span %q holds a flag before the guide names any hpcburst command", m[1])
						continue
					}
					checkHpcburst(t, home, append([]string{sub}, f...))
				default:
					for _, w := range f[1:] {
						if strings.HasPrefix(w, "--") {
							t.Errorf("the span %q holds the flag %s, and the test cannot tell which command it belongs to: start the span with the command, or with the flag", m[1], w)
						}
					}
				}
			}
			continue
		}
		if !opsFenceLangs[s.lang] {
			t.Errorf("a block is fenced as %q, which the test does not know; use a plain fence for commands on the host, cluster or text", s.lang)
			continue
		}
		if s.lang == "text" {
			continue // output, checked by TestOperationsStoppedOutputIsTheBinarys
		}
		for _, l := range joinContinuations(s.text) {
			l = strings.TrimSpace(l)
			if l == "" || strings.HasPrefix(l, "#") {
				continue
			}
			prog := strings.Fields(l)[0]
			if s.lang == "cluster" {
				if !clusterCommands[prog] {
					t.Errorf("%q runs %s on a login node, which the test does not know", l, prog)
				}
				continue
			}
			switch {
			case strings.HasPrefix(l, "STATE=") || strings.HasPrefix(l, "BIN=") || strings.HasPrefix(l, "alias hbk="):
				// The definitions are the quickstart's, which its own tests compare with the launcher.
				if !strings.Contains(readRepoFile(t, "docs/quickstart.md"), "\n"+l+"\n") {
					t.Errorf("%q is not the quickstart's line", l)
				}
			case prog == "bin/hpcburst":
				c := command{line: l, words: shellWords(t, fillOps(l), d, home)}
				hpcburstLines++
				if len(c.words) < 2 {
					t.Errorf("%q names no subcommand", l)
					continue
				}
				checkHpcburst(t, home, c.words[1:])
				sub = c.words[1]
			case strings.HasPrefix(prog, "hack/"):
				c := command{line: l, words: shellWords(t, fillOps(l), d, home)}
				if _, ok := scriptStops[c.words[0]]; !ok {
					t.Errorf("%q: the test has no way to run %s without side effects; add it to scriptStops", l, c.words[0])
					continue
				}
				scripts++
				runScript(t, home, c)
			case prog == "hbk":
				hbkLines++
				checkHbk(t, l, shellWords(t, fillOps(l), d, home), crds)
			case prog == "go":
				checkGoBuild(t, l, shellWords(t, fillOps(l), d, home))
			default:
				t.Errorf("%q: the test cannot check a line that starts with %s", l, prog)
			}
		}
	}
	if hpcburstLines == 0 || hbkLines == 0 || scripts == 0 {
		t.Fatalf("found %d bin/hpcburst, %d hbk and %d hack/ lines in the guide's blocks; a changed fence style must not leave nothing to check", hpcburstLines, hbkLines, scripts)
	}
}

// checkGoBuild accepts the build line of the quickstart's step 2: go build -o bin/ and programs of
// this repository.
func checkGoBuild(t *testing.T, line string, words []string) {
	t.Helper()
	if len(words) < 5 || words[1] != "build" || words[2] != "-o" || words[3] != "bin/" {
		t.Errorf("%q: the test checks only go build -o bin/ <programs>", line)
		return
	}
	for _, p := range words[4:] {
		if fi, err := os.Stat(filepath.Join(root, p)); !strings.HasPrefix(p, "./cmd/") || err != nil || !fi.IsDir() {
			t.Errorf("%q builds %s, which is not a program of this repository", line, p)
		}
	}
}

// crdNames are the names kubectl takes for each kind of config/crd: plural, singular and short
// names, and whether the kind has the scale subresource.
type crdNames struct {
	known    map[string]bool
	scalable map[string]bool
}

func readCRDNames(t *testing.T) crdNames {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "config", "crd", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("config/crd has no CRD (%v)", err)
	}
	n := crdNames{known: map[string]bool{}, scalable: map[string]bool{}}
	for _, f := range files {
		var crd struct {
			Spec struct {
				Names struct {
					Plural     string   `json:"plural"`
					Singular   string   `json:"singular"`
					ShortNames []string `json:"shortNames"`
				} `json:"names"`
				Versions []struct {
					Subresources map[string]any `json:"subresources"`
				} `json:"versions"`
			} `json:"spec"`
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal(b, &crd); err != nil || crd.Spec.Names.Plural == "" {
			t.Fatalf("%s: no CRD names (%v)", f, err)
		}
		names := append([]string{crd.Spec.Names.Plural, crd.Spec.Names.Singular}, crd.Spec.Names.ShortNames...)
		scale := false
		for _, v := range crd.Spec.Versions {
			_, has := v.Subresources["scale"]
			scale = scale || has
		}
		for _, x := range names {
			n.known[x] = true
			n.scalable[x] = scale
		}
	}
	return n
}

// builtinKinds are the Kubernetes kinds the guide reads besides the framework's own: the records
// are ControllerRevisions (HBEP-11, Ownership, records and deletion), the controllers' Leases live
// in hpcburst-system (HBEP-11, Controllers), and Events are HBEP-8's.
var builtinKinds = map[string]bool{
	"controllerrevision": true, "controllerrevisions": true,
	"lease": true, "leases": true,
	"event": true, "events": true,
}

// kubectlVerbs are the kubectl verbs the guide uses.
var kubectlVerbs = map[string]bool{"get": true, "describe": true, "scale": true, "annotate": true, "delete": true}

// kubectlFlags are the kubectl flags the guide uses, each with the check of its value: label keys,
// record kinds and the namespace are the framework's constants.
var kubectlFlags = map[string]func(string) bool{
	"-o":        func(v string) bool { return v == "yaml" || v == "json" || v == "wide" || v == "name" },
	"-n":        func(v string) bool { return v == v1alpha1.ControllerNamespace },
	"--sort-by": func(v string) bool { return strings.HasPrefix(v, ".") },
	"--replicas": func(v string) bool {
		n, err := strconv.Atoi(v)
		return err == nil && n >= 0
	},
	"-l": func(v string) bool {
		k, val, ok := strings.Cut(v, "=")
		switch {
		case !ok:
			return false
		case k == v1alpha1.LabelRecord:
			return val == v1alpha1.RecordAccounting || val == v1alpha1.RecordVerification || val == v1alpha1.RecordIdentity
		}
		return k == v1alpha1.LabelProvider
	},
}

// operatorAnnotations are the annotations the guide tells an operator to set: the decision on an
// unresolved attempt is the operator's (HBEP-11, The Allocation's record).
var operatorAnnotations = map[string]bool{v1alpha1.AnnotationAbandonAttempt: true}

// checkHbk checks one kubectl line of the guide, its words as the shell gives them.
func checkHbk(t *testing.T, line string, words []string, crds crdNames) {
	t.Helper()
	if len(words) < 2 || !kubectlVerbs[words[1]] {
		t.Errorf("%q: the test knows the kubectl verbs %v only", line, kubectlVerbs)
		return
	}
	var pos []string
	for i := 2; i < len(words); i++ {
		w := words[i]
		if !strings.HasPrefix(w, "-") {
			pos = append(pos, w)
			continue
		}
		name, val, inline := strings.Cut(w, "=")
		ok, known := kubectlFlags[name]
		if !known {
			t.Errorf("%q: the test does not know the kubectl flag %s", line, name)
			continue
		}
		if !inline {
			if i+1 == len(words) {
				t.Errorf("%q: %s has no value", line, name)
				continue
			}
			i++
			val = words[i]
		}
		if !ok(val) {
			t.Errorf("%q: %s %q is not a value the framework defines", line, name, val)
		}
	}
	if len(pos) == 0 {
		t.Errorf("%q names no resource", line)
		return
	}
	for _, r := range strings.Split(pos[0], ",") {
		if !crds.known[r] && !builtinKinds[r] {
			t.Errorf("%q: %s is neither a kind of config/crd nor one the guide is known to read", line, r)
		}
	}
	switch words[1] {
	case "scale":
		if !crds.scalable[pos[0]] {
			t.Errorf("%q: %s has no scale subresource", line, pos[0])
		}
	case "annotate":
		if len(pos) != 3 {
			t.Errorf("%q: want a kind, a name and one key=value", line)
			return
		}
		if k, _, _ := strings.Cut(pos[2], "="); !operatorAnnotations[k] {
			t.Errorf("%q: %s is not an annotation an operator sets", line, k)
		}
	}
}

// The stopped report the guide shows is what the binary prints for a state directory where nothing
// was started, with the exit code the guide gives.
func TestOperationsStoppedOutputIsTheBinarys(t *testing.T) {
	var samples []string
	for _, s := range opsSegments(t) {
		if s.fenced && s.lang == "text" {
			samples = append(samples, s.text)
		}
	}
	if len(samples) != 1 {
		t.Fatalf("want one text block in %s, found %d", opsGuide, len(samples))
	}
	// A short path: the launcher refuses a state directory of 80 bytes or more, which t.TempDir's
	// names can reach.
	_, home := testDirs(t)
	out, code := run(t, baseEnv(home), hpcburst(t), "status", "--state-dir", filepath.Join(home, "none"))
	if code != 3 {
		t.Errorf("status on an empty state directory exits %d; the guide says 3", code)
	}
	if out != samples[0] {
		t.Errorf("status prints\n%s\nand the guide shows\n%s", out, samples[0])
	}
	if !strings.Contains(readRepoFile(t, opsGuide), "prints this and exits 3:") {
		t.Errorf("the guide no longer gives the exit code beside the sample")
	}
}

// guideNames is every condition, reason, phase and mode the guide names, with the constant that
// holds it (issue #240: every reason named is a real constant). It is the one list the HBEP-8 drift
// guard (#239) is hooked to. A name the guide drops must leave it, and a name the guide adds must
// join it with its constant.
var guideNames = map[string]string{
	// ModelDeployment conditions and reasons (HBEP-7, Events and conditions).
	"Ready":                 v1alpha1.ConditionReady,
	"Degraded":              v1alpha1.ConditionDegraded,
	"Progressing":           v1alpha1.ConditionProgressing,
	"EnoughReady":           v1alpha1.ReasonEnoughReady,
	"NotEnoughReady":        v1alpha1.ReasonNotEnoughReady,
	"Provisioning":          v1alpha1.ReasonProvisioning,
	"Stable":                v1alpha1.ReasonStable,
	"AsDesired":             v1alpha1.ReasonAsDesired,
	"ManagedSource":         v1alpha1.ReasonManagedSource,
	"CatalogEntryListed":    v1alpha1.ReasonCatalogEntryListed,
	"CatalogEntryUnlisted":  v1alpha1.ReasonCatalogEntryUnlisted,
	"CredentialsRejected":   v1alpha1.ReasonCredentialsRejected,
	"ProviderNotOnPlatform": v1alpha1.ReasonProviderNotOnPlatform,
	"SpecUnsupported":       v1alpha1.ReasonSpecUnsupported,
	"PartitionTooShort":     v1alpha1.ReasonPartitionTooShort,
	"FrontNotConfigured":    v1alpha1.ReasonFrontNotConfigured,
	"VerificationFailed":    v1alpha1.ReasonVerificationFailed,
	"IdentityUnresolved":    v1alpha1.ReasonIdentityUnresolved,
	"RestartLoop":           v1alpha1.ReasonRestartLoop,
	"SubmissionRejected":    v1alpha1.ReasonSubmissionRejected,
	"NoOverlapSlot":         v1alpha1.ReasonNoOverlapSlot,
	"SubmitLimitReached":    v1alpha1.ReasonSubmitLimitReached,
	"ProviderUnavailable":   v1alpha1.ReasonProviderUnavailable,
	"ReplacementLate":       v1alpha1.ReasonReplacementLate,
	"CapacityDropped":       v1alpha1.ReasonCapacityDropped,
	"ShapesFit":             v1alpha1.EventReasonShapesFit,

	// Instance conditions, their reasons and its end reasons (HBEP-7, Instance start, Health and
	// restarts; HBEP-6, Path state).
	"Verified":             v1alpha1.ConditionVerified,
	"Qualified":            v1alpha1.ConditionQualified,
	"WarmedUp":             v1alpha1.ConditionWarmedUp,
	"Healthy":              v1alpha1.ConditionHealthy,
	"AgentUnreachable":     v1alpha1.ConditionAgentUnreachable,
	"TunnelConnected":      v1alpha1.ConditionTunnelConnected,
	"Pending":              v1alpha1.ReasonPending,
	"Passed":               v1alpha1.ReasonPassed,
	"Failed":               v1alpha1.ReasonFailed,
	"WorkerRestart":        v1alpha1.ReasonWorkerRestart,
	"InstanceRestart":      v1alpha1.ReasonInstanceRestart,
	"ProbeFailing":         v1alpha1.ReasonProbeFailing,
	"ErrorsOnly":           v1alpha1.ReasonErrorsOnly,
	"NoProgress":           v1alpha1.ReasonNoProgress,
	"PartExited":           v1alpha1.ReasonPartExited,
	"ReportsMissing":       v1alpha1.ReasonReportsMissing,
	"Reporting":            v1alpha1.ReasonReporting,
	"ProxyOnline":          v1alpha1.ReasonProxyOnline,
	"ProxyOffline":         v1alpha1.ReasonProxyOffline,
	"ProxyListUnreadable":  v1alpha1.ReasonProxyListUnreadable,
	"RegistrationTimeout":  v1alpha1.ReasonRegistrationTimeout,
	"StartTimeout":         v1alpha1.ReasonStartTimeout,
	"TunnelNotConnected":   v1alpha1.ReasonTunnelNotConnected,
	"QualificationFailed":  v1alpha1.ReasonQualificationFailed,
	"AllocationFailed":     v1alpha1.ReasonAllocationFailed,
	"AllocationLost":       v1alpha1.ReasonAllocationLost,
	"AllocationRefused":    v1alpha1.ReasonAllocationRefused,
	"AllocationEndedEarly": v1alpha1.ReasonAllocationEndedEarly,
	"Requeued":             v1alpha1.ReasonRequeued,
	"Drained":              v1alpha1.ReasonDrained,

	// Allocation phases, conditions and reasons (HBEP-4, Core operations, Phases).
	"Submitting":         string(v1alpha1.AllocationSubmitting),
	"SubmissionUnknown":  string(v1alpha1.AllocationSubmissionUnknown),
	"Held":               string(v1alpha1.AllocationHeld),
	"Running":            string(v1alpha1.AllocationRunning),
	"Ended":              string(v1alpha1.AllocationEnded),
	"Lost":               string(v1alpha1.AllocationLost),
	"Refused":            string(v1alpha1.AllocationRefused),
	"JobCompleting":      string(v1alpha1.ConditionJobCompleting),
	"Completing":         string(v1alpha1.ReasonJobCompleting),
	"NotCompleting":      string(v1alpha1.ReasonJobNotCompleting),
	"AdminHold":          string(v1alpha1.ConditionAdminHold),
	"SubmittedAsOther":   string(v1alpha1.ConditionSubmittedAsOther),
	"ReadBackMismatch":   string(v1alpha1.ReasonReadBackMismatch),
	"SubmissionResolved": string(v1alpha1.ConditionSubmissionResolved),
	"NeedsOperator":      string(v1alpha1.ReasonNeedsOperator),
	"Resolved":           string(v1alpha1.ReasonResolved),
	"JobRenamed":         v1alpha1.ReasonJobRenamed,

	// Provider conditions and reasons (HBEP-4; HBEP-11, Credentials).
	"ReadsFailing":        v1alpha1.ReasonReadsFailing,
	"CredentialsInvalid":  v1alpha1.ConditionCredentialsInvalid,
	"PathUnresolved":      v1alpha1.ConditionPathUnresolved,
	"DialectMismatch":     v1alpha1.ConditionDialectMismatch,
	"DialHomeKeyMissing":  v1alpha1.ConditionDialHomeKeyMissing,
	"DialHomeUnreachable": v1alpha1.ConditionDialHomeUnreachable,

	// Slurm's reason names for a full submit limit, as the plugin classifies them (HBEP-4, QOS
	// limits).
	"QOSMaxSubmitJobPerUserLimit": provider.ReasonQOSMaxSubmitJobPerUserLimit,
	"AssocMaxSubmitJobLimit":      provider.ReasonAssocMaxSubmitJobLimit,
	"MaxSubmitJobsPerAccount":     provider.ReasonMaxSubmitJobsPerAccount,
	"QOSGrpSubmitJobsLimit":       provider.ReasonQOSGrpSubmitJobsLimit,
	"AssocGrpSubmitJobsLimit":     provider.ReasonAssocGrpSubmitJobsLimit,

	// Retention modes (HBEP-7, Retention).
	"Keep":            string(v1alpha1.RetentionKeep),
	"ReleaseWhenIdle": string(v1alpha1.RetentionReleaseWhenIdle),
}

// unexportedNames are names the guide gives whose constant is unexported in its package, so this
// package cannot refer to it: each is checked by its identifier in that package's source.
var unexportedNames = map[string]struct{ dir, ident string }{
	"JobHeldAdmin":    {"internal/providerctl", "reasonAdminHold"},
	"NotHeldByAdmin":  {"internal/providerctl", "reasonNoAdminHold"},
	"HeartbeatMissed": {"internal/lifecycle", "reasonHeartbeatMissed"},
	"ObservationGap":  {"internal/lifecycle", "reasonObservationGap"},
}

// notCodeNames are capitalised names the guide gives that no code of the framework defines, each
// with what it is.
var notCodeNames = map[string]string{
	"COMPLETING":     "Slurm's job state flag",
	"KillWait":       "a Slurm setting",
	"MessageTimeout": "a Slurm setting",
	"Lease":          "a Kubernetes kind",
	"Warning":        "a Kubernetes event type",
	"MISSING":        "a build state hpcburst status prints",
	"UNREADABLE":     "a build state hpcburst status prints",
	"WRONG":          "a build state hpcburst status prints",
	"MISMATCH":       "a build state hpcburst status prints",
}

// degradedRanking is HBEP-7's ranking of the Degraded reasons (Events and conditions, The ranking),
// in issue #240's order, which the guide's headings follow. internal/lifecycle's
// TestDegradedRankingPairs holds the code to the same order.
var degradedRanking = []string{
	v1alpha1.ReasonSpecUnsupported, v1alpha1.ReasonPartitionTooShort, v1alpha1.ReasonFrontNotConfigured,
	v1alpha1.ReasonVerificationFailed, v1alpha1.ReasonIdentityUnresolved, v1alpha1.ReasonRestartLoop,
	v1alpha1.ReasonSubmissionRejected, v1alpha1.ReasonNoOverlapSlot, v1alpha1.ReasonSubmitLimitReached,
	v1alpha1.ReasonProviderUnavailable, v1alpha1.ReasonReplacementLate, v1alpha1.ReasonCapacityDropped,
}

// guideCapitalised returns every name in backticks in the guide's prose that is one capitalised
// word, such as `Degraded` or `JobCompleting`, once each.
func guideCapitalised(t *testing.T) []string {
	t.Helper()
	word := regexp.MustCompile("`([A-Z][A-Za-z0-9]*)`")
	seen := map[string]bool{}
	var out []string
	for _, s := range opsSegments(t) {
		if s.fenced {
			continue
		}
		for _, m := range word.FindAllStringSubmatch(s.text, -1) {
			if !seen[m[1]] {
				seen[m[1]] = true
				out = append(out, m[1])
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("found no name in backticks in the guide")
	}
	return out
}

// Every capitalised name in the guide is a constant of the framework, a known unexported one, or a
// name the framework does not define and the test says what it is; and each list names only what
// the guide still names.
func TestOperationsNamesAreConstants(t *testing.T) {
	inGuide := map[string]bool{}
	for _, n := range guideCapitalised(t) {
		inGuide[n] = true
		_, exported := guideNames[n]
		_, unexported := unexportedNames[n]
		_, other := notCodeNames[n]
		if !exported && !unexported && !other {
			t.Errorf("the guide names %s, which no list of this test holds: add it to guideNames with its constant", n)
		}
	}
	for n, v := range guideNames {
		if v != n {
			t.Errorf("guideNames: the guide's %s is held by a constant whose value is %q", n, v)
		}
		if !inGuide[n] {
			t.Errorf("guideNames holds %s, which the guide no longer names", n)
		}
	}
	for n, c := range unexportedNames {
		if v := stringConst(t, c.dir, c.ident); v != n {
			t.Errorf("%s.%s is %q; the guide names %s", c.dir, c.ident, v, n)
		}
		if !inGuide[n] {
			t.Errorf("unexportedNames holds %s, which the guide no longer names", n)
		}
	}
	for n := range notCodeNames {
		if !inGuide[n] {
			t.Errorf("notCodeNames holds %s, which the guide no longer names", n)
		}
	}
}

// stringConst returns the value of the string constant ident declared in the non-test Go files of
// the package at dir, read from the source, since an unexported constant cannot be referred to from
// here. It fails when the package declares no such constant.
func stringConst(t *testing.T, dir, ident string) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range af.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || g.Tok != token.CONST {
				continue
			}
			for _, sp := range g.Specs {
				vs := sp.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if n.Name != ident || i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Fatalf("%s.%s is not a string literal", dir, ident)
					}
					v, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatal(err)
					}
					return v
				}
			}
		}
	}
	t.Fatalf("%s declares no constant %s", dir, ident)
	return ""
}

// The Degraded section has one heading per reason, in HBEP-7's ranking, and each says what the
// reason means, what the controller does and what the operator can do, citing an HBEP (#240).
func TestOperationsDegradedSectionFollowsTheRanking(t *testing.T) {
	doc := readRepoFile(t, opsGuide)
	start := strings.Index(doc, "\n## Why a ModelDeployment is degraded\n")
	if start < 0 {
		t.Fatal("the guide has no section \"Why a ModelDeployment is degraded\"")
	}
	section := doc[start+1:]
	if end := strings.Index(section[3:], "\n## "); end >= 0 {
		section = section[:end+3]
	}
	heads := regexp.MustCompile("(?m)^### `([A-Za-z]+)`\n").FindAllStringSubmatchIndex(section, -1)
	var got []string
	for i, h := range heads {
		name := section[h[2]:h[3]]
		got = append(got, name)
		body := section[h[1]:]
		if i+1 < len(heads) {
			body = section[h[1]:heads[i+1][0]]
		}
		for _, part := range []string{"- **Means:**", "- **The controller", "- **You:**", "HBEP-"} {
			if !strings.Contains(body, part) {
				t.Errorf("the entry for %s has no %q", name, part)
			}
		}
	}
	if strings.Join(got, " ") != strings.Join(degradedRanking, " ") {
		t.Errorf("the Degraded headings are\n%v\nwant HBEP-7's ranking\n%v", got, degradedRanking)
	}
}

// Every link of the guide leads to a file of the repository, and every anchor to a heading of its
// file, by GitHub's rule for heading anchors.
func TestOperationsLinksResolve(t *testing.T) {
	doc := readRepoFile(t, opsGuide)
	n := 0
	for _, m := range regexp.MustCompile(`\]\(([^)#]*)(#[^)]*)?\)`).FindAllStringSubmatch(doc, -1) {
		if strings.Contains(m[1], "://") {
			continue
		}
		n++
		target := opsGuide
		if m[1] != "" {
			target = filepath.Join("docs", m[1])
			if _, err := os.Stat(filepath.Join(root, target)); err != nil {
				t.Errorf("link %q: %v", m[1], err)
				continue
			}
		}
		if m[2] == "" {
			continue
		}
		if fi, err := os.Stat(filepath.Join(root, target)); err != nil || fi.IsDir() {
			t.Errorf("link %q: an anchor into a directory", m[1]+m[2])
			continue
		}
		if !anchors(t, target)[strings.TrimPrefix(m[2], "#")] {
			t.Errorf("link %q: %s has no heading with that anchor", m[1]+m[2], target)
		}
	}
	if n == 0 {
		t.Fatal("found no link in the guide")
	}
}

// anchors returns the anchors GitHub gives the headings of a Markdown file: lowercased, with every
// character but letters, digits, spaces, hyphens and underscores dropped, and spaces made hyphens.
func anchors(t *testing.T, rel string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	drop := regexp.MustCompile(`[^a-z0-9 _-]`)
	for _, m := range regexp.MustCompile(`(?m)^#+ (.+)$`).FindAllStringSubmatch(readRepoFile(t, rel), -1) {
		out[strings.ReplaceAll(drop.ReplaceAllString(strings.ToLower(m[1]), ""), " ", "-")] = true
	}
	return out
}
