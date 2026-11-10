package quickstart

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The names the operator's guide gives (issue #240): every condition, reason, phase and mode is a
// real constant, and the Degraded section follows HBEP-7's ranking.

// guideNames is every condition, reason, phase and mode the guide names, with the constant that
// holds it (issue #240: every reason named is a real constant). The events the guide states and its
// Degraded headings are also held to HBEP-8's table and HBEP-7's ranking, as the drift guard (#239)
// parses them, by operations_drift_test.go. A name the guide drops must leave this list, and a name
// the guide adds must join it with its constant.
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
	"COMPLETING":         provider.FlagCompleting,

	// Provider conditions and reasons (HBEP-4; HBEP-11, Credentials).
	"ReadsFailing":        v1alpha1.ReasonReadsFailing,
	"LoginHostInUse":      v1alpha1.ReasonLoginHostInUse,
	"RelayFailed":         v1alpha1.EventReasonRelayFailed,
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

	// A failed check's reason code, in the message of QualificationFailed (HBEP-7, Instance start,
	// Reasoning models): pkg/agentapi's, as the agent reports it.
	"ReasoningBudgetExhausted": agentapi.ReasonReasoningBudgetExhausted,

	// Retention modes (HBEP-7, Retention).
	"Keep":            string(v1alpha1.RetentionKeep),
	"ReleaseWhenIdle": string(v1alpha1.RetentionReleaseWhenIdle),
}

// unexportedNames are names the guide gives whose constant is unexported in its package, so this
// package cannot refer to it: each is checked by its identifier in that package's source.
var unexportedNames = map[string]struct{ dir, ident string }{
	"JobHeldAdmin":     {"internal/providerctl", "reasonAdminHold"},
	"NotHeldByAdmin":   {"internal/providerctl", "reasonNoAdminHold"},
	"HeartbeatMissed":  {"internal/lifecycle", "reasonHeartbeatMissed"},
	"ObservationGap":   {"internal/lifecycle", "reasonObservationGap"},
	"PathProbeFailing": {"internal/front", "reasonPathProbeFailing"},
}

// notCodeNames are capitalised names the guide gives that no code of the framework defines, each
// with what it is.
var notCodeNames = map[string]string{
	"KillWait":       "a Slurm setting",
	"MessageTimeout": "a Slurm setting",
	"Lease":          "a Kubernetes kind",
	"Warning":        "a Kubernetes event type",
	"MISSING":        "a build state hpcburst status prints",
	"UNREADABLE":     "a build state hpcburst status prints",
	"WRONG":          "a build state hpcburst status prints",
	"MISMATCH":       "a build state hpcburst status prints",
	"KiB":            "a unit hpcburst status prints",
	"MiB":            "a unit hpcburst status prints",
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

// guideCapitalised returns, once each, every name in backticks in the guide's prose that is one
// capitalised word, such as `Degraded`, and every CamelCase word inside a longer span, such as the
// reasons of a quoted message: `SubmitLimitReached: <message> | CapacityDropped: ...`.
func guideCapitalised(t *testing.T) []string {
	t.Helper()
	span := regexp.MustCompile("`([^`\n]+)`")
	single := regexp.MustCompile(`^[A-Z][A-Za-z0-9_]*$`)
	camel := regexp.MustCompile(`^[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]*)+$`)
	seen := map[string]bool{}
	var out []string
	add := func(n string) {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	for _, s := range opsSegments(t) {
		if s.fenced {
			continue
		}
		for _, m := range span.FindAllStringSubmatch(s.text, -1) {
			if single.MatchString(m[1]) {
				add(m[1])
				continue
			}
			for _, w := range strings.FieldsFunc(m[1], func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
				if camel.MatchString(w) {
					add(w)
				}
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
		state := slices.Contains(slurmStates, n) // Slurm's state codes, from its man pages (operations_slurm_test.go)
		if !exported && !unexported && !other && !state {
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
