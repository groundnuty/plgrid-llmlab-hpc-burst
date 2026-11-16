package lifecycle

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The throughput gate's judgement (HBEP-7, Over-allocation and qualification
// (beta), The throughput gate; HBEP-4, Expected performance). It judges the
// warm-up's throughput of each worker against the entry of the Provider's
// platform profile that matches the Instance. This file is the pure part, the
// key, the matching, the limits and the verdict; gate_act.go acts on it: a
// first miss restarts the worker once, a second fails the Instance.

const (
	// gateAge is how long an entry gates from its date (HBEP-4, Ageing and
	// refresh): it records only from 00:00:00 UTC of the 90th day after it.
	gateAge = 90 * 24 * time.Hour
	// gateTTFTFloor is the absolute floor of the time-to-first-token limit
	// (HBEP-7, The limits): one interval per request, so it gets warm-up's
	// 100 ms floor.
	gateTTFTFloor = 100 * time.Millisecond
	// gateDateLayout is an entry's date, a calendar date read as UTC.
	gateDateLayout = "2006-01-02"
	// gateMarker starts the gate's part of the Qualified condition's message.
	// The gate rewrites everything from it, so its message stays one sentence
	// however often the gate judges.
	gateMarker = "; throughput gate: "
)

// ranShape is the concurrent shape one worker's warm-up ran: the sessions
// (requests at once), and the typical context and output in tokens.
type ranShape struct{ sessions, context, output int32 }

// instanceKey is the key an Instance is matched on (HBEP-7, The throughput
// gate: its reference): its deployment ID in display form, its capacity flags
// from the immutable launch, and the concurrent shape one worker ran. The shape
// is what the warm-up ran (gate_shape.go), not the deployment's workload as it
// is declared now: an edit during a warm-up must not key a measurement on a
// shape it never ran, and a miss can release an Allocation.
func instanceKey(in *v1alpha1.Instance, shape ranShape) v1alpha1.ReferenceKey {
	return v1alpha1.ReferenceKey{
		DeploymentID:  in.Spec.DeploymentID.Display(),
		CapacityFlags: launchCapacityFlags(in.Spec.Launch),
		Sessions:      shape.sessions,
		ContextTokens: shape.context,
		OutputTokens:  shape.output,
	}
}

// launchCapacityFlags reads the capacity flags of a launch's command, as
// renderLaunch writes it: --name=value, and a boolean as --name (true) or
// --no-name (false). Only the flags HBEP-3's table classes as capacity are
// kept; a flag given twice takes its last value, as argparse does.
// max-num-seqs is always present, from launch.maxNumSeqs, never from the
// command (the Launch's own rule makes it the one source).
func launchCapacityFlags(l v1alpha1.Launch) map[string]v1alpha1.FlagValue {
	flags := map[string]v1alpha1.FlagValue{}
	for _, arg := range l.Command {
		if name, value, ok := capacityArg(arg); ok {
			flags[name] = v1alpha1.FlagValue(value)
		}
	}
	flags["max-num-seqs"] = v1alpha1.FlagValue(strconv.Itoa(int(l.MaxNumSeqs)))
	return flags
}

// capacityArg reads one argument of a rendered command as a capacity flag:
// its name, normalised, and its value; ok is false for anything else (the
// program, a value that follows a flag, a flag that is no capacity flag).
func capacityArg(arg string) (name, value string, ok bool) {
	body, isFlag := strings.CutPrefix(arg, "--")
	if !isFlag {
		return "", "", false
	}
	if n, v, hasValue := strings.Cut(body, "="); hasValue {
		return flagName(n), v, deploymentid.IsCapacityFlag(n)
	}
	if deploymentid.IsCapacityFlag(body) {
		return flagName(body), "true", true
	}
	if positive, negated := strings.CutPrefix(body, "no-"); negated && deploymentid.IsCapacityFlag(positive) {
		return flagName(positive), "false", true
	}
	return "", "", false
}

// describeKey is the key in a message: what no entry matched.
func describeKey(k v1alpha1.ReferenceKey) string {
	names := make([]string, 0, len(k.CapacityFlags))
	for n := range k.CapacityFlags {
		names = append(names, n)
	}
	sort.Strings(names)
	flags := make([]string, len(names))
	for i, n := range names {
		flags[i] = n + "=" + string(k.CapacityFlags[n])
	}
	return fmt.Sprintf("deployment ID %s, capacity flags %s, %d sessions, %d context tokens, %d output tokens",
		k.DeploymentID, strings.Join(flags, " "), k.Sessions, k.ContextTokens, k.OutputTokens)
}

// sameKey reports whether an entry's key is an Instance's: the same deployment
// ID and shape, and the same set of capacity flags with each value equal by
// sameSetting, so 0.9 is 0.90 and true is True (HBEP-4, Expected performance:
// compared by value as verification compares settings).
func sameKey(entry, instance v1alpha1.ReferenceKey) bool {
	if entry.DeploymentID != instance.DeploymentID || entry.Sessions != instance.Sessions ||
		entry.ContextTokens != instance.ContextTokens || entry.OutputTokens != instance.OutputTokens ||
		len(entry.CapacityFlags) != len(instance.CapacityFlags) {
		return false
	}
	for name, want := range entry.CapacityFlags {
		got, ok := instance.CapacityFlags[name]
		if !ok || !sameSetting(name, string(got), string(want)) {
			return false
		}
	}
	return true
}

// reference is what matching an Instance to the profile's entries found.
type reference struct {
	// entry is the one entry that gates; nil when the gate records only.
	entry *v1alpha1.ExpectedPerformance
	// date is the matched entry's date, set also when it is aged.
	date string
	// why says why the gate records only.
	why string
}

// matchReference finds the entry that gates the Instance with key (HBEP-7, The
// throughput gate: no reference, no gate). One match is the reference. No
// match, two or more (an ambiguous profile never gates), and a match that is
// 90 days or more past its date, the clock being now, record only, each
// saying why. An entry whose date does not parse matches nothing, and the
// message names it.
func matchReference(profile *v1alpha1.PlatformProfile, key v1alpha1.ReferenceKey, now time.Time) reference {
	var matched []*v1alpha1.ExpectedPerformance
	var unreadable []string
	if profile != nil {
		for i := range profile.Spec.ExpectedPerformance {
			e := &profile.Spec.ExpectedPerformance[i]
			switch _, err := time.Parse(gateDateLayout, e.Date); {
			case !sameKey(e.ReferenceKey, key):
			case err != nil:
				unreadable = append(unreadable, strconv.Quote(e.Date))
			default:
				matched = append(matched, e)
			}
		}
	}
	switch len(matched) {
	case 0:
		why := "no entry matches " + describeKey(key)
		if len(unreadable) > 0 {
			why += "; an entry with this key has a date that does not parse: " + strings.Join(unreadable, ", ")
		}
		return reference{why: why}
	case 1:
		e := matched[0]
		date, _ := time.Parse(gateDateLayout, e.Date) // read above
		if !now.Before(date.Add(gateAge)) {
			return reference{date: e.Date, why: fmt.Sprintf("the matching entry dated %s is 90 days or more old", e.Date)}
		}
		return reference{entry: e, date: e.Date}
	}
	dates := make([]string, len(matched))
	for i, e := range matched {
		dates[i] = e.Date
	}
	sort.Strings(dates)
	return reference{why: fmt.Sprintf("%d entries match (dated %s), so none is the reference", len(matched), strings.Join(dates, " and "))}
}

// gateLimits are an entry's limits (HBEP-7, The limits), in integer nanoseconds.
type gateLimits struct{ tpot, ttft time.Duration }

// limitsOf computes the limits of an entry with tolerance t percent: time per
// output token passes at or below the entry's × (100 + t) ÷ 100, and time to
// first token at or below the larger of that and the entry's plus 100 ms.
func limitsOf(e *v1alpha1.ExpectedPerformance) (gateLimits, error) {
	tpot, err := e.TimePerOutputToken.AsDuration()
	if err != nil {
		return gateLimits{}, fmt.Errorf("the entry's timePerOutputToken %q does not parse", e.TimePerOutputToken)
	}
	ttft, err := e.TimeToFirstToken.AsDuration()
	if err != nil {
		return gateLimits{}, fmt.Errorf("the entry's timeToFirstToken %q does not parse", e.TimeToFirstToken)
	}
	pct := 100 + int64(e.TolerancePercentOf())
	floor := ttft + gateTTFTFloor
	if ttft > math.MaxInt64-gateTTFTFloor {
		floor = math.MaxInt64
	}
	return gateLimits{tpot: scaled(tpot, pct), ttft: max(scaled(ttft, pct), floor)}, nil
}

// scaled is d × pct ÷ 100 in integer arithmetic, rounded down, so a limit
// never rounds across a measurement; it saturates at the largest Duration.
func scaled(d time.Duration, pct int64) time.Duration {
	whole, rest := int64(d)/100, int64(d)%100
	if whole > math.MaxInt64/pct {
		return math.MaxInt64
	}
	return time.Duration(whole*pct + rest*pct/100)
}

// workerMeasure is one worker's own measurement: its settled warm-up round's
// mean time per output token and mean time to first token. Never the mean of
// the workers', which would hide one slow GPU (HBEP-7, The throughput gate).
type workerMeasure struct {
	worker     string
	tpot, ttft time.Duration
}

// measuresOf reads each worker's throughput, nil when one is missing or
// unreadable, since nothing can then be judged.
func measuresOf(workers []string, tps []*v1alpha1.Throughput) []workerMeasure {
	if len(tps) != len(workers) {
		return nil
	}
	out := make([]workerMeasure, len(workers))
	for i, tp := range tps {
		if tp == nil {
			return nil
		}
		tpot, errA := tp.TimePerOutputToken.AsDuration()
		ttft, errB := tp.TimeToFirstToken.AsDuration()
		if errA != nil || errB != nil {
			return nil
		}
		out[i] = workerMeasure{worker: workers[i], tpot: tpot, ttft: ttft}
	}
	return out
}

// misses reports whether a measurement is above either limit.
func (l gateLimits) misses(m workerMeasure) bool { return m.tpot > l.tpot || m.ttft > l.ttft }

// gateVerdict is what the gate judged.
type gateVerdict struct {
	// status is the final verdict, GatePassed, GateSuspended or GateRecordOnly; empty for a miss, which the acting
	// path (gate_act.go) decides on, so that no miss is recorded as a final-looking Missed.
	status string
	// date is the matched entry's date, empty when none matched.
	date string
	// text is the gate's part of the Qualified message for a final verdict.
	text string
	// missed names the workers over a limit, in worker order, for a miss; detail then holds every worker's
	// measurement and its limits.
	missed []string
	detail string
	limits gateLimits
}

// judgeGate judges the workers' measurements, nil when they could not be read,
// against the profile's matching entry, the clock being now. The key's sessions
// are each worker's share. suspended says whether the matched entry, by its own
// key and its date, is suspended for the deployment (suspension.go): one that is
// records only.
func judgeGate(profile *v1alpha1.PlatformProfile, key v1alpha1.ReferenceKey, ms []workerMeasure, now time.Time, suspended func(entry v1alpha1.ReferenceKey, date string) bool) gateVerdict {
	ref := matchReference(profile, key, now)
	if ref.entry == nil {
		return recordOnly(ref.date, ref.why)
	}
	if suspended != nil && suspended(ref.entry.ReferenceKey, ref.date) {
		return gateVerdict{status: v1alpha1.GateSuspended, date: ref.date,
			text: fmt.Sprintf("records only: the entry dated %s is suspended for this deployment, since two Allocations in a row missed it", ref.date)}
	}
	limits, err := limitsOf(ref.entry)
	switch {
	case err != nil:
		return recordOnly(ref.date, err.Error())
	case ms == nil:
		return recordOnly(ref.date, "a worker's warm-up gave no readable measurement")
	}
	var parts, missed []string
	for _, m := range ms {
		parts = append(parts, measurementText(m, limits, len(ms) > 1))
		if limits.misses(m) {
			missed = append(missed, m.worker)
		}
	}
	detail := strings.Join(parts, "; ")
	if len(missed) == 0 {
		return gateVerdict{status: v1alpha1.GatePassed, date: ref.date, text: fmt.Sprintf("passed the entry dated %s: %s", ref.date, detail)}
	}
	return gateVerdict{date: ref.date, missed: missed, detail: detail, limits: limits}
}

// measurementText is one worker's measurement and the limits it was judged against, as the gate's messages print
// them; a worker's name labels it when the Instance has several.
func measurementText(m workerMeasure, l gateLimits, labelled bool) string {
	label := ""
	if labelled {
		label = m.worker + ": "
	}
	return fmt.Sprintf("%stime per output token %s (limit %s), time to first token %s (limit %s)", label, m.tpot, l.tpot, m.ttft, l.ttft)
}

func recordOnly(date, why string) gateVerdict {
	return gateVerdict{status: v1alpha1.GateRecordOnly, date: date, text: "records only: " + why}
}

// withGate sets the gate's part of a Qualified message, keeping the checks' text.
func withGate(message, gate string) string {
	checks, _, _ := strings.Cut(message, gateMarker)
	return checks + gateMarker + gate
}

// qualifiedMessage is the Qualified condition's message when the checks have
// just passed: their text, and where the gate stands. Before it has judged it
// says so; once it has, a later qualification (a whole-Instance restart) says
// the verdict stands and later warm-ups are recorded only.
func qualifiedMessage(in *v1alpha1.Instance, checks string) string {
	if g := in.Status.Gate; g != nil && g.Verdict != "" {
		return withGate(checks, fmt.Sprintf("judged on the first qualification (%s); later warm-ups are recorded only", g.Verdict))
	}
	return withGate(checks, "judged after warm-up")
}
