package alpha

// The baseline row of HBEP-764's item 4 (issue #764, test plan "A baseline row first"): Row 1, beta-sano-llmlab-baseline. It runs today's code with no
// Workload: the same small model, Qwen/Qwen3-VL-8B-Instruct, served on one Sano A100 and on LLMLab, with LLMLab attached as HBEP-5's overflow. It shows
// the routing at full load before any amendment, and gives the numbers that #769's token budget is later compared against. Row 2 (Athena) is not built.
//
// The science agent's rulings of 18 Nov 2026 on #764 (comment after 6982178166) that bind it:
//   - H0: the declared shape in P1 and P2, and P1-long, a long phase with no LLMLab traffic that reads the engine's KV use and preemptions at the cap;
//   - H1: waits at the front are "0 by construction" while LLMLab is attached, and the post labels them so;
//   - H2: P3 detaches LLMLab, by deleting the row's managed ModelDeployment, and two sessions over the caps wait in the front's FIFO;
//   - H3: the cap rule floor(0.95 x KV / 32768), Sano 11, Athena 3, k = 2;
//   - H4: the model as shipped, with no --language-model-only.
//
// # The phases, each a config of the row
//
// A phase is a Phase value: its session count N and its shape (context, turn and output tokens, turns, think time, stagger), run by the load generator
// (hack/studyload of #768, by its binary: BaselineSettings.StudyloadBin). DefaultPhases are the defaults; HPCBURST_ALPHA_BASELINE_PHASES overrides a
// phase's fields (PhasesFromEnv), and a change there reaches the generator's config (StudyConfig) and nothing else.
//   - P0, before the GPU job: one 8-token request at the plain model, answered by LLMLab (emitter provider);
//   - the gate: the served Instance routable, by one request at model@<provider>, answered by the Instance;
//   - P1, the control: N = cap, the declared shape (1024 words of context, 128 of turn, 128 of output, 24 turns, 100 ms of think time, no stagger),
//     LLMLab gets none;
//   - P1-long: N = cap, contexts of 24k to 28k tokens, LLMLab gets none; the engine's KV use, its requests waiting and its preemptions are READ at the cap and
//     posted (ruling H0: R4's calibration), never failed on. The generator sends one word per intended token, so the words that reach 26,000 tokens are set
//     from a MEASURED ratio: after the gate, ONE probe at the served Instance alone (model@<provider>, which cannot spill) sends RatioProbeWords words with
//     one output token, the front's usage record gives the prompt tokens, and LongContextWords sets the words. If no whole number of words gives 24,000 to
//     28,672 tokens at that ratio, P1-long is INVALID before it starts. This sets the SHAPE of a phase, never a cap: the caps stay constants fixed before the
//     run (a cap set from what the run itself measures proves nothing);
//   - P2, full load: N = cap + k; LLMLab gets EXACTLY k first-turn requests, with at most k in flight, and at each of its arrivals the Instance held
//     its cap;
//   - P3, detach: the managed ModelDeployment is deleted, then N = cap + 2; two requests wait in the front's FIFO under queueTimeout. A 503 makes it
//     INVALID.
//
// # The caps (the science agent's ruling H3 on #764)
//
// The cap is a constant, fixed before the run from the model's facts, and the row reads no KV to set it. After the Instance is ready it reads the
// engine's logged KV as a check only: a logged KV below cap x 32768 ends the run INVALID before any phase.
//
//	cap = floor(0.95 x KV / 32768)
//	Sano:   KV = 389,120 tokens as shipped (24,320 blocks of 16; 53.4375 GiB at 144 KiB a token; the first load on Sano, #764 comment 6982083163, confirmed by the agent's own reading of the same figure), so
//	        0.95 x 11.875 = 11.28 and the cap is 11. It stays 11 for any reading from 379,419 to 413,911 tokens.
//	Athena: derived (nothing has measured this model on an A100 40 GB). Sano's memory beside the KV cache is 0.92 x 80 GiB - 53.4375 GiB = 20.1625
//	        GiB, assumed the same on the 40 GB card, so KV = 0.92 x 40 GiB - 20.1625 GiB = 16.6375 GiB, about 121,150 tokens, 3.70 sessions, and
//	        0.95 x 3.70 = 3.51: the cap is 3.
//
// The engine's readings are the agent's, from vLLM's own series: vllm:kv_cache_usage_perc (internal/agent/engine/metrics.go:44), the fraction of the KV
// cache in use, which the controller exports as hpcburst_instance_kv_cache_usage_ratio (internal/lifecycle/metrics.go:52); vllm:num_preemptions_total
// (metrics.go:48), hpcburst_instance_preemptions_total; and vllm:num_requests_running and vllm:num_requests_waiting (metrics.go:41-42).
//
// The 5% margin covers a node with less free memory, the agent's launch reading below the hand one, and Athena's figure being derived. Every session
// up to max-model-len then fits, so the engine cannot preempt below the cap, and the front's cap decides the spill. TestTheCapsAreComputedFromTheirInputs
// computes both from those inputs.
//
// # What each measurement is later compared against: #769
//
// #769 (branch hbep-764-amend, head 9cc8368d, "HBEP-3/5/7/8/11: Workloads") makes the front count in tokens per Workload. The baseline measures today's
// rule on the same model, so that the two can be put side by side. Each line names the measurement, the #769 rule, and where its text is (file at that
// head, line). Mapping769 is the same table, and the post prints it.
//
//   - The count cap (P1 and P2: the cap binds at N = cap + k; the engine's running requests stay at or under the cap), against the integer budget,
//     which reduces to today's count cap: hbep/0005-llmlab-compatible-front/README.md:314-318 ("Both are integer comparisons, so nothing rounds. With no
//     Workload request in flight, N is 0, and the second condition is u + 1 <= cap, the count rule exactly").
//   - The cap's value and the engine's KV (P1-long: the engine's KV use at the cap, and its preemptions), against the budget B = kvCacheTokens times the
//     workers, charged at the Workload's typical shape: README.md:314-316 and hbep/0007-lifecycle-controller/README.md:302 (the agent reads
//     kv_cache_size_tokens). The long phase checks the declared charge against the real use (README.md:322, "The charge is the declared typical shape,
//     not a request's real size").
//   - Waits at the front (P3's, measured; the others "0 by construction with LLMLab attached"), against the FIFO per Workload: README.md:313 ("waits in
//     that Workload's own FIFO ... in place of a ModelDeployment's") where today's is one FIFO per ModelDeployment, README.md:286 ("the request waits in
//     the ModelDeployment's FIFO at the front"), and hbep/0008-observability-and-events/README.md:340 (hpcburst_front_waiting).
//   - The cap and the tight bound at full membership (the gate and every phase run with ready 1 = spec.instances 1, so the cap is in force:
//     README.md:280-282), against the budget in force at partial membership: README.md:319 ("At partial membership (ready below desired), the count cap
//     is off as today, and so is the second condition. The first stays in force").
//   - The in-flight count per Instance (from the front's usage records, by instanceUID), against hpcburst_front_in_flight{workload, provider,
//     model_deployment, instance_uid}: hbep/0008-observability-and-events/README.md:339.
//   - The speed per session (p50 and minimum), against hpcburst_front_output_tokens_per_second: hbep/0008-observability-and-events/README.md:341.
//   - LLMLab's unbounded overflow (the guard at 144 requests a phase is this row's own), against the study's bound, managedSessions: README.md:308 and
//     :310 ("The front can't see LLMLab's capacity, so managedSessions is the study's own bound on it").
//   - A session stays on its Instance (P1: each session's requests answered by one Instance), against pinning within a Workload's own deployments:
//     README.md:306.
//   - The served ModelDeployment's workload block drives warm-up and qualification only, and the baseline names no Workload, against Workloads as routing
//     inputs only: README.md:323 ("routing inputs only. Warm-up and qualification still use the ModelDeployment's own workload block") and
//     hbep/0764-single-study-workloads/README.md:82 (R1).
//   - Today's count with no kvCacheTokens known, against the budget's absence of it: README.md:321 ("Without kvCacheTokens ... the count alone applies,
//     as today").
//
// The figures R4's charge would give on Sano, for comparison: the declared typical shape (1,024 words of context plus 128 of output, 1,152 tokens)
// against KV 389,120 is 337 sessions, capped at max-num-seqs 256, where the baseline's cap is 11.
//
// # Reuse
//
// New code, standard library and this repository's packages only. The load generator is #768's hack/studyload (design from AIPerf and vLLM's
// multi_turn benchmark, both Apache-2.0; see its README), run as a binary and never imported.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The baseline row's name and the model it serves.
const (
	BetaBaselineName = "beta-sano-llmlab-baseline"
	// BaselineModel is the plain Hub ID every route answers under: the served ModelDeployment's spec.model, the managed one's, and a client's model.
	BaselineModel = "Qwen/Qwen3-VL-8B-Instruct"
	// BaselineDeploymentID is the display form of the deployment ID (HBEP-3) that the served example derives, 12 hex characters, as #735's rows PR fixed it
	// with the example: the Instance's own ID must equal it, so that the row serves what the example says.
	BaselineDeploymentID = "965fa2409193"
	// BaselineRevision is the checkpoint the served example pins, the first load's (#764).
	BaselineRevision = "0c351dd01ed87e9c1b53cbc748cba10e6187ff3b"
	// SanoQwen3VLDeployment is the example ModelDeployment the row serves (Row.Deployment).
	SanoQwen3VLDeployment = "modeldeployment-sano-qwen3-vl-8b-instruct.yaml"
	// BaselineTracker is where the row posts: #764's own work.
	BaselineTracker = "#764"
	// LLMLabProviderName and LLMLabDeploymentName are the managed Provider's and its ModelDeployment's names in the run's stack. The stack is the
	// run's alone, so the names are fixed, as the examples have them.
	LLMLabProviderName   = "llmlab"
	LLMLabDeploymentName = "llmlab-qwen3-vl"
)

// The inputs of the caps (the file's comment, "The caps").
const (
	// KVSano is the engine's kv_cache_size_tokens on one Sano A100 80 GB as shipped, from a hand launch (the first load on Sano, #764 comment 6982083163), and the agent's own reading of it was the same (team lead, 18 Nov 2026), so the caps stand
	// (the science agent's ruling H3).
	KVSano = 389_120
	// MaxModelLen is the deployment's max-model-len.
	MaxModelLen = 32_768
	// CapMarginPercent is the share of the engine's KV the cap counts on: the 5% margin.
	CapMarginPercent = 95
	// KVBytesPerToken is Qwen3-VL-8B's text decoder: 36 layers x 8 KV heads x 128 x K and V x 2 bytes (the model's config.json, #764 comment 6982083163).
	KVBytesPerToken = 147_456
	// GPUMemoryUtilizationPercent is vLLM v0.32.0's default --gpu-memory-utilization, 0.92 (vllm/config/cache.py:102).
	GPUMemoryUtilizationPercent = 92
	// GiB is a gibibyte in bytes.
	GiB = 1 << 30
)

// The caps, fixed before the run (CapOf is the rule). Sano 11 and Athena 3; k = 2.
const (
	CapSano   = 11
	CapAthena = 3
	// LLMLabK is k: how many sessions beyond the caps P2 runs, which LLMLab answers.
	LLMLabK = 2
	// LLMLabGuard is the most requests LLMLab may be asked in one phase: 3 x k x 24 turns. Past it the phase is stopped and FAILS.
	LLMLabGuard = 144
)

// CapOf is the cap rule: floor(0.95 x kv / maxModelLen), in integers.
func CapOf(kv, maxModelLen int64) int64 { return kv * CapMarginPercent / 100 / maxModelLen }

// KVAthenaDerived is the Athena A100 40 GB's KV cache in tokens, derived from Sano's measured one: the memory beside the KV cache on Sano (0.92 x 80
// GiB less the KV cache) is assumed the same on the 40 GB card.
func KVAthenaDerived() int64 {
	besideKV := int64(80)*GiB*GPUMemoryUtilizationPercent/100 - int64(KVSano)*KVBytesPerToken
	return (int64(40)*GiB*GPUMemoryUtilizationPercent/100 - besideKV) / KVBytesPerToken
}

// What P0, the gate and the phases ask of the front.
const (
	// P0Tokens is the length of P0's request.
	P0Tokens = 8
	// LongPromptMin and LongPromptMax bound P1-long's first-turn prompts, in the front's own count (promptTokens): 24k tokens to the deployment's
	// declared longest context, 28,672 (the served example's workload block). The generator sizes in words, so the size is read, never assumed.
	LongPromptMin = 24_000
	LongPromptMax = 28_672
	// EngineProbeInterval is Sano's health.probeInterval (profiles/sano.yaml:143): the agent reads the engine's metrics once an interval, and the controller's
	// gauges repeat the last read between reads, so a scrape every BaselinePoll sees the same read several times. The agent's own read time
	// (hpcburst_instance_metrics_read_timestamp_seconds, internal/lifecycle/metrics.go:132) tells the reads apart, and only DISTINCT reads are samples.
	EngineProbeInterval = 10 * time.Second
	// EngineReadLag is how long before the first scrape that saw it a read may have been taken: the agent's report reaches the controller up to a probe
	// interval after the read. A read is placed by the time of the scrape that first saw it, so it counts as taken inside a stretch only when that scrape was
	// more than this after the stretch began: a stated limit, never a read taken before the stretch posted as at it.
	EngineReadLag = EngineProbeInterval
	// EngineWaitingSustain is how many consecutive DISTINCT engine reads with requests waiting fail a phase: 3, which at Sano's 10 s probe interval is about
	// 20 to 30 s of waiting. N sessions that start together can have their first prefills briefly above the engine's batched-token budget, so a read or two with
	// requests waiting is no sign that the engine is the bottleneck: such a blip is a reading, posted with its count and its longest run (the science agent's
	// ruling of 18 Nov 2026 on #764's row, and its ruling on #776's review, point 7: the window is in reads, not in polls). Running above the cap, or
	// preemptions rising, fail at any read.
	EngineWaitingSustain = 3
	// RatioProbeWords is the context, in words, of the one probe that measures the tokens a word comes to on the engine's own tokenizer, PhaseRatio, and
	// LongTargetTokens the length P1-long's contexts are set to reach, 26,000 tokens, the middle of LongPromptMin to LongPromptMax. The probe sets the SHAPE of
	// P1-long, never a cap, so a cap set from what the run measures, which proves nothing, is not what this is: the caps stay constants fixed before the run.
	RatioProbeWords  = 2000
	LongTargetTokens = 26_000
	// maxStudyWords is the most words the generator takes in one context (hack/studyload/config.go, maxTokens).
	maxStudyWords = 1 << 20
	// ArrivalTick is the tolerance of the cap-spill rule: one tick of the usage records' timestamps, a millisecond (internal/front/usage, Time). A P2 request
	// that LLMLab answered is a cap spill only if the front's records show at least the cap's requests in flight on the Instance at its StartTime or within one
	// tick after it (#776's review, point 5; the science agent's and team lead's rulings). StartTime is the arrival stamp, taken before the route is chosen
	// (internal/front/request.go:78), and a request decided late can be stamped a tick earlier than one it spilled past; EndTime is stamped in finish() after the
	// reservation's release (proxy.go:238, :595), so the records never show fewer in flight than the front held, which is why the rule asks for AT LEAST the cap.
	// A request that ends at an instant is released at it. MEASURED on a real-clock front harness (one Instance with routing.instanceCap 11 and the LLMLab double;
	// 13 simultaneous first turns of 13 sessions, the front's own usage records, 200 runs and 400 cap spillers): with the one-tick tolerance 0 of 400 spillers
	// were rejected; with none 45 of 400 (11%), in 36 of 200 runs (18%); the least tolerance that passed every one was 1 ms. The reviewer's case, no request of ours
	// in flight at the arrival and all 11 arriving 50 ms later, is rejected. What the rule cannot do alone is tell a request resent to LLMLab after a failed dial
	// of the Instance from an overflow, since the record names only the final target: the front's own log does (DialFailedRequests, guard 2).
	ArrivalTick = time.Millisecond
	// OverCapTolerance is how long the records may show more than the cap in flight on the Instance outside the detach phase: a record's EndTime is stamped
	// after the reservation's release, so a request that has been released still shows for a moment (internal/front/proxy.go:238-239, :595).
	OverCapTolerance = 100 * time.Millisecond
)

// BaselineRow is Row 1: one job of one A100 on Sano's gpu partition under host-dialed mode, the 106-minute lease, posting to #764, with the cap, the
// managed LLMLab leg and the generator.
func BaselineRow() Row {
	return Row{Name: BetaBaselineName, Cluster: "sano", Partition: "gpu", GPUs: 1, Lease: MinLease(), Budget: 150 * time.Minute, MostJobs: 1,
		PostsTo: BaselineTracker, Deployment: SanoQwen3VLDeployment, ProviderStaging: true, InstanceCap: CapSano, LLMLab: true, Studyload: true}
}

// IsBaselineRow says whether a row is the baseline row; the dispatch of Run, WorstCase and the dry steps take it.
func IsBaselineRow(name string) bool { return name == BetaBaselineName }

// PhaseKind says what a phase asks of the routing.
type PhaseKind string

// The kinds of phase.
const (
	// KindControl gives LLMLab none: N = cap.
	KindControl PhaseKind = "control"
	// KindLong is the control with long contexts, reading the engine's KV use at the cap.
	KindLong PhaseKind = "long"
	// KindFull is N = cap + k: LLMLab gets exactly k first-turn requests.
	KindFull PhaseKind = "full"
	// KindDetach deletes the managed ModelDeployment, then runs N = cap + 2.
	KindDetach PhaseKind = "detach"
	// KindProbe is the ratio probe: one session of one turn and one output token at the served Instance alone (model@<provider>, so it cannot spill).
	KindProbe PhaseKind = "probe"
)

// Phase is one phase of the row, a config: its name, what it asks, the session count and the shape of every session, and how long it may run.
// Sizes are in words, as the generator takes them (one word per intended token; the front's promptTokens say what the tokenizer made of them).
type Phase struct {
	Name     string        `json:"name"`
	Kind     PhaseKind     `json:"kind"`
	Sessions int           `json:"sessions"`
	Turns    int           `json:"turns"`
	Context  int           `json:"contextTokens"`
	Turn     int           `json:"turnTokens"`
	Output   int           `json:"outputTokens"`
	Think    time.Duration `json:"thinkTime"`
	Stagger  time.Duration `json:"startStagger"`
	// Bound is how long the generator may run; past it the phase is stopped and FAILS.
	Bound time.Duration `json:"bound"`
	// TargetTokens, when set, says the context is not given in words but derived from the tokens a word came to in the ratio probe, so that the first turns'
	// prompts reach this many tokens: the words are then resolved by the sequencer (LongContextWords) and Context is 0 until they are.
	TargetTokens int `json:"targetTokens,omitempty"`
	// OnServed says the generator addresses the served Instance alone, as model@<provider>, which cannot spill to LLMLab: the ratio probe's.
	OnServed bool `json:"onServed,omitempty"`
}

// The phases' names.
const (
	PhaseP1     = "P1"
	PhaseP1Long = "P1-long"
	PhaseP2     = "P2"
	PhaseP3     = "P3"
	// PhaseRatio names the ratio probe, which is no phase of the run's reading (resolveLongShape).
	PhaseRatio = "ratio"
)

// DefaultPhases are the phases with their defaults, for a Sano cap: the declared shape in P1, P2 and P3, long contexts in P1-long.
func DefaultPhases(cap int) []Phase {
	declared := Phase{Turns: 24, Context: 1024, Turn: 128, Output: 128, Think: 100 * time.Millisecond, Bound: 10 * time.Minute}
	mk := func(name string, kind PhaseKind, n int) Phase {
		p := declared
		p.Name, p.Kind, p.Sessions = name, kind, n
		return p
	}
	long := mk(PhaseP1Long, KindLong, cap)
	long.Turns, long.Context, long.Turn, long.Output, long.Think, long.Bound = 2, 0, 128, 128, time.Second, 15*time.Minute
	long.TargetTokens = LongTargetTokens // the words come from the ratio probe
	return []Phase{mk(PhaseP1, KindControl, cap), long, mk(PhaseP2, KindFull, cap+LLMLabK), mk(PhaseP3, KindDetach, cap+2)}
}

// phaseEnv is a phase's override: only the fields that are set replace the phase's.
type phaseEnv struct {
	Sessions *int    `json:"sessions"`
	Turns    *int    `json:"turns"`
	Context  *int    `json:"contextTokens"`
	Turn     *int    `json:"turnTokens"`
	Output   *int    `json:"outputTokens"`
	Think    *string `json:"thinkTime"`
	Stagger  *string `json:"startStagger"`
	Bound    *string `json:"bound"`
	// TargetTokens replaces the tokens the long phase's contexts are set to reach. An explicit contextTokens replaces the target: the words are then given.
	TargetTokens *int `json:"targetTokens"`
}

// PhasesFromEnv applies HPCBURST_ALPHA_BASELINE_PHASES to the defaults: a JSON object from a phase's name to the fields it replaces, durations as
// Go duration strings. An unknown phase or field, and a shape the generator would refuse, is an error naming it. An empty value is no override.
func PhasesFromEnv(base []Phase, v string) ([]Phase, error) {
	out := append([]Phase(nil), base...)
	if strings.TrimSpace(v) == "" {
		return out, nil
	}
	var over map[string]phaseEnv
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&over); err != nil {
		return nil, fmt.Errorf("%s is not a JSON object of phase names to known fields: %w", envBaselinePhases, err)
	}
	for name, o := range over {
		i := -1
		for j := range out {
			if out[j].Name == name {
				i = j
			}
		}
		if i < 0 {
			return nil, fmt.Errorf("%s names no phase %q", envBaselinePhases, name)
		}
		p := &out[i]
		for _, f := range []struct {
			set *int
			to  *int
		}{{o.Sessions, &p.Sessions}, {o.Turns, &p.Turns}, {o.Context, &p.Context}, {o.Turn, &p.Turn}, {o.Output, &p.Output}, {o.TargetTokens, &p.TargetTokens}} {
			if f.set != nil {
				*f.to = *f.set
			}
		}
		if o.Context != nil && o.TargetTokens == nil {
			p.TargetTokens = 0 // the words are given: no probe sets them
		}
		for _, f := range []struct {
			set *string
			to  *time.Duration
		}{{o.Think, &p.Think}, {o.Stagger, &p.Stagger}, {o.Bound, &p.Bound}} {
			if f.set == nil {
				continue
			}
			d, err := time.ParseDuration(*f.set)
			if err != nil {
				return nil, fmt.Errorf("%s: phase %q: a duration %q is not a Go duration", envBaselinePhases, name, *f.set)
			}
			*f.to = d
		}
	}
	for _, p := range out {
		if err := p.Check(); err != nil {
			return nil, fmt.Errorf("%s: phase %q: %w", envBaselinePhases, p.Name, err)
		}
	}
	return out, nil
}

// Check holds a phase to what the generator takes (hack/studyload, config.go): turn tokens only beyond one turn, and sizes of at least one.
func (p Phase) Check() error {
	switch {
	case p.Sessions < 1:
		return errors.New("sessions must be at least 1")
	case p.Turns < 1 || p.Turns > 1000:
		return errors.New("turns must be 1 to 1000")
	case p.Context < 1 && p.TargetTokens < 1:
		return errors.New("contextTokens, or targetTokens to derive them, must be at least 1")
	case p.Output < 1:
		return errors.New("outputTokens must be at least 1")
	case p.Turns > 1 && p.Turn < 1:
		return errors.New("turnTokens must be at least 1 when there is more than one turn")
	case p.Turns == 1 && p.Turn != 0:
		return errors.New("turnTokens must be 0 for a one-turn phase")
	case p.Think < 0 || p.Stagger < 0:
		return errors.New("thinkTime and startStagger must not be negative")
	case p.Bound <= 0:
		return errors.New("bound must be positive")
	}
	return nil
}

// studyWorkload and studyConfig are the generator's JSON config (hack/studyload/config.go of #768, README "Config"): written out here by name, because
// the row runs the binary and imports nothing of it. The generator refuses an unknown field, so a key written wrong here fails the run at its start,
// and TestTheStudyConfigIsTheGeneratorsOwnSchema holds the keys to the ones #768 documents.
type studyWorkload struct {
	Name          string `json:"name"`
	Sessions      int    `json:"sessions"`
	Turns         int    `json:"turns"`
	ContextTokens int    `json:"contextTokens"`
	TurnTokens    int    `json:"turnTokens"`
	OutputTokens  int    `json:"outputTokens"`
	ThinkTime     string `json:"thinkTime,omitempty"`
	StartStagger  string `json:"startStagger,omitempty"`
}

type studyConfig struct {
	BaseURL        string          `json:"baseURL"`
	Model          string          `json:"model"`
	APIKeyFile     string          `json:"apiKeyFile,omitempty"`
	SessionHeader  string          `json:"sessionHeader"`
	RunID          string          `json:"runId"`
	OmitMinTokens  bool            `json:"omitMinTokens,omitempty"`
	RequestTimeout string          `json:"requestTimeout"`
	Workloads      []studyWorkload `json:"workloads"`
}

// StudyRun is what a phase's generator config says besides the phase's shape: where the front is, the key file, the run's ID, and whether min_tokens
// is left out (the min_tokens probe's verdict; internal/front, TestLLMLabMinTokens).
type StudyRun struct {
	BaseURL, APIKeyFile, RunID string
	// ServedProvider is the served Provider's name, which a phase that is OnServed addresses as model@<provider>.
	ServedProvider string
	OmitMinTokens  bool
	// RequestTimeout is the deployment's own deadline, the generator's bound on each request.
	RequestTimeout time.Duration
}

// runIDForm is the generator's own form of a run ID (config.go: 1 to 32 characters of letters, digits, dot, underscore and hyphen).
var runIDForm = regexp.MustCompile(`^[A-Za-z0-9._-]{1,32}$`)

// PhaseRunID is the generator's run ID of a phase: the phase and the run's ID, so that no two phases and no two runs share a session ID, in the
// generator's 32 characters.
func PhaseRunID(runID, phase string) string {
	id := strings.ToLower(strings.ReplaceAll(phase, " ", "")) + "-" + runID
	if len(id) > 32 {
		id = id[:32]
	}
	return id
}

// StudyConfig is the generator's config for a phase, as JSON: one workload named for the phase, with the phase's shape and nothing else, so that a
// change of a phase's config is a change of this and of nothing besides.
func StudyConfig(ph Phase, run StudyRun) ([]byte, error) {
	if err := ph.Check(); err != nil {
		return nil, fmt.Errorf("phase %q: %w", ph.Name, err)
	}
	if ph.Context < 1 {
		return nil, fmt.Errorf("phase %q has no context in words yet: the ratio probe sets it from its target of %d tokens", ph.Name, ph.TargetTokens)
	}
	if !runIDForm.MatchString(run.RunID) {
		return nil, fmt.Errorf("phase %q: the generator's run ID is not 1 to 32 characters of letters, digits, dot, underscore and hyphen", ph.Name)
	}
	w := studyWorkload{Name: ph.Name, Sessions: ph.Sessions, Turns: ph.Turns, ContextTokens: ph.Context, TurnTokens: ph.Turn, OutputTokens: ph.Output}
	if ph.Think > 0 {
		w.ThinkTime = ph.Think.String()
	}
	if ph.Stagger > 0 {
		w.StartStagger = ph.Stagger.String()
	}
	model := BaselineModel
	if ph.OnServed {
		if run.ServedProvider == "" {
			return nil, fmt.Errorf("phase %q addresses the served Instance alone and has no served Provider's name", ph.Name)
		}
		model += "@" + run.ServedProvider
	}
	c := studyConfig{BaseURL: run.BaseURL, Model: model, APIKeyFile: run.APIKeyFile, SessionHeader: "x-session-id", RunID: run.RunID,
		OmitMinTokens: run.OmitMinTokens, RequestTimeout: run.RequestTimeout.String(), Workloads: []studyWorkload{w}}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetIndent("", "  ")
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// Mapping769 is the table of the file's last section: a measurement of this row, the #769 rule it will be compared against, and where that rule's
// text is, at #769's head 9cc8368d. The post prints it.
var Mapping769 = []struct{ Measurement, Rule, Where string }{
	{"the count cap binds at N = cap + k (P1, P2)", "the integer budget, which reduces to today's count cap",
		"hbep/0005-llmlab-compatible-front/README.md:314-318 (Workloads, The count in tokens)"},
	{"the engine's KV use and preemptions at the cap (P1-long)", "the token budget B = kvCacheTokens times the workers, charged at the Workload's declared typical shape",
		"hbep/0005-llmlab-compatible-front/README.md:314-316 and :322; hbep/0007-lifecycle-controller/README.md:302"},
	{"waits at the front (0 by construction with LLMLab attached; P3's measured)", "the FIFO per Workload, against today's single FIFO per ModelDeployment, and hpcburst_front_waiting",
		"hbep/0005-llmlab-compatible-front/README.md:313 against :286; hbep/0008-observability-and-events/README.md:340"},
	{"the cap in force at full membership (ready 1 = instances 1)", "the budget in force at partial membership",
		"hbep/0005-llmlab-compatible-front/README.md:319 against :280-282"},
	{"requests in flight per Instance, by instanceUID, from the usage records", "hpcburst_front_in_flight{workload, provider, model_deployment, instance_uid}",
		"hbep/0008-observability-and-events/README.md:339"},
	{"speed per session, p50 and minimum", "hpcburst_front_output_tokens_per_second",
		"hbep/0008-observability-and-events/README.md:341"},
	{"LLMLab's overflow, unbounded at the front (the row's own guard is 144 a phase)", "the study's bound on it, managedSessions",
		"hbep/0005-llmlab-compatible-front/README.md:308 and :310"},
	{"a session's requests answered by one Instance (P1)", "pinning within a Workload's own deployments",
		"hbep/0005-llmlab-compatible-front/README.md:306"},
	{"the served deployment's workload block drives warm-up and qualification only; no Workload is named", "Workloads are routing inputs only",
		"hbep/0005-llmlab-compatible-front/README.md:323; hbep/0764-single-study-workloads/README.md:82 (R1)"},
	{"the count alone, with no kvCacheTokens known", "the count alone applies, as today, without kvCacheTokens",
		"hbep/0005-llmlab-compatible-front/README.md:321"},
}

// ParseLoggedKV reads the engine's logged KV cache size from the one line the live row's read prints: vLLM's "GPU KV cache size: 389,120 tokens" with the
// thousands commas (vllm/v1/core/kv_cache_utils.py:2327-2333), reduced to its number by the read's sed. False for anything that is not one positive whole
// number: an unread log is never a figure.
func ParseLoggedKV(out []byte) (int64, bool) {
	s := strings.TrimSpace(string(out))
	if s == "" || strings.ContainsAny(s, "\r\n") {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.ReplaceAll(s, ",", ""), 10, 64)
	if err != nil || n <= 0 || strings.TrimLeft(s, "0123456789,") != "" {
		return 0, false
	}
	return n, true
}

// genRecord is a line of the generator's records file (hack/studyload/record.go of #768): only the members the join and the comparison read.
type genRecord struct {
	Session   string `json:"session"`
	Turn      int    `json:"turn"`
	RequestID string `json:"requestId"`
	Emitter   string `json:"emitter"`
	Provider  string `json:"provider"`
	Instance  string `json:"instance"`
}

// ParseGenRecords reads the generator's records, one JSON line per request. A line that is not a record is an error: a half-written run is never read as
// a short one. A record with no requestId is kept: it is a request the front gave no answer to.
func ParseGenRecords(r io.Reader) ([]GenRow, error) {
	var out []GenRow
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var g genRecord
		if err := json.Unmarshal(line, &g); err != nil {
			return nil, fmt.Errorf("line %d of the generator's records is not a record", n)
		}
		if g.Session == "" || g.Turn < 1 {
			return nil, fmt.Errorf("line %d of the generator's records has no session or turn", n)
		}
		out = append(out, GenRow{Session: g.Session, Turn: g.Turn, RequestID: g.RequestID, Emitter: g.Emitter, Provider: g.Provider, Instance: g.Instance})
	}
	return out, sc.Err()
}

// LongContextWords maps the ratio probe's reading to the words of the long contexts: the probe sent probeWords words and the front's record says the prompt
// was promptTokens tokens, so a word comes to promptTokens/probeWords tokens, and the contexts are the number of words whose tokens come nearest target. False
// when the probe read nothing (no prompt tokens), or no whole number of words gives a prompt of LongPromptMin to LongPromptMax tokens at that ratio (a word
// that comes to more than the window is wide), or the words would pass what the generator takes: the long shape cannot be set, and P1-long is INVALID.
func LongContextWords(probeWords int, promptTokens int64, target int) (words int, ok bool) {
	if probeWords < 1 || promptTokens < 1 {
		return 0, false
	}
	ratio := float64(promptTokens) / float64(probeWords)
	exact := float64(target) / ratio
	best, bestGap := 0, math.MaxFloat64
	for _, w := range []int{int(math.Floor(exact)), int(math.Ceil(exact))} {
		tokens := ratio * float64(w)
		if w < 1 || w > maxStudyWords || tokens < LongPromptMin || tokens > LongPromptMax {
			continue
		}
		if gap := math.Abs(tokens - float64(target)); gap < bestGap {
			best, bestGap = w, gap
		}
	}
	return best, best > 0
}

// dialFailureMsg is the front's log message when a connection to the target could not be made and nothing was sent (internal/front/proxy.go:253, slog at
// WARN, JSON on stderr: <state>/logs/front.log, internal/launcher/state.go:57). The request is then sent again, to another target, and may go to LLMLab even
// with a free slot on the Instance (proxy.go:266-275: the retry excludes the Instance that failed), so the usage record, which names only the final target,
// cannot tell it from an overflow. The log can: its "request" is the usage record's requestID and its "target" the Instance's namespace/name.
const dialFailureMsg = "no connection to the target; nothing was sent"

// DialFailure is a request the front resent after a failed dial of a target: its request ID and the target. It holds nothing else of the log line: the front's
// log is the stack's PRIVATE state, read only to match request IDs, and only a COUNT of such requests reaches the text to post, the test's log or the screen
// (team lead's condition on guard 2). The error text of the line is never read.
type DialFailure struct{ Request, Target string }

// ParseDialFailures reads the front's log, one slog JSON object a line, and returns the requests it reports resent after a failed dial of target. A line
// counts only if its "msg" EQUALS dialFailureMsg (never a substring of the line: the error field of another line may quote it), its "level" is WARN and its
// "target" is target. A line that is not a JSON object with string members, or has no request, is skipped: it is never a failure found, and never a pass.
func ParseDialFailures(log []byte, target string) []DialFailure {
	var out []DialFailure
	for _, line := range bytes.Split(log, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var l struct {
			Level   string `json:"level"`
			Msg     string `json:"msg"`
			Request string `json:"request"`
			Target  string `json:"target"`
		}
		if json.Unmarshal(line, &l) != nil {
			continue
		}
		if l.Msg == dialFailureMsg && l.Level == "WARN" && l.Target == target && l.Request != "" {
			out = append(out, DialFailure{Request: l.Request, Target: l.Target})
		}
	}
	return out
}
