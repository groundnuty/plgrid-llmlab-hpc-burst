package alpha

// The router row (issue #521; betarouter.go) over the fake cluster: one job, run on the manual clock; what the stack would show is
// stood in for. Each fault departs from the healthy run in one way that must fail the row, or, for the glibc faults, end it NOT RUN
// (TestARouterNodeWithAnOldGlibcIsNotRun). The healthy reading is made by Add from S13's own cases (dryRouterBytes).

import (
	"context"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/cases"
)

// dryRefusals is how many valid variants the healthy reading has refused with a 4xx: every one in one of S13's groups that a server may
// still refuse, as the ruling (c) on #521 asks of the passing fixture.
const dryRefusals = 38

// dryRouterBytes is the healthy response-level reading, made by Add from S13's own cases: every variant answered whole; the 2 invalid ones
// and the first dryRefusals valid ones in a refusal group with a 400; the rest with a 200. change, when set, alters one variant's answer
// before Add counts it, for a fault.
func dryRouterBytes(change ...func(a *Answer)) BytesObs {
	okJSON, okStream, refusal := []byte(`{"choices":[{"text":"hi"}]}`), []byte("data: {\"x\":1}\n\ndata: [DONE]\n\n"), []byte(`{"error":{"message":"no"}}`)
	var b BytesObs
	refused := 0
	for _, c := range cases.All() {
		for _, v := range c.Variants() {
			a := Answer{Chat: c.Endpoint == cases.Chat, Stream: v.Stream, Valid: v.Valid(), Group: RefusalGroupOf(c), Key: v.Key(), Status: 200, Body: okJSON}
			if v.Stream {
				a.Body = okStream
			}
			if !v.Valid() || (a.Group != "" && refused < dryRefusals) {
				a.Status, a.Body = 400, refusal
				if v.Valid() {
					refused++
				}
			}
			for _, ch := range change {
				ch(&a)
			}
			b.Add(a)
		}
	}
	b.Expected = b.Variants
	return b
}

// answerTo sets the answer of the variant with the key, for a fault.
func answerTo(key string, status int, body string) func(a *Answer) {
	return func(a *Answer) {
		if a.Key == key {
			a.Status, a.Body = status, []byte(body)
		}
	}
}

// BetaRouter is the router row over the fake cluster.
func (d *drySteps) BetaRouter(_ context.Context, p Plan) (RouterObs, error) {
	if d.fault("pend-past-bound") {
		return RouterObs{}, d.pendPastBound(p)
	}
	tm := p.Row.Clock()
	tag := d.tag(p, "router")
	n, id := d.submit(tag, d.partitionFor(true), d.cfg.Row.QOS)
	j := d.running(id)
	started := *j.StartTime
	d.extras(p, tag)
	d.tick(8 * time.Minute) // a warm start, inside the profile's 12 minutes
	ready := d.now()
	drain := started.Add(d.deployment().LeaseLimit - DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0))
	glibc := GlibcReads{Probe: GlibcRead{OK: true, Version: GlibcVersion{2, 34}}}
	switch {
	case d.fault("glibc-old-probe"):
		glibc.Probe.Version = GlibcVersion{2, 28}
	case d.fault("glibc-old-agent"):
		glibc.Agent = GlibcRead{OK: true, Version: GlibcVersion{2, 28}}
	case d.fault("glibc-at-the-minimum"):
		glibc.Probe.Version = MinEnvoyGlibc
	case d.fault("glibc-one-under"):
		glibc.Probe.Version = GlibcVersion{2, 29}
	case d.fault("glibc-unread"):
		glibc = GlibcReads{}
	}
	if err := glibc.TooOld(); err != nil {
		// The run opens nothing and cancels its job by its tag, as the live steps do.
		if cerr := d.cancelTag(tag); cerr != nil {
			return RouterObs{}, cerr
		}
		return RouterObs{}, err
	}
	obs := RouterObs{
		Deployment: d.deployment(), ReadyAt: ready, DrainStart: drain,
		LaunchRouter: "envoy", HealthyAtReady: 1, HealthyAfterRestart: 1,
		Envoy: EnvoyObs{Procs: 1, PinnedDigest: strings.Repeat("1d", 32), RunningDigest: strings.Repeat("1d", 32), Serving: true},
		Glibc: glibc, Rate: 30, Bytes: dryRouterBytes(), WorkerRestarts: 1, Ladder: LadderObs{WorkerRestarts: 1}, Responses: 460, Attributed: 460,
	}
	switch {
	case d.fault("not-ready"):
		obs.ReadyAt = time.Time{}
	case d.fault("no-router-launch"):
		obs.LaunchRouter = ""
	case d.fault("several-workers"):
		obs.LaunchWorkers = 2
	case d.fault("healthy-zero-at-ready"):
		obs.HealthyAtReady = 0
	case d.fault("healthy-absent-after-restart"):
		obs.HealthyAfterRestart = -1
	case d.fault("router-refused"):
		obs.Envoy.Refused, obs.Envoy.Serving = true, false
	case d.fault("not-serving"):
		obs.Envoy.Serving = false
	case d.fault("no-envoy"):
		obs.Envoy.Procs, obs.Envoy.RunningDigest = 0, ""
	case d.fault("two-envoys"):
		obs.Envoy.Procs = 2
	case d.fault("not-bare"):
		obs.Envoy.Contained = true
	case d.fault("wrong-digest"):
		obs.Envoy.RunningDigest = strings.Repeat("ab", 32)
	case d.fault("unread-executable"):
		obs.Envoy.RunningDigest = ""
	case d.fault("no-pin"):
		obs.Envoy.PinnedDigest = ""
	case d.fault("no-rate"):
		obs.Rate = 0
	case d.fault("bytes-short"):
		obs.Bytes.Variants = 439
	case d.fault("bytes-unfinished"):
		obs.Bytes.Complete, obs.Bytes.Status200 = 439, 399
	case d.fault("bytes-router-made"):
		obs.Bytes.RouterMade = 3
	case d.fault("bytes-no-stream-200"):
		obs.Bytes.StreamOK = 0
	case d.fault("bytes-no-chat-200"):
		obs.Bytes.ChatOK = 0
	case d.fault("bytes-refused-outside"):
		// The ruling's example: a 400 on a plain temperature case, in no group S13 says a server may refuse.
		obs.Bytes = dryRouterBytes(answerTo("chat/temperature#plain", 400, `{"error":{"message":"no"}}`))
	case d.fault("bytes-5xx-in-group"):
		// A 5xx is never allowed, even to a variant in a refusal group.
		obs.Bytes = dryRouterBytes(answerTo("completions/suffix#plain", 500, `{"error":{"message":"engine"}}`))
	case d.fault("bytes-past-bound"):
		obs.Bytes = dryRouterBytes(func(a *Answer) {
			if a.Key == "chat/temperature#stream" {
				a.Status, a.Body, a.Cut, a.PastBound = 0, nil, true, true
			}
		})
	case d.fault("unattributed"):
		obs.Attributed = 459
	case d.fault("no-responses"):
		obs.Responses, obs.Attributed = 0, 0
	case d.fault("no-restart"):
		obs.WorkerRestarts = 0
	case d.fault("two-restarts"):
		obs.WorkerRestarts = 2
	case d.fault("ladder-climb"):
		obs.WorkerRestarts, obs.Ladder = 2, LadderObs{WorkerRestarts: 2, InstanceRestarts: 2, RestartLoop: true}
	}
	d.tick(20 * time.Minute) // the Bytes, the worker restart and the rest, before the drain start
	d.tick(time.Minute)
	d.end(n, id)
	obs.Tags = append([]string(nil), d.tags...)
	return obs, nil
}

// betaRouterCases are the router row's dry cases. The glibc faults that end NOT RUN are not here: Run returns their error and writes no
// verdict (TestARouterNodeWithAnOldGlibcIsNotRun); at the minimum, the node's glibc is no failure and no NOT RUN.
var betaRouterCases = []dryCase{
	{"", ""},
	{"glibc-at-the-minimum", ""},
	{"not-ready", "the Instance was not seen ready"},
	{"no-router-launch", "the Instance's spec.launch.router is not envoy"},
	{"several-workers", "the Instance has several workers; the row's deployment has one"},
	{"healthy-zero-at-ready", "status.healthyWorkers was not 1 when the Instance was first ready"},
	{"healthy-absent-after-restart", "status.healthyWorkers was not 1 once the Instance served again after the worker restart"},
	{"router-refused", "the agent's log says the router did not start"},
	{"not-serving", "the agent's log does not say the router is serving"},
	{"no-envoy", "the node does not show exactly one Envoy process in the job"},
	{"two-envoys", "the node does not show exactly one Envoy process in the job"},
	{"not-bare", "Envoy does not run bare: an Apptainer process is among its ancestors"},
	{"wrong-digest", "the running Envoy is not the build the Provider staged by its pinned digest"},
	{"unread-executable", "the running Envoy's executable was not read on the node"},
	{"no-pin", "the digest the Provider stages Envoy by was not read"},
	{"glibc-unread", "the node's glibc was read neither on the node nor from the agent's log"},
	{"no-rate", "no request through the front was measured"},
	{"bytes-short", "the response-level reading did not send every variant of S13's cases"},
	{"bytes-unfinished", "a response of the response-level reading did not finish"},
	{"bytes-router-made", "a response of the response-level reading was a failure the router or the path made"},
	{"bytes-no-stream-200", "no streamed variant answered 200"},
	{"bytes-no-chat-200", "no chat variant answered 200"},
	{"bytes-refused-outside", "a valid variant outside S13's groups that a server may refuse was answered other than 200: chat/temperature#plain 400 [outside S13's groups]"},
	{"bytes-5xx-in-group", "a valid variant of S13's cases was answered with a 5xx: completions/suffix#plain 500 [suffix]"},
	{"bytes-past-bound", "a response of the response-level reading did not finish"},
	{"unattributed", "a response lacked its attribution headers"},
	{"no-responses", "no response, so attribution is unchecked"},
	{"no-restart", "0 restarts at the worker rung, want 1"},
	{"two-restarts", "2 restarts at the worker rung, want 1"},
	{"ladder-climb", "the worker restart climbed to a new Allocation (rung 3 of 3: 2 restarts at the worker rung and 2 at the whole-Instance rung, then the Instance failed with RestartLoop)"},
	{"untagged", "a job tag was not written down before its submission"},
	{"wrong-partition", "a job ran on another partition than the row's"},
	{"default-lease", "the deployment's leaseLimit is not the row's shortest lease"},
	{"short-lease", "not longer than the lead an Instance needs (PartitionTooShort)"},
	{"duplicate-tag", "a tag names more than one job"},
	{"extra-allocation", "exactly one Allocation"},
}
