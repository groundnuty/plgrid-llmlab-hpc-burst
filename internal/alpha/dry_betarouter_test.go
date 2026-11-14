package alpha

// The router row (issue #521; betarouter.go) over the fake cluster: one job, run on the manual clock; what the stack would show is
// stood in for. Each fault departs from the healthy run in one way that must fail the row, or, for the glibc faults, end it NOT RUN
// (TestARouterNodeWithAnOldGlibcIsNotRun). The healthy reading and its direct-to-engine control are made from S13's own cases
// (dryRouterReading).

import (
	"context"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/cases"
)

// dryRefusals is how many valid variants the healthy reading's engine refuses with a 400, alike on both paths: the deployment's own
// refusals, which the direct-to-engine control lists and does not fail (the science agent's ruling (a) on the router row, #522).
const dryRefusals = 38

// dryEngineRefusal is the engine's refusal of a variant, written in vLLM's shape: the same on both paths.
func dryEngineRefusal(key string) []byte {
	return []byte(`{"error":{"message":"this deployment does not serve ` + key + `","type":"BadRequestError","param":null,"code":400}}`)
}

// dryAnswer is the engine's 200 to a variant, as one path received it: vLLM's shape, with what the sampler decides (the text, the id, the
// counts of the completion) differing by path, as two sends of one body do, and the prompt's tokens the same.
func dryAnswer(stream bool, path string) []byte {
	if stream {
		return []byte("data: {\"id\":\"c-" + path + "\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"" + path + "\"},\"finish_reason\":null}]}\n\n" +
			"data: {\"id\":\"c-" + path + "\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"total_tokens\":11,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	}
	return []byte(`{"id":"x-` + path + `","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"` + path +
		`"},"finish_reason":"length"}],"usage":{"prompt_tokens":9,"total_tokens":17,"completion_tokens":8}}`)
}

// dryRouterReading is the healthy reading and its control, made from S13's own cases: every variant answered whole on both paths; the 2
// invalid ones and the first dryRefusals valid ones (the temperature cases aside) refused alike by the engine; the rest answered 200 on both, the sampler's text
// differing. front alters a variant's answer through the router before Add counts it, and direct its answer straight to the engine (nil
// for none), for a fault.
func dryRouterReading(front func(a *Answer), direct func(key string, r *Reply) *Reply) (BytesObs, ControlObs) {
	var b BytesObs
	var cvs []ControlVariant
	refused := 0
	for _, c := range cases.All() {
		for _, v := range c.Variants() {
			a := Answer{Chat: c.Endpoint == cases.Chat, Stream: v.Stream, Valid: v.Valid(), Key: v.Key(), Status: 200, Body: dryAnswer(v.Stream, "front")}
			d := &Reply{Status: 200, Body: dryAnswer(v.Stream, "direct")}
			// The temperature cases, which the faults change, are answered 200 in the healthy reading.
			if !v.Valid() || (refused < dryRefusals && !strings.Contains(v.Key(), "temperature")) {
				a.Status, a.Body = 400, dryEngineRefusal(v.Key())
				d = &Reply{Status: 400, Body: dryEngineRefusal(v.Key())}
				if v.Valid() {
					refused++
				}
			}
			if front != nil {
				front(&a)
			}
			if direct != nil {
				d = direct(v.Key(), d)
			}
			b.Add(a)
			cvs = append(cvs, ControlVariant{Key: v.Key(), Stream: v.Stream, Front: Reply{Status: a.Status, Body: a.Body, Cut: a.Cut}, Direct: d, FrontPastBound: a.PastBound})
		}
	}
	b.Expected = b.Variants
	c := ControlOf(cvs)
	c.Took = 14 * time.Second
	return b, c
}

// dryRouterBytes is the healthy reading's front half, for the checks that read it alone.
func dryRouterBytes() BytesObs {
	b, _ := dryRouterReading(nil, nil)
	return b
}

// onKey is a change of one variant's answer through the router, for a fault.
func onKey(key string, change func(a *Answer)) func(a *Answer) {
	return func(a *Answer) {
		if a.Key == key {
			change(a)
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
		Glibc: glibc, Rate: 30, WorkerRestarts: 1, Ladder: LadderObs{WorkerRestarts: 1}, Responses: 460, Attributed: 460,
	}
	obs.Bytes, obs.Control = dryRouterReading(nil, nil)
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
	case d.fault("bytes-past-bound"):
		obs.Bytes, obs.Control = dryRouterReading(onKey("chat/temperature#stream", func(a *Answer) {
			a.Status, a.Body, a.Cut, a.PastBound = 0, nil, true, true
		}), nil)
	// The control (the ruling (a) on the router row, #522).
	case d.fault("control-router-alters"):
		// Envoy refuses a variant the engine serves: the router's doing.
		obs.Bytes, obs.Control = dryRouterReading(onKey("chat/temperature#plain", func(a *Answer) {
			a.Status, a.Body = 400, dryEngineRefusal("chat/temperature#plain")
		}), nil)
	case d.fault("control-both-refuse"):
		// The engine refuses a plain temperature case on both paths alike: the deployment's refusal, listed, no failure.
		refuse := func(key string, r *Reply) *Reply {
			if key == "chat/temperature#plain" {
				return &Reply{Status: 400, Body: dryEngineRefusal(key)}
			}
			return r
		}
		obs.Bytes, obs.Control = dryRouterReading(onKey("chat/temperature#plain", func(a *Answer) {
			a.Status, a.Body = 400, dryEngineRefusal("chat/temperature#plain")
		}), refuse)
	case d.fault("control-prompt-differs"):
		obs.Bytes, obs.Control = dryRouterReading(onKey("chat/temperature#plain", func(a *Answer) {
			a.Body = []byte(strings.Replace(string(a.Body), `"prompt_tokens":9`, `"prompt_tokens":10`, 1))
		}), nil)
	case d.fault("control-stream-ending"):
		obs.Bytes, obs.Control = dryRouterReading(nil, func(key string, r *Reply) *Reply {
			if key == "chat/temperature#stream" {
				return &Reply{Status: 200, Body: []byte("data: {\"error\":{\"message\":\"engine error\",\"type\":\"InternalServerError\",\"param\":null,\"code\":500}}\n\ndata: [DONE]\n\n")}
			}
			return r
		})
	case d.fault("control-incomplete"):
		obs.Bytes, obs.Control = dryRouterReading(nil, func(key string, r *Reply) *Reply {
			if key == "chat/temperature#plain" {
				return &Reply{Status: 200, Body: []byte(`{"id":`), Cut: true}
			}
			return r
		})
	case d.fault("control-unavailable"):
		obs.Control = ControlObs{Why: "the srun step that sends the variants straight to the engine exited 21: control: curl is not on the node, in 3 tries or by its bound of 10m0s"}
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
	{"bytes-past-bound", "a response of the response-level reading did not finish"},
	{"control-router-alters", "a variant was answered differently through the router than straight to the engine: chat/temperature#plain: status 400 through the router, 200 straight to the engine"},
	{"control-both-refuse", ""},
	{"control-prompt-differs", "a variant was answered differently through the router than straight to the engine: chat/temperature#plain: usage.prompt_tokens 10 through the router, 9 straight to the engine"},
	{"control-stream-ending", "a variant was answered differently through the router than straight to the engine: chat/temperature#stream: the answer ended done through the router, error straight to the engine"},
	{"control-incomplete", "the engine did not answer every variant completely straight from the node: 439 of 440 answered (chat/temperature#plain)"},
	{"control-unavailable", "the direct-to-engine control could not be read: the srun step that sends the variants straight to the engine exited 21: control: curl is not on the node, in 3 tries or by its bound of 10m0s"},
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
