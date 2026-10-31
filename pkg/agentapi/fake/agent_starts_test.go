package fake_test

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

func TestStartsRecordEachStartWithExactlyTheLaunchItWasSent(t *testing.T) {
	r := newRig(t, fake.Engine{})
	first := instance()
	r.set(run(first))
	r.step()

	want0 := fake.Start{
		Instance: "inst-uid-1", Worker: "worker-0", PID: 4000,
		Generation: 0, InstanceGeneration: 1,
		Launch: first.Launch, ImageFormat: agentapi.ImageFormatSIF, Time: t0,
	}
	eq(t, "the first start", r.a.Starts(), []fake.Start{want0})

	// A restart with the same launch, as the controller always sends it.
	r.clk.Advance(10 * time.Second)
	r.set(run(instance(withWorkers(1))))
	r.step()
	want1 := want0
	want1.PID, want1.Generation, want1.Time = 4001, 1, t0.Add(10*time.Second)

	// A restart whose desired launch differs: the Agent records what it was
	// sent, so a controller that changed a launch shows in Starts.
	changed := first.Launch
	changed.Command = []string{"engine", "--max-model-len", "8192"}
	changed.Env = []agentapi.EnvVar{{Name: "FLAG", Value: "on"}}
	tmpl := "{{ messages }}"
	changed.ChatTemplate = &tmpl
	changed.MaxNumSeqs = 128
	r.clk.Advance(10 * time.Second)
	r.set(run(instance(withGeneration(2), withWorkers(1), func(i *agentapi.DesiredInstance) { i.Launch = changed })))
	r.step()
	want2 := want0
	want2.PID, want2.Generation, want2.InstanceGeneration = 4002, 1, 2
	want2.Launch, want2.Time = changed, t0.Add(20*time.Second)

	got := r.a.Starts()
	eq(t, "all three starts, the earlier ones unchanged", got, []fake.Start{want0, want1, want2})
	eq(t, "the third start's launch", got[2].Launch.Command, []string{"engine", "--max-model-len", "8192"})
	if got[2].Launch.ChatTemplate == nil || *got[2].Launch.ChatTemplate != tmpl {
		t.Errorf("ChatTemplate = %v, want %q", got[2].Launch.ChatTemplate, tmpl)
	}
}

func TestStartsReturnsCopiesAndSpansARequeue(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.step()

	got := r.a.Starts()
	got[0].Launch.Command[0] = "tampered"
	got[0].PID = 1
	eq(t, "Command after the caller changed a copy", r.a.Starts()[0].Launch.Command[0], "engine")
	eq(t, "PID after the caller changed a copy", r.a.Starts()[0].PID, 4000)

	r.a.Requeue()
	r.step()
	starts := r.a.Starts()
	eq(t, "starts after the requeue", len(starts), 2)
	eq(t, "the second execution's PID", starts[1].PID, 4001)
}

func TestTheAgentReadsTheImageFormatItWasSent(t *testing.T) {
	r := newRig(t, fake.Engine{})
	eq(t, "ImageFormat before any desired state", r.a.ImageFormat(), agentapi.ImageFormatSIF)

	r.set(run(instance())) // Settings.ImageFormat empty, as an older controller sends it
	r.step()
	eq(t, "ImageFormat when the field is empty: sif", r.a.ImageFormat(), agentapi.ImageFormatSIF)
	eq(t, "start recorded with sif", r.a.Starts()[0].ImageFormat, agentapi.ImageFormatSIF)

	sandbox := run(instance(withWorkers(1)))
	sandbox.Settings.ImageFormat = agentapi.ImageFormatSandbox
	r.set(sandbox)
	r.step()
	eq(t, "ImageFormat after the controller sent sandbox", r.a.ImageFormat(), agentapi.ImageFormatSandbox)
	eq(t, "restart recorded with sandbox", r.a.Starts()[1].ImageFormat, agentapi.ImageFormatSandbox)
	eq(t, "the earlier start keeps its format", r.a.Starts()[0].ImageFormat, agentapi.ImageFormatSIF)

	explicit := run(instance(withWorkers(2)))
	explicit.Settings.ImageFormat = agentapi.ImageFormatSIF
	r.set(explicit)
	r.step()
	eq(t, "ImageFormat when sif is sent explicitly", r.a.ImageFormat(), agentapi.ImageFormatSIF)

	idle := agentapi.DesiredState{Action: agentapi.ActionIdle}
	idle.Settings.ImageFormat = agentapi.ImageFormatSandbox
	r.set(idle)
	r.step()
	eq(t, "ImageFormat from an idle state's settings", r.a.ImageFormat(), agentapi.ImageFormatSandbox)
}
