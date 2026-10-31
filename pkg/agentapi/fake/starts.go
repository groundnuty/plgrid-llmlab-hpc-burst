package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Start is one worker process the Agent started, as a test reads it. The Agent
// records exactly what it was sent when it started the process: a restart
// carries the Launch of the desired state that caused it, whatever an earlier
// start carried.
type Start struct {
	// Instance is the UID of the Instance the process belongs to.
	Instance string
	// Worker and PID name the process.
	Worker string
	PID    int
	// Generation is the worker's restart generation the process was started
	// for, InstanceGeneration the whole Instance's.
	Generation         int64
	InstanceGeneration int64
	// Launch is the launch the process was started from.
	Launch agentapi.Launch
	// ImageFormat is the format of the image the Agent built for it: the one
	// in the latest desired state, sif when that was empty.
	ImageFormat agentapi.ImageFormat
	// Time is the Agent's time at the start.
	Time time.Time
}

// Starts returns every worker process the Agent has started, oldest first. It
// spans Requeue.
func (a *Agent) Starts() []Start {
	out := make([]Start, len(a.starts))
	for i, s := range a.starts {
		s.Launch = cloneLaunch(s.Launch)
		out[i] = s
	}
	return out
}

// ImageFormat returns the image format the latest desired state asked for
// (Settings.ImageFormat), sif when it was empty, as the desired state of an
// older controller is, and before the Agent has seen any.
func (a *Agent) ImageFormat() agentapi.ImageFormat { return a.imageFormat }

func imageFormatOf(f agentapi.ImageFormat) agentapi.ImageFormat {
	if f == "" {
		return agentapi.ImageFormatSIF
	}
	return f
}
