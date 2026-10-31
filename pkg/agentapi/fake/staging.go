package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// staging returns the Instance's staging as of now, from the Engine's settings
// (Engine.StagingDuration, Engine.StagingError) and the clock alone: inProgress
// until stageStart plus the duration, then failed when the Engine has an error,
// else done. Nothing about a worker enters it, because no worker starts before
// staging is done.
func (in *instance) staging(e Engine, now time.Time) agentapi.Staging {
	if now.Before(in.stageStart.Add(e.StagingDuration)) {
		return agentapi.Staging{Phase: agentapi.StagingInProgress}
	}
	if e.StagingError != "" {
		return agentapi.Staging{
			Phase:   agentapi.StagingFailed,
			Message: agentapi.TruncateUTF8(e.StagingError, agentapi.MaxMessageBytes),
		}
	}
	return agentapi.Staging{Phase: agentapi.StagingDone}
}

// restage starts staging again when it has failed and the desired Instance
// generation has risen past the one it was for. A staging that is inProgress or
// done is left alone: a rise does not repeat a staging that succeeded.
func (in *instance) restage(e Engine, generation int64, now time.Time) {
	if generation > in.stageGen && in.staging(e, now).Phase == agentapi.StagingFailed {
		in.stageStart, in.stageGen = now, generation
	}
}
