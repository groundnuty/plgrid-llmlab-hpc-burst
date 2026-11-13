package alpha

import (
	"context"
	"errors"
)

// BetaRouter is the router row's live flow (issue #521). Not built yet in this commit.
func (r *liveRun) BetaRouter(context.Context, Plan) (RouterObs, error) {
	return RouterObs{}, errors.New("the router row's live flow is not built yet")
}
