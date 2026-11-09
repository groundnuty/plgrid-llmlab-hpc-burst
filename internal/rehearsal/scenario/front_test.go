package scenario_test

// The front as a client sees it (HBEP-5): requests by plain model name on its
// client listener, its usage records and its health. Step (b) of issue #146.

import (
	"context"
	"net/http"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// checkNoGap fails unless the counters run without a gap.
func (r *rig) checkNoGap(rows []stack.UsageRow) {
	r.t.Helper()
	for i := 1; i < len(rows); i++ {
		if rows[i].Counter != rows[i-1].Counter+1 {
			r.t.Errorf("usage counters jump from %d to %d", rows[i-1].Counter, rows[i].Counter)
		}
	}
}

// stepRequests is step (b): chat, streamed chat and completions through the
// front by the plain model name, with a conversation key the ring maps to the
// primary (stack.Client.KeyFor), since its replacement is routable too. Each is served by
// the primary, and its usage record names it; the counters have no gap. The
// front's record names an Instance by its inventory key, namespace/name
// (internal/front/inventory/build.go), and its UID.
func (r *rig) stepRequests(in *v1alpha1.Instance, key string) {
	r.t.Helper()
	reqs := []struct{ path, body string }{
		{"/v1/chat/completions", stack.ChatBody(stack.Model, 16, 0, false, "")},
		{"/v1/chat/completions", stack.ChatBody(stack.Model, 16, 0, true, "")},
		{"/v1/completions", `{"model":"` + stack.Model + `","prompt":"hello","max_tokens":16}`},
	}
	for i, q := range reqs {
		rp := r.fc.SendCtx(context.Background(), q.path, q.body, key)
		if rp.Err != nil || rp.Status != http.StatusOK {
			r.t.Fatalf("step (b) request %d (%s): %d %v: %.300s", i, q.path, rp.Status, rp.Err, rp.Body)
		}
		if i == 1 {
			if done, finished := stack.SSEDone(rp.Body); !done || !finished {
				r.t.Errorf("step (b) streamed chat: [DONE] %v, a finish reason %v: %.300s", done, finished, rp.Body)
			}
		}
		row := r.fc.RowOf(rp.ID)
		switch {
		case row.Path != q.path || row.RequestedModel != stack.Model || row.Status != http.StatusOK:
			r.t.Errorf("step (b) record %d: %+v, want %s, %s, 200", i, row, q.path, stack.Model)
		case row.InstanceUID != string(in.UID) || row.Instance != in.Namespace+"/"+in.Name || row.Emitter != "instance":
			r.t.Errorf("step (b) record %d names Instance %s (%s), emitter %s; want %s/%s (%s), instance", i, row.Instance, row.InstanceUID, row.Emitter, in.Namespace, in.Name, in.UID)
		case row.Finish == nil || *row.Finish == "stream_cut" || row.CutBy != nil:
			r.t.Errorf("step (b) record %d: finish %v, cut by %v", i, row.Finish, row.CutBy)
		case row.Stream != (i == 1):
			r.t.Errorf("step (b) record %d: stream %v", i, row.Stream)
		case row.CompletionTokens == nil || time.Duration(row.DurationMs)*time.Millisecond < time.Duration(*row.CompletionTokens)*r.tokenDelay:
			// Each token takes tokenDelay: the mode reached the engine
			// through the launch's env and Apptainer's --cleanenv.
			r.t.Errorf("step (b) record %d: %v tokens in %d ms, below %s a token: the engine double's token-delay mode did not reach the engine", i, row.CompletionTokens, row.DurationMs, r.tokenDelay)
		}
	}
	r.checkNoGap(r.fc.UsageRows())
}
