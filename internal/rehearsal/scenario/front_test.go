package scenario_test

// The front as a client sees it (HBEP-5): requests by plain model name on its
// client listener, its usage records and its health. Step (b) of issue #146.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// usageRow is the part of a usage record (internal/front/usage, Record) the
// rehearsal reads.
type usageRow struct {
	Counter        uint64  `json:"counter"`
	RequestID      string  `json:"requestID"`
	StartTime      string  `json:"startTime"`
	EndTime        string  `json:"endTime"`
	Path           string  `json:"path"`
	RequestedModel string  `json:"requestedModel"`
	Instance       string  `json:"instance"`
	InstanceUID    string  `json:"instanceUID"`
	Emitter        string  `json:"emitter"`
	Status         int     `json:"status"`
	Stream         bool    `json:"stream"`
	Finish         *string `json:"finish"`
	CutBy          *string `json:"cutBy"`
	ErrorCode      *string `json:"errorCode"`
	// CompletionTokens and DurationMs: the reply's tokens and the request's
	// time at the front.
	CompletionTokens *int64 `json:"completionTokens"`
	DurationMs       int64  `json:"durationMs"`
}

// usageRows reads every usage record so far, by counter.
func (r *rig) usageRows() []usageRow {
	r.t.Helper()
	files, _ := filepath.Glob(filepath.Join(r.h.usageDir, "usage*.jsonl"))
	var rows []usageRow
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			r.t.Fatalf("usage file %s: %v", f, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(b))
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var row usageRow
			if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
				r.t.Fatalf("usage record in %s: %v: %s", f, err, sc.Bytes())
			}
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Counter < rows[j].Counter })
	return rows
}

// checkNoGap fails unless the counters run without a gap.
func (r *rig) checkNoGap(rows []usageRow) {
	r.t.Helper()
	for i := 1; i < len(rows); i++ {
		if rows[i].Counter != rows[i-1].Counter+1 {
			r.t.Errorf("usage counters jump from %d to %d", rows[i-1].Counter, rows[i].Counter)
		}
	}
}

// sseDone reports whether a streamed body ends with "data: [DONE]" and has a
// chunk with a finish reason.
func sseDone(body []byte) (done, finished bool) {
	var last string
	for _, l := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "data:") {
			continue
		}
		last = strings.TrimSpace(strings.TrimPrefix(l, "data:"))
		if strings.Contains(last, `"finish_reason":"`) {
			finished = true
		}
	}
	return last == "[DONE]", finished
}

// stepRequests is step (b): chat, streamed chat and completions through the
// front by the plain model name, with a conversation key the ring maps to the
// primary (keyFor), since its replacement is routable too. Each is served by
// the primary, and its usage record names it; the counters have no gap. The
// front's record names an Instance by its inventory key, namespace/name
// (internal/front/inventory/build.go), and its UID.
func (r *rig) stepRequests(in *v1alpha1.Instance, key string) {
	r.t.Helper()
	reqs := []struct{ path, body string }{
		{"/v1/chat/completions", chatBody(16, 0, false)},
		{"/v1/chat/completions", chatBody(16, 0, true)},
		{"/v1/completions", `{"model":"` + model + `","prompt":"hello","max_tokens":16}`},
	}
	for i, q := range reqs {
		rp := r.send(q.path, q.body, key)
		if rp.err != nil || rp.status != http.StatusOK {
			r.t.Fatalf("step (b) request %d (%s): %d %v: %.300s", i, q.path, rp.status, rp.err, rp.body)
		}
		if i == 1 {
			if done, finished := sseDone(rp.body); !done || !finished {
				r.t.Errorf("step (b) streamed chat: [DONE] %v, a finish reason %v: %.300s", done, finished, rp.body)
			}
		}
		row := r.rowOf(rp.id)
		switch {
		case row.Path != q.path || row.RequestedModel != model || row.Status != http.StatusOK:
			r.t.Errorf("step (b) record %d: %+v, want %s, %s, 200", i, row, q.path, model)
		case row.InstanceUID != string(in.UID) || row.Instance != in.Namespace+"/"+in.Name || row.Emitter != "instance":
			r.t.Errorf("step (b) record %d names Instance %s (%s), emitter %s; want %s/%s (%s), instance", i, row.Instance, row.InstanceUID, row.Emitter, in.Namespace, in.Name, in.UID)
		case row.Finish == nil || *row.Finish == "stream_cut" || row.CutBy != nil:
			r.t.Errorf("step (b) record %d: finish %v, cut by %v", i, row.Finish, row.CutBy)
		case row.Stream != (i == 1):
			r.t.Errorf("step (b) record %d: stream %v", i, row.Stream)
		case row.CompletionTokens == nil || time.Duration(row.DurationMs)*time.Millisecond < time.Duration(*row.CompletionTokens)*tokenDelay:
			// Each token takes tokenDelay: the mode reached the engine
			// through the launch's env and Apptainer's --cleanenv.
			r.t.Errorf("step (b) record %d: %v tokens in %d ms, below %s a token: the engine double's token-delay mode did not reach the engine", i, row.CompletionTokens, row.DurationMs, tokenDelay)
		}
	}
	r.checkNoGap(r.usageRows())
}
