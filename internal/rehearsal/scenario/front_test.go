package scenario_test

// The front as a client sees it (HBEP-5): requests by plain model name on its
// client listener, its usage records and its health. Step (b) of issue #146.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

// post sends a request to the front's client listener with the client key.
func (r *rig) post(path, body string, timeout time.Duration) (int, http.Header, []byte, error) {
	req, err := http.NewRequest(http.MethodPost, "http://"+clientAddr+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.h.clientKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b, err
}

// routable is the number of routable Instances the front reports for the
// deployment on its health address (base+4).
func (r *rig) routable() int {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", portBase+4))
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var h struct {
		Deployments map[string]int `json:"deployments"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&h) != nil {
		return -1
	}
	return h.Deployments[namespace+"/"+mdName]
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
// front by the plain model name. Each is served by the ready Instance, and its
// usage record names that Instance; the counters have no gap.
func (r *rig) stepRequests(in *v1alpha1.Instance) {
	r.t.Helper()
	waitFor(r.t, waitDeadline, "the front to route the deployment", func() bool { return r.routable() >= 1 })
	before := len(r.usageRows())
	reqs := []struct{ path, body string }{
		{"/v1/chat/completions", `{"model":"` + model + `","messages":[{"role":"user","content":"hello"}],"max_tokens":16}`},
		{"/v1/chat/completions", `{"model":"` + model + `","messages":[{"role":"user","content":"hello"}],"max_tokens":16,"stream":true,"stream_options":{"include_usage":true}}`},
		{"/v1/completions", `{"model":"` + model + `","prompt":"hello","max_tokens":16}`},
	}
	for i, q := range reqs {
		code, _, body, err := r.post(q.path, q.body, 60*time.Second)
		if err != nil || code != http.StatusOK {
			r.t.Fatalf("step (b) request %d (%s): %d %v: %.300s", i, q.path, code, err, body)
		}
		if i == 1 {
			if done, finished := sseDone(body); !done || !finished {
				r.t.Errorf("step (b) streamed chat: [DONE] %v, a finish reason %v: %.300s", done, finished, body)
			}
		}
	}
	rows := r.usageRows()
	if len(rows) != before+len(reqs) {
		r.t.Fatalf("step (b): %d usage records after %d requests, want %d", len(rows), len(reqs), before+len(reqs))
	}
	for i, row := range rows[before:] {
		switch {
		case row.Path != reqs[i].path || row.RequestedModel != model || row.Status != http.StatusOK:
			r.t.Errorf("step (b) record %d: %+v, want %s, %s, 200", i, row, reqs[i].path, model)
		case row.Instance != in.Name || row.InstanceUID != string(in.UID):
			r.t.Errorf("step (b) record %d names Instance %s (%s), want %s (%s)", i, row.Instance, row.InstanceUID, in.Name, in.UID)
		case row.Finish == nil || *row.Finish == "stream_cut" || row.CutBy != nil:
			r.t.Errorf("step (b) record %d: finish %v, cut by %v", i, row.Finish, row.CutBy)
		case row.Stream != (i == 1):
			r.t.Errorf("step (b) record %d: stream %v", i, row.Stream)
		}
	}
	r.checkNoGap(rows)
}
