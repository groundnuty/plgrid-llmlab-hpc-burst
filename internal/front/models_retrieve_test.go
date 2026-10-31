package front

import (
	"encoding/json"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// TestModelsRetrieve (@hpc-burst-science-agent, 31 Oct 2026): GET
// /v1/models/{name} is the front's additive extension, answered from its
// inventory. The name is the path's rest, decoded once, so a raw "/" and
// %2F name the same model. A listed name answers 200 with the list's model
// object, routable or not; any other name, a trailing slash or a
// double-encoded %252F included, answers 404 model_not_found.
func TestModelsRetrieve(t *testing.T) {
	const served, idle = "Qwen/Qwen3.6-27B", "Org/Idle-Model"
	h := newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", served, 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, key, instanceOpts{})
	h.servedMD("md-idle", idle, 1, "helios-a") // listed, with no Instance: not routable
	h.apply()

	get := func(path string) (int, map[string]any) {
		t.Helper()
		resp := h.errGet(path)
		var v map[string]any
		if err := json.Unmarshal([]byte(readAll(t, resp)), &v); err != nil {
			t.Fatalf("GET %s: body is not JSON: %v", path, err)
		}
		return resp.StatusCode, v
	}
	for _, c := range []struct {
		path     string
		status   int
		id       string
		routable bool
	}{
		{"/v1/models/Qwen/Qwen3.6-27B", 200, served, true},
		{"/v1/models/Qwen%2FQwen3.6-27B", 200, served, true},
		{"/v1/models/Org/Idle-Model", 200, idle, false},
		{"/v1/models/Unlisted/Model", 404, "", false},
		{"/v1/models/Qwen/Qwen3.6-27B/", 404, "", false},
		{"/v1/models/Qwen%252FQwen3.6-27B", 404, "", false},
	} {
		status, v := get(c.path)
		if status != c.status {
			t.Errorf("GET %s: status %d, want %d: %v", c.path, status, c.status, v)
			continue
		}
		if c.status == 404 {
			if errObj, _ := v["error"].(map[string]any); errObj == nil || errObj["code"] != codeModelNotFound {
				t.Errorf("GET %s: body %v, want error code model_not_found", c.path, v)
			}
			continue
		}
		if v["id"] != c.id || v["object"] != "model" || v["routable"] != c.routable {
			t.Errorf("GET %s: %v, want id %q, object model, routable %v", c.path, v, c.id, c.routable)
		}
	}
}
