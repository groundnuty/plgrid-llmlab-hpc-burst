package launcher

// The launcher's part of the front's bulk path (HBEP-6, The bulk path; #448, part 6): the front gets
// --bulk-path only when the record has the bulk path, and status posts its no-op call to the
// front's bulk plugin path then.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestFrontArgsWithoutTheBulkPathAreUnchanged: without the bulk path the front's command line is as
// before (TestSupervisedSetRunsTheFront pins it); with it, exactly --bulk-path more, last.
func TestFrontArgsWithoutTheBulkPathAreUnchanged(t *testing.T) {
	st := &State{
		Ports:       Ports{API: 29220, HTTPS: 29221, KCM: 29222, Agent: 29223, FrontHealth: 29224, LB: 29225, Controller: 29226},
		Front:       Front{Mode: "host-dialed"},
		Controllers: &ControllerConfig{Binary: "/opt/hpcburst-controller"},
		FrontRun:    &FrontRunConfig{Binary: "/opt/hpcburst-front", Config: "/etc/front.json"},
	}
	want := []string{"--config", "/etc/front.json", "--state-dir", "/run/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29224", "--mode", "host-dialed"}
	for _, bulk := range []bool{false, true} {
		st.BulkPath = bulk
		set, err := st.supervisedSet(layout{"/run/st"})
		if err != nil {
			t.Fatal(err)
		}
		var front *supervised
		for i := range set {
			if set[i].Name == "front" {
				front = &set[i]
			}
		}
		if front == nil {
			t.Fatalf("no front in %+v", set)
		}
		w := want
		if bulk {
			w = append(slices.Clone(want), "--bulk-path")
		}
		if !slices.Equal(front.Args, w) {
			t.Errorf("bulk path %v: front argv\n got %q\nwant %q", bulk, front.Args, w)
		}
	}
}

// TestCheckFrontBulkPlugin: the no-op call goes to the front's bulk plugin path and counts as an
// answer only when the front accepts it there; a front that serves only the first path fails it.
func TestCheckFrontBulkPlugin(t *testing.T) {
	accept := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"reject":false,"unchange":true}`)
	}
	for _, c := range []struct {
		name   string
		paths  []string
		wantOK bool
	}{
		{"the front serves both paths", []string{agentapi.FrontFrpsPluginPath, agentapi.FrontFrpsBulkPluginPath}, true},
		{"the front serves the first path only", []string{agentapi.FrontFrpsPluginPath}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			mux := http.NewServeMux()
			for _, p := range c.paths {
				mux.HandleFunc(p, accept)
			}
			srv := httptest.NewServer(mux)
			defer srv.Close()
			var eh EntryHealth
			var probs []string
			checkFrontBulkPlugin(strings.TrimPrefix(srv.URL, "http://"), &eh, func(f string, a ...any) { probs = append(probs, f) })
			if eh.FrpsBulkPluginOK == nil || *eh.FrpsBulkPluginOK != c.wantOK || (len(probs) == 0) != c.wantOK {
				t.Errorf("FrpsBulkPluginOK %v, problems %v; want ok %v", eh.FrpsBulkPluginOK, probs, c.wantOK)
			}
		})
	}
}
