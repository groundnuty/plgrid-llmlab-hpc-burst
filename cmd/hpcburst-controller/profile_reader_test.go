package main

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A batch Provider's profile reader checks the partition's dialHomeModes against
// the launcher's recorded dial-home mode, the one the front settings carry; with
// no front recorded there is no mode, so nothing is checked (HBEP-4, Core
// operations, ReadProfile).
func TestProfileReaderTakesTheLaunchersRecordedMode(t *testing.T) {
	for _, c := range []struct {
		name  string
		front *providerctl.FrontSettings
		want  agentapi.DialHomeMode
	}{
		{"no front", nil, ""},
		{"direct", &providerctl.FrontSettings{Mode: agentapi.DialHomeDirect}, "direct"},
		{"ssh", &providerctl.FrontSettings{Mode: agentapi.DialHomeSSH}, "ssh"},
		{"host-dialed", &providerctl.FrontSettings{Mode: agentapi.DialHomeHostDialed}, "host-dialed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := wiring{front: c.front}.profileReader(nil)
			if r.DialHomeMode != c.want {
				t.Errorf("the profile reader's dial-home mode is %q, want %q", r.DialHomeMode, c.want)
			}
		})
	}
}
