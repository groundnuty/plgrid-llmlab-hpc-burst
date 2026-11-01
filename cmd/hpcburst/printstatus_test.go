package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// TestPrintStatusEventTTL: the text status names the event TTL the report carries; for a host whose
// record has none (started before #109) it prints exactly the default line; and it prints nothing
// when the report has none (a stopped host). The lines are literals.
func TestPrintStatusEventTTL(t *testing.T) {
	text := func(r *launcher.StatusReport) string {
		var b bytes.Buffer
		printStatus(&b, r)
		return b.String()
	}
	if out := text(&launcher.StatusReport{Health: launcher.Healthy, EventTTL: "168h"}); !strings.Contains(out, "event TTL: 168h\n") || strings.Contains(out, "default") {
		t.Errorf("a host with the TTL recorded:\n%s", out)
	}
	if out := text(&launcher.StatusReport{Health: launcher.Healthy, EventTTL: "1h", EventTTLDefault: true}); !strings.Contains(out, "event TTL: 1h (default; restart to apply 168h)\n") {
		t.Errorf("a host without the TTL in its record:\n%s", out)
	}
	if out := text(&launcher.StatusReport{Health: launcher.Stopped, Reasons: []string{"nothing was started here"}}); strings.Contains(out, "event TTL") {
		t.Errorf("a stopped host:\n%s", out)
	}
}
