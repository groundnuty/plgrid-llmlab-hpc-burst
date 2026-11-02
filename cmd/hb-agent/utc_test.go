package main

import (
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // the zone below resolves on a machine without zoneinfo, and in the child, which is this binary

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The agent's log, which the job keeps as agent.log, carries UTC times, not
// the node's local ones (HBEP-7: the agent's times are formatted in UTC;
// science-agent ruling, 1 Nov 2026, issue #94). The job runs in a zone with a
// non-zero offset (Europe/Warsaw, +01:00 or +02:00), as the clusters' nodes do;
// the agent is a child process, since a process fixes its local zone at its
// first use of the time package.
func TestAgentLogTimesAreUTC(t *testing.T) {
	const zone = "Europe/Warsaw"
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Fatalf("zone %s does not resolve: %v", zone, err)
	}
	if _, off := time.Now().In(loc).Zone(); off == 0 {
		t.Fatalf("%s has no offset now: the test would prove nothing", zone)
	}
	// Control: a child given this TZ prints local times with an offset, not Z.
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctl := exec.Command(self)
	ctl.Env = append(os.Environ(), "HB_TEST_ROLE="+roleLocalTime, "TZ="+zone)
	b, err := ctl.Output()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(b)); !regexp.MustCompile(`[+-]0[12]:00$`).MatchString(got) {
		t.Fatalf("a child with TZ=%s prints %q, want a +01:00 or +02:00 offset: the control does not hold", zone, got)
	}

	root := shortRoot(t)
	ep := startEndpoint(t, root)
	j := newJob(t, root, agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeDirect,
		Endpoint: ep.addr, CertSPKISHA256: ep.pin, FrpcDigest: strings.Repeat("d", 64),
	})
	j.extraEnv = []string{"TZ=" + zone}
	out := exercise(t, ep, j)

	utc := regexp.MustCompile(`^time=\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z level=`)
	logged := 0
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "time=") {
			continue
		}
		logged++
		if !utc.MatchString(line) {
			t.Errorf("log line with a time that is not UTC: %.120s", line)
		}
	}
	if logged < 3 {
		t.Errorf("%d log lines in the agent's output, want at least 3:\n%s", logged, out)
	}
}

// utcTime moves a logged time attribute as well as the record's own time.
func TestUTCTimeReplacesEveryTimeAttribute(t *testing.T) {
	loc := time.FixedZone("CET", 3600)
	in := time.Date(2026, 11, 1, 15, 4, 5, 0, loc)
	for _, key := range []string{"time", "leaseEnd"} {
		a := utcTime(nil, slog.Time(key, in))
		if got := a.Value.Time(); !got.Equal(in) || got.Location() != time.UTC {
			t.Errorf("%s: %v in %v, want the same instant in UTC", key, got, got.Location())
		}
	}
}
