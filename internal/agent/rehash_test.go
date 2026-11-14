package agent

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// A staging that ends logs its rehash once, numbers only, by the msg and keys the alpha rows read (issue #575;
// internal/alpha, AgentRehashMsg and RehashOf): the files found present and rehashed, of those listed, their bytes and the
// rehash's milliseconds. A cold start logs files=0. A staging that failed logs no rehash line.
func TestAStagingThatEndsLogsItsRehash(t *testing.T) {
	const msgRehashed = "checkpoint rehashed"
	for _, c := range []struct {
		name string
		r    RehashStat
		want string
	}{
		{"warm", RehashStat{Files: 3, Listed: 3, Bytes: 4_500_000_000, Duration: 7250 * time.Millisecond}, " files=3 listed=3 bytes=4500000000 ms=7250"},
		{"cold", RehashStat{Files: 0, Listed: 3}, " files=0 listed=3 bytes=0 ms=0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			a := &Agent{ctx: context.Background(), log: slog.New(slog.NewTextHandler(&buf, nil))}
			in := &instance{uid: "u-1", stageGen: 2, stageRunning: true}
			a.inst = in
			a.stagingDone(in, 2, ImageResult{}, CheckpointResult{Rehash: c.r}, nil, nil)
			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if len(lines) != 1 || !strings.Contains(lines[0], `level=INFO msg="checkpoint rehashed"`) || !strings.HasSuffix(lines[0], c.want) {
				t.Errorf("the log is %q, want one INFO line %q ending %q", buf.String(), msgRehashed, c.want)
			}
		})
	}
	var buf bytes.Buffer
	a := &Agent{ctx: context.Background(), log: slog.New(slog.NewTextHandler(&buf, nil))}
	in := &instance{uid: "u-1", stageGen: 2, stageRunning: true}
	a.inst = in
	a.stagingDone(in, 2, ImageResult{}, CheckpointResult{Rehash: RehashStat{Files: 1, Listed: 1}}, nil, errors.New("the hub refused"))
	if strings.Contains(buf.String(), msgRehashed) {
		t.Errorf("a failed staging logged a rehash: %q", buf.String())
	}
}
