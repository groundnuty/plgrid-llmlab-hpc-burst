package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
)

// TestRouterRefreshCost measures, rather than tests, what one registration change costs the
// front's router at the re-login sizes: the front's Tunnel calls its subscribers on every
// change of a UID's registration, and the router's subscriber (Router.Refresh) rebuilds its
// whole view from the snapshot under its lock. It times Refresh, unchanged, over the fixture's
// snapshot read by the front's own watch, with every transport registered. It asserts nothing
// and runs only with S16_MEASURE_REFRESH=1 (README, Results: the router's rebuild).
func TestRouterRefreshCost(t *testing.T) {
	if os.Getenv("S16_MEASURE_REFRESH") != "1" {
		t.Skip("a measurement: set S16_MEASURE_REFRESH=1")
	}
	for _, n := range []int{100, 300, 600} {
		sp := Spec{Mode: ModeDirect, DeploymentsNS: "s16", ProviderName: "example-a", ProfileName: "example-batch"}
		for i := 0; i < n; i++ {
			sp.Tunnels = append(sp.Tunnels, Tunnel{Index: i, UID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1),
				Allocation: fmt.Sprintf("10000000-0000-4000-8000-%012d", i+1), Token: "k", Model: "s16-m" + strconv.Itoa(i), Socket: "/s"})
		}
		objs, err := fixtureObjects(sp, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		w := inventory.NewWatch(fixtureClient(objs), nil, func() {})
		ctx, cancel := context.WithCancel(context.Background())
		if err := w.Start(ctx); err != nil {
			t.Fatal(err)
		}
		snap, _ := w.Snapshot()
		trans := front.NewStaticTransports()
		for _, tun := range sp.Tunnels {
			trans.Set(types.UID(tun.UID), "127.0.0.1:9")
		}
		cfg := front.Config{ClientKeysFile: "/dev/null", UsageDir: t.TempDir()}.WithDefaults()
		r := front.NewRouter(cfg, clock.Real{}, trans, &countRecorder{log: slog.New(slog.NewTextHandler(io.Discard, nil))},
			slog.New(slog.NewTextHandler(io.Discard, nil)), front.NewMetrics(nil, t.TempDir(), func(string) (uint64, error) { return 1 << 40, nil }, nil))
		r.Update(snap)
		const rounds = 50
		start := time.Now()
		for k := 0; k < rounds; k++ {
			r.Refresh()
		}
		per := time.Since(start) / rounds
		t.Logf("%d tunnels: one Router.Refresh %v; %d refreshes (a re-login's registrations) %v; twice that (a Bump and a pass per UID) %v",
			n, per, n, per*time.Duration(n), 2*per*time.Duration(n))
		cancel()
	}
}
