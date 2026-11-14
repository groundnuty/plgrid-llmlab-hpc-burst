package alpha

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func shippedProfile(t *testing.T, name string) v1alpha1.PlatformProfileSpec {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "profiles", name+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(b, &pp); err != nil {
		t.Fatal(err)
	}
	return pp.Spec
}

// The shortest lease is worked out two ways. By hand, from the lead as
// profiles/athena.yaml:39 states it: a 5-minute routing.maxRequestDuration gives a
// 1 h 45 min lead, and a time limit must be longer than the lead in whole
// minutes, so 1 h 46 min. And from the shipped profiles, with the API's own
// InstanceDrainWindow and the lifecycle controller's three terms
// (internal/lifecycle/timelimit.go:22): drain window, handoverMargin and
// health.startTimeout, each at the profile's value or its schema default.
func TestTheShortestLeaseIsJustAboveThePartitionTooShortMinimum(t *testing.T) {
	if Lead() != 105*time.Minute || MinLease() != 106*time.Minute {
		t.Fatalf("lead %s, shortest lease %s; want 1h45m0s and 1h46m0s", Lead(), MinLease())
	}
	maxReq := v1alpha1.Duration(MaxRequestDuration.String())
	for _, cluster := range []string{"helios", "athena"} {
		t.Run(cluster, func(t *testing.T) {
			spec := shippedProfile(t, cluster)
			window, err := v1alpha1.InstanceDrainWindow(spec, &v1alpha1.Routing{MaxRequestDuration: &maxReq})
			if err != nil {
				t.Fatal(err)
			}
			handover, start := 30*time.Minute, time.Hour // the schema defaults
			if spec.HandoverMargin != nil {
				if handover, err = spec.HandoverMargin.AsDuration(); err != nil {
					t.Fatal(err)
				}
			}
			if spec.Health != nil && spec.Health.StartTimeout != nil {
				if start, err = spec.Health.StartTimeout.AsDuration(); err != nil {
					t.Fatal(err)
				}
			}
			if lead := window + handover + start; lead != Lead() {
				t.Errorf("the shipped %s profile gives a lead of %s, the package assumes %s", cluster, lead, Lead())
			}
		})
	}
	for _, r := range Rows() {
		want := MinLease()
		switch {
		case strings.HasPrefix(r.Name, "tunnel-hour-"):
			want = DefaultTunnelLease()
		case r.Name == "alpha-athena-now-serve", r.Name == BetaRouterName, r.Name == BetaKeeperName, r.Name == BetaBulkName:
			// Row A on plgrid-now runs under athena-now.yaml, whose lead is 20 minutes; its lease is the hour less its start
			// wait and two minutes of cleanup (the science agent's ruling Q3 on #376): 60 - 10 - 2.
			want = 48 * time.Minute
		case r.Name == "alpha-athena-now-nooverlap":
			// Row B on plgrid-now: the shortest lease athena-now.yaml places, its 20-minute lead and a minute.
			want = 21 * time.Minute
		}
		if r.Lease != want {
			t.Errorf("%s: lease %s, want %s", r.Name, r.Lease, want)
		}
	}
}

// The tunnel rows' lease is chosen by the GH200 measurement (issue #94, comment 6557821833): the second real-engine
// run's job started at 11:42:26Z and its first Instance had qualified and warmed up by 12:06:05Z, which by hand is
// 23 min 39 s. That is over the 20 minutes under which the short lease is kept, so the default is the lease that
// holds a start of up to health.startTimeout: 1 h + 61 min + the 15-minute drain window + 1 minute, 2 h 17 min, by hand.
func TestTheTunnelLeaseFollowsTheMeasurement(t *testing.T) {
	start := time.Date(2026, 11, 1, 11, 42, 26, 0, time.UTC)
	ready := time.Date(2026, 11, 1, 12, 6, 5, 0, time.UTC)
	if got := ready.Sub(start); got != MeasuredStartToReady {
		t.Errorf("the measurement is %s in the posts and %s in the code", got, MeasuredStartToReady)
	}
	if MeasuredStartToReady != 23*time.Minute+39*time.Second {
		t.Errorf("the measurement is %s, want 23m39s by hand", MeasuredStartToReady)
	}
	if TunnelLease() != 137*time.Minute || DefaultTunnelLease() != 137*time.Minute {
		t.Errorf("tunnel lease %s, default %s, want 2h17m0s", TunnelLease(), DefaultTunnelLease())
	}
	// The rule at its boundary.
	for _, c := range []struct {
		measured time.Duration
		want     time.Duration
	}{
		{19*time.Minute + 59*time.Second, MinLease()},
		{20 * time.Minute, TunnelLease()},
		{23*time.Minute + 39*time.Second, TunnelLease()},
	} {
		if got := tunnelLeaseFor(c.measured); got != c.want {
			t.Errorf("a measured %s gives a lease of %s, want %s", c.measured, got, c.want)
		}
	}
	// What each lease allows: by hand, the lease less the 15-minute drain window less the 61 minutes of traffic.
	if LatestReady(TunnelLease()) != 61*time.Minute || LatestReady(MinLease()) != 30*time.Minute {
		t.Errorf("latest ready %s under the default and %s under the short lease, want 1h1m0s and 30m0s", LatestReady(TunnelLease()), LatestReady(MinLease()))
	}
	if LatestReady(TunnelLease()) < MeasuredStartToReady {
		t.Error("the default lease does not hold the measured start")
	}
}

func TestTheOwnerMayChooseAnotherTunnelLease(t *testing.T) {
	for _, c := range []struct {
		name  string
		row   string
		lease time.Duration
		ok    bool
	}{
		{"the short lease", "tunnel-hour-helios", MinLease(), true},
		{"a minute under the short lease", "tunnel-hour-helios", MinLease() - time.Minute, false},
		{"the longest allowed", "tunnel-hour-athena", MaxTunnelLease, true},
		{"a minute over the longest", "tunnel-hour-athena", MaxTunnelLease + time.Minute, false},
		{"a part of a minute", "tunnel-hour-helios", MinLease() + time.Second, false},
		{"a handover row", "alpha-athena-handover", MinLease(), false},
		{"a no-overlap row", "alpha-helios-nooverlap", MinLease(), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, _ := RowByName(c.row)
			got, err := WithTunnelLease(r, c.lease)
			if (err == nil) != c.ok {
				t.Fatalf("err %v, want ok %t", err, c.ok)
			}
			if c.ok {
				if got.Lease != c.lease || got.Plan().LeaseLimit != c.lease {
					t.Errorf("lease %s, plan %s, want %s", got.Lease, got.Plan().LeaseLimit, c.lease)
				}
				// The slot hold follows the lease on Helios, and a row with no slot has none.
				if wantHold := map[bool]time.Duration{true: c.lease, false: 0}[r.QOS != ""]; got.SlotHold != wantHold {
					t.Errorf("slot hold %s, want %s", got.SlotHold, wantHold)
				}
			} else if got.Lease != r.Lease {
				t.Errorf("a refused lease changed the row to %s", got.Lease)
			}
		})
	}
}

func TestDeploymentBoundaries(t *testing.T) {
	cases := []struct {
		name   string
		row    string
		mutate func(*DeploymentSpec)
		want   string
	}{
		{"as planned", "alpha-helios-nooverlap", func(*DeploymentSpec) {}, ""},
		{"Athena added to the Helios deployment", "alpha-helios-nooverlap", func(d *DeploymentSpec) {
			d.Providers = append(d.Providers, ProviderRef{Name: "athena", Cluster: "athena"})
		}, "does not allow exactly one Provider"},
		{"no Provider", "alpha-helios-nooverlap", func(d *DeploymentSpec) { d.Providers = nil }, "does not allow exactly one Provider"},
		{"the other cluster's Provider alone", "alpha-helios-nooverlap", func(d *DeploymentSpec) { d.Providers[0].Cluster = "athena" }, "not on the row's cluster"},
		{"a lease at the lead", "alpha-athena-handover", func(d *DeploymentSpec) { d.LeaseLimit = Lead() }, "not longer than the lead"},
		{"a lease a minute below the lead", "alpha-athena-handover", func(d *DeploymentSpec) { d.LeaseLimit = Lead() - time.Minute }, "not longer than the lead"},
		{"the shortest lease", "alpha-athena-handover", func(d *DeploymentSpec) { d.LeaseLimit = Lead() + time.Minute }, ""},
		{"a lease a minute over the shortest", "alpha-athena-handover", func(d *DeploymentSpec) { d.LeaseLimit = MinLease() + time.Minute }, "not the row's shortest lease"},
		{"the short lease on a tunnel row, which is not its default", "tunnel-hour-athena", func(d *DeploymentSpec) { d.LeaseLimit = MinLease() }, "not the row's shortest lease"},
		{"the tunnel lease on a tunnel row", "tunnel-hour-athena", func(d *DeploymentSpec) { d.LeaseLimit = TunnelLease() }, ""},
		{"a shorter request deadline than the row's", "tunnel-hour-athena", func(d *DeploymentSpec) { d.MaxRequestDuration = 4 * time.Minute }, "maxRequestDuration is not the row's"},
		{"the default drain window's request deadline", "tunnel-hour-athena", func(d *DeploymentSpec) { d.MaxRequestDuration = 2 * time.Hour }, "maxRequestDuration is not the row's"},
		{"no ReleaseWhenIdle", "alpha-helios-nooverlap", func(d *DeploymentSpec) { d.ReleaseWhenIdle = false }, "lacks ReleaseWhenIdle"},
		{"an idle limit of 0", "alpha-helios-nooverlap", func(d *DeploymentSpec) { d.IdleLimit = 0 }, "lacks ReleaseWhenIdle"},
		{"an idle limit of 10 minutes", "alpha-helios-nooverlap", func(d *DeploymentSpec) { d.IdleLimit = 10 * time.Minute }, ""},
		{"an idle limit over 10 minutes", "alpha-helios-nooverlap", func(d *DeploymentSpec) { d.IdleLimit = 10*time.Minute + time.Second }, "lacks ReleaseWhenIdle"},
		// Only the no-overlap row asks for ReleaseWhenIdle.
		{"ReleaseWhenIdle on a handover row", "alpha-athena-handover", func(d *DeploymentSpec) { d.ReleaseWhenIdle, d.IdleLimit = true, time.Minute }, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row, _ := RowByName(c.row)
			d := row.Plan()
			d.Providers = append([]ProviderRef(nil), d.Providers...)
			c.mutate(&d)
			wantFailure(t, deploymentChecks(row, d), c.want)
		})
	}
}

// The accounting row under QOS now has the start-wait guard of conformance-helios-gh200 (the science agent's
// rule of 2 Nov 2026: a job that has not started within 10 minutes of its acceptance is cancelled by its tag and
// the row is NOT RUN); the rows with no slot have none, and the stack rows keep the queue wait of the operator, but row A
// on plgrid-now (issue #376, the plan's section 3), whose first clock is a start wait of 10 minutes in the owner's slot,
// and row B, each of whose two jobs has one. So has the accounting row on plgrid-now (issue #145, the science agent's ruling
// of 12 Nov 2026): the same test as the Athena row of plgrid-gpu-a100, in the partition's one-job slot. The expected values
// are written out here, not read from the rows.
func TestOnlyTheAccountingRowOnHeliosHasAStartWait(t *testing.T) {
	want := map[string]time.Duration{"gpu-accounting-helios": 10 * time.Minute, "gpu-accounting-athena-now": 10 * time.Minute, "alpha-athena-now-serve": 10 * time.Minute, "beta-athena-now-router": 10 * time.Minute, "beta-athena-now-keeper": 10 * time.Minute, "beta-athena-now-bulk": 10 * time.Minute, "alpha-athena-now-nooverlap": 10 * time.Minute}
	for _, r := range append(Rows(), AccountingRows()...) {
		if r.StartWait != want[r.Name] {
			t.Errorf("%s: start wait %s, want %s", r.Name, r.StartWait, want[r.Name])
		}
	}
	// Their budget covers the wait, the job's end and the deletion: 10 + 10 + 5 minutes, and a margin.
	for _, name := range []string{"gpu-accounting-helios", "gpu-accounting-athena-now"} {
		r, ok := RowByName(name)
		if !ok {
			t.Fatalf("no row %s", name)
		}
		if r.Budget < r.StartWait+15*time.Minute {
			t.Errorf("%s: budget %s is below the start wait and the test's other deadlines, %s", name, r.Budget, r.StartWait+15*time.Minute)
		}
	}
}
