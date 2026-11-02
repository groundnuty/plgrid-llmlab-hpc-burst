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
		if strings.HasPrefix(r.Name, "tunnel-hour-") {
			want = TunnelLease()
		}
		if r.Lease != want {
			t.Errorf("%s: lease %s, want %s", r.Name, r.Lease, want)
		}
	}
}

// The tunnel rows need the hour to fit between the Instance becoming ready, which can take health.startTimeout
// (1 h), and its drain start, the drain window (5 min + 10 min) before the lease ends: 1 h + 61 min + 15 min, and a
// minute, is 2 h 17 min, by hand; and it is longer than the lead.
func TestTheTunnelLeaseFitsTheHourAfterASlowStart(t *testing.T) {
	if TunnelLease() != 137*time.Minute {
		t.Fatalf("tunnel lease %s, want 2h17m0s", TunnelLease())
	}
	if TunnelLease() <= Lead() {
		t.Errorf("the tunnel lease %s is not longer than the lead %s", TunnelLease(), Lead())
	}
	// A slow start, then the hour, then the drain window: all inside the lease.
	if StartTimeout+TunnelHour+MaxRequestDuration+DrainMargin >= TunnelLease() {
		t.Errorf("the hour does not fit after a start of %s in a lease of %s", StartTimeout, TunnelLease())
	}
	// And the shorter lease of the handover rows would not hold it.
	if StartTimeout+TunnelHour+MaxRequestDuration+DrainMargin < MinLease() {
		t.Error("the handover lease would hold the hour after a slow start, so the tunnel rows need no longer one")
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
		{"the handover lease on a tunnel row", "tunnel-hour-athena", func(d *DeploymentSpec) { d.LeaseLimit = MinLease() }, "not the row's shortest lease"},
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
