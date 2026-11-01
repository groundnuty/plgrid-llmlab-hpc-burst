package e2e

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Every spec field of type Duration carries a CEL rule that parses it, so a
// value that overflows Go's time.Duration is refused at admission; a field that
// needs a positive value also refuses 0s, and one that allows zero accepts it
// (api/v1alpha1/common_types.go, Duration). There is one rule per field, so
// the table has one subtest per field: neutralising any one rule fails exactly
// its subtest, and the refusal is checked against that field's own message.
const (
	// overflows is one hour more than the largest whole number of hours a Go
	// time.Duration holds (2562047h).
	overflows = "2562048h"
	// largest is the largest whole number of hours a Go time.Duration holds.
	largest = "2562047h"
)

// durationField is one Duration field with a rule.
type durationField struct {
	path   string // the field's path, the subtest's name
	name   string // its JSON name, which its rule's message starts with
	zeroOK bool   // 0s is accepted (the rule is >= 0s, not > 0s)
	// create creates a valid object whose only change is this field set to d,
	// and registers its deletion when it was created.
	create func(t *testing.T, d v1alpha1.Duration) error
}

// durationFields lists every Duration field of the five kinds that has a rule.
func durationFields(ns string) []durationField {
	var fields []durationField

	alloc := func(path, name string, set func(*v1alpha1.Allocation, v1alpha1.Duration)) {
		fields = append(fields, durationField{path: path, name: name,
			create: func(t *testing.T, d v1alpha1.Duration) error {
				a := newAllocation(ns, uniq("dur"))
				set(a, d)
				return c.Create(bg(), a)
			}})
	}
	alloc("Allocation spec.forProvider.batch.timeLimit", "timeLimit", func(a *v1alpha1.Allocation, d v1alpha1.Duration) {
		a.Spec.ForProvider.Batch.TimeLimit = d
	})
	alloc("Allocation spec.retentionPolicy.idleLimit", "idleLimit", func(a *v1alpha1.Allocation, d v1alpha1.Duration) {
		a.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: &d}
	})

	deployment := func(path, name string, set func(*v1alpha1.ModelDeployment, v1alpha1.Duration)) {
		fields = append(fields, durationField{path: path, name: name,
			create: func(t *testing.T, d v1alpha1.Duration) error {
				md := newModelDeployment(ns, uniq("dur"))
				set(md, d)
				return c.Create(bg(), md)
			}})
	}
	deployment("ModelDeployment spec.leaseLimit", "leaseLimit", func(md *v1alpha1.ModelDeployment, d v1alpha1.Duration) {
		md.Spec.LeaseLimit = &d
	})
	deployment("ModelDeployment spec.routing.maxRequestDuration", "maxRequestDuration", func(md *v1alpha1.ModelDeployment, d v1alpha1.Duration) {
		md.Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: &d}
	})
	deployment("ModelDeployment spec.retentionPolicy.idleLimit", "idleLimit", func(md *v1alpha1.ModelDeployment, d v1alpha1.Duration) {
		md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: &d}
	})

	profile := func(path, name string, zeroOK bool, set func(*v1alpha1.PlatformProfile, v1alpha1.Duration)) {
		fields = append(fields, durationField{path: "PlatformProfile " + path, name: name, zeroOK: zeroOK,
			create: func(t *testing.T, d v1alpha1.Duration) error {
				p := newProfile(uniq("dur"))
				set(p, d)
				err := c.Create(bg(), p)
				if err == nil {
					cleanup(t, p)
				}
				return err
			}})
	}
	profile("spec.partitions[].maxTime", "maxTime", false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) {
		p.Spec.Partitions[0].MaxTime = &d
	})
	profile("spec.lostGrace", "lostGrace", false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) { p.Spec.LostGrace = &d })
	profile("spec.inflightWindow", "inflightWindow", false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) {
		p.Spec.InflightWindow = &d
	})
	profile("spec.unknownBound", "unknownBound", false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) { p.Spec.UnknownBound = &d })
	profile("spec.drainWindow", "drainWindow", false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) { p.Spec.DrainWindow = &d })
	profile("spec.maxIdle", "maxIdle", false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) { p.Spec.MaxIdle = &d })
	profile("spec.qos[].maxWall", "maxWall", false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) {
		p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast", MaxWall: &d}}
	})

	health := func(name string, set func(*v1alpha1.HealthThresholds, v1alpha1.Duration)) {
		profile("spec.health."+name, name, false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) {
			p.Spec.Health = &v1alpha1.HealthThresholds{}
			set(p.Spec.Health, d)
		})
	}
	health("probeInterval", func(h *v1alpha1.HealthThresholds, d v1alpha1.Duration) { h.ProbeInterval = &d })
	health("noProgressTimeout", func(h *v1alpha1.HealthThresholds, d v1alpha1.Duration) { h.NoProgressTimeout = &d })
	health("errorOnlyWindow", func(h *v1alpha1.HealthThresholds, d v1alpha1.Duration) { h.ErrorOnlyWindow = &d })
	health("kvSkewTimeout", func(h *v1alpha1.HealthThresholds, d v1alpha1.Duration) { h.KVSkewTimeout = &d })
	health("agentTimeout", func(h *v1alpha1.HealthThresholds, d v1alpha1.Duration) { h.AgentTimeout = &d })
	health("registrationTimeout", func(h *v1alpha1.HealthThresholds, d v1alpha1.Duration) { h.RegistrationTimeout = &d })
	health("startTimeout", func(h *v1alpha1.HealthThresholds, d v1alpha1.Duration) { h.StartTimeout = &d })

	profile("spec.restart.window", "window", false, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) {
		p.Spec.Restart = &v1alpha1.RestartPolicy{Window: &d}
	})
	// These five allow zero: no grace, no backoff, no minimum age, no margin.
	profile("spec.restart.stopGrace", "stopGrace", true, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) {
		p.Spec.Restart = &v1alpha1.RestartPolicy{StopGrace: &d}
	})
	profile("spec.restart.backoff", "backoff", true, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) {
		p.Spec.Restart = &v1alpha1.RestartPolicy{Backoff: &d}
	})
	profile("spec.minJobAge", "minJobAge", true, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) { p.Spec.MinJobAge = &d })
	profile("spec.drainMargin", "drainMargin", true, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) { p.Spec.DrainMargin = &d })
	profile("spec.handoverMargin", "handoverMargin", true, func(p *v1alpha1.PlatformProfile, d v1alpha1.Duration) {
		p.Spec.HandoverMargin = &d
	})
	return fields
}

func TestDurationsArePositiveAndFit(t *testing.T) {
	ns := newNamespace(t)
	fields := durationFields(ns)
	// The list is the 25 spec fields of type Duration that carry a rule. A
	// field added without a row here is caught by hack/mutate-cel.sh, which
	// maps every rule to this test; this count catches a row dropped by mistake.
	if len(fields) != 25 {
		t.Fatalf("the table has %d fields, want the 25 Duration fields that carry a rule", len(fields))
	}
	for _, f := range fields {
		t.Run(f.path, func(t *testing.T) {
			// The start of the rule's message, which names the field and says
			// whether zero is allowed.
			msg := f.name + " must be a positive duration that fits Go's time.Duration"
			if f.zeroOK {
				msg = f.name + " must be a duration that fits Go's time.Duration"
			}
			t.Run("refuse a value that overflows time.Duration", func(t *testing.T) {
				requireRefused(t, f.create(t, overflows), msg)
			})
			if f.zeroOK {
				t.Run("accept zero", func(t *testing.T) {
					must(t, f.create(t, "0s"), "create with "+f.name+"=0s")
				})
			} else {
				t.Run("refuse zero", func(t *testing.T) {
					requireRefused(t, f.create(t, "0s"), msg)
				})
			}
			t.Run("accept a normal value", func(t *testing.T) {
				must(t, f.create(t, "1s"), "create with "+f.name+"=1s")
			})
			t.Run("accept the largest value that fits", func(t *testing.T) {
				must(t, f.create(t, largest), "create with "+f.name+"="+largest)
			})
		})
	}
}
