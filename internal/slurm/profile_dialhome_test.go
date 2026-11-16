package slurm

// ReadProfile's dial-home check (HBEP-4, Core operations, ReadProfile; Platform
// profile schema, The remaining groups). Every expectation is written by hand
// from the HBEP's rule: when the partition lists dialHomeModes and the launcher's
// mode is ssh or host-dialed, the list must hold that mode; an absent list, an
// empty mode and direct are never checked. Each mode that is checked is paired
// with a list that does NOT hold it, since a fixture whose list always holds the
// launcher's mode proves nothing (HBEP-4, Test plan).

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

const (
	modeSSH        = v1alpha1.ProfileDialHomeSSH
	modeHostDialed = v1alpha1.ProfileDialHomeHostDialed
)

// readUnder reads the rig's Provider under a launcher in the given mode.
func (r *profileRig) readUnder(mode agentapi.DialHomeMode) (Validated, error) {
	r.t.Helper()
	return (&ProfileReader{Reader: r.client(), Home: r.dir, DialHomeMode: mode}).Read(context.Background(), "example-provider")
}

func TestReadProfileChecksTheLaunchersDialHomeMode(t *testing.T) {
	const field = "spec.partitions[example-gpu].dialHomeModes"
	for _, c := range []struct {
		name    string
		mode    agentapi.DialHomeMode
		list    []v1alpha1.ProfileDialHomeMode
		refused bool
	}{
		// Checked: the list lacks the launcher's mode.
		{"host-dialed, list only ssh", agentapi.DialHomeHostDialed, []v1alpha1.ProfileDialHomeMode{modeSSH}, true},
		{"ssh, list only host-dialed", agentapi.DialHomeSSH, []v1alpha1.ProfileDialHomeMode{modeHostDialed}, true},
		// Checked: the list holds it.
		{"host-dialed, list holds host-dialed", agentapi.DialHomeHostDialed, []v1alpha1.ProfileDialHomeMode{modeHostDialed}, false},
		{"host-dialed, list holds both", agentapi.DialHomeHostDialed, []v1alpha1.ProfileDialHomeMode{modeSSH, modeHostDialed}, false},
		{"ssh, list holds ssh", agentapi.DialHomeSSH, []v1alpha1.ProfileDialHomeMode{modeSSH}, false},
		{"ssh, list holds both", agentapi.DialHomeSSH, []v1alpha1.ProfileDialHomeMode{modeHostDialed, modeSSH}, false},
		// Never checked: an absent list, whatever the mode.
		{"host-dialed, no list", agentapi.DialHomeHostDialed, nil, false},
		{"ssh, no list", agentapi.DialHomeSSH, nil, false},
		// Never checked: direct and no mode, with a list that holds neither of them.
		{"direct, list only ssh", agentapi.DialHomeDirect, []v1alpha1.ProfileDialHomeMode{modeSSH}, false},
		{"direct, list only host-dialed", agentapi.DialHomeDirect, []v1alpha1.ProfileDialHomeMode{modeHostDialed}, false},
		{"no mode, list only ssh", "", []v1alpha1.ProfileDialHomeMode{modeSSH}, false},
		{"no mode, list only host-dialed", "", []v1alpha1.ProfileDialHomeMode{modeHostDialed}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newProfileRig(t)
			r.pp.Spec.Partitions[0].DialHomeModes = c.list
			_, err := r.readUnder(c.mode)
			if !c.refused {
				if err != nil {
					t.Fatalf("ReadProfile refused: %v; want a read", err)
				}
				return
			}
			var inv *provider.InvalidProfileError
			if !errors.As(err, &inv) {
				t.Fatalf("got %v, want an InvalidProfileError", err)
			}
			if len(inv.Fields) != 1 || inv.Fields[0].Field != field {
				t.Fatalf("fields %v, want exactly %s", inv.Fields, field)
			}
			if !strings.Contains(inv.Fields[0].Message, string(c.mode)) {
				t.Errorf("the message %q does not name the launcher's mode %s", inv.Fields[0].Message, c.mode)
			}
		})
	}
}

// Only the Provider's own partition is checked: another partition of the
// profile may list a mode the launcher does not use.
func TestDialHomeModesOfAnotherPartitionAreNotChecked(t *testing.T) {
	r := newProfileRig(t)
	r.pp.Spec.Partitions[0].DialHomeModes = []v1alpha1.ProfileDialHomeMode{modeHostDialed}
	other := r.pp.Spec.Partitions[0]
	other.Name = "example-other"
	other.DialHomeModes = []v1alpha1.ProfileDialHomeMode{modeSSH}
	r.pp.Spec.Partitions = append(r.pp.Spec.Partitions, other)
	if _, err := r.readUnder(agentapi.DialHomeHostDialed); err != nil {
		t.Fatalf("a partition the Provider does not use, listing only ssh, refused the read: %v", err)
	}
	// And the same two partitions refuse once the Provider names the other one.
	r.prov.Spec.AllowedPartitions = []string{"example-other"}
	if _, err := r.readUnder(agentapi.DialHomeHostDialed); err == nil || !strings.Contains(err.Error(), "spec.partitions[example-other].dialHomeModes") {
		t.Fatalf("got %v, want a refusal naming spec.partitions[example-other].dialHomeModes", err)
	}
}

// Every invalid field is still named when the dial-home check fails beside
// another (HBEP-4, ReadProfile: it fails with every invalid field named).
func TestDialHomeRefusalIsNamedBesideOtherInvalidFields(t *testing.T) {
	r := newProfileRig(t)
	r.pp.Spec.Partitions[0].DialHomeModes = []v1alpha1.ProfileDialHomeMode{modeSSH}
	r.pp.Spec.StagingDir = ""
	_, err := r.readUnder(agentapi.DialHomeHostDialed)
	var inv *provider.InvalidProfileError
	if !errors.As(err, &inv) {
		t.Fatalf("got %v, want an InvalidProfileError", err)
	}
	got := map[string]bool{}
	for _, f := range inv.Fields {
		got[f.Field] = true
	}
	for _, want := range []string{"spec.partitions[example-gpu].dialHomeModes", "spec.stagingDir"} {
		if !got[want] {
			t.Errorf("field %s is not named; got %v", want, inv.Fields)
		}
	}
}

// A reader built with no mode, as every existing caller builds it, checks nothing.
func TestAReaderWithNoModeNeverChecksTheList(t *testing.T) {
	r := newProfileRig(t)
	r.pp.Spec.Partitions[0].DialHomeModes = []v1alpha1.ProfileDialHomeMode{modeSSH}
	if _, err := r.read(); err != nil {
		t.Fatalf("the zero-value reader refused: %v", err)
	}
}
