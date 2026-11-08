package managed

import (
	"errors"
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// setupManager is a manager.Manager of which Setup calls only what it builds
// the controller from and Add.
type setupManager struct {
	manager.Manager
	c      client.Client
	addErr error
	added  int
}

func (m *setupManager) GetClient() client.Client    { return m.c }
func (m *setupManager) GetAPIReader() client.Reader { return m.c }
func (m *setupManager) GetEventRecorder(string) events.EventRecorder {
	return events.NewFakeRecorder(1)
}
func (m *setupManager) GetLogger() logr.Logger     { return logr.Discard() }
func (m *setupManager) Add(manager.Runnable) error { m.added++; return m.addErr }
func newSetupManager(t *testing.T, addErr error) *setupManager {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return &setupManager{c: clientfake.NewClientBuilder().WithScheme(scheme).Build(), addErr: addErr}
}

// TestSetupCreatesTheSeriesOnlyOnceTheControllerIsAdded: a managed Provider's
// series are created at 0 once its controller is built and added to its
// manager, not by a Setup that fails (an empty Provider name fails New, a
// manager that refuses the controller fails Add); and Forget, the managed
// supervisor's Dropped hook, deletes exactly them (review of PR 278).
func TestSetupCreatesTheSeriesOnlyOnceTheControllerIsAdded(t *testing.T) {
	for _, tc := range []struct {
		name, provider string
		addErr         error
	}{
		{"New fails", "", nil},
		{"Add fails", "example", errors.New("example: the manager refuses the controller")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := eventcount.New()
			if err := Setup(c)(newSetupManager(t, tc.addErr), tc.provider); err == nil {
				t.Fatal("Setup succeeded, want an error")
			}
			if got := raisedCounts(c); len(got) != 0 {
				t.Errorf("a failed Setup created the series %v", got)
			}
		})
	}

	c := eventcount.New()
	m := newSetupManager(t, nil)
	if err := Setup(c)(m, "example"); err != nil {
		t.Fatal(err)
	}
	if m.added != 1 {
		t.Fatalf("Setup added %d runnables, want the controller", m.added)
	}
	controller := eventcount.ControllerProvider("example")
	got := raisedCounts(c)
	for _, s := range RaisedEvents() {
		k := eventcount.Key{Controller: controller, Reason: s.Reason, Action: s.Action, Type: s.Type}
		if n, ok := got[k]; !ok || n != 0 {
			t.Errorf("%s reads %g (present %v) after Setup, want 0", k, n, ok)
		}
	}
	if len(got) != len(RaisedEvents()) {
		t.Errorf("Setup created %d series, want the %d of RaisedEvents", len(got), len(RaisedEvents()))
	}
	Forget(c)("example")
	if got := raisedCounts(c); len(got) != 0 {
		t.Errorf("after Forget: %v, want no series", got)
	}
}
