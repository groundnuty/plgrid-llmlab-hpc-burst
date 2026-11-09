package main

// Issue #279: the wiring of PR #278's Forget. The managed supervisor's Dropped
// hook is components.go's `Dropped: managed.Forget(w.events)`; the hook
// (providerctl) and Forget (managed, eventcount) are each tested, but a test
// that removes the wiring line failed nowhere. This one runs the binary's run
// function, so the components are the ones main builds, against a real API
// server and the LLMLab double: two managed Providers are set up, one is
// deleted, and the serving copy's /metrics must stop exporting the deleted
// Provider's provider-<name> series while the other's stay.
//
// Calling Forget in the test proves nothing about the wiring, so the test never
// does: the only thing that can delete a series here is the supervisor calling
// the hook that mount installed. The API server is envtest's with kernel-chosen
// ports (run_test.go); run it under the shared envtest lock.

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// raisedByController counts, per controller label, the hpcburst_events_raised_total
// series of a /metrics body, whatever their reason, action and type. A body with
// no such series gives an empty map; one that does not parse is an error, never
// an empty answer.
func raisedByController(body []byte) (map[string]int, error) {
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("parse the /metrics body: %w", err)
	}
	out := map[string]int{}
	if f := fams[eventcount.Name]; f != nil {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == eventcount.LabelController {
					out[l.GetValue()]++
				}
			}
		}
	}
	return out, nil
}

// missingSeries lists the keys of the series that body lacks under controller.
func missingSeries(t *testing.T, body []byte, controller string, series []eventcount.Series) []string {
	t.Helper()
	var missing []string
	for _, s := range series {
		key := eventcount.Key{Controller: controller, Reason: s.Reason, Action: s.Action, Type: s.Type}
		r, err := eventcount.ParseRead(body, key)
		if err != nil {
			t.Fatalf("read %s from /metrics: %v", key, err)
		}
		if !r.HasCount {
			missing = append(missing, key.String())
		}
	}
	return missing
}

// TestControllerBinaryStopsExportingADroppedManagedProvidersSeries is #279.
func TestControllerBinaryStopsExportingADroppedManagedProvidersSeries(t *testing.T) {
	api := startAPIServer(t)
	ctx := context.Background()
	c := api.client
	f, _, _ := prepareState(t, api, "direct", exampleFront)

	// One LLMLab double and one credentials file serve both Providers.
	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gwKey := "example-key-drop-0001"
	gw := llmlabtest.Start(t, llmlabtest.Options{Listener: gwLn, Keys: []string{gwKey}})
	gw.SetModels(llmlabtest.Model{ID: "example-27b", Revision: "2026-09"})
	creds, err := provider.ManagedCredentials{Key: gwKey, BaseURL: gw.BaseURL()}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	credsPath := filepath.Join(t.TempDir(), "llmlab.json")
	if err := os.WriteFile(credsPath, creds, 0o600); err != nil {
		t.Fatal(err)
	}

	profile := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "drop-llmlab"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformManaged}}
	newManaged := func(name string) *v1alpha1.Provider {
		return &v1alpha1.Provider{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.LabelPlatform: "llmlab"}},
			Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: profile.Name},
				CredentialsRef: &v1alpha1.CredentialsReference{File: credsPath}},
		}
	}
	// Neither name is a prefix of the other, so a label match cannot be a prefix match.
	gone, kept := newManaged("drop-gone"), newManaged("drop-kept")
	goneCtl, keptCtl := eventcount.ControllerProvider(gone.Name), eventcount.ControllerProvider(kept.Name)
	for _, o := range []client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ControllerNamespace}}, profile, gone, kept} {
		if err := c.Create(ctx, o); err != nil {
			t.Fatal(err)
		}
	}

	b := startBinary(t, &f, deps{providerScheme: providerScheme})

	// Both managers run: each holds its own Lease, so the drop below goes
	// through the managed supervisor's real stop of a running manager.
	for _, lease := range []string{v1alpha1.LeaseLifecycle, v1alpha1.ProviderLeaseName(gone.Name), v1alpha1.ProviderLeaseName(kept.Name)} {
		b.eventually(t, "the Lease "+lease+" is held", 90*time.Second, func() bool { return held(ctx, c, lease) })
	}

	// Before the drop, in one read: every series Setup creates at 0 is exported
	// for both Providers. Without this, the later absence could mean the series
	// were never created, or that the keys below do not match the exporter's.
	series := managed.RaisedEvents()
	b.eventually(t, "/metrics exports every managed series of both Providers", 90*time.Second, func() bool {
		body, ok := metricsBody(f.host.ControllerMetricsAddr())
		if !ok {
			return false
		}
		have, err := raisedByController(body)
		if err != nil {
			t.Fatal(err)
		}
		return have[goneCtl] >= len(series) && have[keptCtl] >= len(series) &&
			len(missingSeries(t, body, goneCtl, series)) == 0 && len(missingSeries(t, body, keptCtl, series)) == 0
	})

	// Delete one Provider. The supervisor lists every 5 s, stops the manager, and
	// only then calls Dropped, so the Lease released is the sign, independent of
	// /metrics, that the drop is under way: a run without the wiring fails the
	// wait below, not this one.
	if err := c.Delete(ctx, gone); err != nil {
		t.Fatal(err)
	}
	b.eventually(t, "the deleted Provider's manager has stopped and released its Lease", 90*time.Second, func() bool {
		return !held(ctx, c, v1alpha1.ProviderLeaseName(gone.Name))
	})

	// After the drop, in one read: none of the deleted Provider's series is
	// exported (any series under its controller label, not only the managed
	// keys), while every series of the other Provider and of the lifecycle
	// controller still is.
	var last string
	b.eventually(t, "/metrics no longer exports the deleted Provider's series, and still exports the other Provider's and the lifecycle controller's", 90*time.Second, func() bool {
		body, ok := metricsBody(f.host.ControllerMetricsAddr())
		if !ok {
			return false
		}
		have, err := raisedByController(body)
		if err != nil {
			t.Fatal(err)
		}
		var problems []string
		if n := have[goneCtl]; n != 0 {
			problems = append(problems, fmt.Sprintf("%d series of %s are still exported after its manager stopped", n, goneCtl))
		}
		if m := missingSeries(t, body, keptCtl, series); len(m) != 0 {
			problems = append(problems, fmt.Sprintf("%s lost %d series: %s", keptCtl, len(m), strings.Join(m, ", ")))
		}
		if m := missingSeries(t, body, eventcount.ControllerLifecycle, lifecycle.RaisedEvents()); len(m) != 0 {
			problems = append(problems, fmt.Sprintf("%s lost %d series: %s", eventcount.ControllerLifecycle, len(m), strings.Join(m, ", ")))
		}
		sort.Strings(problems)
		if now := strings.Join(problems, "; "); now != last {
			last = now
			t.Logf("after the drop: %q", now)
		}
		return len(problems) == 0
	})

	if err := b.stop(t); err != nil {
		t.Fatalf("run returned %v after its context ended, want nil", err)
	}
}
