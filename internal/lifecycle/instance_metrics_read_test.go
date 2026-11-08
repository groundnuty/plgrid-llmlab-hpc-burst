package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The tests in this file cover HBEP-8, Unknown samples and Test plan, A failed
// Instance inventory read (`@hpc-burst-science-agent`, 6 Nov 2026, for #249;
// PR #254 review, B1): the inventory read is the Instance list and the cache
// reads of each Instance's Allocation, Provider and PlatformProfile. Any error
// but not-found fails it, and then hpcburst_collector_success{collector=
// "instances"} is 0 and no Instance series is exposed, while /metrics answers
// 200 and the accounting gauges are untouched; after the next successful read
// it is 1 and the series are back.

// faultyReader fails one read of the collector's on demand: the list of a
// kind, or a get of one, with an error or a not-found.
type faultyReader struct {
	client.Reader
	mu       sync.Mutex
	verb     string // "list" or "get"; "" fails nothing
	kind     string // the object's Go type, %T, without the package
	notFound bool
}

func (f *faultyReader) set(verb, kind string, notFound bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verb, f.kind, f.notFound = verb, kind, notFound
}

func (f *faultyReader) fault(verb string, obj any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	kind := fmt.Sprintf("%T", obj)
	if f.verb != verb || kind[strings.LastIndex(kind, ".")+1:] != f.kind {
		return nil
	}
	if f.notFound {
		return apierrors.NewNotFound(schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: strings.ToLower(f.kind) + "s"}, "x")
	}
	return errors.New("the cache cannot serve the read")
}

func (f *faultyReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := f.fault("get", obj); err != nil {
		return err
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

func (f *faultyReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := f.fault("list", list); err != nil {
		return err
	}
	return f.Reader.List(ctx, list, opts...)
}

// TestMetricsInstanceInventoryReadFailure: each of the four reads failing on
// its own gives instances 0 and no Instance series, with accounting at 1 and
// the scrape answering 200 (scrapeCollectors fails on any other status); the
// next successful read gives 1 and the Instance's nine engine samples again.
// A not-found from any of the three gets is no failure: instances stays 1 and
// the Instance keeps its info series. The rig's ready Instance reports known
// values, so "no series" is not an Instance with nothing to show.
func TestMetricsInstanceInventoryReadFailure(t *testing.T) {
	rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	uid := string(in.UID)
	reader := &faultyReader{Reader: rg.c}
	scrapeVia := func() exposition {
		c := rg.collector(always)
		c.instances.cache = reader
		c.accounting.allocs = reader
		return scrapeCollectors(t, c)
	}
	checkRead := func(when string) {
		t.Helper()
		ex := scrapeVia()
		if got := collectorSuccess(t, ex, collectorInstances); got != "1" {
			t.Fatalf("%s: hpcburst_collector_success{collector=instances} %s, want 1", when, got)
		}
		if got := len(ex.engineSeries(uid)); got != 9 {
			t.Fatalf("%s: %d engine samples, want all 9", when, got)
		}
	}
	checkRead("before any failure")
	for _, tc := range []struct{ verb, kind string }{
		{"list", "InstanceList"},
		{"get", "Allocation"},
		{"get", "Provider"},
		{"get", "PlatformProfile"},
	} {
		what := tc.verb + " " + tc.kind
		reader.set(tc.verb, tc.kind, false)
		ex := scrapeVia()
		if got := collectorSuccess(t, ex, collectorInstances); got != "0" {
			t.Errorf("while the %s fails: hpcburst_collector_success{collector=instances} %s, want 0", what, got)
		}
		if got := collectorSuccess(t, ex, collectorAccounting); got != "1" {
			t.Errorf("while the %s fails: hpcburst_collector_success{collector=accounting} %s, want 1: its read is its own", what, got)
		}
		for _, s := range ex.samples {
			if strings.HasPrefix(s.name, "hpcburst_instance_") {
				t.Errorf("while the %s fails: Instance series %v, want none", what, s)
			}
		}
		reader.set("", "", false)
		checkRead("after the " + what + " recovers")
	}
	for _, kind := range []string{"Allocation", "Provider", "PlatformProfile"} {
		reader.set("get", kind, true)
		ex := scrapeVia()
		if got := collectorSuccess(t, ex, collectorInstances); got != "1" {
			t.Errorf("a get of the %s that finds none: hpcburst_collector_success{collector=instances} %s, want 1 (not-found is no failure)", kind, got)
		}
		if n := len(ex.find("hpcburst_instance_info", map[string]string{"instance_uid": uid})); n != 1 {
			t.Errorf("a get of the %s that finds none: %d info series, want 1", kind, n)
		}
		reader.set("", "", false)
	}
}
