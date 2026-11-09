package stack

// The API side: clients from up's kubeconfig, creating objects, and reading
// events.

import (
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// KubeClient is a polling client of up's API server.
func KubeClient(t T, state string) client.Client {
	t.Helper()
	c, err := client.New(PollingConfig(t, state), client.Options{Scheme: APIScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// RestConfig is the API's client configuration from up's kubeconfig. Several
// waits poll at once, so the client-side rate limit is raised above
// client-go's default of 5 per second, which would slow every wait.
func RestConfig(t T, state string) *rest.Config {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", filepath.Join(state, "kubeconfig"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.QPS, cfg.Burst = 50, 100
	return cfg
}

// PollingConfig is RestConfig for the client that polls: each request also
// bounded by APICallTimeout, behind the per-call contexts. Not for a watch,
// which the timeout would cut.
func PollingConfig(t T, state string) *rest.Config {
	cfg := RestConfig(t, state)
	cfg.Timeout = APICallTimeout
	return cfg
}

// APIScheme holds client-go's types and this repository's.
func APIScheme(t T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// Create creates each object, failing the test at the first refusal.
func Create(t T, c client.Client, objs ...client.Object) {
	t.Helper()
	for _, o := range objs {
		ctx, cancel := CallCtx(t)
		err := c.Create(ctx, o)
		cancel()
		if err != nil {
			t.Fatalf("create %T %s: %v", o, o.GetName(), err)
		}
	}
}

// EventsFor lists the events about an object, newest last.
func EventsFor(t T, c client.Client, name string) []corev1.Event {
	t.Helper()
	var l corev1.EventList
	ctx, cancel := CallCtx(t)
	defer cancel()
	if err := c.List(ctx, &l); err != nil {
		t.Fatalf("list events: %v", err)
	}
	var out []corev1.Event
	for _, e := range l.Items {
		if e.InvolvedObject.Name == name {
			out = append(out, e)
		}
	}
	return out
}

// Events lists the events.k8s.io Events of every namespace.
func Events(t T, c client.Client) []eventsv1.Event {
	t.Helper()
	var l eventsv1.EventList
	ctx, cancel := CallCtx(t)
	defer cancel()
	if err := c.List(ctx, &l); err != nil {
		t.Fatalf("list events.k8s.io events: %v", err)
	}
	return l.Items
}
