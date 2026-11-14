package main

// The tunnel rig's condition watch is bounded at its start (#549): an API server that accepts the watch
// and never answers fails the test within the bound, with a message naming the server, and the stalled
// request is cancelled; a watch that starts is returned as it is.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestTheConditionWatchFailsFastOnAStalledAPIServer(t *testing.T) {
	// A server that accepts every request and never answers, until the client gives up.
	var watched, cancelled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" && strings.HasSuffix(r.URL.Path, "/instances") {
			watched.Store(true)
		}
		<-r.Context().Done()
		cancelled.Store(true)
	}))
	t.Cleanup(srv.Close)
	scheme, err := providerScheme()
	if err != nil {
		t.Fatal(err)
	}
	// A static mapper, so the client asks the server for nothing but the watch.
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(v1alpha1.GroupVersion.WithKind("Instance"), meta.RESTScopeNamespace)
	wc, err := client.NewWithWatch(&rest.Config{Host: srv.URL}, client.Options{Scheme: scheme, Mapper: mapper})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const within = 300 * time.Millisecond
	start := time.Now()
	w, err := openWatch(ctx, cancel, srv.URL, within, func(ctx context.Context) (watch.Interface, error) {
		return wc.Watch(ctx, &v1alpha1.InstanceList{}, client.InNamespace(tunnelNS))
	})
	took := time.Since(start)
	if err == nil || w != nil {
		t.Fatalf("a stalled API server: watch %v, error %v; want no watch and an error", w, err)
	}
	if !strings.Contains(err.Error(), srv.URL) || !strings.Contains(err.Error(), within.String()) {
		t.Errorf("error %q, want it to name the server %s and the bound %s", err, srv.URL, within)
	}
	if took > 10*time.Second {
		t.Errorf("it took %s to give up, want about %s", took, within)
	}
	if !watched.Load() {
		t.Error("the server never saw the Instance watch: the test proves nothing")
	}
	// The stalled request ends: the server's handler sees its client go.
	deadline := time.Now().Add(10 * time.Second)
	for !cancelled.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !cancelled.Load() {
		t.Error("the stalled watch request was never cancelled")
	}
}

func TestAWatchThatStartsIsReturned(t *testing.T) {
	fake := watch.NewFake()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := openWatch(ctx, cancel, "http://127.0.0.1:1", time.Minute, func(context.Context) (watch.Interface, error) { return fake, nil })
	if err != nil || w != fake {
		t.Fatalf("watch %v, error %v; want the started watch and no error", w, err)
	}
	if ctx.Err() != nil {
		t.Error("a watch that started had its context cancelled")
	}
}
