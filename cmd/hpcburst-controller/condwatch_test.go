package main

// The tunnel rig's condition watch is bounded at its start (#549): an API server that accepts the watch
// and never answers fails the test once the bound passes, with a message naming the server, and the
// stalled request is cancelled; a watch that starts is returned as it is. No assertion races a wall-clock
// window: the stall is proven by the server's own signal that it holds the request, before the test lets
// the bound pass.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// condWatchWait bounds the test's own waits on the server's signals: generous, since it only turns a hang
// into a failure and is never what the test measures.
const condWatchWait = 30 * time.Second

func TestTheConditionWatchFailsFastOnAStalledAPIServer(t *testing.T) {
	// A server that accepts every request and never answers, until the client gives up. It signals when
	// it holds the Instance watch, and when that request's client went away.
	holding, gone := make(chan struct{}), make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isWatch := r.URL.Query().Get("watch") == "true" && strings.HasSuffix(r.URL.Path, "/instances")
		if isWatch {
			once.Do(func() { close(holding) })
		}
		<-r.Context().Done()
		if isWatch {
			close(gone)
		}
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
	const within = 30 * time.Second // named in the error; the test fires the expiry itself
	expire := make(chan time.Time)
	type result struct {
		w   watch.Interface
		err error
	}
	done := make(chan result, 1)
	go func() {
		w, err := openWatchUntil(ctx, cancel, srv.URL, within, expire, func(ctx context.Context) (watch.Interface, error) {
			return wc.Watch(ctx, &v1alpha1.InstanceList{}, client.InNamespace(tunnelNS))
		})
		done <- result{w, err}
	}()
	select {
	case <-holding:
	case <-time.After(condWatchWait):
		t.Fatal("the server never received the Instance watch: the test proves nothing")
	}
	select {
	case r := <-done:
		t.Fatalf("the watch returned (%v, %v) while the server held it unanswered", r.w, r.err)
	default:
	}
	close(expire)
	var r result
	select {
	case r = <-done:
	case <-time.After(condWatchWait):
		t.Fatal("the bound passed and the watch's start never gave up")
	}
	if r.err == nil || r.w != nil {
		t.Fatalf("a stalled API server: watch %v, error %v; want no watch and an error", r.w, r.err)
	}
	if !strings.Contains(r.err.Error(), srv.URL) || !strings.Contains(r.err.Error(), within.String()) {
		t.Errorf("error %q, want it to name the server %s and the bound %s", r.err, srv.URL, within)
	}
	select {
	case <-gone:
	case <-time.After(condWatchWait):
		t.Error("the stalled watch request was never cancelled")
	}
}

// openWatch's own timer gives up on an open that never returns until it is cancelled. The open blocks on
// its context alone, so however slow the machine, only the timer can end it.
func TestOpenWatchsTimerEndsAStartThatNeverReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := openWatch(ctx, cancel, "http://127.0.0.1:1", 50*time.Millisecond, func(ctx context.Context) (watch.Interface, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err == nil || w != nil || !strings.Contains(err.Error(), "50ms") {
		t.Fatalf("watch %v, error %v; want no watch and an error naming the bound", w, err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Error("the stalled open's context was not cancelled")
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
