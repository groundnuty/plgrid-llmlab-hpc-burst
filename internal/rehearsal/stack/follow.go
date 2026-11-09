package stack

// Follow: a LIST, then a watch from its version, over connections that resume with
// no gap (watch.go records what it passes on; catchup.go relies on its order).

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Stream is what Follow passes on.
type Stream struct {
	// Event is called with every added, modified and deleted object.
	Event func(watch.EventType, client.Object)
	// Reached is called with each version the stream reached, an event's, after
	// Event, or a bookmark's or a LIST's: the stream has passed on every event
	// up to it.
	Reached func(resourceVersion string)
	// Reset is called when Follow has to start again from the current versions,
	// so that what came between can be gone for good.
	Reset func(why string)
}

// connectionSeconds is how long one connection of Follow lasts. The server
// sends a quiet stream a bookmark once a minute, and one two seconds before a
// connection's deadline, so that the length of a connection is how long a
// catch-up waits for its stream to reach a LIST's version. Measured on the
// rehearsal's k3s with no writes to the kind (PR #292), from a LIST to the
// stream's bookmark at its version or later: 3 s, p50 1.3 s, p90 2.6 s, max 4.5 s;
// 5 s, p50 1.9 s, p90 3.8 s, max 4.7 s; one long connection, p50 29 s, max 60 s.
const connectionSeconds int64 = 3

// followRetry is how long Follow waits after a call to the API server failed.
const followRetry = time.Second

// Follow passes on the namespace's objects of the list's kind and then every
// change to them, until ctx ends: a LIST, then a watch from its version. A
// connection that ends is opened again from the last version reached, with no
// gap. Only that version having expired starts it again from a LIST, which is
// a reset.
func Follow(ctx context.Context, wc client.WithWatch, namespace string, list client.ObjectList, s Stream) {
	FollowEvery(ctx, wc, namespace, list, s, connectionSeconds)
}

// FollowEvery is Follow with connections of the seconds given, for the measurement
// of what the short ones cost (TestWatchOverhead); every run uses Follow.
func FollowEvery(ctx context.Context, wc client.WithWatch, namespace string, list client.ObjectList, s Stream, seconds int64) {
	rv := ""
	wait := func() {
		select {
		case <-ctx.Done():
		case <-time.After(followRetry):
		}
	}
	for ctx.Err() == nil {
		if rv == "" {
			var err error
			if rv, err = followList(ctx, wc, namespace, list, s); err != nil {
				wait()
				continue
			}
		}
		timeout := seconds
		raw := &metav1.ListOptions{ResourceVersion: rv, AllowWatchBookmarks: true, TimeoutSeconds: &timeout}
		w, err := wc.Watch(ctx, list, client.InNamespace(namespace), &client.ListOptions{Raw: raw})
		if err == nil {
			rv, err = followConnection(w, rv, s)
		}
		switch {
		case err == nil:
		case expired(err):
			s.Reset(fmt.Sprintf("the watch could not resume from version %s: %v", rv, err))
			rv = ""
		default:
			wait()
		}
	}
}

// followList passes on what a LIST shows, and returns its version.
func followList(ctx context.Context, wc client.WithWatch, namespace string, list client.ObjectList, s Stream) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, APICallTimeout)
	defer cancel()
	got, ok := list.DeepCopyObject().(client.ObjectList)
	if !ok {
		return "", fmt.Errorf("%T is not a list", list)
	}
	if err := wc.List(ctx, got, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	rv := got.GetResourceVersion()
	if rv == "" {
		return "", fmt.Errorf("the LIST of %T gave no version", list)
	}
	err := meta.EachListItem(got, func(o runtime.Object) error {
		if co, ok := o.(client.Object); ok {
			s.Event(watch.Added, co)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	s.Reached(rv)
	return rv, nil
}

// followConnection passes on what one connection delivers until it ends, and
// returns the version reached, and the error it ended with, if any.
func followConnection(w watch.Interface, rv string, s Stream) (string, error) {
	defer w.Stop()
	for ev := range w.ResultChan() {
		switch ev.Type {
		case watch.Error:
			return rv, apierrors.FromObject(ev.Object)
		case watch.Bookmark:
			if o, ok := ev.Object.(client.Object); ok {
				rv = o.GetResourceVersion()
				s.Reached(rv)
			}
		default:
			if o, ok := ev.Object.(client.Object); ok {
				rv = o.GetResourceVersion()
				s.Event(ev.Type, o)
				s.Reached(rv)
			}
		}
	}
	return rv, nil
}

// expired reports whether the server no longer holds the version asked for.
func expired(err error) bool {
	return apierrors.IsGone(err) || apierrors.IsResourceExpired(err)
}
