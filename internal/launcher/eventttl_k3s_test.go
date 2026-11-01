package launcher

// Tests on a real k3s of the event TTL (HBEP-8, Retention; issue #109): the API server expires an
// Event after the TTL it was started with, and an upgrade of a host that was started without the
// argument leaves a record with 168h. They start k3s in this process, as gc_test.go does, so the
// options that a test sets (a short TTL, no TTL) are unexported and cannot be reached from the
// command line.
//
// Both tests are parallel: they run together, after the serial tests of the package, each on a
// block of its own (the launcher's two blocks of 13 ports inside 29220-29246, IMPL-COMMON), and
// the 2-minute wait of the first overlaps the second.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The port bases of the two tests: 29220-29232 and 29233-29245.
const (
	ttlTestBase     = 29220
	upgradeTestBase = 29233
)

// guardRoot checks that the system k3s listens as it did when the test began, at the end of it.
func guardRoot(t *testing.T) {
	t.Helper()
	before := rootK3sListening(t)
	if os.Getenv("HPCBURST_TEST_ROOT_K3S") == "1" && before != "6443,6444,10250" {
		t.Fatalf("HPCBURST_TEST_ROOT_K3S=1, but the root k3s listens only on %q", before)
	}
	t.Cleanup(func() {
		after := rootK3sListening(t)
		t.Logf("root k3s check after the test: listens on %s (before: %s)", after, before)
		if after != before {
			t.Errorf("the root k3s's listeners changed: %q -> %q", before, after)
		}
	})
}

// downOnCleanup stops the host in dir at the end of the test.
func downOnCleanup(t *testing.T, dir string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := Down(context.Background(), DownOptions{StateDir: dir, Grace: 20 * time.Second, Log: testLog{t}}); err != nil {
			t.Errorf("down: %v", err)
		}
	})
}

func clientFor(t *testing.T, dir string) *kubernetes.Clientset {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", filepath.Join(dir, "kubeconfig"))
	if err != nil {
		t.Fatal(err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

// recordedEventTTL reads state.json with a map of the test's own, not with State: whether the key
// is there, and what it holds.
func recordedEventTTL(t *testing.T, dir string) (ttl any, present bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	ttl, present = m["event_ttl"]
	return ttl, present
}

// createEvent creates an events.k8s.io/v1 Event, the kind the controllers emit (HBEP-8), in the
// default namespace.
func createEvent(t *testing.T, cs *kubernetes.Clientset, name string) *eventsv1.Event {
	t.Helper()
	ev, err := cs.EventsV1().Events("default").Create(context.Background(), &eventsv1.Event{
		ObjectMeta:          metav1.ObjectMeta{Name: name, Namespace: "default"},
		EventTime:           metav1.NowMicro(),
		ReportingController: "hpcburst.test/event-ttl",
		ReportingInstance:   "event-ttl-test",
		Action:              "Probing",
		Reason:              "EventTTLProbe",
		Regarding:           corev1.ObjectReference{Kind: "ConfigMap", Namespace: "default", Name: "ttl-probe-subject"},
		Note:                "created by the event TTL test",
		Type:                provider.EventKindOrdinary,
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create the Event: %v", err)
	}
	return ev
}

// TestEventTTLIsHonouredByTheAPIServer starts k3s the way the launcher does, with a TTL of two
// minutes in place of 168h, creates an Event, and watches the API server expire it. This is the proof
// that the argument has an effect: the record and the arguments only say what the launcher meant.
// The Event is there at the first look, is not gone before a minute (the API server may reuse a
// lease, which shortens a life by a little, never by half), and is gone by TTL plus three minutes.
// The state.json that up wrote and the launcher's status both name the two minutes.
func TestEventTTLIsHonouredByTheAPIServer(t *testing.T) {
	t.Parallel()
	bins := k3sBinDir(t)
	guardRoot(t)
	sd := filepath.Join(shortDir(t), "st")
	if _, err := Up(context.Background(), UpOptions{StateDir: sd, BinDir: bins, PortBase: ttlTestBase, Nice: 10, ReadyTimeout: 3 * time.Minute,
		Log: testLog{t}, eventTTL: "2m"}); err != nil {
		t.Fatal(err)
	}
	downOnCleanup(t, sd)
	if ttl, ok := recordedEventTTL(t, sd); !ok || ttl != "2m" {
		t.Errorf("state.json: event_ttl %v (present %v), want 2m", ttl, ok)
	}
	srep, err := Status(context.Background(), StatusOptions{StateDir: sd, NoGCProbe: true})
	if err != nil {
		t.Fatal(err)
	}
	if srep.EventTTL != "2m" || srep.EventTTLDefault {
		t.Errorf("status: event TTL %q (default %v), want 2m", srep.EventTTL, srep.EventTTLDefault)
	}

	cs := clientFor(t, sd)
	ctx := context.Background()
	const name = "ttl-probe"
	ev := createEvent(t, cs, name)
	created := time.Now()
	deadline := created.Add(2*time.Minute + 3*time.Minute)
	for {
		got, err := cs.EventsV1().Events("default").Get(ctx, name, metav1.GetOptions{})
		age := time.Since(created).Round(time.Second)
		if apierrors.IsNotFound(err) {
			t.Logf("the Event was gone %s after it was created, with a TTL of 2m", age)
			if age < time.Minute {
				t.Errorf("the Event was gone after %s, well before the TTL of 2m", age)
			}
			return
		}
		if err != nil {
			t.Fatalf("get the Event: %v", err)
		}
		if got.UID != ev.UID {
			t.Fatalf("the Event %s was replaced: UID %s, was %s", name, got.UID, ev.UID)
		}
		if time.Now().After(deadline) {
			t.Errorf("the Event is still there %s after it was created, though k3s was started with event-ttl=2m", age)
			return
		}
		time.Sleep(5 * time.Second)
	}
}

// TestUpgradeAddsTheEventTTL: a host started without the argument, as the releases before #109 started
// it, has a record without event_ttl, and status says it runs with the API server's default: 1h, and
// healthy (an upgrade refuses a host that is not). After an upgrade to the next minor, which starts
// k3s from arguments built afresh, the record holds 168h, status says so, and an Event created before
// the upgrade is still listed (HBEP-8: after a restart, an event created before it is still present).
func TestUpgradeAddsTheEventTTL(t *testing.T) {
	t.Parallel()
	bins := k3sBinDir(t)
	if _, err := os.Stat(filepath.Join(bins, "k3s-1.35.9", "k3s")); err != nil {
		skipOrFail(t, "no pinned k3s 1.35.9: "+err.Error())
	}
	guardRoot(t)
	sd := filepath.Join(shortDir(t), "st")
	ctx := context.Background()
	if _, err := Up(ctx, UpOptions{StateDir: sd, BinDir: bins, Version: "1.35.9", PortBase: upgradeTestBase, Nice: 10, ReadyTimeout: 3 * time.Minute,
		Log: testLog{t}, omitEventTTL: true}); err != nil {
		t.Fatal(err)
	}
	downOnCleanup(t, sd)
	if ttl, ok := recordedEventTTL(t, sd); ok {
		t.Fatalf("the old host was meant to start without the argument, but state.json has event_ttl %v", ttl)
	}
	srep, err := Status(ctx, StatusOptions{StateDir: sd, NoGCProbe: true})
	if err != nil {
		t.Fatal(err)
	}
	if srep.Health != Healthy || srep.EventTTL != "1h" || !srep.EventTTLDefault {
		t.Errorf("status of the old host: %s %v, event TTL %q (default %v); want healthy, 1h, the default", srep.Health, srep.Reasons, srep.EventTTL, srep.EventTTLDefault)
	}

	const name = "pre-upgrade"
	ev := createEvent(t, clientFor(t, sd), name)
	urep, err := Upgrade(ctx, UpgradeOptions{StateDir: sd, BinDir: bins, To: "1.36.5", ReadyTimeout: 3 * time.Minute, Grace: 20 * time.Second, Log: testLog{t}})
	if err != nil {
		t.Fatal(err)
	}
	if !urep.Verification.OK() {
		t.Errorf("the upgrade did not keep the objects: %+v", urep.Verification)
	}
	if ttl, ok := recordedEventTTL(t, sd); !ok || ttl != "168h" {
		t.Errorf("state.json after the upgrade: event_ttl %v (present %v), want 168h", ttl, ok)
	}
	srep, err = Status(ctx, StatusOptions{StateDir: sd, NoGCProbe: true})
	if err != nil {
		t.Fatal(err)
	}
	if srep.Health != Healthy || srep.EventTTL != "168h" || srep.EventTTLDefault {
		t.Errorf("status after the upgrade: %s %v, event TTL %q (default %v); want healthy, 168h", srep.Health, srep.Reasons, srep.EventTTL, srep.EventTTLDefault)
	}
	if got, err := clientFor(t, sd).EventsV1().Events("default").Get(ctx, name, metav1.GetOptions{}); err != nil || got.UID != ev.UID {
		t.Errorf("the Event created before the upgrade is not there after it: %v", err)
	}
}
