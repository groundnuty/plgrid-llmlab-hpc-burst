package lifecycle

// IdentityUnresolved (HBEP-3 as revised, 31 Oct 2026): a class has no current
// ID when a permanent failure, by hubfiles.Retryable, stops the checkpoint's
// file list, or the ID's display form collides with another identity record.
// The message names the cause (Hub 401, 403, 404, a blob mismatch, an invalid
// listing, or the colliding record id-<12 hex>); a permanent failure is read
// again only when the spec or the token changes, or at the periodic resync. A
// transient failure sets no reason and backs off, honouring Retry-After. A
// collision never loops.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/hubfiles"
)

// scriptedSource answers Files with the next scripted error, or the rig's
// files once the script is spent; it records the time of every call.
type scriptedSource struct {
	clock interface{ Now() time.Time }
	mu    sync.Mutex
	errs  []error
	calls []time.Time
}

func (s *scriptedSource) Files(context.Context, v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, s.clock.Now())
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return nil, err
	}
	return append([]deploymentid.File(nil), rigFiles...), nil
}

func (s *scriptedSource) TemplateSources(context.Context, v1alpha1.CheckpointSpec, []deploymentid.File) (deploymentid.TemplateSources, error) {
	return deploymentid.TemplateSources{}, nil
}

func (s *scriptedSource) callTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.calls...)
}

func hubErr(status int, retryAfter time.Duration) error {
	return &hubfiles.HTTPError{Method: "GET", Path: "/api/models/example-org/example-model/tree/" + rigRevision, Status: status, RetryAfter: retryAfter}
}

func wantDegraded(t *testing.T, rg *rig, reason, fragment string) {
	t.Helper()
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != reason || !strings.Contains(c.Message, fragment) {
		t.Fatalf("Degraded = %s/%s %q; want True/%s naming %q", c.Status, c.Reason, c.Message, reason, fragment)
	}
}

// identityTokenMD is mdSpec with a Hub token file, so a test can replace the
// token.
func identityTokenMD(t *testing.T, token string) (*v1alpha1.ModelDeployment, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hub-token")
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
	md := mdSpec("md")
	md.Spec.Source.Served.Checkpoint.HubTokenRef = &v1alpha1.CredentialsReference{File: path}
	return md, path
}

// TestIdentityUnresolvedPermanentWaitsForTheToken: a 401, 403 or 404 is
// permanent by hubfiles.Retryable (HBEP-3 as revised, 31 Oct 2026): the
// condition names the status, nothing is provisioned, and the Hub is not asked
// again, however long the passes run, until the token file's content changes;
// then the next pass reads again and the ID resolves.
func TestIdentityUnresolvedPermanentWaitsForTheToken(t *testing.T) {
	for _, status := range []int{401, 403, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			rg := newRig(t)
			src := &scriptedSource{clock: rg.clock, errs: []error{hubErr(status, 0)}}
			rg.r.Checkpoints = NewCachedSource(src, rg.clock)
			md, token := identityTokenMD(t, "hf_old_token")
			rg.create(md)
			rg.Step() // 08:00:00: the read fails
			wantDegraded(t, rg, v1alpha1.ReasonIdentityUnresolved, fmt.Sprintf("Hub %d", status))
			rg.run(time.Hour, 10*time.Second) // to 09:00:00
			if n := len(src.callTimes()); n != 1 {
				t.Fatalf("Hub asked %d times in an hour with the same token, want 1: %v", n, src.callTimes())
			}
			wantDegraded(t, rg, v1alpha1.ReasonIdentityUnresolved, fmt.Sprintf("Hub %d", status))
			if n := len(rg.allocations("md")); n != 0 || rg.submissions() != 0 {
				t.Fatalf("with no current ID, want no Allocation; got %d and %d submissions", n, rg.submissions())
			}
			// The operator replaces the token: the next pass reads again.
			if err := os.WriteFile(token, []byte("hf_new_token"), 0o600); err != nil {
				t.Fatal(err)
			}
			rg.run(10*time.Second, 10*time.Second) // 09:00:10
			if calls := src.callTimes(); len(calls) != 2 || !calls[1].Equal(rigStart.Add(time.Hour+10*time.Second)) {
				t.Fatalf("Hub calls %v, want a second one at 09:00:10, the first pass after the token changed", calls)
			}
			if ids := rg.md("md").Status.CurrentIDs; len(ids) != 1 {
				t.Fatalf("after the new token, want one current ID; got %v", ids)
			}
		})
	}
}

// TestIdentityUnresolvedPermanentRetriesOnASpecChange: a permanent failure is
// read again at once when the spec names another revision.
func TestIdentityUnresolvedPermanentRetriesOnASpecChange(t *testing.T) {
	rg := newRig(t)
	src := &scriptedSource{clock: rg.clock, errs: []error{hubErr(404, 0)}}
	rg.r.Checkpoints = NewCachedSource(src, rg.clock)
	rg.create(mdSpec("md"))
	rg.Step()
	rg.run(30*time.Minute, time.Minute)
	if n := len(src.callTimes()); n != 1 {
		t.Fatalf("Hub asked %d times for an unchanged spec, want 1", n)
	}
	rg.mutateMD("md", func(md *v1alpha1.ModelDeployment) {
		md.Spec.Source.Served.Checkpoint.Revision = "89abcdef0123456789abcdef0123456789abcdef"
	})
	rg.run(10*time.Second, 10*time.Second)
	if n := len(src.callTimes()); n != 2 {
		t.Fatalf("Hub asked %d times after the revision changed, want 2", n)
	}
}

// TestIdentityUnresolvedBlobMismatchAndInvalidListingArePermanent: a small file
// whose blob id does not match the listing, and a listing the strict decoder
// refuses, are permanent by hubfiles.Retryable: IdentityUnresolved names the
// cause, and the Hub is not asked again until the periodic resync, 10 h later
// (HBEP-3 as revised, 31 Oct 2026).
func TestIdentityUnresolvedBlobMismatchAndInvalidListingArePermanent(t *testing.T) {
	for _, tc := range []struct {
		name, cause string
		err         error
	}{
		{"blob mismatch", "blob id mismatch", fmt.Errorf("%w: config.json", hubfiles.ErrBlobMismatch)},
		{"invalid listing", "invalid listing", fmt.Errorf("%w: a page is not an array", hubfiles.ErrInvalidListing)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			src := &scriptedSource{clock: rg.clock, errs: []error{tc.err}}
			rg.r.Checkpoints = NewCachedSource(src, rg.clock)
			rg.create(mdSpec("md"))
			rg.Step() // 08:00:00
			wantDegraded(t, rg, v1alpha1.ReasonIdentityUnresolved, tc.cause)
			rg.run(9*time.Hour+50*time.Minute, 10*time.Minute) // to 17:50:00
			if n := len(src.callTimes()); n != 1 {
				t.Fatalf("Hub asked %d times before the resync, want 1: %v", n, src.callTimes())
			}
			wantDegraded(t, rg, v1alpha1.ReasonIdentityUnresolved, tc.cause)
			rg.run(10*time.Minute, 10*time.Minute) // 18:00:00, 10 h after the failure
			if calls := src.callTimes(); len(calls) != 2 || !calls[1].Equal(rigStart.Add(10*time.Hour)) {
				t.Fatalf("Hub calls %v, want a second one at the resync, 18:00:00", calls)
			}
			if ids := rg.md("md").Status.CurrentIDs; len(ids) != 1 {
				t.Fatalf("after the resync read, want one current ID; got %v", ids)
			}
		})
	}
}

// TestIdentityTransientSetsNoReasonAndBacksOff: a transient failure sets no
// reason (HBEP-3 as revised): a 503 with Retry-After 2 min is retried at
// 08:02:00, not before; a 502 without one backs off 60 s (the second failure:
// 30 s doubled), so the next read is at 08:03:00; it succeeds, the class gets
// its ID and an Allocation follows.
func TestIdentityTransientSetsNoReasonAndBacksOff(t *testing.T) {
	rg := newRig(t)
	src := &scriptedSource{clock: rg.clock, errs: []error{hubErr(503, 2*time.Minute), hubErr(502, 0)}}
	rg.r.Checkpoints = NewCachedSource(src, rg.clock)
	rg.create(mdSpec("md"))
	for end := rigStart.Add(2*time.Minute + 50*time.Second); ; {
		rg.Step()
		if c := rg.condition("md", v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonIdentityUnresolved {
			t.Fatalf("at %s a transient Hub failure set Degraded %s (%s); it sets no reason", rg.clock.Now().Format(time.TimeOnly), c.Reason, c.Message)
		}
		if !rg.clock.Now().Before(end) {
			break
		}
		rg.clock.Advance(10 * time.Second)
	}
	rg.run(70*time.Second, 10*time.Second) // to 08:04:00
	want := []time.Time{rigStart, rigStart.Add(2 * time.Minute), rigStart.Add(3 * time.Minute)}
	got := src.callTimes()
	if len(got) != len(want) {
		t.Fatalf("Hub calls at %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("Hub call %d at %s, want %s", i+1, got[i], want[i])
		}
	}
	if c := rg.md("md").Status.CurrentIDs; len(c) != 1 {
		t.Fatalf("after the Hub answers, want one current ID; got %v", c)
	}
	if rg.submissions() != 1 {
		t.Fatalf("submissions = %d, want 1 once the ID resolves", rg.submissions())
	}
}

// TestIdentityUnresolvedCollisionNeverLoops: an identity record id-<12 hex>
// already holds another full ID. The class gets no ID, the condition names the
// record, exactly one degraded event fires over many passes, nothing is
// provisioned; once the record goes (a record change) the ID resolves.
func TestIdentityUnresolvedCollisionNeverLoops(t *testing.T) {
	rg := newRig(t)
	md := mdSpec("md")
	// The true ID, from the library (pkg/deploymentid), with the empty template
	// the stub's checkpoint gives.
	tmpl, err := deploymentid.ChatTemplateSHA256("")
	if err != nil {
		t.Fatal(err)
	}
	id, err := deploymentid.Compute(deploymentid.ServingSpec{
		Checkpoint:         deploymentid.Checkpoint{Repository: "example-org/example-model", Revision: rigRevision},
		Engine:             deploymentid.Engine{Name: "vllm", Version: "0.32.0", ImageDigest: rigDigest},
		Flags:              []deploymentid.Flag{{Name: "max-model-len", Value: "32768"}},
		ChatTemplateSHA256: tmpl,
	}, deploymentid.Accelerator{GPUModel: rigGPU, CPUArch: rigArch}, rigFiles)
	if err != nil {
		t.Fatal(err)
	}
	other := id.SHA256[:12] + strings.Repeat("f", 52)
	if other == id.SHA256 {
		other = id.SHA256[:12] + strings.Repeat("e", 52)
	}
	data, _ := json.Marshal(identityData{Scheme: "v1", SHA256: other, Canonical: json.RawMessage(`{}`)})
	recName := "id-" + id.SHA256[:12]
	rg.create(&appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: recName, Namespace: rigNS},
		Data:       runtime.RawExtension{Raw: data}, Revision: 1,
	})
	rg.create(md)
	rg.run(5*time.Minute, 10*time.Second)
	wantDegraded(t, rg, v1alpha1.ReasonIdentityUnresolved, recName)
	if n := len(rg.rec.Find(eventDegraded, v1alpha1.ReasonIdentityUnresolved)); n != 1 {
		t.Fatalf("degraded/IdentityUnresolved events = %d over 30 passes, want exactly 1", n)
	}
	if rg.submissions() != 0 || len(rg.allocations("md")) != 0 {
		t.Fatal("a collided class must provision nothing")
	}
	// The operator removes the stale record: the ID resolves.
	var rec appsv1.ControllerRevision
	rec.Name, rec.Namespace = recName, rigNS
	if err := rg.c.Delete(rg.ctx, &rec); err != nil {
		t.Fatal(err)
	}
	rg.run(30*time.Second, 10*time.Second)
	ids := rg.md("md").Status.CurrentIDs
	if len(ids) != 1 || string(ids[0].ID) != id.SHA256 {
		t.Fatalf("currentIDs = %v, want %s", ids, id.SHA256)
	}
	if rg.submissions() != 1 {
		t.Fatalf("submissions = %d, want 1", rg.submissions())
	}
}
