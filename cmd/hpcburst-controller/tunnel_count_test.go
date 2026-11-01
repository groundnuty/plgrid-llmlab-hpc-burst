package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
)

// TestTunnelReadsCountsAReadAfterItsWrites pins the interleaving behind the CI failure of
// TestTunnelTriggerCausesARead at c4378ca (1 Nov 2026, "the condition turned True with 0 reads
// since frpc started"): a read's TunnelConnected True is stored, and visible to anyone who reads
// the Instance, BEFORE tunnelReads counts that read. The interleaving is forced: the write of
// True blocks in an interceptor while the test reads the stored condition and the counter. So a
// test that loads the counter once, the moment it sees True, can miss the read that wrote it, and
// the True here came from a read whose list said the proxy is online: not from an earlier list.
func TestTunnelReadsCountsAReadAfterItsWrites(t *testing.T) {
	in := unitInstance("in-1", "inst-uid-1", 1)
	stored, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c := tunnelScheme(t).WithObjects(in).WithInterceptorFuncs(interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if err := cl.SubResource(sub).Update(ctx, obj, opts...); err != nil {
				return err
			}
			once.Do(func() { close(stored) })
			<-release
			return nil
		},
	}).Build()
	listed := 0
	rd := readerFunc(func(_ context.Context, uids []string) frpsplugin.Report {
		listed++
		rep := frpsplugin.Report{Conditions: map[string]metav1.Condition{}}
		for _, u := range uids {
			rep.Conditions[u] = cond(metav1.ConditionTrue, "ProxyOnline", "frps lists the Instance's proxy as online")
		}
		return rep
	})
	s := stateFor(c, c, rd, nil)

	before := tunnelReads.Load()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.readOnce(t.Context())
	}()
	select {
	case <-stored:
	case <-time.After(30 * time.Second):
		close(release)
		t.Fatal("the read wrote no condition within 30 s")
	}
	got := meta.FindStatusCondition(getInstance(t, c, "in-1").Status.Conditions, "TunnelConnected")
	if got == nil || got.Status != metav1.ConditionTrue || got.Reason != "ProxyOnline" {
		close(release)
		t.Fatalf("while the write is held, the stored TunnelConnected is %+v, want True/ProxyOnline", got)
	}
	if n := tunnelReads.Load(); n != before {
		close(release)
		t.Fatalf("the read is counted (%d) while its write is still held, want the count to move only after it", n-before)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the read did not finish within 30 s of the release")
	}
	if n := tunnelReads.Load(); n != before+1 || listed != 1 {
		t.Fatalf("after the read: %d counted, %d lists read; want exactly one of each", n-before, listed)
	}
}
