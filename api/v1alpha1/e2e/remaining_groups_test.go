package e2e

import (
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// HBEP-4, Platform profile schema, The remaining groups (beta), and HBEP-8's
// billing (Provider accounting, Billing): the admission rows of HBEP-4's Test
// plan for the four new profile fields, the Allocation's billing count, and the
// Provider's grant end (#625). Every value is chosen by hand.

// A partition's billing unit is gpu-hour or cpu-hour, or absent (unknown).
func TestPartitionBillingUnit(t *testing.T) {
	for _, u := range []v1alpha1.BillingUnit{"gpu-hours", "GPU-hour", "hour", "node-hour"} {
		t.Run("refuse "+string(u), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Partitions[0].BillingUnit = u
			requireRefused(t, c.Create(bg(), p), "billingUnit", "Unsupported value")
		})
	}
	for _, u := range []v1alpha1.BillingUnit{v1alpha1.BillingUnitGPUHour, v1alpha1.BillingUnitCPUHour, ""} {
		t.Run("accept "+string(u), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Partitions[0].BillingUnit = u
			must(t, c.Create(bg(), p), "create with billingUnit "+string(u))
			cleanup(t, p)
			got := &v1alpha1.PlatformProfile{}
			must(t, c.Get(bg(), objKey(p), got), "get")
			if got.Spec.Partitions[0].BillingUnit != u {
				t.Fatalf("billingUnit = %q, want %q", got.Spec.Partitions[0].BillingUnit, u)
			}
		})
	}
}

// dialHomeModes lists ssh and host-dialed, each at most once; direct is no
// profile value, and neither is any other mode.
func TestPartitionDialHomeModes(t *testing.T) {
	modes := func(m ...v1alpha1.ProfileDialHomeMode) []v1alpha1.ProfileDialHomeMode { return m }
	for _, tc := range []struct {
		name  string
		modes []v1alpha1.ProfileDialHomeMode
		want  string
	}{
		{"a mode twice", modes("ssh", "ssh"), "Duplicate value"},
		{"host-dialed twice", modes("host-dialed", "host-dialed"), "Duplicate value"},
		{"direct", modes("direct"), "Unsupported value"},
		{"an unknown mode", modes("frp"), "Unsupported value"},
		{"three entries", modes("ssh", "host-dialed", "ssh"), "dialHomeModes"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Partitions[0].DialHomeModes = tc.modes
			requireRefused(t, c.Create(bg(), p), "dialHomeModes", tc.want)
		})
	}
	for _, tc := range []struct {
		name  string
		modes []v1alpha1.ProfileDialHomeMode
	}{
		{"both", modes(v1alpha1.ProfileDialHomeSSH, v1alpha1.ProfileDialHomeHostDialed)},
		{"host-dialed alone", modes(v1alpha1.ProfileDialHomeHostDialed)},
		{"ssh alone", modes(v1alpha1.ProfileDialHomeSSH)},
		{"absent", nil},
	} {
		t.Run("accept "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.Partitions[0].DialHomeModes = tc.modes
			must(t, c.Create(bg(), p), "create with dialHomeModes")
			cleanup(t, p)
			got := &v1alpha1.PlatformProfile{}
			must(t, c.Get(bg(), objKey(p), got), "get")
			if len(got.Spec.Partitions[0].DialHomeModes) != len(tc.modes) {
				t.Fatalf("dialHomeModes = %v, want %v", got.Spec.Partitions[0].DialHomeModes, tc.modes)
			}
		})
	}
}

// newManagedProfile is a managed platform's profile: no batch fields.
func newManagedProfile(name string) *v1alpha1.PlatformProfile {
	return &v1alpha1.PlatformProfile{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformManaged},
	}
}

// The gateway group is a managed gateway's behaviour, so a batch profile that
// sets it is refused, and a managed one is accepted with it set or absent.
func TestGatewayOnlyOnAManagedProfile(t *testing.T) {
	const msg = "gateway is a managed gateway's behaviour: only a managed profile may set it"
	for _, kind := range []v1alpha1.PlatformKind{v1alpha1.PlatformBatch, v1alpha1.PlatformStatic} {
		t.Run("refuse on "+string(kind), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.ProviderKind = kind
			p.Spec.Gateway = &v1alpha1.GatewayProfile{RewritesRequests: ptr.To(true)}
			requireRefused(t, c.Create(bg(), p), msg)
		})
	}
	t.Run("refuse an empty group on batch", func(t *testing.T) {
		p := newProfile(uniq("prof"))
		p.Spec.Gateway = &v1alpha1.GatewayProfile{}
		requireRefused(t, c.Create(bg(), p), msg)
	})
	for name, g := range map[string]*v1alpha1.GatewayProfile{
		"absent":           nil,
		"empty":            {},
		"rewrites":         {RewritesRequests: ptr.To(true)},
		"does not rewrite": {RewritesRequests: ptr.To(false)},
	} {
		t.Run("accept on managed, "+name, func(t *testing.T) {
			p := newManagedProfile(uniq("prof"))
			p.Spec.Gateway = g
			must(t, c.Create(bg(), p), "create a managed profile")
			cleanup(t, p)
			got := &v1alpha1.PlatformProfile{}
			must(t, c.Get(bg(), objKey(p), got), "get")
			if g != nil && g.RewritesRequests != nil && (got.Spec.Gateway == nil || got.Spec.Gateway.RewritesRequests == nil || *got.Spec.Gateway.RewritesRequests != *g.RewritesRequests) {
				t.Fatalf("gateway = %+v, want rewritesRequests %v", got.Spec.Gateway, *g.RewritesRequests)
			}
		})
	}
}

// overloadStatus is an HTTP error status, 400 to 599.
func TestGatewayOverloadStatusBounds(t *testing.T) {
	for _, s := range []int32{399, 600, 200, 0} {
		t.Run("refuse "+strconv.Itoa(int(s)), func(t *testing.T) {
			p := newManagedProfile(uniq("prof"))
			p.Spec.Gateway = &v1alpha1.GatewayProfile{OverloadStatus: ptr.To(s)}
			requireRefused(t, c.Create(bg(), p), "overloadStatus")
		})
	}
	for _, s := range []int32{429, 400, 599, 503} {
		t.Run("accept "+strconv.Itoa(int(s)), func(t *testing.T) {
			p := newManagedProfile(uniq("prof"))
			p.Spec.Gateway = &v1alpha1.GatewayProfile{OverloadStatus: ptr.To(s)}
			must(t, c.Create(bg(), p), "create with overloadStatus")
			cleanup(t, p)
			got := &v1alpha1.PlatformProfile{}
			must(t, c.Get(bg(), objKey(p), got), "get")
			if got.Spec.Gateway == nil || got.Spec.Gateway.OverloadStatus == nil || *got.Spec.Gateway.OverloadStatus != s {
				t.Fatalf("gateway = %+v, want overloadStatus %d", got.Spec.Gateway, s)
			}
		})
	}
}

// The Allocation's billing count is a count: never negative. A known value
// round-trips through the status subresource.
func TestAllocationAccountingBilling(t *testing.T) {
	ns := newNamespace(t)
	a := newAllocation(ns, uniq("alloc"))
	must(t, c.Create(bg(), a), "create")
	read := metav1.NewTime(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC))
	a.Status.AtProvider.Accounting = &v1alpha1.AllocationAccounting{AllocatedBilling: ptr.To(int32(-1)), ReadTime: read}
	requireRefused(t, c.Status().Update(bg(), a), "allocatedBilling")

	got := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), got), "get")
	got.Status.AtProvider.Accounting = &v1alpha1.AllocationAccounting{
		AllocatedCPUs: ptr.To(int32(16)), AllocatedGPUs: ptr.To(int32(2)), AllocatedBilling: ptr.To(int32(3)),
		ElapsedSeconds: ptr.To(int64(5400)), ReadTime: read,
	}
	must(t, c.Status().Update(bg(), got), "write a billing count of 3")
	again := &v1alpha1.Allocation{}
	must(t, c.Get(bg(), objKey(a), again), "get")
	acct := again.Status.AtProvider.Accounting
	if acct == nil || acct.AllocatedBilling == nil || *acct.AllocatedBilling != 3 {
		t.Fatalf("accounting = %+v, want allocatedBilling 3", acct)
	}
	if s, ok := acct.BilledUnitSeconds(); !ok || s != 16200 {
		t.Fatalf("BilledUnitSeconds = %d, %v; want 16200 (4.5 billed units)", s, ok)
	}
}

// grantEnd is an optional time on the Provider, kept as given.
func TestProviderGrantEnd(t *testing.T) {
	end := metav1.NewTime(time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC))
	p := newProvider(uniq("prov"))
	p.Spec.GrantEnd = &end
	must(t, c.Create(bg(), p), "create with a grant end")
	cleanup(t, p)
	got := &v1alpha1.Provider{}
	must(t, c.Get(bg(), objKey(p), got), "get")
	if got.Spec.GrantEnd == nil || !got.Spec.GrantEnd.Time.Equal(end.Time) {
		t.Fatalf("grantEnd = %v, want %v", got.Spec.GrantEnd, end)
	}
	none := newProvider(uniq("prov"))
	must(t, c.Create(bg(), none), "create with no grant end")
	cleanup(t, none)
}
