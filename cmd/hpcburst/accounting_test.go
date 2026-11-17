package main

// hpcburst accounting over a fake API client (HBEP-8, Provider accounting, Billing, Totals per grant). The
// objects are HBEP-8's totals row, written by hand: the records as the JSON the controller writes them,
// the live Allocations and the Providers and profiles that give the grant, the account and the unit. The
// expected lines are literals worked out from the row, never the command's own sum:
//
//	g1/a1, gpu-hour: billing 1 for 7,200 s (2.0) + billing 3 for 5,400 s (4.5) + a running Allocation with
//	  billing 1 at 1,800 s (0.5) = 7.0 GPU-hours; unknown: a record with its billing unknown, a record
//	  written before the beta fields, a pending Allocation: 3 of 6 members. grantEnd 2026-12-31T00:00:00Z
//	  against the clock at 2026-11-16T00:00:00Z: 45 days left.
//	g1/a2, gpu-hour: billing 4 for 43,200 s = 48.0 GPU-hours.
//	g1/c1, cpu-hour: billing 4 for 1,800 s = 2.0 CPU-hours.
//
// Every record has 16 CPUs and 2 GPUs, so a total built from either count shows. Grant and account names
// are synthetic.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/accounting"
)

var acctNow = time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)

func acctScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// acctRecord is a ControllerRevision of the accounting kind with the given data.
func acctRecord(ns, name, data string) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{v1alpha1.LabelRecord: v1alpha1.RecordAccounting, v1alpha1.LabelProvider: "prov-a"}},
		Data:       runtime.RawExtension{Raw: []byte(data)}, Revision: 1,
	}
}

func recJSON(provider, partition string, elapsed int, rest string) string {
	return `{"provider":"` + provider + `","partition":"` + partition + `","tag":"example-tag","jobIDs":["1234567"],` +
		`"allocatedCPUs":16,"allocatedGPUs":2,"elapsedSeconds":` + itoa(elapsed) + rest + `,"basis":"allocated"}`
}

func acctProvider(name, grant, account, profile string, end *time.Time) *v1alpha1.Provider {
	p := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.ProviderSpec{
		Grant: grant, Account: account, PlatformProfileRef: v1alpha1.ObjectReference{Name: profile},
	}}
	if end != nil {
		t := metav1.NewTime(*end)
		p.Spec.GrantEnd = &t
	}
	return p
}

func acctProfile(name, partition string, unit v1alpha1.BillingUnit) *v1alpha1.PlatformProfile {
	return &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.PlatformProfileSpec{
		Partitions: []v1alpha1.PartitionProfile{{Name: partition, BillingUnit: unit}},
	}}
}

func acctAlloc(name string, uid types.UID, partition string, acct *v1alpha1.AllocationAccounting) *v1alpha1.Allocation {
	return &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Namespace: "example-ns-1", Name: name, UID: uid},
		Spec: v1alpha1.AllocationSpec{
			ProviderConfigReference: &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: "prov-a"},
			ForProvider:             v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{Partition: partition}},
		},
		Status: v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{Accounting: acct}},
	}
}

func i32p(v int32) *int32 { return &v }
func i64p(v int64) *int64 { return &v }

// totalsRowObjects is HBEP-8's row, plus a verification record and an unlabelled revision that the command
// must not read.
func totalsRowObjects() []client.Object {
	end := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	running := &v1alpha1.AllocationAccounting{AllocatedCPUs: i32p(16), AllocatedGPUs: i32p(2), AllocatedBilling: i32p(1),
		ElapsedSeconds: i64p(1800), ReadTime: metav1.NewTime(acctNow)}
	return []client.Object{
		acctProvider("prov-a", "g1", "a1", "prof-gpu", &end),
		acctProvider("prov-b", "g1", "a2", "prof-gpu", nil),
		acctProvider("prov-c", "g1", "c1", "prof-cpu", nil),
		acctProfile("prof-gpu", "gpu-part", v1alpha1.BillingUnitGPUHour),
		acctProfile("prof-cpu", "cpu-part", v1alpha1.BillingUnitCPUHour),
		acctRecord("example-ns-1", "acct-r1", recJSON("prov-a", "gpu-part", 7200, `,"allocatedBilling":1,"billingUnit":"gpu-hour","grant":"g1","account":"a1"`)),
		acctRecord("example-ns-1", "acct-r2", recJSON("prov-a", "gpu-part", 5400, `,"allocatedBilling":3,"billingUnit":"gpu-hour","grant":"g1","account":"a1"`)),
		// Its billing count is unknown, named so.
		acctRecord("example-ns-2", "acct-r3", recJSON("prov-a", "gpu-part", 3600, `,"billingUnit":"gpu-hour","grant":"g1","account":"a1","unknown":["allocatedBilling"]`)),
		// Written before the beta fields: no billing count, no names, nothing named unknown.
		acctRecord("example-ns-2", "acct-r4", recJSON("prov-a", "gpu-part", 3600, ``)),
		acctRecord("example-ns-1", "acct-r5", recJSON("prov-b", "gpu-part", 43200, `,"allocatedBilling":4,"billingUnit":"gpu-hour","grant":"g1","account":"a2"`)),
		acctRecord("example-ns-2", "acct-r6", recJSON("prov-c", "cpu-part", 1800, `,"allocatedBilling":4,"billingUnit":"cpu-hour","grant":"g1","account":"c1"`)),
		acctAlloc("live-running", "uid-live-running", "gpu-part", running),
		acctAlloc("live-pending", "uid-live-pending", "gpu-part", nil),
		// Other kinds of record are not accounting: a total that read them would add 1,000 to a1.
		&appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Namespace: "example-ns-1", Name: "ver-x", Labels: map[string]string{v1alpha1.LabelRecord: v1alpha1.RecordVerification}},
			Data: runtime.RawExtension{Raw: []byte(recJSON("prov-a", "gpu-part", 3600000, `,"allocatedBilling":1000,"grant":"g1","account":"a1"`))}, Revision: 1},
		&appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Namespace: "example-ns-1", Name: "unlabelled"},
			Data: runtime.RawExtension{Raw: []byte(recJSON("prov-a", "gpu-part", 3600000, `,"allocatedBilling":1000,"grant":"g1","account":"a1"`))}, Revision: 1},
	}
}

func acctClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return clientfake.NewClientBuilder().WithScheme(acctScheme(t)).WithObjects(objs...).Build()
}

// The text report: the heading, the three groups with the literal totals, the unknown count beside the
// first, the days left, and nothing that adds GPU-hours to CPU-hours.
func TestAccountingTextReport(t *testing.T) {
	var out, errb bytes.Buffer
	if code := runAccounting(context.Background(), acctClient(t, totalsRowObjects()...), acctNow, false, &out, &errb); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	if errb.Len() != 0 {
		t.Errorf("stderr %q, want none", errb.String())
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 5 {
		t.Fatalf("%d lines, want a heading, a note and three groups:\n%s", len(lines), out.String())
	}
	if lines[0] != "Accounting totals per grant and account: allocated, not billed." {
		t.Errorf("heading %q", lines[0])
	}
	for i, want := range []string{
		"grant g1, account a1: 7.0 GPU-hours, 3 unknown of 6 member(s); 45 day(s) left (grant ends 2026-12-31)",
		"grant g1, account a2: 48.0 GPU-hours, 0 unknown of 1 member(s)",
		"grant g1, account c1: 2.0 CPU-hours, 0 unknown of 1 member(s)",
	} {
		if lines[2+i] != want {
			t.Errorf("group line %d\n got %q\nwant %q", i, lines[2+i], want)
		}
	}
	for _, line := range lines[2:] {
		if strings.Contains(line, "GPU") && strings.Contains(line, "CPU") {
			t.Errorf("line %q puts GPU-hours and CPU-hours together", line)
		}
	}
	for _, sum := range []string{"57.0", "55.0", "9.0", "50.0", "1000"} {
		if strings.Contains(out.String(), sum) {
			t.Errorf("the report shows %s: a sum across units or another kind of record:\n%s", sum, out.String())
		}
	}
}

// --json prints the same data: the label, the clock, and the groups with the same literals.
func TestAccountingJSONReport(t *testing.T) {
	var out, errb bytes.Buffer
	if code := runAccounting(context.Background(), acctClient(t, totalsRowObjects()...), acctNow, true, &out, &errb); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	var got accounting.Report
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if got.Label != "allocated, not billed" || !got.Now.Time.Equal(acctNow) {
		t.Errorf("label %q now %v", got.Label, got.Now)
	}
	type line struct {
		grant, account string
		unit           v1alpha1.BillingUnit
		billed         float64
		known, unknown int
	}
	want := []line{
		{"g1", "a1", v1alpha1.BillingUnitGPUHour, 7.0, 3, 3},
		{"g1", "a2", v1alpha1.BillingUnitGPUHour, 48.0, 1, 0},
		{"g1", "c1", v1alpha1.BillingUnitCPUHour, 2.0, 1, 0},
	}
	if len(got.Groups) != len(want) {
		t.Fatalf("%d groups, want %d:\n%s", len(got.Groups), len(want), out.String())
	}
	for i, w := range want {
		g := got.Groups[i]
		if g.Grant != w.grant || g.Account != w.account || g.Unit != w.unit || g.Billed != w.billed || g.Known != w.known || g.Unknown != w.unknown {
			t.Errorf("group %d = %+v, want %+v", i, g, w)
		}
	}
	if d := got.Groups[0].DaysLeft; d == nil || *d != 45 {
		t.Errorf("a1's days left %v, want 45", d)
	}
	if got.Groups[1].DaysLeft != nil || got.Groups[2].DaysLeft != nil {
		t.Errorf("a2 and c1 have no grantEnd and show no days left")
	}
	if !strings.Contains(out.String(), `"unit": "gpu-hour"`) {
		t.Errorf("the JSON names no unit:\n%s", out.String())
	}
}

// A record whose data is JSON but not an accounting record's (the API server stores only valid JSON) is
// named and left out of every total; the totals stay those of the readable ones.
func TestAccountingNamesAnUnreadableRecord(t *testing.T) {
	objs := append(totalsRowObjects(), acctRecord("example-ns-2", "acct-bad", `{"provider":5}`))
	var out, errb bytes.Buffer
	if code := runAccounting(context.Background(), acctClient(t, objs...), acctNow, false, &out, &errb); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	text := out.String()
	if !strings.Contains(text, "1 accounting record(s) could not be read and are in no total: example-ns-2/acct-bad") {
		t.Errorf("the report does not name the unreadable record:\n%s", text)
	}
	if !strings.Contains(text, "grant g1, account a1: 7.0 GPU-hours, 3 unknown of 6 member(s)") {
		t.Errorf("a1's line changed:\n%s", text)
	}
}

// A grant whose end has passed says so, and no list that fails is read as an empty one.
func TestAccountingAfterTheGrantEndsAndWhenAListFails(t *testing.T) {
	var out, errb bytes.Buffer
	after := time.Date(2027, 1, 5, 0, 0, 0, 0, time.UTC)
	if code := runAccounting(context.Background(), acctClient(t, totalsRowObjects()...), after, false, &out, &errb); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	if !strings.Contains(out.String(), "3 unknown of 6 member(s); the grant ended 2026-12-31") {
		t.Errorf("a1's line after the end:\n%s", out.String())
	}

	for _, kind := range []string{"AllocationList", "ProviderList", "PlatformProfileList", "ControllerRevisionList"} {
		t.Run(kind, func(t *testing.T) {
			c := clientfake.NewClientBuilder().WithScheme(acctScheme(t)).WithObjects(totalsRowObjects()...).
				WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if strings.HasSuffix(typeName(list), "."+kind) {
						return errors.New("example: the API server does not answer")
					}
					return c.List(ctx, list, opts...)
				}}).Build()
			var out, errb bytes.Buffer
			code := runAccounting(context.Background(), c, acctNow, false, &out, &errb)
			if code != exitFail || out.Len() != 0 || !strings.Contains(errb.String(), "does not answer") {
				t.Errorf("exit %d, stdout %q, stderr %q; want exit %d, no totals and the error", code, out.String(), errb.String(), exitFail)
			}
		})
	}
}

func typeName(v any) string { return reflect.TypeOf(v).String() }

// The command line: the help says what it does, an unknown flag is a usage error, and a kubeconfig that is
// not there is a failure that says so, before any read.
func TestAccountingCommandLine(t *testing.T) {
	code, out, errs := runInProcess(t, "accounting", "-h")
	if code != exitOK || !strings.Contains(out+errs, "allocated, not billed") || !strings.Contains(out+errs, "hpcburst accounting") {
		t.Errorf("accounting -h: exit %d, output %q", code, out+errs)
	}
	if code, _, _ := runInProcess(t, "accounting", "--no-such-flag"); code != exitUsage {
		t.Errorf("an unknown flag: exit %d, want %d", code, exitUsage)
	}
	if code, _, _ := runInProcess(t, "accounting", "extra"); code != exitUsage {
		t.Errorf("an argument: exit %d, want %d", code, exitUsage)
	}
	code, out, errs = runInProcess(t, "accounting", "--kubeconfig", t.TempDir()+"/no-such-kubeconfig")
	if code != exitFail || out != "" || !strings.Contains(errs, "read the kubeconfig") {
		t.Errorf("a missing kubeconfig: exit %d, stdout %q, stderr %q", code, out, errs)
	}
	code, out, errs = runInProcess(t, "accounting", "--state-dir", t.TempDir())
	if code != exitFail || out != "" || !strings.Contains(errs, "read the kubeconfig") {
		t.Errorf("a state directory with no kubeconfig: exit %d, stdout %q, stderr %q", code, out, errs)
	}
	if code, out, _ := runInProcess(t, "-h"); code != exitOK || !strings.Contains(out, "accounting") {
		t.Errorf("hpcburst -h: exit %d, stdout %q, want the summary to list accounting", code, out)
	}
}
