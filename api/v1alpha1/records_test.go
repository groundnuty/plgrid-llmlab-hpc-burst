package v1alpha1

import (
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const goldenID = "0e1dd69efe8a57c3eadd5e0293fd8c0f0cc185629e79297b6530de361b887852" // HBEP-3, worked example

// The expected values were computed outside this code: the 12 hex characters
// with  printf '%s' <value> | sha256sum | cut -c1-12  (HBEP-11 asks for golden
// values from outside our code).
func TestLabelValue(t *testing.T) {
	long100 := strings.Repeat("abcdefghij", 10)
	for _, tc := range []struct{ name, in, want string }{
		{"short", "my-deployment", "my-deployment"},
		{"empty", "", ""},
		{"exactly 63 characters is kept", strings.Repeat("y", 63), strings.Repeat("y", 63)},
		{"64 characters is shortened", strings.Repeat("x", 64), strings.Repeat("x", 50) + "-7ce100971f64"},
		{"100 characters", long100, "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghij-fcbb61d058e8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := LabelValue(tc.in)
			if got != tc.want {
				t.Fatalf("LabelValue(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if errs := validation.IsValidLabelValue(got); len(errs) != 0 && tc.in != "" {
				t.Fatalf("%q is not a valid label value: %v", got, errs)
			}
		})
	}
}

func TestIdentityRecordName(t *testing.T) {
	for _, id := range []string{goldenID, goldenID[:12]} {
		got, err := IdentityRecordName(id)
		if err != nil || got != "id-0e1dd69efe8a" {
			t.Errorf("IdentityRecordName(%q) = %q, %v; want id-0e1dd69efe8a", id, got, err)
		}
	}
	for _, bad := range []string{"", "0e1dd69efe8", goldenID[:13], goldenID + "0", strings.ToUpper(goldenID), "0e1dd69efe8z", "id-0e1dd69efe8a"} {
		if got, err := IdentityRecordName(bad); err == nil {
			t.Errorf("IdentityRecordName(%q) = %q, want an error", bad, got)
		}
	}
}

func TestDeploymentIDDisplay(t *testing.T) {
	if got := DeploymentID(goldenID).Display(); got != "0e1dd69efe8a" {
		t.Errorf("Display = %q", got)
	}
	if got := DeploymentID(goldenID[:12]).Display(); got != "0e1dd69efe8a" {
		t.Errorf("Display of a display form = %q", got)
	}
	if got := FullDeploymentID(goldenID).Display(); got != "0e1dd69efe8a" {
		t.Errorf("Display = %q", got)
	}
}

func TestNewIdentityRecord(t *testing.T) {
	data := []byte(`{"scheme":"v1"}`)
	rec, err := NewIdentityRecord("ns", "md", goldenID, data)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Name != "id-0e1dd69efe8a" || rec.Namespace != "ns" || rec.Revision != 1 || string(rec.Data.Raw) != string(data) {
		t.Errorf("record = %+v", rec)
	}
	if len(rec.OwnerReferences) != 0 {
		t.Errorf("an identity record must have no owner reference, got %v", rec.OwnerReferences)
	}
	want := map[string]string{
		LabelRecord: RecordIdentity, LabelDeploymentID: "0e1dd69efe8a", LabelModelDeployment: "md",
	}
	if len(rec.Labels) != len(want) {
		t.Errorf("labels = %v, want %v", rec.Labels, want)
	}
	for k, v := range want {
		if rec.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, rec.Labels[k], v)
		}
	}
	// The record keeps its own copy of the data.
	data[0] = 'X'
	if rec.Data.Raw[0] != '{' {
		t.Error("the record shares the caller's data slice")
	}

	// A short name is kept whole, in the label and in the annotation.
	rec, err = NewIdentityRecord("ns", "md", goldenID, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Annotations[AnnotationModelDeployment]; got != "md" {
		t.Errorf("annotation for a short name = %q, want md", got)
	}
}

// A ModelDeployment name can be up to 253 characters but a label value holds 63,
// so the label is shortened. The record outlives its ModelDeployment, and the
// annotation is then the only place the full name survives (review finding B7).
func TestRecordsKeepALongModelDeploymentName(t *testing.T) {
	uid := types.UID("11111111-2222-3333-4444-555555555555")
	// Two names that share their first 63 characters: only the hash suffix of
	// the label tells them apart, and only the annotation holds either in full.
	common := strings.Repeat("abcdefghij", 7) // 70 characters
	names := map[string]string{
		"a 100-character name": strings.Repeat("abcdefghij", 10),
		"a 253-character name": strings.Repeat("abcdefghij", 25) + "xyz",
		"a name that shares its first 63 characters with the next": common + "-first",
		"the name that shares its first 63 characters":             common + "-second",
	}
	labels := map[string]string{}
	for what, full := range names {
		for kind, build := range map[string]func() (*appsv1.ControllerRevision, error){
			"identity": func() (*appsv1.ControllerRevision, error) {
				return NewIdentityRecord("ns", full, goldenID, []byte(`{}`))
			},
			"verification": func() (*appsv1.ControllerRevision, error) {
				return NewVerificationRecord("ns", full, goldenID, uid, 1, []byte(`{}`))
			},
		} {
			rec, err := build()
			if err != nil {
				t.Fatalf("%s, %s record: %v", what, kind, err)
			}
			label := rec.Labels[LabelModelDeployment]
			if len(label) != 63 || label != LabelValue(full) {
				t.Errorf("%s, %s record: label %q is not the shortened name", what, kind, label)
			}
			if errs := validation.IsValidLabelValue(label); len(errs) != 0 {
				t.Errorf("%s, %s record: label %q is invalid: %v", what, kind, label, errs)
			}
			// What a controller reads from a record that outlived its ModelDeployment:
			if got := rec.Annotations[AnnotationModelDeployment]; got != full {
				t.Errorf("%s, %s record: annotation = %q, want the full name %q", what, kind, got, full)
			}
			if len(rec.OwnerReferences) != 0 {
				t.Errorf("%s, %s record: owner references %v", what, kind, rec.OwnerReferences)
			}
			if kind == "identity" {
				labels[what] = label
			}
		}
	}
	first, second := labels["a name that shares its first 63 characters with the next"], labels["the name that shares its first 63 characters"]
	if first == second {
		t.Errorf("two different long names got one label %q", first)
	}
	if first[:50] != second[:50] {
		t.Errorf("the test names do not share their first 50 characters: %q %q", first, second)
	}
}

func TestNewIdentityRecordRefusesBadInput(t *testing.T) {
	good := []byte(`{}`)
	for name, call := range map[string]func() error{
		"a display-form ID": func() error { _, err := NewIdentityRecord("ns", "md", goldenID[:12], good); return err },
		"a malformed ID":    func() error { _, err := NewIdentityRecord("ns", "md", "nothex", good); return err },
		"data that is not JSON": func() error {
			_, err := NewIdentityRecord("ns", "md", goldenID, []byte(`{`))
			return err
		},
		"no ModelDeployment": func() error { _, err := NewIdentityRecord("ns", "", goldenID, good); return err },
	} {
		if call() == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// One record per verification, ver-<instance UID>-<generation> (HBEP-7,
// Instance start): a worker restart and a whole-Instance restart each give a
// new name, and the generation is the record's Revision.
func TestNewVerificationRecord(t *testing.T) {
	uid := types.UID("11111111-2222-3333-4444-555555555555")
	names := map[string]bool{}
	for gen, want := range map[int64]string{
		1: "ver-11111111-2222-3333-4444-555555555555-1",
		2: "ver-11111111-2222-3333-4444-555555555555-2",
		3: "ver-11111111-2222-3333-4444-555555555555-3",
	} {
		rec, err := NewVerificationRecord("ns", "md", goldenID, uid, gen, []byte(`{"verdict":"pass"}`))
		if err != nil {
			t.Fatal(err)
		}
		if rec.Name != want || rec.Revision != gen || len(rec.OwnerReferences) != 0 {
			t.Errorf("generation %d: record = %+v, want name %s", gen, rec.ObjectMeta, want)
		}
		if errs := validation.IsDNS1123Subdomain(rec.Name); len(errs) != 0 {
			t.Errorf("%q is not a valid object name: %v", rec.Name, errs)
		}
		names[rec.Name] = true
		wantLabels := map[string]string{
			LabelRecord: RecordVerification, LabelDeploymentID: "0e1dd69efe8a", LabelModelDeployment: "md", LabelInstanceUID: string(uid),
		}
		for k, v := range wantLabels {
			if rec.Labels[k] != v {
				t.Errorf("label %s = %q, want %q", k, rec.Labels[k], v)
			}
		}
	}
	if len(names) != 3 {
		t.Errorf("three generations gave %d names", len(names))
	}
	for what, call := range map[string]func() error{
		"no Instance UID":  func() error { _, err := NewVerificationRecord("ns", "md", goldenID, "", 1, []byte(`{}`)); return err },
		"generation 0":     func() error { _, err := NewVerificationRecord("ns", "md", goldenID, uid, 0, []byte(`{}`)); return err },
		"generation -1":    func() error { _, err := NewVerificationRecord("ns", "md", goldenID, uid, -1, []byte(`{}`)); return err },
		"data not JSON":    func() error { _, err := NewVerificationRecord("ns", "md", goldenID, uid, 1, []byte(`{`)); return err },
		"no deployment ID": func() error { _, err := NewVerificationRecord("ns", "md", "", uid, 1, []byte(`{}`)); return err },
	} {
		if call() == nil {
			t.Errorf("a verification record with %s was accepted", what)
		}
	}
	// A verification record may carry the display form of the ID.
	if _, err := NewVerificationRecord("ns", "md", goldenID[:12], uid, 1, []byte(`{}`)); err != nil {
		t.Errorf("display-form ID: %v", err)
	}
}

func TestParseVerificationRecordName(t *testing.T) {
	uid, gen, err := ParseVerificationRecordName("ver-11111111-2222-3333-4444-555555555555-12")
	if err != nil || uid != "11111111-2222-3333-4444-555555555555" || gen != 12 {
		t.Errorf("parse = %q, %d, %v", uid, gen, err)
	}
	for _, bad := range []string{
		"", "ver-", "ver-uid", "ver--1", "ver-uid-0", "ver-uid-01", "ver-uid--1", "ver-uid-x",
		"id-0e1dd69efe8a", "ver-11111111-2222-3333-4444-555555555555", // the old single-record form
	} {
		if uid, gen, err := ParseVerificationRecordName(bad); err == nil {
			t.Errorf("ParseVerificationRecordName(%q) = %q, %d; want an error", bad, uid, gen)
		}
	}
}

// The expected tags were computed outside this code:
//
//	printf '%s' '<namespace>/<name>/<uid>' | sha256sum | cut -c1-12
//
// (HBEP-3, Allocation and job identity: hb-<first 40 characters of the
// name>-<12 hex>).
func TestJobTag(t *testing.T) {
	const uid = types.UID("11111111-2222-3333-4444-555555555555")
	long := "abcdefghijabcdefghijabcdefghijabcdefghij" // 40 characters
	for _, tc := range []struct{ name, want string }{
		{"helios-a-x7k2p", "hb-helios-a-x7k2p-1bda24d00504"},
		{long + "-x7k2p", "hb-" + long + "-805860179c9c"},
		{long + "-q9z3m", "hb-" + long + "-90b5f99d164b"},
	} {
		got := JobTag("team-a", tc.name, uid)
		if got != tc.want {
			t.Errorf("JobTag(%q) = %q, want %q", tc.name, got, tc.want)
		}
		if len(got) > JobTagLength {
			t.Errorf("JobTag(%q) has %d characters, more than %d", tc.name, len(got), JobTagLength)
		}
	}
}

// The last 20 waits, keyed by job ID, oldest dropped first (HBEP-4, Provider
// status).
func TestAddQueueWait(t *testing.T) {
	at := func(m int) metav1.Time { return metav1.NewTime(time.Date(2026, 10, 30, 12, m, 0, 0, time.UTC)) }
	wait := func(id string, waitMin int) QueueWait {
		return QueueWait{JobID: id, SubmitTime: at(0), StartTime: at(waitMin)}
	}
	var waits []QueueWait
	for i := 1; i <= MaxQueueWaits; i++ {
		waits = AddQueueWait(waits, wait(strconv.Itoa(i), i))
	}
	if len(waits) != 20 || waits[0].JobID != "1" || waits[19].JobID != "20" {
		t.Fatalf("after 20 adds: %d waits, first %s, last %s", len(waits), waits[0].JobID, waits[len(waits)-1].JobID)
	}
	if got := waits[4].Wait(); got != 5*time.Minute {
		t.Errorf("Wait of job 5 = %v, want 5m", got)
	}
	// A repeated observe adds nothing.
	same := AddQueueWait(waits, wait("7", 99))
	if len(same) != 20 || same[6].Wait() != 7*time.Minute {
		t.Errorf("a repeated job ID changed the waits: %v", same[6])
	}
	// A 21st wait drops the oldest, and the caller's slice is unchanged.
	more := AddQueueWait(waits, wait("21", 21))
	if len(more) != 20 || more[0].JobID != "2" || more[19].JobID != "21" {
		t.Errorf("after the 21st: first %s, last %s, len %d", more[0].JobID, more[19].JobID, len(more))
	}
	if waits[0].JobID != "1" {
		t.Error("AddQueueWait changed the slice it was given")
	}
}

func TestRestartGenerationAnnotationAndLeases(t *testing.T) {
	for part, want := range map[string]string{
		PartInstance:  "hpcburst.groundnuty.github.io/restart-instance",
		WorkerName(0): "hpcburst.groundnuty.github.io/restart-worker-0",
		WorkerName(3): "hpcburst.groundnuty.github.io/restart-worker-3",
	} {
		got := RestartGenerationAnnotation(part)
		if got != want {
			t.Errorf("RestartGenerationAnnotation(%q) = %q, want %q", part, got, want)
		}
		if errs := validation.IsQualifiedName(got); len(errs) != 0 {
			t.Errorf("%q is not a valid annotation key: %v", got, errs)
		}
	}
	if ProviderLeaseName("lifecycle") == LeaseLifecycle {
		t.Error("a Provider named lifecycle shares the lifecycle controller's Lease")
	}
	if errs := validation.IsDNS1123Subdomain(ProviderLeaseName(strings.Repeat("a", 63))); len(errs) != 0 {
		t.Errorf("a Provider's Lease name is invalid: %v", errs)
	}
}

// The Lease names are part of the contract: the lifecycle controller's leader
// Lease is "lifecycle" (HBEP-7) and the front's heartbeat Lease is "front"
// (HBEP-8, Observers). The values are the HBEPs', written out here, not read
// back from the constants.
func TestControllerLeaseNames(t *testing.T) {
	if LeaseLifecycle != "lifecycle" {
		t.Errorf("LeaseLifecycle = %q, want %q", LeaseLifecycle, "lifecycle")
	}
	if LeaseFront != "front" {
		t.Errorf("LeaseFront = %q, want %q", LeaseFront, "front")
	}
	if ProviderLeaseName("front") == LeaseFront {
		t.Error("a Provider named front shares the front's Lease")
	}
}

func TestDuration(t *testing.T) {
	d, err := Duration("1m30s").AsDuration()
	if err != nil || d.Seconds() != 90 {
		t.Errorf("1m30s = %v, %v", d, err)
	}
	if _, err := Duration("soon").AsDuration(); err == nil {
		t.Error("soon parsed")
	}
}
