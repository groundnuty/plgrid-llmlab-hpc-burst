package v1alpha1

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
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
				return NewVerificationRecord("ns", full, goldenID, uid, []byte(`{}`))
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

func TestNewVerificationRecord(t *testing.T) {
	uid := types.UID("11111111-2222-3333-4444-555555555555")
	rec, err := NewVerificationRecord("ns", "md", goldenID, uid, []byte(`{"verdict":"pass"}`))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Name != "ver-"+string(uid) || len(rec.OwnerReferences) != 0 {
		t.Errorf("record = %+v", rec)
	}
	want := map[string]string{
		LabelRecord: RecordVerification, LabelDeploymentID: "0e1dd69efe8a", LabelModelDeployment: "md", LabelInstanceUID: string(uid),
	}
	for k, v := range want {
		if rec.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, rec.Labels[k], v)
		}
	}
	if _, err := NewVerificationRecord("ns", "md", goldenID, "", []byte(`{}`)); err == nil {
		t.Error("a verification record without an Instance UID was accepted")
	}
	// A verification record may carry the display form of the ID.
	if _, err := NewVerificationRecord("ns", "md", goldenID[:12], uid, []byte(`{}`)); err != nil {
		t.Errorf("display-form ID: %v", err)
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
