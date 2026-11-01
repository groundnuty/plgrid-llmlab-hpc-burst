package lifecycle

// The verification record on a real API server (issue #175, fix round of PR #182). The scenario tests of
// verify_test.go run on controller-runtime's fake client (rig_test.go), where no API server takes part;
// this test starts internal/testenv's control plane, writes the records with the Reconciler's own
// writeVerification through a client of that API server, and reads them back through another client, so what
// the test sees is what the API server stored and returned. It uses #61's envtest block, 29204-29207, as
// TestManagerOnControlPlane does, so run it under envtest.lock.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestVerificationRecordRoundTripsThroughTheAPIServer(t *testing.T) {
	t.Setenv("HPCBURST_TESTENV_PORT_BASE", managerPortBase)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cp, err := testenv.Start(ctx, t.TempDir(), testenv.Options{})
	if err != nil {
		t.Fatalf("start the control plane: %v", err)
	}
	defer func() {
		if err := cp.Stop(); err != nil {
			t.Errorf("stop the control plane: %v", err)
		}
		if err := cp.AssertAllDead(); err != nil {
			t.Errorf("leftover process: %v", err)
		}
	}()
	s, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	writer, err := client.New(cp.Config, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := client.New(cp.Config, client.Options{Scheme: s}) // another client: the API server's answer, not the writer's
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: rigNS}}); err != nil {
		t.Fatal(err)
	}
	r := &Reconciler{Client: writer, Reader: writer}
	in := &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "md-abcde", Namespace: rigNS, UID: types.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")},
		Spec:       v1alpha1.InstanceSpec{DeploymentID: v1alpha1.FullDeploymentID(strings.Repeat("d", 64))},
	}

	// Three verifications of one Instance, each its own immutable record: a pass for a checkpoint with no
	// generation_config.json, a failure (a changed file) for one whose file has keys, and a pass for an agent
	// that did not say, whose empty field must be left out of the record.
	cases := []struct {
		gen      int64
		file     string
		keys     map[string]string
		mismatch bool
		verdict  string
	}{
		{1, agentapi.GenerationConfigAbsent, nil, false, "pass"},
		{2, agentapi.GenerationConfigPresent, map[string]string{"temperature": "0.7"}, true, "fail"},
		{3, "", nil, false, "pass"},
	}
	for _, c := range cases {
		res := judgeResult()
		res.GenerationConfigFile, res.GenerationConfig = c.file, c.keys
		if c.mismatch {
			res.Files[0].Outcome = agentapi.FileMismatch
		}
		data := judgeVerify(judgeDeclared(), res, true, judgeShapes())
		if err := r.writeVerification(ctx, "md", in, c.gen, data); err != nil {
			t.Fatalf("generation %d: write the record: %v", c.gen, err)
		}
	}
	for _, c := range cases {
		name, err := v1alpha1.VerificationRecordName(in.UID, c.gen)
		if err != nil {
			t.Fatal(err)
		}
		var rec appsv1.ControllerRevision
		if err := reader.Get(ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &rec); err != nil {
			t.Fatalf("generation %d: read %s back from the API server: %v", c.gen, name, err)
		}
		if rec.UID == "" || rec.ResourceVersion == "" || rec.Labels[v1alpha1.LabelRecord] != v1alpha1.RecordVerification {
			t.Errorf("generation %d: the stored record is %+v, want one the API server created with the verification label", c.gen, rec.ObjectMeta)
		}
		var got map[string]any
		if err := json.Unmarshal(rec.Data.Raw, &got); err != nil {
			t.Fatalf("generation %d: the record's data is not JSON: %v\n%s", c.gen, err, rec.Data.Raw)
		}
		if got["verdict"] != c.verdict {
			t.Errorf("generation %d: verdict %v, want %s", c.gen, got["verdict"], c.verdict)
		}
		file, has := got["generationConfigFile"]
		if c.file == "" {
			if has {
				t.Errorf("generation %d: the agent said nothing and the record has generationConfigFile %v", c.gen, file)
			}
		} else if file != c.file {
			t.Errorf("generation %d: generationConfigFile %v, want %s", c.gen, file, c.file)
		}
		keys, hasKeys := got["generationConfig"].(map[string]any)
		if len(c.keys) == 0 && hasKeys || len(c.keys) != len(keys) {
			t.Errorf("generation %d: generationConfig %v, want %v", c.gen, got["generationConfig"], c.keys)
		}
		for k, v := range c.keys {
			if keys[k] != v {
				t.Errorf("generation %d: generationConfig[%s] = %v, want %s", c.gen, k, keys[k], v)
			}
		}
		// The controller reads the record it wrote the same way.
		d, found, err := r.readVerification(ctx, rigNS, name)
		if err != nil || !found || d.GenerationConfigFile != c.file || string(d.Verdict) != c.verdict {
			t.Errorf("generation %d: readVerification = %+v, %v, %v", c.gen, d, found, err)
		}
	}
	// Records are immutable: writing generation 1 again with another value leaves the stored one.
	res := judgeResult()
	res.GenerationConfigFile = agentapi.GenerationConfigPresent
	if err := r.writeVerification(ctx, "md", in, 1, judgeVerify(judgeDeclared(), res, true, judgeShapes())); err != nil {
		t.Fatal(err)
	}
	if d, _, _ := r.readVerification(ctx, rigNS, "ver-"+string(in.UID)+"-1"); d.GenerationConfigFile != agentapi.GenerationConfigAbsent {
		t.Errorf("a second write changed record 1 to %q", d.GenerationConfigFile)
	}
}
