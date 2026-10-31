package hubfiles

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

func integrationSpec() (deploymentid.ServingSpec, deploymentid.Accelerator) {
	return deploymentid.ServingSpec{
			Checkpoint:         deploymentid.Checkpoint{Repository: testRepo, Revision: testRev},
			Engine:             deploymentid.Engine{Name: "vllm", Version: "0.32.0", ImageDigest: "sha256:" + strings.Repeat("6", 64)},
			Flags:              []deploymentid.Flag{{Name: "dtype", Value: "bfloat16"}},
			ChatTemplateSHA256: strings.Repeat("1", 64),
		},
		deploymentid.Accelerator{GPUModel: "GH200", CPUArch: "aarch64"}
}

// deploymentIDOf fetches the file list from a fresh fake and computes the ID.
func deploymentIDOf(t *testing.T, files []fakeFile, pageSize int, reorder func([]map[string]any)) deploymentid.ID {
	t.Helper()
	h := newFakeHub(t, files)
	h.pageSize = pageSize
	h.reorder = reorder
	entries, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	spec, accel := integrationSpec()
	id, err := deploymentid.Compute(spec, accel, Inputs(entries))
	if err != nil {
		t.Fatalf("deploymentid.Compute rejected the package's output: %v", err)
	}
	return id
}

func TestDeploymentIDIsStableAcrossFetches(t *testing.T) {
	// Two separate fetches that differ in everything the Hub is free to vary:
	// the order of the entries and the way the listing is cut into pages.
	first := deploymentIDOf(t, allFiles(), 5, reverseByPath)
	second := deploymentIDOf(t, allFiles(), 4, rotate(7))
	if first.SHA256 != second.SHA256 || !bytes.Equal(first.Canonical, second.Canonical) {
		t.Fatalf("two fetches gave different IDs:\n %s\n %s", first.SHA256, second.SHA256)
	}

	// The files array of the canonical object holds the expected per-file
	// hashes: the pasted fixture constants, written by hand in JCS form.
	for _, want := range []string{
		`{"path":".gitattributes","sha256":"ee05350c7fda0af4da116020dd7b976d731de3beed4f5e717d5bcc95ead24550"}`,
		`{"path":"a/b.json","sha256":"241714c62f70695b332fd13a5feeb58b25bf8f3899a0e41d80d2dd6892d8f87a"}`,
		`{"path":"` + lfsPath1 + `","sha256":"` + sha256ABC + `"}`,
	} {
		if !bytes.Contains(first.Canonical, []byte(want)) {
			t.Errorf("the canonical object lacks %s\n%s", want, first.Canonical)
		}
	}
}

func TestDeploymentIDChangesWithTheFiles(t *testing.T) {
	base := deploymentIDOf(t, allFiles(), 0, reverseByPath)

	// Another LFS hash: the listing's lfs.oid is an input.
	lfsChanged := allFiles()
	for i := range lfsChanged {
		if lfsChanged[i].path == lfsPath1 {
			lfsChanged[i].lfsOID = strings.Repeat("7", 64)
		}
	}
	if got := deploymentIDOf(t, lfsChanged, 0, reverseByPath); got.SHA256 == base.SHA256 {
		t.Error("changing one lfs.oid did not change the ID")
	}

	// Another config.json whose blob id is consistent with its content: the
	// downloaded and hashed content is an input.
	contentChanged := allFiles()
	for i := range contentChanged {
		if contentChanged[i].path == "config.json" {
			contentChanged[i].content = "a\n"
			contentChanged[i].oid = fixtures[6].blob // git blob id of "a\n"
		}
	}
	if got := deploymentIDOf(t, contentChanged, 0, reverseByPath); got.SHA256 == base.SHA256 {
		t.Error("changing one downloaded file did not change the ID")
	}
}
