package crdinstall

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// crdYAML is a CRD manifest of an example kind.
func crdYAML(plural, kind string) string {
	return `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: ` + plural + `.example.invalid
spec:
  group: example.invalid
  names: {kind: ` + kind + `, listKind: ` + kind + `List, plural: ` + plural + `, singular: ` + strings.ToLower(kind) + `}
  scope: Namespaced
  versions:
  - name: v1
    served: true
    storage: true
`
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func names(crds []*apiextensionsv1.CustomResourceDefinition) []string {
	var out []string
	for _, c := range crds {
		out = append(out, c.Name)
	}
	return out
}

// A directory gives every .yaml file in it, in name order; files give the files named, in the order named, so a rig that wants some of a
// directory's CRDs (internal/managed/e2e wants two) names them.
func TestReadCRDsReadsADirectoryInNameOrderAndFilesInTheOrderNamed(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "b.yaml"), crdYAML("gadgets", "Gadget"))
	writeFile(t, filepath.Join(dir, "a.yaml"), crdYAML("widgets", "Widget"))
	writeFile(t, filepath.Join(dir, "notes.txt"), "not a manifest")

	got, err := ReadCRDs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := "widgets.example.invalid gadgets.example.invalid"; strings.Join(names(got), " ") != want {
		t.Errorf("a directory read as %v, want %q (a.yaml, then b.yaml, and no .txt)", names(got), want)
	}
	got, err = ReadCRDs(filepath.Join(dir, "b.yaml"), filepath.Join(dir, "a.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "gadgets.example.invalid widgets.example.invalid"; strings.Join(names(got), " ") != want {
		t.Errorf("two files read as %v, want %q", names(got), want)
	}
	if got[0].Spec.Names.Kind != "Gadget" || got[0].Spec.Names.ListKind != "GadgetList" {
		t.Errorf("the first CRD decoded as %+v, want kind Gadget and list kind GadgetList", got[0].Spec.Names)
	}
}

// A path with no CRD, a manifest that is not a CRD, and a path that does not exist are errors that name the path: a rig that reads
// nothing must not go on as if it had installed the framework's CRDs.
func TestReadCRDsRefusesWhatIsNotACRD(t *testing.T) {
	empty := t.TempDir()
	if _, err := ReadCRDs(empty); err == nil || !strings.Contains(err.Error(), empty) {
		t.Errorf("a directory with no manifest: %v, want an error naming it", err)
	}
	notCRD := filepath.Join(t.TempDir(), "cm.yaml")
	writeFile(t, notCRD, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n")
	if _, err := ReadCRDs(notCRD); err == nil || !strings.Contains(err.Error(), notCRD) || !strings.Contains(err.Error(), "ConfigMap") {
		t.Errorf("a ConfigMap manifest: %v, want an error naming the file and its kind", err)
	}
	if _, err := ReadCRDs(filepath.Join(empty, "missing.yaml")); err == nil {
		t.Error("a path that does not exist was read")
	}
	if _, err := ReadCRDs(); err == nil {
		t.Error("no path at all was read")
	}
}

// InstallCRDs creates a CRD that is not there and updates one that is, with the stored resource version, so a rig can run over a control
// plane that already holds some of the CRDs (the AlreadyExists the other installers tolerated).
func TestInstallCRDsCreatesWhatIsMissingAndUpdatesWhatExists(t *testing.T) {
	existing := widgetCRD()
	existing.Spec.Scope = apiextensionsv1.ClusterScoped
	cl := fake.NewClientBuilder().WithScheme(crdTestScheme(t)).WithObjects(existing).Build()

	widget := widgetCRD()
	widget.Spec.Scope = apiextensionsv1.NamespaceScoped
	gadget := widgetCRD()
	gadget.Name = "gadgets.example.invalid"
	gadget.Spec.Names = apiextensionsv1.CustomResourceDefinitionNames{Kind: "Gadget", ListKind: "GadgetList", Plural: "gadgets", Singular: "gadget"}
	gadget.Spec.Scope = apiextensionsv1.NamespaceScoped
	if err := InstallCRDs(context.Background(), cl, []*apiextensionsv1.CustomResourceDefinition{widget, gadget}); err != nil {
		t.Fatalf("InstallCRDs over one CRD that exists and one that does not: %v", err)
	}
	for name, scope := range map[string]apiextensionsv1.ResourceScope{
		"widgets.example.invalid": apiextensionsv1.NamespaceScoped, "gadgets.example.invalid": apiextensionsv1.NamespaceScoped,
	} {
		got := &apiextensionsv1.CustomResourceDefinition{}
		if err := cl.Get(context.Background(), client.ObjectKey{Name: name}, got); err != nil {
			t.Errorf("%s was not installed: %v", name, err)
		} else if got.Spec.Scope != scope {
			t.Errorf("%s has scope %q, want %q: the one that existed was not updated", name, got.Spec.Scope, scope)
		}
	}
}

// InstallAndWait installs and then waits. The control plane here establishes a CRD only when it is read for the third time, after it was
// created: InstallAndWait returns nil only once it has read it that often, so a version that installs and returns (the mutant "no wait")
// is caught by the count of reads, and a CRD that is never Established is WaitCRDsServed's test, with a short bound.
func TestInstallAndWaitWaitsUntilTheInstalledCRDsAreServed(t *testing.T) {
	var gets atomic.Int32
	cl := fake.NewClientBuilder().WithScheme(crdTestScheme(t)).WithStatusSubresource(&apiextensionsv1.CustomResourceDefinition{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				// The first read is InstallCRDs' look for an existing CRD, which finds none. The CRD is created, then read by the wait.
				crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
				if ok && gets.Add(1) == 3 {
					establish(crd)
					return c.Status().Update(ctx, crd)
				}
				return nil
			},
		}).Build()
	if err := InstallAndWait(context.Background(), cl, []*apiextensionsv1.CustomResourceDefinition{widgetCRD()}); err != nil {
		t.Fatalf("InstallAndWait: %v", err)
	}
	if n := gets.Load(); n < 3 {
		t.Errorf("InstallAndWait returned after %d reads of the CRD, before the control plane established it (the third)", n)
	}
}
