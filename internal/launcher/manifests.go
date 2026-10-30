package launcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

// fieldManager names the launcher in server-side apply, so the objects it installs have a
// recorded owner.
const fieldManager = "hpcburst-launcher"

func isCRD(o *unstructured.Unstructured) bool {
	return o.GetKind() == "CustomResourceDefinition" && strings.HasPrefix(o.GetAPIVersion(), "apiextensions.k8s.io/")
}

// readManifests decodes every .yaml, .yml and .json file of dir (not recursive), in name
// order, into objects. A file that does not decode, or an object without apiVersion, kind and
// name, is an error naming the file: nothing is applied from a directory that is not clean.
func readManifests(dir string) ([]*unstructured.Unstructured, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("manifests directory: %w", err)
	}
	var names []string
	for _, e := range ents {
		switch strings.ToLower(filepath.Ext(e.Name())) {
		case ".yaml", ".yml", ".json":
			if !e.IsDir() {
				names = append(names, e.Name())
			}
		}
	}
	sort.Strings(names)
	var out []*unstructured.Unstructured
	for _, n := range names {
		f, err := os.Open(filepath.Join(dir, n))
		if err != nil {
			return nil, err
		}
		dec := yaml.NewYAMLOrJSONDecoder(f, 4096)
		for i := 1; ; i++ {
			obj := &unstructured.Unstructured{}
			err := dec.Decode(&obj.Object)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("%s, document %d: %w", n, i, err)
			}
			if len(obj.Object) == 0 {
				continue // an empty document between separators
			}
			if obj.GetAPIVersion() == "" || obj.GetKind() == "" || obj.GetName() == "" {
				f.Close()
				return nil, fmt.Errorf("%s, document %d: apiVersion, kind and metadata.name are all required", n, i)
			}
			out = append(out, obj)
		}
		f.Close()
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no manifests (*.yaml, *.yml, *.json) found in %s", dir)
	}
	return out, nil
}

// ApplyManifests applies the manifests of dir to the host with server-side apply, so repeating
// it is safe. CustomResourceDefinitions go first, and the rest waits until each CRD is
// Established. It returns how many objects it applied. This is the hook for the framework's CRDs.
func ApplyManifests(ctx context.Context, cfg *rest.Config, dir string, log io.Writer) (int, error) {
	objs, err := readManifests(dir)
	if err != nil {
		return 0, err
	}
	var crds, others []*unstructured.Unstructured
	for _, o := range objs {
		if isCRD(o) {
			crds = append(crds, o)
		} else {
			others = append(others, o)
		}
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return 0, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return 0, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))
	n := 0
	for _, o := range crds {
		if err := applyOne(ctx, dyn, mapper, o); err != nil {
			return n, err
		}
		n++
		logf(log, "applied CustomResourceDefinition %s", o.GetName())
	}
	for _, o := range crds {
		if err := waitEstablished(ctx, dyn, o.GetName()); err != nil {
			return n, err
		}
	}
	mapper.Reset()
	for _, o := range others {
		if err := applyOne(ctx, dyn, mapper, o); err != nil {
			return n, err
		}
		n++
		logf(log, "applied %s %s", o.GetKind(), o.GetName())
	}
	return n, nil
}

func applyOne(ctx context.Context, dyn dynamic.Interface, mapper meta.ResettableRESTMapper, o *unstructured.Unstructured) error {
	gvk := o.GroupVersionKind()
	var mapping *meta.RESTMapping
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		mapping, err = mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err == nil {
			break
		}
		// A kind from a CRD just applied may need a moment to appear in discovery.
		if !meta.IsNoMatchError(err) || time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("%s %s: %w", gvk.Kind, o.GetName(), err)
		}
		mapper.Reset()
		time.Sleep(300 * time.Millisecond)
	}
	ri := dyn.Resource(mapping.Resource)
	var ni dynamic.ResourceInterface = ri
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := o.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		ni = ri.Namespace(ns)
	}
	body, err := o.MarshalJSON()
	if err != nil {
		return err
	}
	force := true
	if _, err := ni.Patch(ctx, o.GetName(), types.ApplyPatchType, body, metav1.PatchOptions{FieldManager: fieldManager, Force: &force}); err != nil {
		return fmt.Errorf("apply %s %s: %w", gvk.Kind, o.GetName(), err)
	}
	return nil
}

func waitEstablished(ctx context.Context, dyn dynamic.Interface, name string) error {
	gvr := apiextensionsCRD
	deadline := time.Now().Add(60 * time.Second)
	for {
		o, err := dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			conds, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
			for _, c := range conds {
				m, _ := c.(map[string]any)
				if m["type"] == "Established" && m["status"] == "True" {
					return nil
				}
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("CustomResourceDefinition %s did not become Established in 60 s (last error: %v)", name, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

var apiextensionsCRD = schemaGVR("apiextensions.k8s.io", "v1", "customresourcedefinitions")
