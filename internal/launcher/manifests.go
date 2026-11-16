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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
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
		objs, err := decodeManifests(f, n)
		f.Close()
		if err != nil {
			return nil, err
		}
		out = append(out, objs...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no manifests (*.yaml, *.yml, *.json) found in %s", dir)
	}
	return out, nil
}

// decodeManifests decodes the YAML or JSON documents of r into objects, in order. name is what
// the errors call the source. Every object needs apiVersion, kind and metadata.name; an empty
// document between separators is skipped. The guard (guard.go) goes through it as the files of
// --manifests do, so both are checked the same way.
func decodeManifests(r io.Reader, name string) ([]*unstructured.Unstructured, error) {
	var out []*unstructured.Unstructured
	dec := yaml.NewYAMLOrJSONDecoder(r, 4096)
	for i := 1; ; i++ {
		obj := &unstructured.Unstructured{}
		err := dec.Decode(&obj.Object)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s, document %d: %w", name, i, err)
		}
		if len(obj.Object) == 0 {
			continue // an empty document between separators
		}
		if obj.GetAPIVersion() == "" || obj.GetKind() == "" || obj.GetName() == "" {
			return nil, fmt.Errorf("%s, document %d: apiVersion, kind and metadata.name are all required", name, i)
		}
		out = append(out, obj)
	}
}

// servedLimit bounds the wait for a CRD's kind to be served, and servedEvery is how often it looks.
const (
	servedLimit = 60 * time.Second
	servedEvery = 200 * time.Millisecond
)

// ApplyManifests applies the manifests of dir to the host with server-side apply, so repeating
// it is safe. CustomResourceDefinitions go first, and the rest waits until each CRD is
// Established AND its kind is served (waitServed). It returns how many objects it applied. This
// is the hook for the framework's CRDs.
func ApplyManifests(ctx context.Context, cfg *rest.Config, dir string, log io.Writer) (int, error) {
	objs, err := readManifests(dir)
	if err != nil {
		return 0, err
	}
	dyn, mapper, err := newApplier(cfg)
	if err != nil {
		return 0, err
	}
	return applyManifests(ctx, dyn, mapper, objs, log, servedLimit, servedEvery)
}

// applyManifests is ApplyManifests over the clients it is given: the CRDs go first, and no other
// object is applied, and nothing after the manifests step runs, until every CRD is Established and
// a list of its kind answers through the same discovery a later apply uses (issue #603). The
// CRD's Established condition is set before the API server serves its kind: the discovery that
// maps the kind, and the endpoint that lists it, can lag, and applyOne's own retry covers only
// the first of them for the object it applies. The later steps of up (the Allocation list of the
// relay keeper's guard, among them) have no such retry.
func applyManifests(ctx context.Context, dyn dynamic.Interface, mapper meta.ResettableRESTMapper, objs []*unstructured.Unstructured, log io.Writer, limit, every time.Duration) (int, error) {
	var crds, others []*unstructured.Unstructured
	for _, o := range objs {
		if isCRD(o) {
			crds = append(crds, o)
		} else {
			others = append(others, o)
		}
	}
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
	if err := waitServed(ctx, dyn, mapper, crds, limit, every); err != nil {
		return n, err
	}
	if len(crds) > 0 {
		logf(log, "the %d CustomResourceDefinitions are Established and their kinds are served", len(crds))
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

// servedKind is what a CRD says it serves: its group, the first version it serves, and its kind.
type servedKind struct {
	crd  string
	kind schema.GroupVersionKind
}

// servedKinds reads, off each CRD manifest, the kind it will serve and the version to list it by.
// A CRD that serves no version, or names no group or kind, is an error that names it.
func servedKinds(crds []*unstructured.Unstructured) ([]servedKind, error) {
	out := make([]servedKind, 0, len(crds))
	for _, o := range crds {
		group, _, _ := unstructured.NestedString(o.Object, "spec", "group")
		kind, _, _ := unstructured.NestedString(o.Object, "spec", "names", "kind")
		versions, _, _ := unstructured.NestedSlice(o.Object, "spec", "versions")
		version := ""
		for _, v := range versions {
			m, _ := v.(map[string]any)
			if served, _ := m["served"].(bool); served {
				version, _ = m["name"].(string)
				break
			}
		}
		if group == "" || kind == "" || version == "" {
			return nil, fmt.Errorf("CustomResourceDefinition %s names no group (%q), kind (%q) or served version (%q), so there is no kind to wait for", o.GetName(), group, kind, version)
		}
		out = append(out, servedKind{crd: o.GetName(), kind: schema.GroupVersionKind{Group: group, Version: version, Kind: kind}})
	}
	return out, nil
}

// waitServed returns once, for every CRD in crds, the mapper finds its kind in a fresh discovery
// and a list of the mapped resource answers, or fails after limit with an error that names the
// first CRD or kind that is not served yet. Established is not served: the Established condition
// is set before the API server serves the kind, so a later apply or list can meet "no matches
// for kind" or a 404 while discovery and the endpoint catch up (#589, #593's CI run; #600). Each
// round resets the mapper first, so the discovery it asks is never one built before the CRD
// existed, and it is the discovery applyOne maps through.
func waitServed(ctx context.Context, dyn dynamic.Interface, mapper meta.ResettableRESTMapper, crds []*unstructured.Unstructured, limit, every time.Duration) error {
	kinds, err := servedKinds(crds)
	if err != nil {
		return err
	}
	last := "no CRD checked yet"
	check := func(ctx context.Context) (bool, error) {
		mapper.Reset()
		for _, k := range kinds {
			mapping, err := mapper.RESTMapping(k.kind.GroupKind(), k.kind.Version)
			if err != nil {
				last = fmt.Sprintf("kind %s of CRD %s is not in discovery yet: %v", k.kind.Kind, k.crd, err)
				return false, nil
			}
			if _, err := dyn.Resource(mapping.Resource).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
				last = fmt.Sprintf("kind %s of CRD %s is not served yet: %v", k.kind.Kind, k.crd, err)
				return false, nil
			}
		}
		return true, nil
	}
	if err := wait.PollUntilContextTimeout(ctx, every, limit, true, check); err != nil {
		return fmt.Errorf("the CRDs were not served within %s: %s", limit, last)
	}
	return nil
}

// newApplier returns the dynamic client and the discovery-backed mapper that applyOne needs.
func newApplier(cfg *rest.Config) (dynamic.Interface, meta.ResettableRESTMapper, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	return dyn, restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc)), nil
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
