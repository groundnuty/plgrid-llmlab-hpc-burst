// Package crdinstall installs the committed CRDs into a test API server and waits until each is served.
//
// Creating a CRD returns before the API server serves it. A rig that creates an object of one of the kinds right away can meet "no matches
// for kind" while discovery catches up, and the more CRDs a rig installs, the wider that window (#589; #593's CI run failed on
// PlatformProfile; issue #600). An Established condition alone is not enough either: the kind must answer a list. So every rig that installs
// config/crd installs it through this package, or, when envtest installs it, waits through it afterwards, and one place decides when a CRD
// counts as served.
//
// It is for test rigs. It imports the apiextensions types and controller-runtime's client, which internal/testenv itself does not, so it is
// a package of its own; no shipped binary links it (go list -deps of cmd/... names it nowhere).
package crdinstall

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DefaultLimit bounds InstallAndWait's wait for the CRDs to be served.
	DefaultLimit = 60 * time.Second
	// DefaultEvery is how often the wait looks.
	DefaultEvery = 200 * time.Millisecond
)

// ReadCRDs reads the CRDs of paths. A path that is a directory gives every .yaml file in it, in name order; any other path is a file
// that holds one CRD. It fails for a path that holds no CRD, and for a manifest that is not one.
func ReadCRDs(paths ...string) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	var files []string
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !fi.IsDir() {
			files = append(files, p)
			continue
		}
		matches, err := filepath.Glob(filepath.Join(p, "*.yaml"))
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no CRD manifest (*.yaml) in %s", p)
		}
		sort.Strings(matches)
		files = append(files, matches...)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no CRD manifest named")
	}
	crds := make([]*apiextensionsv1.CustomResourceDefinition, 0, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(crd); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if crd.Kind != "CustomResourceDefinition" || crd.Name == "" {
			return nil, fmt.Errorf("%s is not a CustomResourceDefinition manifest (kind %q, name %q)", f, crd.Kind, crd.Name)
		}
		crds = append(crds, crd)
	}
	return crds, nil
}

// NewClient is a client for cfg whose scheme holds the CRD type, which this package's functions need and a rig's own scheme may lack.
func NewClient(cfg *rest.Config) (client.Client, error) {
	s := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(s); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: s})
}

// InstallCRDs creates each CRD through cl, or updates one that exists. It does not wait: WaitCRDsServed does.
func InstallCRDs(ctx context.Context, cl client.Client, crds []*apiextensionsv1.CustomResourceDefinition) error {
	for _, crd := range crds {
		existing := &apiextensionsv1.CustomResourceDefinition{}
		switch err := cl.Get(ctx, client.ObjectKeyFromObject(crd), existing); {
		case apierrors.IsNotFound(err):
			if err := cl.Create(ctx, crd); err != nil {
				return fmt.Errorf("create %s: %w", crd.Name, err)
			}
		case err != nil:
			return fmt.Errorf("read %s: %w", crd.Name, err)
		default:
			crd.ResourceVersion = existing.ResourceVersion
			if err := cl.Update(ctx, crd); err != nil {
				return fmt.Errorf("update %s: %w", crd.Name, err)
			}
		}
	}
	return nil
}

// InstallAndWait installs crds (InstallCRDs) and waits for them to be served (WaitCRDsServed), within DefaultLimit.
func InstallAndWait(ctx context.Context, cl client.Client, crds []*apiextensionsv1.CustomResourceDefinition) error {
	if err := InstallCRDs(ctx, cl, crds); err != nil {
		return err
	}
	return WaitCRDsServed(ctx, cl, crds, DefaultLimit, DefaultEvery)
}

// WaitCRDsServed returns once every CRD in crds is Established and a list of its
// kind answers, or fails after limit with an error that names the first CRD or
// kind that is not served yet. Creating a CRD returns before the API server
// serves it: a rig that creates an object of one of the kinds right away can meet
// "no matches for kind" while discovery catches up, and the more CRDs a rig
// installs, the wider that window (#589, #593's CI: PlatformProfile). An
// Established condition alone is not enough either, so the wait also lists each
// kind. cl's scheme must hold the CRD type (NewClient).
func WaitCRDsServed(ctx context.Context, cl client.Client, crds []*apiextensionsv1.CustomResourceDefinition, limit, every time.Duration) error {
	last := "no CRD checked yet"
	check := func(ctx context.Context) (bool, error) {
		for _, want := range crds {
			got := &apiextensionsv1.CustomResourceDefinition{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(want), got); err != nil {
				last = fmt.Sprintf("CRD %s cannot be read: %v", want.Name, err)
				return false, nil
			}
			if !crdEstablished(got) {
				last = fmt.Sprintf("CRD %s is not Established", want.Name)
				return false, nil
			}
		}
		// Established is not served: list each kind until the API answers.
		for _, want := range crds {
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(schema.GroupVersionKind{
				Group: want.Spec.Group, Version: servedVersion(want), Kind: want.Spec.Names.ListKind,
			})
			if err := cl.List(ctx, list); err != nil {
				last = fmt.Sprintf("kind %s of CRD %s is not served: %v", want.Spec.Names.Kind, want.Name, err)
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

func crdEstablished(crd *apiextensionsv1.CustomResourceDefinition) bool {
	for _, c := range crd.Status.Conditions {
		if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
			return true
		}
	}
	return false
}

// servedVersion is the version of crd that the API server serves.
func servedVersion(crd *apiextensionsv1.CustomResourceDefinition) string {
	for _, v := range crd.Spec.Versions {
		if v.Served {
			return v.Name
		}
	}
	return crd.Spec.Versions[0].Name
}
