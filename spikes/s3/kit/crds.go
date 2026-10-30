package kit

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"time"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:embed crds.yaml
var crdYAML []byte

// InstallCRDs creates the spike CRDs (an existing one is left alone) and waits until
// every one is Established and the API serves its resource.
func InstallCRDs(ctx context.Context, c client.Client) error {
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(crdYAML), 4096)
	var names []string
	for {
		crd := &apiextv1.CustomResourceDefinition{}
		if err := dec.Decode(crd); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("decode embedded CRDs: %w", err)
		}
		if crd.Name == "" {
			continue
		}
		if err := c.Create(ctx, crd); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create CRD %s: %w", crd.Name, err)
		}
		names = append(names, crd.Name)
	}
	if len(names) != 3 {
		return fmt.Errorf("expected 3 embedded CRDs, got %d", len(names))
	}
	return wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		for _, n := range names {
			crd := &apiextv1.CustomResourceDefinition{}
			if err := c.Get(ctx, client.ObjectKey{Name: n}, crd); err != nil {
				return false, nil
			}
			ok := false
			for _, cond := range crd.Status.Conditions {
				if cond.Type == apiextv1.Established && cond.Status == apiextv1.ConditionTrue {
					ok = true
				}
			}
			if !ok {
				return false, nil
			}
		}
		// Established can precede the REST storage being served; prove it with a list.
		for _, gvk := range []schema.GroupVersionKind{FleetGVK, WorkerGVK, SlotGVK} {
			if err := c.List(ctx, NewList(gvk), client.InNamespace("default"), client.Limit(1)); err != nil {
				return false, nil
			}
		}
		return true, nil
	})
}
