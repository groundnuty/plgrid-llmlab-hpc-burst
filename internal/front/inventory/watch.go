package inventory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The resources the front reads.
var (
	GVRModelDeployments = v1alpha1.GroupVersion.WithResource("modeldeployments")
	GVRInstances        = v1alpha1.GroupVersion.WithResource("instances")
	GVRProviders        = v1alpha1.GroupVersion.WithResource("providers")
	GVRPlatformProfiles = v1alpha1.GroupVersion.WithResource("platformprofiles")
)

// Watch keeps the front's copy of the control plane current, by watch
// (HBEP-5, Risks: membership from the ready set, read by watch; L-4.11). It
// runs one informer per kind and namespace, and calls OnChange after every
// add, update or delete. It reads only: the front writes no object but its
// events.
type Watch struct {
	namespaced []cache.SharedIndexInformer // per namespace: deployments, instances
	cluster    []cache.SharedIndexInformer // providers, profiles
	factories  []dynamicinformer.DynamicSharedInformerFactory
	byKind     map[schema.GroupVersionResource][]cache.SharedIndexInformer

	mu   sync.Mutex
	last map[string]time.Time // kind -> last event or sync

	onChange func()
	now      func() time.Time
}

// NewWatch prepares the informers. namespaces empty means all namespaces.
// onChange runs on the informers' goroutines and must not block.
func NewWatch(client dynamic.Interface, namespaces []string, onChange func()) *Watch {
	w := &Watch{byKind: map[schema.GroupVersionResource][]cache.SharedIndexInformer{}, last: map[string]time.Time{}, onChange: onChange, now: time.Now}
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}
	for _, ns := range namespaces {
		f := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, 0, ns, nil)
		w.factories = append(w.factories, f)
		for _, gvr := range []schema.GroupVersionResource{GVRModelDeployments, GVRInstances} {
			w.add(f, gvr)
		}
	}
	f := dynamicinformer.NewDynamicSharedInformerFactory(client, 0)
	w.factories = append(w.factories, f)
	for _, gvr := range []schema.GroupVersionResource{GVRProviders, GVRPlatformProfiles} {
		w.add(f, gvr)
	}
	return w
}

func (w *Watch) add(f dynamicinformer.DynamicSharedInformerFactory, gvr schema.GroupVersionResource) {
	inf := f.ForResource(gvr).Informer()
	kind := gvr.Resource
	touch := func() {
		w.mu.Lock()
		w.last[kind] = w.now()
		w.mu.Unlock()
		if w.onChange != nil {
			w.onChange()
		}
	}
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { touch() },
		UpdateFunc: func(any, any) { touch() },
		DeleteFunc: func(any) { touch() },
	})
	w.byKind[gvr] = append(w.byKind[gvr], inf)
}

// Start runs the informers until ctx ends and waits for their first list.
func (w *Watch) Start(ctx context.Context) error {
	for _, f := range w.factories {
		f.Start(ctx.Done())
	}
	for gvr, infs := range w.byKind {
		for _, inf := range infs {
			if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
				return fmt.Errorf("the watch of %s did not sync", gvr.Resource)
			}
		}
		w.mu.Lock()
		if _, ok := w.last[gvr.Resource]; !ok {
			w.last[gvr.Resource] = w.now()
		}
		w.mu.Unlock()
	}
	return nil
}

// Synced reports whether every informer has listed once.
func (w *Watch) Synced() bool {
	for _, infs := range w.byKind {
		for _, inf := range infs {
			if !inf.HasSynced() {
				return false
			}
		}
	}
	return true
}

// LastEvents returns, per kind, the time of its last event or first sync, for
// the health endpoint's watch ages (HBEP-5, Operational readiness 1).
func (w *Watch) LastEvents() map[string]time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]time.Time, len(w.last))
	for k, v := range w.last {
		out[k] = v
	}
	return out
}

// Snapshot copies the informers' stores into typed objects. An object that
// does not convert is skipped and named in the returned problems.
func (w *Watch) Snapshot() (Snapshot, []string) {
	var s Snapshot
	var problems []string
	convert := func(gvr schema.GroupVersionResource, each func(u *unstructured.Unstructured) error) {
		for _, inf := range w.byKind[gvr] {
			for _, o := range inf.GetStore().List() {
				u, ok := o.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				if err := each(u); err != nil {
					problems = append(problems, fmt.Sprintf("%s %s/%s: %v", gvr.Resource, u.GetNamespace(), u.GetName(), err))
				}
			}
		}
	}
	convert(GVRModelDeployments, func(u *unstructured.Unstructured) error {
		var o v1alpha1.ModelDeployment
		err := fromUnstructured(u, &o)
		if err == nil {
			s.Deployments = append(s.Deployments, o)
		}
		return err
	})
	convert(GVRInstances, func(u *unstructured.Unstructured) error {
		var o v1alpha1.Instance
		err := fromUnstructured(u, &o)
		if err == nil {
			s.Instances = append(s.Instances, o)
		}
		return err
	})
	convert(GVRProviders, func(u *unstructured.Unstructured) error {
		var o v1alpha1.Provider
		err := fromUnstructured(u, &o)
		if err == nil {
			s.Providers = append(s.Providers, o)
		}
		return err
	})
	convert(GVRPlatformProfiles, func(u *unstructured.Unstructured) error {
		var o v1alpha1.PlatformProfile
		err := fromUnstructured(u, &o)
		if err == nil {
			s.Profiles = append(s.Profiles, o)
		}
		return err
	})
	return s, problems
}

// fromUnstructured converts without failing on fields this version does not
// know, so a downgraded front reads newer status (HBEP-5, Compatibility).
func fromUnstructured(u *unstructured.Unstructured, into any) error {
	return runtime.DefaultUnstructuredConverter.FromUnstructuredWithValidation(u.Object, into, false)
}
