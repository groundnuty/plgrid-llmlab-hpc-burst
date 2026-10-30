package launcher

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	probeNamespace   = "kube-system"
	probeName        = "hpcburst-launcher-probe"
	gcProbeNamespace = "hpcburst-launcher"
)

// gcProbeTimeout is how long the dependent may take to disappear: "within a minute" (HBEP-11,
// Operational readiness 1). Tests that start a host without a garbage collector shorten it.
var gcProbeTimeout = time.Minute

// writeProbe writes one small object to the store: it sets the time on a ConfigMap, creating
// it the first time. HBEP-11 wants the launcher's health to report write failures (a full disk
// stops SQLite from accepting writes while reads and /readyz still work), and only a write
// shows that. The object lives in kube-system and carries only the time of the last probe.
func writeProbe(ctx context.Context, cfg *rest.Config, timeout time.Duration) error {
	c := rest.CopyConfig(cfg)
	c.Timeout = timeout
	cs, err := kubernetes.NewForConfig(c)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	patch := []byte(fmt.Sprintf(`{"data":{"checked":%q}}`, now))
	_, err = cs.CoreV1().ConfigMaps(probeNamespace).Patch(ctx, probeName, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: fieldManager})
	if apierrors.IsNotFound(err) {
		_, err = cs.CoreV1().ConfigMaps(probeNamespace).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: probeName, Namespace: probeNamespace,
				Labels: map[string]string{"app.kubernetes.io/managed-by": fieldManager}},
			Data: map[string]string{"checked": now},
		}, metav1.CreateOptions{FieldManager: fieldManager})
	}
	return err
}

// gcProbe is HBEP-11's end-to-end check of the garbage collector (Operational readiness 1 and 3).
// It creates an owner and a dependent ConfigMap in the launcher's namespace, deletes the owner,
// and requires the dependent, by UID, to be gone within timeout. /readyz, the controller manager's
// /healthz and a write all succeed on a host whose garbage collector does nothing; only this shows
// that it works. It leaves nothing behind but the namespace.
func gcProbe(ctx context.Context, cfg *rest.Config, timeout time.Duration) error {
	c := rest.CopyConfig(cfg)
	c.Timeout = 15 * time.Second
	cs, err := kubernetes.NewForConfig(c)
	if err != nil {
		return err
	}
	labels := map[string]string{"app.kubernetes.io/managed-by": fieldManager}
	create := metav1.CreateOptions{FieldManager: fieldManager}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: gcProbeNamespace, Labels: labels}}
	if _, err := cs.CoreV1().Namespaces().Create(ctx, ns, create); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create the namespace %s: %w", gcProbeNamespace, err)
	}
	cms := cs.CoreV1().ConfigMaps(gcProbeNamespace)
	cleanup := func(name string, uid types.UID) {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = cms.Delete(cctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	}
	owner, err := cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{GenerateName: "gc-probe-owner-", Labels: labels}}, create)
	if err != nil {
		return fmt.Errorf("create the probe owner: %w", err)
	}
	dep, err := cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{GenerateName: "gc-probe-dependent-", Labels: labels,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID}}}}, create)
	if err != nil {
		cleanup(owner.Name, owner.UID)
		return fmt.Errorf("create the probe dependent: %w", err)
	}
	bg := metav1.DeletePropagationBackground
	if err := cms.Delete(ctx, owner.Name, metav1.DeleteOptions{PropagationPolicy: &bg, Preconditions: &metav1.Preconditions{UID: &owner.UID}}); err != nil {
		cleanup(dep.Name, dep.UID)
		return fmt.Errorf("delete the probe owner: %w", err)
	}
	deadline := time.Now().Add(timeout)
	for {
		got, err := cms.Get(ctx, dep.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || (err == nil && got.UID != dep.UID) {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			cleanup(dep.Name, dep.UID)
			if err != nil {
				return fmt.Errorf("the probe dependent %s could not be read after its owner was deleted: %w", dep.Name, err)
			}
			return fmt.Errorf("the probe dependent %s (uid %s) still exists %s after its owner was deleted: the garbage collector is not working", dep.Name, dep.UID, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
