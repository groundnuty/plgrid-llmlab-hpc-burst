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
	probeNamespace = "kube-system"
	probeName      = "hpcburst-launcher-probe"
)

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
