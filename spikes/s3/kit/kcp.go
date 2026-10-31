package kit

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var workspaceGVK = schema.GroupVersionKind{Group: "tenancy.kcp.io", Version: "v1alpha1", Kind: "Workspace"}

// PrepareKCPWorkspace creates (or finds) the child workspace "s3e2e" of type universal under
// root, using kcp's admin kubeconfig, and writes a kubeconfig that points at it to out. The
// e2e binary then needs no kcp-specific code: it sees an ordinary kubeconfig.
func PrepareKCPWorkspace(ctx context.Context, admin, out string) (string, error) {
	cfg, err := clientcmd.LoadFromFile(admin)
	if err != nil {
		return "", err
	}
	rc, err := clientcmd.NewNonInteractiveClientConfig(*cfg, "root", &clientcmd.ConfigOverrides{}, nil).ClientConfig()
	if err != nil {
		return "", err
	}
	c, err := client.New(rc, client.Options{})
	if err != nil {
		return "", err
	}
	ws := &unstructured.Unstructured{}
	ws.SetGroupVersionKind(workspaceGVK)
	key := client.ObjectKey{Name: "s3e2e"}
	if err := c.Get(ctx, key, ws); err != nil {
		ws.SetName("s3e2e")
		_ = unstructured.SetNestedField(ws.Object, "universal", "spec", "type", "name")
		_ = unstructured.SetNestedField(ws.Object, "root", "spec", "type", "path")
		if err := c.Create(ctx, ws); err != nil {
			return "", fmt.Errorf("create workspace: %w", err)
		}
	}
	var url string
	for end := time.Now().Add(60 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if err := c.Get(ctx, key, ws); err != nil {
			continue
		}
		u, _, _ := unstructured.NestedString(ws.Object, "spec", "URL")
		if u != "" && ws.GetLabels()["tenancy.kcp.io/phase"] == "Ready" {
			url = u
			break
		}
	}
	if url == "" {
		return "", fmt.Errorf("workspace s3e2e not ready within 60s")
	}
	cl := cfg.Contexts["root"].Cluster
	cfg.Clusters[cl].Server = url
	cfg.CurrentContext = "root"
	return out, clientcmd.WriteToFile(*cfg, out)
}
