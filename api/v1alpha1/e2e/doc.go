// Package e2e holds the tests of the v1alpha1 custom resources that need a
// running Kubernetes API server: the CRDs are installed from config/crd, then
// real objects are created, refused, updated and deleted.
//
// By default the suite starts the assembled upstream stack (etcd,
// kube-apiserver and kube-controller-manager with its garbage collector) from
// internal/testenv. It can instead run against another host:
//
//	HPCBURST_TEST_HOST=k3s HPCBURST_KUBECONFIG=<kubeconfig of a user-level k3s>
//
// That is how the same tests run on the single-user host (HBEP-11): the k3s
// launcher, internal/launcher (#41), starts the server and writes the
// kubeconfig. Without the switch the k3s test reports a skip that names it.
package e2e
