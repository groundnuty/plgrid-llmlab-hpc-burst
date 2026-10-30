// Package testenv starts a Kubernetes control plane for tests: etcd,
// kube-apiserver and kube-controller-manager as plain processes of the current
// user, with the controller manager limited to its garbage-collector and
// namespace controllers. It is the assembled upstream stack of HBEP-11
// (Single-user host), the alternative to k3s.
//
// A bare kube-apiserver with etcd, as controller-runtime's envtest starts it,
// deletes no dependents (spikes S3 and S4), so a test that needs garbage
// collection must not run on it. That is why this package exists.
//
// Binaries come from the environment, never from fixed paths:
//
//	HPCBURST_ENVTEST_ASSETS            directory holding etcd and kube-apiserver
//	HPCBURST_KUBE_CONTROLLER_MANAGER   path of kube-controller-manager
//
// When either is unset, Start runs hack/test-binaries.sh, which finds or
// fetches them and checks their sha256. HPCBURST_TOOLS_CACHE chooses the
// script's cache directory.
//
// Every listener is on loopback in a block of ports starting at
// HPCBURST_TESTENV_PORT_BASE (default 29200): etcd client, etcd peer,
// kube-apiserver, kube-controller-manager. The block must be free: a second
// control plane started at the same time on the same block is refused.
package testenv
