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
// Start always runs hack/test-binaries.sh, which finds or fetches the binaries
// and checks their sha256; the two variables say where it looks, and a binary
// that differs from its pin is refused. HPCBURST_TOOLS_CACHE chooses the
// script's cache directory. HPCBURST_TESTENV_UNPINNED=1 is the explicit
// development override: with both variables set it uses the binaries without
// the check, and says so on stderr.
//
// Every listener is on loopback in a block of four ports starting at
// HPCBURST_TESTENV_PORT_BASE (default 29200): etcd client, etcd peer,
// kube-apiserver, kube-controller-manager. The whole block must lie inside
// 29200-29219, issue #40's range, so the base is 29200 to 29216; any other
// value is refused. The block must be free: a second control plane started at
// the same time on the same block is refused.
//
// A root k3s may run on the same machine (ports 6443, 6444 and 10250). Nothing
// here connects to those ports: GuardSystemPorts reads the kernel's table of
// listening sockets, and ExternalConfig refuses a kubeconfig whose server is
// not loopback on 29200-29239 before any client exists. Children are killed
// when the test process dies (Pdeathsig).
package testenv
