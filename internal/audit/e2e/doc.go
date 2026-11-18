// Package e2e is HBEP-10's audit, end to end (issue #660, PR B): one combined run of the real lifecycle controller,
// on a test API server (internal/testenv, a real kube-apiserver, etcd and kube-controller-manager) that writes the
// audit log under the embedded policy (config/audit) with a log size of 1 MB, so that the log rotates during the
// run. The controller runs on its own ServiceAccount's token (the launcher's PrepareControllerIdentities, as hpcburst
// up issues it), two agents and the person act through their own credentials, and every assertion reads the API
// server's audit log FILES, the plain one and the rotated gzip ones, never an object through the client that wrote it.
//
// It binds loopback ports through internal/testenv in the block from 28560, inside testenv's second range (28560-28579),
// of which this package uses 28560-28563 and 28570-28579 is #697's. Run it under the shared envtest lock on a machine
// where other agents test too, or on Sano (IMPL-COMMON).
package e2e
