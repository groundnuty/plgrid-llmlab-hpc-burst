// Package managed is the managed Provider's controller (HBEP-4, Managed
// provider in alpha; issue #69): Client reads a gateway's catalog with a
// Provider's credentials (provider.Managed over HTTP), and Controller runs
// the read-only catalog loop that writes the Provider's status.catalog,
// lastCatalogRead and its credentials conditions. It takes no lifecycle
// action. Its subpackage llmlabtest is the HTTP double of LLMLab that this
// package and the front (#64) test against.
package managed
