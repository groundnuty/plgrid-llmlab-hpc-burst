// Package frpsplugin is the admission half and the path-state half of HBEP-6's alpha tunnel, as a
// library for the lifecycle controller (#88) to mount:
//
//   - Handler is frps's HTTP server plugin (HBEP-6, Admission: the server plugin). It answers
//     Login, NewProxy and CloseProxy from an in-memory Index of the Allocations that may log in
//     and the Instances placed on them, within a deadline below one second, and refuses anything
//     it cannot decide from memory.
//   - Reader reads frps's proxy list (HBEP-6, Path state and the inventory) from its web API and
//     turns it into each Instance's TunnelConnected condition. The list is the only source of
//     the condition: the plugin's calls are triggers (Notifier), never the truth.
//
// Nothing here starts a server, a goroutine of its own, or a Kubernetes client. The controller
// serves the handler on agentapi.Host.FrpsPluginAddr, feeds a MemIndex (the Feeder) from its caches
// and the credential files, and writes the reader's conditions.
//
// Provenance: the wire format is frp v0.71.1's (github.com/fatedier/frp, Apache-2.0), read at
// pkg/plugin/server/types.go, pkg/msg/msg.go and server/http/model/types.go; no frp code is used.
package frpsplugin
