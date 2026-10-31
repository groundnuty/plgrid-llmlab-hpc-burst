// Package front is the LLMLab-compatible front (HBEP-5): the one endpoint a
// user points their client at. It serves PLGrid LLMLab's OpenAI-compatible
// contract on the client listener, resolves each request's model name by
// HBEP-3's grammar, keys the conversation, chooses one of the framework's own
// Instances by bounded-load consistent hashing before an attached managed
// provider, stops sending to an Instance at its lease end minus the drain
// window, and names what answered on every response and in a usage record.
// Its dial-home listener passes /agent/v1/ through to the lifecycle
// controller in direct mode, and frpc's /~!frp websocket to frps in both
// modes; the front reaches each Instance through frps's tcpmux CONNECT,
// registered while its probe passes (HBEP-6, The front's half).
//
// Layout:
//   - inventory: the routing view, built from the control plane by watch;
//   - balancer: one KubeAI CHWBL group per ModelDeployment, wrapped;
//   - convkey and naming: the conversation key and the name grammar;
//   - usage: the usage records and their counter;
//   - this package: configuration, keys, the router, the request path, the
//     stream judge, errors and attribution, and the listeners.
//
// What is reused, and from where (HBEP-15):
//   - KubeAI's CHWBL group (kubeai-project/kubeai, Apache-2.0, commit
//     89dfff46): copied code under third_party/forked/kubeai, driven by the
//     balancer package.
//   - github.com/cespare/xxhash v1.1.0 (MIT): the ring hash, the fork's.
//   - k8s.io/client-go v0.35.3 (Apache-2.0, already a dependency): the
//     dynamic informers of the watch and the events recorder.
//   - The Go standard library's httputil.ReverseProxy for the /agent/v1/
//     passthrough.
//   - frp v0.71.1 (fatedier/frp, Apache-2.0, https://github.com/fatedier/frp,
//     commit d8cf180): no code is copied. The Tunnel speaks frps's tcpmux
//     httpconnect CONNECT and the /~!frp passthrough feeds frps's websocket
//     listener, as HBEP-6's frp facts read them; spike S1c (PR #78) measured
//     the path. New: the Tunnel, the probe and the passthrough.
//   - Spikes S10, S10b and S10c (this repository, PRs #34, #50, #53): the
//     balancer wrapper's rules and their tests, rewritten as product code;
//     S2 phase A (PR #44) for the deadlines and the keep-alive rule.
//   - www-2027's lessons L-2.5, L-4.1, L-4.3, L-4.14, L-6.1 to L-6.7 (HBEP-5,
//     Field lessons answered).
//   - New: the request path, the stream judge, the error table, the usage
//     sink and the router.
package front
