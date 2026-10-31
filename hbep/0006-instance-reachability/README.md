# HBEP-6: Instance reachability

## Summary

On batch providers, compute nodes are reachable only through login nodes, and on some clusters not at all. This HBEP decides how the front (HBEP-5) reaches an instance: the instance dials home. For alpha the tunnel is frp v0.71.1. Each Instance's frpc connects out over `wss` to the front's dial-home listener, which passes it to frps on the control-plane host. frps routes the front's requests by the Instance's UID. A server plugin in the lifecycle controller admits each tunnel with the Allocation's credential. The HBEP sets the wiring of both dial-home modes, the heartbeats, the front's deadlines and end-to-end probe, path state and teardown. Login-node relays and port forwards are not used in alpha. Every path has one owner, a liveness signal and a defined teardown.

## Motivation

This HBEP serves stage 2 of the user story (HBEP-2), getting resources on HPC, and stage 5, serving through one endpoint. For teardown it also serves stage 7, cleanup.

Problems:
- Listeners on compute nodes were reachable only through login nodes. Some clusters could not be reached from other sites' networks at all, and node-to-node ssh inside jobs was denied.
- Hand-made tunnels grew to hundreds of ssh forwards and watchdogs. Ports derived from indices by arithmetic were misread. Two operators' watchdogs shared a name and pid files.
- Parallel logins tripped a login node's connection throttle. The shared ssh master that fixed it glitched in bursts, and tunnels riding on it broke kill-by-PID.
- The kernel's ephemeral range took fixed ports. Ports derived from job IDs collided when two servers ran in one job, and loopback relay ports clashed between jobs on shared nodes.
- To join a pool on another node, workers had to listen on all interfaces without a key.
- A job that hit its time limit left its endpoint file behind, so the inventory was wrong.
- A multi-homed node changed address between relaunches while the old address still answered.
- A follow-on on a new node changed the URL, so clients had to re-point.
- At the end, keyword sweeps missed hundreds of tunnels, and a cron entry revived killed relays the next day.

### Goals

- The front reaches every instance by its instance identity. No client or operator computes a host, address or port.
- Each path has exactly one owner, and the path ends when its owner ends.
- Path liveness is checked end to end, through the path to the engine.
- Teardown leaves nothing behind. Every process the framework starts outside an allocation is in the inventory, and is removed by record, never by pattern.
- Only the path carries traffic from outside an allocation. Engine HTTP listeners bind to loopback or a Unix socket and are reached through the tunnel agent.

### Non-goals

- One tunnel technology for every provider. Profiles may differ (HBEP-4).
- Instance health and restarts (HBEP-7). A path that is down is not a dead instance.
- Traffic inside a multi-node instance. Its ranks use the cluster fabric, unauthenticated by default: a known limitation (Risks).
- Reachability of non-LLM services. These are out of scope for now.

## Proposal

### User stories

- An instance starts on Helios. Its frpc connects out to the front and logs in with its Allocation's credential. Once the controller marks the instance ready and the front's probe through the tunnel passes, the front routes to it. Nobody opens a port or a login session.
- The allocation reaches its hard end. The job ends, frpc's connection closes, frps drops the proxy, and the controller marks the path gone. Nothing is left on login nodes or on the control-plane host.
- The engine dies while its tunnel stays up. The front's probe fails twice, the front stops sending new requests to the Instance and reports it, and HBEP-7's own checks decide the restart.
- An operator or agent lists every path, with its owner, instance and last heartbeat: the Instances' tunnel conditions, plus frps's list for anything that has no Instance.

### Notes, constraints and caveats

- **Compute nodes connect out on port 443.** Spike S1's step 0 and probe 2 (issue #18, 30 Oct 2026) measured it.
  - Short jobs on Helios CPU, Helios GH200 and Athena A100 all reached public HTTPS endpoints directly, with no proxy configured.
  - The development VM behind a NAT accepted only port 22 from those nodes. There, the tunnel runs inside ssh over port 22, with a restricted key (ssh mode).
  - Sano was not measured. It is not an alpha platform (HBEP-4).
  - Where dial home works stays a per-partition fact in the platform profile (HBEP-4).
- **The front needs an address that compute nodes reach** (HBEP-11, open question 8): directly on the dial-home port (direct mode), or through ssh on port 22 with a restricted key (ssh mode). A host reachable by neither is out of alpha, and HBEP-11's launcher checks it.
- **The center consents** to outbound tunnels from compute nodes (owner, 31 Oct 2026; open question 9).
- **Inside the instance,** the engine listens on a Unix socket in the job's private temporary directory (HBEP-7). frpc reaches it through that socket, with the Instance's own key on the serving path (HBEP-9). That key is not the Allocation's credential, which admits the tunnel. A key covers only part of the engine's HTTP API (Risks).
- **All tokens of every reply cross the path.** Its throughput and latency under many concurrent streams are part of serving capacity (Operational readiness 5).
- **Spike S1c (PR #78) is the main evidence** for the tunnel's numbers. It ran both ends on one VM, on loopback, behind a link emulator, at head `2f55545`. Its figures carry their conditions below.

### Risks and mitigations

- **Outbound is blocked or proxied.** It is a profile fact, measured per partition. Every alpha partition dials out (S1's step 0), so no relay is built for alpha (open question 3).
- **Streams on one tunnel block each other (head-of-line blocking).** frp multiplexes every stream of a tunnel over one connection (yamux, with up to 6 MiB of send credit per stream). In S1c this happened in every valid run, on every transport: on a 1 MB/s link, one 4 MiB reply held every other stream's tokens back for 4.47–4.54 s, and one slowly read reply for up to 7.4 s. With separate connections behind the same shared cap the delay stayed under 0.15 s. Mitigations:
  - the agent's control traffic has its own connection, never the token tunnel (Dial-home modes);
  - in alpha the tunnel carries only HBEP-5's alpha endpoints. These include `/v1/embeddings` and non-streamed replies, which can be large, so they can still stall the streams of their Instance for seconds on a slow path. The path rate is measured in S1's one-hour runs (Graduation criteria);
  - after alpha, a bulk endpoint gets its own frpc proxy with `transport.bandwidthLimit`, or a separate frpc with `transport.tcpMux = false` (open question 12).
- **A short heartbeat timeout drops a healthy tunnel.** With a 6 s timeout, a slow reader queued more than 6 s of data in S1c, a heartbeat timed out, and all 16 streams were cut, in both passes. S1's set survived every run. Heartbeat timeouts are budgeted from the queueing delay (Heartbeats). yamux's ping timeout is fixed at 10 s, so on a slow path a queue of more than 10 s drops the tunnel at any setting.
- **The tunnel drops while the instance is healthy.** frpc and the ssh forwards reconnect on their own. Meanwhile the front holds the Instance as not eligible, not dead (HBEP-5), and the controller restarts nothing on that basis (HBEP-7).
- **Requests hang in a dead tunnel before frps notices.** frps answers the front's `CONNECT` before it takes a work connection (frp facts), so a silent path still accepts requests. In S1c most such requests waited frps's `userConnTimeout` of 10 s, and one per run waited until the session was torn down (over 15 s at frp's defaults). frps itself bounds neither. The front enforces its own deadlines and probes the path end to end (The front's half).
- **A lifecycle-controller outage cuts every tunnel** (a gap in frp, under review: see Design details, Admission). The plugin is subscribed to `Ping` so that the controller can record heartbeats. frps turns a failed plugin call into an error answer to frpc's ping, and frpc then closes its session. So while the plugin endpoint is down, every tunnel drops at its next ping, within 10 s, and cannot log in again until the plugin returns. The heartbeat metric depends on this subscription.
- **A false instance registers and receives prompts.** frps admits a tunnel only through the plugin: the login must carry a live Allocation's credential, and a proxy may claim only the UID of an Instance placed on that Allocation (Admission). The Instance takes traffic only after the controller's verification (HBEP-7, HBEP-9). One limit remains: frps attaches a work connection to a session by its run ID alone, and neither the plugin nor frps's empty token checks it (frp facts). frps draws run IDs at random. They appear in frps's log and the plugin's calls on the control-plane host, and frpc logs its own and prefixes every later line with it. Anyone who read that log could, in direct mode, attach a work connection through the public dial-home listener and receive requests, with the Instance's key in their headers. So frpc's log goes only to a file in the job's private temporary directory, never to the job's output files on shared storage (frpc in the job). A shared frps token with `auth.additionalScopes = ["NewWorkConns"]` would close this; it is not in alpha.
- **The tunnel's ping succeeds while the engine is dead.** The front's probe goes through the path to the engine.
- **An engine API key is taken for isolation.** A vLLM API key covers only part of the engine's HTTP API (vLLM's security documentation), and traffic between an engine's ranks is unauthenticated by default. So engine HTTP listeners bind to a Unix socket or loopback and are reached through frpc. A multi-node instance's rank traffic still crosses the cluster network unauthenticated. That is a known limitation, recorded in each platform profile (HBEP-4).
- **Engine internals listen off loopback.** In spike S2 phase B each vLLM worker had its API port on the IPv4 loopback address and six more TCP listeners on the node's cluster address, although `--host` and `VLLM_HOST_IP` were both set to that loopback address (HBEP-5, Risks). The serving path needs the Instance's key. The other listeners are engine internals, which no key covers, so an Instance on a shared cluster network exposes them unless the provider's network isolates it.
- **Loopback is shared.** Other users' jobs on a shared compute node can reach the ssh forwards' local ports, and other local users on the control-plane host can reach frps and the plugin endpoint (L-7.5). The forwards lead only to endpoints that check the Allocation's credential or the Instance's key. The alpha host is single-user. Recorded as a known limit.
- **No end-to-end encryption from front to engine.** TLS ends at the front's dial-home listener; from frps to frpc's socket the bytes are plain inside the TLS hop (open question 2). Security is not the main concern for alpha (owner), so this is a recorded limit.
- **Paths outlive their owner.** frpc ends with its Instance and at the latest with the job. Owner references and garbage collection (HBEP-11) remove the front-side records.
- **Teardown kills the wrong process.** The launcher stops frps by its recorded process ID plus start time (HBEP-11). The agent stops frpc and its ssh forwards as its own children. No respawners outside the controller and the launcher.
- **The front's address changes.** frpc and the agent reach the front by DNS name in direct mode, never by a stored address.

### Open questions

1. **Tunnel protocol.** **Decided** (science-agent decision, 31 Oct 2026): frp v0.71.1 (`fatedier/frp`), as two pinned release binaries, frps and frpc, with frpc's `wss` transport to the front's dial-home listener and `tcpmux` routing (`httpconnect`) by the Instance's UID. Source: spike S1c (PR #78). With no impairment, at 1 to 256 streams of 20 tokens/s, frp added 1–4 ms to the median time to first token over tcpmux, wss and ssh. wss costs the front 7.9–8.7% of a core per 1,000 tokens/s against 6.6–6.9% for frp's plain tcp, and about 5% more payload, and it is ordinary HTTPS. frps v0.71.1 cannot serve wss itself, so the front's TLS terminator passes the websocket on (S1c's self-test; frp facts).
2. **Encryption.** **Decided for alpha** (science-agent decision, 31 Oct 2026, under the owner's "security is not the main concern"): TLS terminates at the front's dial-home listener, which frpc verifies against the launcher's certificate. In ssh mode that TLS runs inside ssh. Front to frps is loopback on the control-plane host. frpc reaches the engine through its Unix socket on the node, and the engine checks the Instance's key (HBEP-9). There is no end-to-end TLS from front to engine (Risks). Source: S1c's wss arm, where frpc verified the front's certificate and every request completed.
3. **Relay fallback form.** **Not needed for alpha:** every alpha partition dials out on 443 with no proxy (S1's step 0: Helios CPU, Helios GH200 and Athena A100). A partition without outbound needs a relay first.
4. **Port forwards.** **Not needed for alpha:** a port forward is never a routing path in alpha. Whether forwards ever become a supported path, with an owner and a teardown, stays the owner's decision after alpha.
5. **Multi-node instances.** **Not needed for alpha:** alpha Instances are single-node (HBEP-7, Provisioning).
6. **Heartbeats.** **Decided** (science-agent decision, 31 Oct 2026): S1's set, on both ends, as the tested starting point (Heartbeats). Source: S1c. frps noticed a silent path (a frozen path or a node crash) in 12–20 s, against 28–34 s at frp's defaults, and the set survived every head-of-line run. The tight set (2 s interval, 6 s timeout) noticed in 4–6 s but dropped a healthy tunnel. S1c ran on loopback, so the real path rate from compute node to front is unknown. It is measured in S1's one-hour runs, and is an alpha graduation criterion.
7. **Credential.** **Decided for alpha** (science-agent decision, 31 Oct 2026): HBEP-7's per-Allocation random 256-bit token, valid for the Allocation's life. frpc sends it in its login metadata, and the frps server plugin checks it (Admission). Source: frp v0.71.1's plugin interface (frp facts). **Not needed for alpha:** renewal over the tunnel, since the token lives no longer than its Allocation's lease.
8. **A front behind NAT,** as in a single-user setup. **Decided for alpha** by HBEP-11 (its open question 8): no rendezvous relay. The alpha host is reached directly on the dial-home port, or through ssh on port 22 with a restricted key. Source: S1's step 0, where the development VM behind a NAT accepted port 22 from the compute nodes and nothing else. **Not needed for alpha:** a relay for other setups.
9. **Center consent.** **Decided** (owner, 31 Oct 2026): "The outbound tunnels are acceptable use, yes."
10. **Compute-node reachability.** **Decided for alpha's partitions** by S1's step 0 and probe 2 (issue #18): outbound 443 works on Helios CPU, Helios GH200 and Athena A100, and the home directory, which holds the ssh-mode key, is readable from their compute nodes (HBEP-4, open question 16). Inbound reachability is not needed, since Instances dial home. **Not needed for alpha:** Sano, which is not an alpha platform (HBEP-4).
11. **Many tunnels on one front.** **Open (blocks beta):** S1c measured one frpc. frps's CPU and memory with hundreds of tunnels, and many tunnels sharing one link, need a measurement.
12. **Bulk endpoints off the token tunnel.** **Open (blocks beta):** embeddings, file transfers and large non-streamed replies get their own frpc proxy with `transport.bandwidthLimit`, or a separate frpc with `transport.tcpMux = false`. S1c tested neither.

## Design details

**Dial home** (decided). The allocation agent that the Allocation's job runs (HBEP-7) starts frpc, the tunnel agent, as its child.
- The agent's own control connection carries what the Allocation should run and its reports. It belongs to the Allocation, and goes to the lifecycle controller's agent endpoint (HBEP-7).
- The serving tunnel belongs to the Instance. frpc starts when the agent is told to run the Instance and stops when the Instance ends.
- The two never share a connection (Dial-home modes).

### Components

| Where | Component | Role |
|---|---|---|
| control-plane host | the front's dial-home listener (HBEP-5) | terminates TLS with the launcher's certificate; passes `/agent/v1/` to the agent endpoint in direct mode, and frpc's websocket to frps |
| control-plane host | frps | routes the front's `CONNECT <UID>` to the Instance's tunnel; started and supervised by the launcher (HBEP-11) |
| control-plane host | the plugin endpoint, in the lifecycle controller's process | admits logins and proxies, and records path state and heartbeats |
| job | the allocation agent | starts frpc, and in ssh mode its two ssh forwards |
| job | frpc | dials home over `wss` and serves one `tcpmux` proxy, named by the Instance's UID, from the engine's Unix socket |

**Loopback ports on the control-plane host,** by offset from the launcher's port base. The offsets marked "proposed" are proposals for the code agent, who owns `pkg/agentapi` and `internal/launcher`. With them the block grows to base+9, and `PortsFromBase`'s range check grows with it.

| Offset | Listener | Status |
|---|---|---|
| base+3 | the agent endpoint (HBEP-7) | `agentapi.AgentEndpointPortOffset`, on `main` |
| base+4 | the frps plugin endpoint, plain HTTP | proposed |
| base+6 | frps's bind port | proposed |
| base+7 | the dial-home listener's default in ssh mode (HBEP-5) | `agentapi.DialHomePortOffset`, on `main` |
| base+8 | frps's tcpmux `CONNECT` port | proposed |
| base+9 | frps's web server, for its API | proposed |

The plugin endpoint has its own port. It is not on base+3, because the restricted key opens base+3 to every job (Dial-home modes), and a job could then send the plugin forged calls.

### frps on the control-plane host

The launcher verifies frps's checksum against `build/dependencies.yaml` (HBEP-15), writes its configuration file (mode 600, in the launcher's state directory), starts it, records it by process ID plus start time, and restarts it when it exits, as it does for k3s (HBEP-11).

```toml
bindAddr = "127.0.0.1"
bindPort = <base+6>
proxyBindAddr = "127.0.0.1"
tcpmuxHTTPConnectPort = <base+8>
webServer.addr = "127.0.0.1"
webServer.port = <base+9>
webServer.user = "hpcburst"
webServer.password = "<random, from the launcher>"
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatTimeout = 25
transport.tcpKeepalive = 30

[[httpPlugins]]
name = "hpcburst"
addr = "127.0.0.1:<base+4>"
path = "/frp/v1/handler"
ops = ["Login", "NewProxy", "CloseProxy", "Ping"]
```

- **Loopback only.** frp's `bindAddr` defaults to every interface, and `proxyBindAddr` follows it, so both are set. The web server defaults to loopback. Every other listener (KCP, QUIC, HTTP and HTTPS vhosts, the ssh tunnel gateway) stays off at its default of port 0. The launcher's health check requires frps to hold exactly its three loopback ports, as it does for k3s (HBEP-11).
- **The web server** runs because frps keeps its proxy list only while it runs (frp facts). It also serves frp's dashboard pages; nothing uses them, and they sit behind the password on loopback. Its unauthenticated `/healthz` is the launcher's liveness check.
- **Auth.** frps keeps frp's default `token` method with no token, and frpc sends none. frps accepts that, so its own check passes every login, and the plugin is the gate (Admission).
- **Everything else** stays at frp v0.71.1's defaults: `userConnTimeout` 10 s, `tcpmuxPassthrough` off.
- **Logs** go to a file in the launcher's state directory. frps logs proxy names (Instance UIDs) and run IDs, never the login metadata.

### frpc in the job

The Provider's controller stages frpc for the partition's CPU architecture by digest, next to the agent's binary, together with the launcher's certificate, which is public (HBEP-4, Submission). The agent checks frpc's SHA-256 before it runs it. It writes frpc's configuration into the job's private temporary directory, mode 600, because it holds the credential. frpc reads it by path, so the credential is never on a command line.

```toml
serverAddr = "<direct: the front's DNS name; ssh: 127.0.0.1>"
serverPort = <direct: the dial-home port; ssh: connection 2's local port>
loginFailExit = false
log.to = "<the job's private temporary directory>/frpc.log"
metadatas.token = "<the Allocation's credential>"
transport.protocol = "wss"
transport.tls.trustedCaFile = "<the staged launcher certificate>"
transport.tls.serverName = "dial-home.hpcburst.invalid"
transport.poolCount = 0
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatInterval = 10
transport.heartbeatTimeout = 25
transport.dialServerKeepalive = 30

[[proxies]]
name = "<the Instance's UID>"
type = "tcpmux"
multiplexer = "httpconnect"
customDomains = ["<the Instance's UID>"]
[proxies.plugin]
type = "unix_domain_socket"
unixPath = "<the engine's socket>"
```

- **Certificate checks.** frpc skips verification silently when `trustedCaFile` is empty (frp facts). So the agent refuses to start frpc without the staged certificate, and only after checking that the certificate's public-key pin equals `agent.json`'s pin (HBEP-7). Go accepts a self-signed certificate that is itself in the trusted pool (`crypto/x509/verify.go:835-836` at go1.25.0) and then checks `serverName` against its DNS names.
- **The server name** is a fixed synthetic name in the launcher's certificate (HBEP-11). `dial-home.hpcburst.invalid` is a proposal for the code agent; the `.invalid` top-level domain never resolves. It is the same in both modes, because in ssh mode frpc dials loopback.
- **`loginFailExit = false`.** frp's default makes frpc exit when its first login fails. With `false` it keeps retrying, like the ssh forwards. frpc is part of the agent's transport, not a part of the Instance: the agent restarts it with backoff if it exits, without asking the controller, as it keeps its ssh forwards (HBEP-7). A tunnel down is not a dead Instance.
- **`user` is not set,** so frps sees the proxy name unchanged (frp facts).
- **Logs.** frpc logs to a file in the job's private temporary directory (`log.to`), and its console output is discarded. frpc logs its run ID, which is enough to attach a work connection to its session (Risks), so the log never reaches the job's output files on shared storage.
- **The proxy** reaches the engine through frp's `unix_domain_socket` client plugin. A `tcpmux` proxy hands each work connection to its plugin, so no TCP listener serves the Instance on the node (frp facts). In beta, a router that listens on loopback (HBEP-5) is reached through `localIP` and `localPort` instead.
- **Heartbeats** are S1's set (Heartbeats). `poolCount = 0` is S1's tested value; frps still keeps one spare work connection (frp facts).

### Dial-home modes

**Direct mode.** frpc's `wss` connection goes to the front's dial-home listener, on the address compute nodes reach. The agent's control traffic uses its own HTTPS connection to `/agent/v1/` on the same listener, pinned as before (HBEP-7). frpc's connection carries only the token tunnel.

**Ssh mode.** The job runs two `ssh -N -L` connections over port 22, with the same restricted key, each in its own reconnect loop.
- **Connection 1** forwards to the agent endpoint's loopback port (base+3), for the agent's control traffic.
- **Connection 2** forwards to the dial-home listener's loopback port (base+7), for frpc's `wss`.
- **Local ends.** Each forward listens on the node's IPv4 loopback address, on a port the agent asks the kernel for at start, never a fixed or derived port (L-2.4, L-7.7). The agent runs ssh with `ExitOnForwardFailure=yes`, so a port another job took in between fails at once and the loop retries with a new port. It also sets `ServerAliveInterval=10` and `ServerAliveCountMax=3`, and the pinned host key (HBEP-7).
- **Dead nodes.** A node that crashes sends no FIN, so its forwards' sshd sessions on the control-plane host stay until sshd notices. The key's options cannot set that, so the control-plane host's sshd sets `ClientAliveInterval 10` and `ClientAliveCountMax 3`, an operator setting. Otherwise the sessions linger until TCP keepalive ends them, a known limit; they carry nothing once frps has dropped the proxy.
- **The key's line** in `authorized_keys` on the control-plane host (owner decision, 31 Oct 2026), one key per cluster, used by S1's one-hour runs and by alpha:

  ```
  restrict,port-forwarding,permitopen="127.0.0.1:<agent endpoint port>",permitopen="127.0.0.1:<dial-home listener port>",permitlisten="127.0.0.1:1",command="/bin/false"
  ```

  The two ports are base+3 and base+7, so they are fixed for a given launcher base. A custom base needs new key lines.

**The dial-home listener's paths** (HBEP-5) in both modes: `/agent/v1/` goes to the agent endpoint. A `GET /~!frp` websocket upgrade goes to frps's bind port on loopback. frps matches the exact bytes `GET /~!frp` (frp facts), so the listener forwards the request line unchanged, never re-encoded. Every other path gets 404.

### Admission: the server plugin

The lifecycle controller's process serves frps's HTTP plugin on loopback at base+4, next to the agent API. frps posts `{"version", "op", "content"}` and reads `{"reject", "reject_reason", "unchange", "content"}` (frp facts). The plugin always answers `unchange: true` and never edits content.
- **`Login`.** The content is frpc's login message, with `metas` from frpc's `metadatas`. The plugin accepts only when `metas.token` equals the credential of an Allocation that exists and has not ended, compared in constant time against the files the controller keeps (HBEP-7). Anything else is refused, and frps answers frpc with a login error.
- **`NewProxy`.** The content carries `user.metas` from the session's login, so the plugin rechecks the token on every call and keeps no session state. It accepts only a proxy with:
  - `proxy_type` `tcpmux` and `multiplexer` `httpconnect`;
  - exactly one entry in `custom_domains`, equal to `proxy_name`;
  - that value the UID of an Instance placed on the token's Allocation, which has not ended;
  - no `subdomain`, `group`, `http_user` or `route_by_http_user`.

  Anything else is refused, and frps does not register the proxy. This is what the mitigation for "a false instance registers" rests on.
- **`CloseProxy`.** A notification: it carries the proxy name and the session's user info. The plugin takes it as a reason to re-read frps's list (Path state).
- **`Ping`.** frpc's application heartbeat, every 10 s. The plugin records its time in memory and accepts it.
- **Answers come from memory.** frps calls `NewProxy` and `Ping` on the session's own read loop, with no timeout on the plugin's HTTP client (frp facts). So a slow answer stalls that tunnel's control messages, and a stall past 25 s drops it. The plugin answers from the controller's caches and never waits on the Kubernetes API.
- **Reject reasons** reach frpc and frps's log, so they name the rule that failed and never the token.
- **Fail closed.** A plugin that cannot be reached refuses every login and proxy (frp facts). frpc retries.
- **The `Ping` gap** (Risks). A failed `Ping` call becomes an error answer to frpc, and frpc closes its session. So while the plugin endpoint is down, as during a restart of the controller binary, every tunnel drops within one heartbeat interval. The heartbeat metric (Path state) depends on subscribing `Ping`. Dropping `Ping` from `ops` would remove the coupling and the metric with it. The science agent reviews this before alpha.

### Path state and the inventory

- **The condition.** The lifecycle controller records whether each Instance's tunnel is connected as an Instance condition, which it writes. Proposed names, for the code agent, who owns `api/v1alpha1/conditions.go`:
  - type `TunnelConnected`;
  - status True with reason `ProxyOnline`: frps lists the Instance's proxy as online;
  - status False with reason `ProxyOffline`: frps lists it as offline, or not at all;
  - status Unknown with reason `ProxyListUnreadable`: frps's list could not be read. The condition is never left True on a list it could not read.
- **frps's list is the truth; the plugin's calls are triggers.** The plugin sees `NewProxy` before frps registers the proxy, and registration can still fail afterwards with no `CloseProxy`. `CloseProxy` is sent asynchronously and its errors are dropped, and on a reconnect, which keeps frpc's run ID, the new session's `NewProxy` can arrive before the old session's `CloseProxy` (frp facts). So the controller sets the condition only from frps's list. It reads the list about a second after each `NewProxy` and `CloseProxy`, and every 10 s.
- **The list** is `GET /api/proxy/tcpmux` on frps's web server, on the IPv4 loopback address at base+9, with the web server's password. Each entry has `name`, `status` (`online` or `offline`), `conf` (with the custom domains), `lastStartTime` and `lastCloseTime`. Offline entries stay in it, so only `online` counts.
- **Soft state.** Path state lives in memory and in the condition. After a restart the lifecycle controller rebuilds it from the list before it writes any condition.
- **The heartbeat.** The controller keeps each session's last `Ping` in memory, keyed by the run ID it learned at that session's `NewProxy`, and exposes it per Instance as a metric. The proposed name is `hpcburst_tunnel_last_ping_timestamp_seconds`; HBEP-8 owns metric names. After a controller restart the value is unknown, never zero, until the next ping (HBEP-8).
- **The inventory** that the user story asks for is the Instances' `TunnelConnected` conditions, plus each online proxy in frps's list whose name is no current Instance's UID. Such an orphan gets an event from the controller (proposed reason `TunnelOrphaned`). The front never routes to it, since it routes only to Instances.

### The front's half

- **The transport per Instance UID** (HBEP-5, Reachability). The front dials frps's tcpmux port on the IPv4 loopback address at base+8, sends `CONNECT <UID>:80 HTTP/1.1` with `Host: <UID>:80`, and reads the reply. On 200 the connection carries plain HTTP to the engine, and may be kept alive and reused under HBEP-5's `upstreamIdleTimeout`. On 404 frps has no proxy for that UID.
- **The set-up timeout** (`tunnelSetupTimeout`, 5 s, proposed). It runs from the dial until the 200 or the 404.
  - frps writes the 200 once the UID is routed, before it takes a work connection to frpc (frp facts). So the front never sees the connection to the Instance being established, and the timeout bounds only the front's exchange with frps on loopback.
  - A refused dial, a 404 or the timeout means that no byte of the request was written. HBEP-5's open question 5 then resends it once, and it counts toward the Instance's pause.
  - A frozen path still answers 200, so this timeout cannot catch it. The streamed-headers deadline and the probe do.
- **The streamed-headers deadline** (`streamHeadersTimeout`, 30 s, proposed). For a streamed request only, it runs from the end of writing the request until the response's status line and headers arrive.
  - vLLM v0.32.0 sends a stream's status and headers before the engine starts the request. The routes return a `StreamingResponse` around a generator that has not started (`vllm/entrypoints/openai/chat_completion/api_router.py:62-80`, `chat_completion/serving.py:408-409`; `completion/api_router.py:55-72`; `responses/api_router.py:65-75`). The first chunk waits for the engine's first output (`chat_completion/serving.py:527-537`).
  - So the deadline covers frps's work connection, frpc, the socket, and vLLM's validation and rendering of the request. It never covers queueing, prefill or the first token, which HBEP-5 allows up to `idleTimeout` (a first token after 1,100 s is healthy).
  - On expiry the request has been written, so it is not resent (HBEP-5, open question 5). The client gets 504 `request_timeout` with detail `headers`, and it counts toward the pause.
- **No deadline before a non-streamed reply's first byte.** vLLM sends a non-streamed reply's headers only with its body (`chat_completion/api_router.py:69-73`), after the whole generation. Such a request relies on the probe. In the undetected window it waits until frps drops the session (12–20 s at S1's settings), and the client gets the error.
- **The probe.** Every 10 s (`probeInterval`, proposed) the front probes each Instance in phase `verifying`, `ready` or `draining`, end to end:
  - a new connection through the transport, then `GET /health`, with no key: vLLM v0.32.0 guards only paths under `/v1`, `/v2`, `/inference` and `/cohere` (`vllm/entrypoints/serve/middleware/authenticate.py:11`, `:59-60`);
  - it passes on a 200 within `probeTimeout` (10 s, proposed), which exceeds S1c's worst head-of-line delay of 7.4 s. vLLM answers 503 when its engine is dead (`vllm/entrypoints/serve/instrumentator/health.py:29-33`). Anything else fails, a set-up failure included.
- **What the probe decides.** An Instance's transport counts as registered (HBEP-5) from its first passing probe. Two consecutive failures (`probeFailures`, proposed) make it not registered, so the Instance is not eligible: HBEP-5's skip, which keeps its place in its group and its in-flight count. It is not a leave, and not a dead Instance. One passing probe makes it eligible again. Routing never rests on frps's view or on the condition alone.
- **Reporting.** When a `ready` Instance stops being eligible this way while its `TunnelConnected` is True, the front emits a `degraded` event (proposed reason `PathProbeFailing`, distinct from HBEP-7's `ProbeFailing`). The controller restarts nothing on it; HBEP-7 decides restarts from its own health checks.

### Heartbeats

S1's set, on both ends (open question 6):

| End | Setting |
|---|---|
| frps | `transport.tcpMuxKeepaliveInterval = 10`, `transport.heartbeatTimeout = 25`, `transport.tcpKeepalive = 30` |
| frpc | `transport.tcpMuxKeepaliveInterval = 10`, `transport.heartbeatInterval = 10`, `transport.heartbeatTimeout = 25`, `transport.dialServerKeepalive = 30` |

- **Detection.** In S1c frps noticed a silent path in 12–20 s, through the yamux keepalive (10 s interval plus yamux's fixed 10 s timeout), before the 25 s application heartbeat. At frp's defaults it took 28–34 s. A killed frpc is noticed in 0.07 s, through the kernel's FIN.
- **The budget.** Each heartbeat timeout must cover the heartbeat interval, the full delay of a control message on a loaded tunnel, the round trip, and a margin. The control-message delay includes head-of-line queueing. On S1c's 1 MB/s fixture it reached 4.5 s behind one 4 MiB reply and 7.4 s behind one slow reader. That is an observed scale, not a bound: it grows with the bytes queued ahead and shrinks with the path's rate. 25 s covers 10 s plus 7.4 s with room for the round trip and a margin.
- **The fixed yamux timeout.** yamux's ping timeout is fixed at 10 s (S1c, from fatedier/yamux v0.2.0), so a queue of more than 10 s, possible on a slow path, drops the tunnel at any setting.
- **The path rate** from compute node to front is unknown, because S1c ran on loopback. S1's one-hour runs measure it, and alpha graduation needs it.
- **ssh mode** adds ssh's own liveness, `ServerAliveInterval=10` with `ServerAliveCountMax=3`, which S1c did not test.

### Local listeners

Engine and router listeners bind a Unix socket or loopback, never a cluster address, and are reached only through frpc. In alpha the engine listens on a Unix socket in the job's private temporary directory (HBEP-7), so no port can clash on a shared node (L-7.7). Rank traffic between the nodes of a multi-node instance is the engine's own and stays unauthenticated by default, a known limitation (Risks). Nothing is computed from job IDs or indices.

### Paths

| Path | Owner | Liveness | Teardown |
|---|---|---|---|
| Dial-home tunnel (frpc) | the Instance | frp's heartbeats, plus the front's probe through the path to the engine | frpc stops when the Instance ends, and at the latest the job's end closes its connection. frps drops the proxy on the close, or within 12–20 s if the path went silent. The condition follows frps's list |
| Control connection (the agent's HTTPS) | the Allocation | the agent's reports (HBEP-7) | ends with the agent, at the latest with the job |
| ssh forwards (ssh mode) | the Allocation | `ServerAliveInterval` and the traffic they carry | the agent's children; they end with the job, and their sshd sessions with them |
| Login-node relay | none in alpha (open question 3) | | |
| Port forward | never a routing path in alpha (open question 4) | | |

### frp facts this design rests on

Read at frp v0.71.1 (`fatedier/frp`, tag `v0.71.1`, commit `d8cf180`). Paths are relative to the repository root.

| Fact | Source |
|---|---|
| The plugin operations are `Login`, `NewProxy`, `CloseProxy`, `Ping`, `NewWorkConn` and `NewUserConn` | `pkg/plugin/server/plugin.go:24-29` |
| The request is `{version, op, content}`, the answer `{reject, reject_reason, unchange, content}`; frps posts to `addr` + `path` with `version` and `op` in the query | `pkg/plugin/server/types.go:21-32`; `pkg/plugin/server/http.go:85-114` |
| `Login` content is the login message, with `metas`, plus the client address | `pkg/plugin/server/types.go:34-38`; `pkg/msg/msg.go:76-93` |
| `NewProxy` content is the session's user info (`user`, `metas`, `run_id`) plus the proxy message: `proxy_name`, `proxy_type`, `custom_domains`, `subdomain`, `group`, `http_user`, `route_by_http_user`, `multiplexer` | `pkg/plugin/server/types.go:40-49`; `pkg/msg/msg.go:102-134`; `server/control.go:659-664`, `:740-743` |
| `CloseProxy` content is the user info plus the proxy name; `Ping` content is the user info plus the ping | `pkg/plugin/server/types.go:51-59`; `pkg/msg/msg.go:142-144`, `:177-180` |
| A plugin can reject a login and a proxy: a `reject` or a failed call stops the chain with an error | `pkg/plugin/server/manager.go:70-106` |
| A rejected login gets a login error and no session | `server/service.go:486-529` |
| A rejected proxy is not registered, and frpc gets an error | `server/control.go:736-768` |
| The `Login` plugin runs first, then frps's own `VerifyLogin`; both must pass | `server/service.go:489-507`, `:838-845` |
| frps's default auth method is `token`, an empty token passes validation, and `VerifyLogin` compares the key derived from the configured token | `pkg/config/v1/server.go:136-139`; `pkg/config/v1/validation/auth.go:24-32`; `pkg/auth/token.go:64-69` |
| Pings and work connections are checked by frps's auth only with `additionalScopes` | `pkg/auth/token.go:71-91` |
| `NewProxy` reaches the plugin before registration, which can still fail, for example on a duplicate name | `server/control.go:745-748`, `:871-874` |
| `CloseProxy` is sent in a goroutine, and its errors are only logged | `server/control.go:667-681`; `pkg/plugin/server/manager.go:137-157` |
| frpc keeps its run ID across reconnects, logs it, and prefixes every later log line with it | `client/service.go:344`, `:353-355` |
| `NewProxy` and `Ping` are handled on the session's read loop; only handlers wrapped as async run apart | `server/control.go:727-734`; `pkg/msg/handler.go:104-108`, `:170-186` |
| The plugin's HTTP client has no timeout | `pkg/plugin/server/http.go:43-51` |
| A failed `Ping` call becomes an error `Pong`, and frpc closes its session on an error `Pong` | `server/control.go:771-794`; `client/control.go:189-200` |
| frps closes a session whose last ping is older than `heartbeatTimeout` | `server/control.go:639-652` |
| A work connection attaches to a session by its run ID | `server/service.go:882-921` |
| The tcpmux `CONNECT` is answered 200 when the domain is routed, before the connection is handed to the proxy; the work connection is taken after that, waiting up to `userConnTimeout` | `pkg/util/vhost/vhost.go:168-224`; `pkg/util/tcpmux/httpconnect.go:73-82`; `server/proxy/proxy.go:147-155`; `server/control.go:595-637` |
| An unknown domain gets 404 | `pkg/util/vhost/vhost.go:184-188`; `pkg/util/tcpmux/httpconnect.go:99-106` |
| frps keeps one spare work connection, replacing each one it hands out | `server/control.go:634-635` |
| `bindAddr` defaults to every interface, and `proxyBindAddr` follows it; the tcpmux listener binds `proxyBindAddr` | `pkg/config/v1/server.go:110-114`; `server/service.go:199-211` |
| The web server defaults to loopback; frps keeps proxy statistics only while it runs | `pkg/config/v1/common.go:50-73`; `server/service.go:155-167` |
| `GET /api/proxy/{type}` lists the proxies of a type, online and offline, with `name`, `status`, `conf`, `lastStartTime` and `lastCloseTime`; the API sits behind the web server's auth, and `/healthz` before it | `server/api_router.go:27-49`; `server/http/controller.go:151-161`, `:234-257`; `server/http/model/types.go:100-115` |
| The same web server serves the dashboard pages | `server/api_router.go:62-70` |
| frps's websocket listener matches the plain bytes `GET /~!frp`; a TLS client hello goes to frp's own TLS listener | `server/service.go:296-301`, `:360-364`; `pkg/util/net/websocket.go:16` |
| frpc's `wss` always uses TLS; with no `trustedCaFile` it skips verification | `client/connector.go:183-206`, `:223-227`; `pkg/transport/tls.go:128-155` |
| `loginFailExit` defaults to true | `pkg/config/v1/client.go:85-88` |
| With no `user`, the proxy name is sent unchanged | `pkg/naming/names.go:5-11` |
| A `tcpmux` client proxy is a general TCP proxy, which hands each work connection to its client plugin; `unix_domain_socket` dials the configured socket | `client/proxy/general_tcp.go:23-33`; `client/proxy/proxy.go:110-118`, `:217-223`; `pkg/plugin/client/unix_domain_socket.go:37-69` |
| frpc's default transport settings | `pkg/config/v1/client.go:147-163` |

### Test plan

Against the launcher's frps, the front, the agent and an engine double on one host first, then on Helios and Athena.
- **Wiring, both modes.** An Instance's frpc logs in and registers its proxy. The front's `CONNECT <UID>:80` gets 200 and a reply from the engine double. In ssh mode both forwards carry their traffic, and the agent's control traffic never uses frpc's connection.
- **Loopback only.** frps holds exactly its three loopback ports, and `ss` shows no frps listener on another address. A positive control: started without `bindAddr`, frps listens on every interface, so the check can fail.
- **Plugin refusals** (alpha graduation): a login with a wrong token, an ended Allocation's token, or no token; a proxy whose domain is not one of the token's Allocation's Instances; a proxy of type `tcp`, or `tcpmux` with another multiplexer; two domains; a `subdomain`, a `group` or an `http_user`. Each is refused, and frps lists no proxy. A test that only shows a valid login accepted proves nothing.
- **Certificate.** frpc with the right certificate connects. frpc facing another certificate, and frpc with the right certificate but another `serverName`, are refused. The agent refuses to start frpc without the certificate file, and with a certificate whose pin differs from `agent.json`'s. A test with no `trustedCaFile` that "connects" proves nothing, since frpc then verifies nothing.
- **Key restriction.** With the key's line, the two forwards work. A forward to a third port, a remote forward and a command are refused.
- **Path state.** The condition turns True after registration and False after frpc is killed. A `CloseProxy` dropped while the plugin endpoint is down is corrected by the next periodic read. A plugin accept followed by a failed registration (a duplicate name) leaves the condition False. With frps's web server stopped the condition is Unknown, never True. After a controller restart the conditions are rebuilt from frps's list.
- **Heartbeat metric.** It advances every 10 s, and reads unknown after a controller restart until the next ping.
- **The probe** (alpha graduation). Kill the engine while frpc stays up: two probes fail, the Instance stops being eligible while keeping its count, the `degraded` event appears, and no restart follows from the probe. Restart the engine: one passing probe makes it eligible again.
- **Deadlines.** Freeze the path (stop frpc with SIGSTOP): a streamed request gets 504 `request_timeout` with detail `headers` after `streamHeadersTimeout`, and is not resent. A `CONNECT` for an unknown UID gets 404 and is resent once to the next Instance. A first token after 1,100 s still completes (HBEP-5).
- **Heartbeats.** Freeze the path: frps drops the proxy within 12–20 s, and the condition turns False. A slow reader on the tunnel, as in S1c, drops no healthy tunnel.
- **Reconnects.** Restart frps: every frpc logs in again without help, and the probes pass again. Restart the front: the same.
- **The `Ping` gap.** Stop the plugin endpoint for 30 s: record whether tunnels drop, as frp's source says they do. This test documents the gap; it is not a pass criterion until the science agent's review.
- **Teardown** (alpha graduation). End the allocation at its hard limit, and cancel one early: no proxy in frps's list, no ssh process in the job's process tree or on the control-plane host, and no record left. Both cases end with a live node that sends a FIN, so also freeze the path with no FIN, through a relay on the path stopped with SIGSTOP as in S1c: frps drops the proxy within 12–20 s, and the forwards' sshd sessions end within sshd's client-alive window. frps itself is stopped only by its recorded process ID and start time.
- **One-hour runs** (alpha graduation). S1's one-hour runs pass with this wiring on Helios and on Athena, in the mode each uses, with the path rate measured.

### Compatibility and upgrades

- Clients see only the front, so a path change is invisible to them.
- **frps and frpc move together.** Both are pinned to one frp version in `build/dependencies.yaml`. New Allocations get the current frpc. An frps upgrade waits until no running Allocation has another frpc version, or until that pair is tested.
- **The agent protocol** (`/agent/v1/`) is unchanged; frpc's tunnel is separate from it.
- **Stored state:** none of this HBEP's own. Path state is soft, rebuilt from frps's list; frps's state lives in memory and is rebuilt by reconnects.
- **Downgrade:** start the earlier frps with the earlier frpc pin, under the same rule.

### Security

- **Credentials.** The Allocation's credential is in frpc's configuration file, mode 600, in the job's private temporary directory, read by path. It never appears in the batch script, on a command line, or in a log. frps and the plugin log neither the metadata nor the token.
- **The plugin endpoint** answers only frps's calls, on loopback. The restricted key does not open its port.
- **frps's web server** is on loopback, behind a password the launcher generates into frps's configuration file, mode 600.
- **Trust boundary:** an Instance can reach only the agent endpoint and the dial-home listener's two paths. The front reaches an Instance only through frps and its tunnel.
- **Prompts** cross center networks inside the dial-home listener's TLS, and in ssh mode inside ssh as well. There is no end-to-end TLS from front to engine (Risks). Nothing on the path stores prompts or replies.

### Graduation criteria

- **Alpha:**
  - S1's one-hour runs pass with this wiring on Helios and on Athena, with the path rate from compute node to front measured;
  - the plugin's refusals are tested: a wrong token, a domain that is not one of the Allocation's Instances, and a wrong proxy type;
  - a dead engine behind a live tunnel is caught by the probe;
  - teardown leaves no frps proxy and no ssh process behind.
- **Beta:**
  - many tunnels on one front are measured (open question 11);
  - bulk endpoints run off the token tunnel (open question 12);
  - a restart of the lifecycle controller cuts no tunnel (Risks: the `Ping` gap).
- **Stable:** an frps and frpc version skew is tested, and end-to-end encryption is decided with HBEP-9.

## Operational readiness

1. **Working:**
   - component health: the launcher's check of frps (its process, its three loopback ports, `/healthz`), the plugin endpoint answering, and each Instance's `TunnelConnected` condition and heartbeat metric;
   - end to end: the front's probe through each tunnel to the engine;
   - not working: a probe that fails on a connected tunnel (the `degraded` event), a condition that is False or Unknown for a running Instance, or a heartbeat older than 25 s.
2. **Restart:**
   - frps: every tunnel drops and frpc logs in again on its own. Streams through it are cut. The controller's next list read shows the new state.
   - The lifecycle controller: while its plugin endpoint is down, logins and new proxies are refused, and with `Ping` subscribed every tunnel drops (Risks). After the restart it rebuilds path state from frps's list.
   - The front: its dial-home listener carries frpc's connections, so every tunnel drops and reconnects. Its probe state starts empty, and each Instance is eligible again at its first passing probe.
   - frpc or an ssh forward: the agent restarts it with backoff.
3. **Silent failure:**
   - a tunnel that is up with a dead engine behind it: the probe;
   - a frozen path that still answers `CONNECT`: the streamed-headers deadline and the probe;
   - a proxy that the plugin accepted but frps never registered, or a lost `CloseProxy`: the condition follows frps's list, read every 10 s;
   - a list that cannot be read: the condition says Unknown, never True;
   - frpc that does not verify the front's certificate: the agent refuses to start it without the certificate and its pin;
   - frps listening off loopback: the launcher's check of its ports;
   - a record left after its allocation ended: the inventory compared with frps's list and the provider's job list.
4. **Provider limits:** outbound 443 per partition (S1's step 0); in ssh mode, port 22 to the control-plane host and its sshd; loopback shared with other users' jobs on a node; lease ends, which end the tunnel.
5. **Capacity:** S1c's figures, from one tunnel at up to 256 streams on loopback.
   - **Front CPU:** 7.9–8.7% of a core per 1,000 tokens/s over wss (frps plus the TLS terminator), 6.6–6.9% over frp's plain tcp, and 7.5–7.8% for the ssh path (frps plus sshd). The cost per token was higher at 64 streams (8.8–16%).
   - **Per front core:** about 580–630 wss streams at 20 tokens/s (11,500–12,600 tokens/s), or 720–760 over tcp. This is extrapolated linearly from 36–39% of a core at 256 streams; the front near full CPU was not measured.
   - **Memory:** about 50 MB for frps at 256 streams, and 70 MB for frpc on the instance side.
   - **The link is the more likely limit:** about payload bytes/s ÷ (300 × tokens/s per stream) streams, since each 222-byte token chunk took 290–335 bytes of TCP payload. That is about 85 streams of 20 tokens/s at 512 kB/s.
   - **Overload:** every stream on the tunnel slows evenly, and time to first token grows to seconds; in S1c no stream failed.
   - **Not measured:** many tunnels on one front (open question 11), which blocks beta.
6. **Idle cost:**
   - per Instance: one frpc process, one `wss` connection, a yamux ping and an application heartbeat every 10 s, and one probe every 10 s;
   - per Allocation in ssh mode: two ssh connections with their keepalives;
   - on the control-plane host: frps and the plugin endpoint;
   - no grant hours beyond the Allocation's own, and nothing while the job waits in the queue.
7. **Rollback:** stop frps, or stop the Instances' frpc. Instances keep running but take no traffic until a path returns, and streams in flight are cut. frps keeps no stored state, so nothing needs cleaning beyond the launcher's record.

## Drawbacks

- Every token makes an extra hop through the front's host.
- The front must be reachable from every center's compute network.
- All streams of an Instance share one connection, so a large reply delays the others (Risks).
- frps's admission depends on the lifecycle controller's process (Risks: the `Ping` gap).

## Alternatives

- **Login-node relays everywhere.** They work without outbound, but they are subject to login throttles and login-node process policies, and each relay is one more process to own.
- **Port forwards from the front.** Simple for one instance. In bulk they became the unowned tunnels listed in Motivation.
- **Instances listening on cluster interfaces.** Reachable only inside the cluster, and exposed to every user on it.
- **A center-provided ingress.** Where a center offers one, it may be the best path. Whether any target center does is not known yet.
- **frp's plain tcp transport to a public port.** It is 15–30% cheaper on the front's CPU (S1c), but it is not HTTPS on 443, which is what compute nodes were measured to reach.
- **frp's Go packages inside our binaries.** Pinned release binaries keep frp's code unmodified and its version visible in one place (HBEP-15).
- **`tcpmuxPassthrough`.** It would pass the `CONNECT` to the Instance, so the 200 would come from the far end. The Instance side would then have to answer `CONNECT` itself, and the setting applies to every proxy. Not needed for alpha, given the streamed-headers deadline and the probe.
- **wstunnel** ([erebe/wstunnel](https://github.com/erebe/wstunnel)), a tunnel over websockets. frp's `wss` covers HTTPS, and frp has the server plugin that admission needs.

## Prior art and reuse

Dial home replaces www-2027's central relays and forward watchdogs, which kept hand-made ssh forwards alive, probed each path end to end, and kept a flapping path out of pools.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| frp v0.71.1 (frps, frpc) | [fatedier/frp](https://github.com/fatedier/frp), release `v0.71.1` | Apache-2.0 (its `LICENSE`, read 2026-10-31) | Dependency: two pinned release binaries, recorded in `build/dependencies.yaml` (HBEP-15). frps runs on the control-plane host, started by the launcher; frpc runs in each job, staged by the Provider's controller. Its server plugin interface admits each tunnel. frp's Go packages are not imported |
| vLLM | [vllm-project/vllm](https://github.com/vllm-project/vllm) v0.32.0 | Apache-2.0 | Dependency (HBEP-9): the engine behind the tunnel; its unguarded `/health` is the probe's target |
| Headscale + tsnet | [juanfont/headscale](https://github.com/juanfont/headscale); [tsnet](https://pkg.go.dev/tailscale.com/tsnet) | BSD-3-Clause (both) | Optional, not in alpha: a front behind NAT without root (open question 8) |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| An SSH relay behind a forced-command key, with measured overhead | GWDG SAIA ([gwdg/saia-hpc](https://github.com/gwdg/saia-hpc)) | The restricted key of ssh mode; open question 3 |
| Dial home from jobs over outbound-only connections | OpenTela with libp2p ([eth-easl/opentela](https://github.com/eth-easl/opentela)); interLink's mesh ([interlink-hq/interLink](https://github.com/interlink-hq/interLink)) | Dial home, and its alternatives |
| frp's tunnel through a TLS terminator on 443, its heartbeat settings, capacity and head-of-line figures | spike S1 (issue #18) and spike S1c (PR #78), this project | Open questions 1 and 6, Heartbeats, Risks, Operational readiness 5 |
| An end-to-end probe that keeps a flapping path out of the pool and lets it back in | www-2027's forward watchdogs | The front's probe |

### Field lessons answered

Spike S1 (step 0 and probe 2, issue #18) and spike S1c (PR #78) measured the facts behind several answers below.

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| Compute-node listeners were reachable only through login nodes. | L-7.1 | Instances dial home on 443 or through ssh; nobody opens a port. |
| Hand-made tunnels and watchdogs multiplied, with clashing names and pid files. | L-7.2 | One tunnel per Instance, owned by it, named by its UID, and ending with it. |
| Parallel logins tripped a login node's connection limit. | L-7.3 | No login-node connections in alpha: jobs dial the control-plane host. |
| Pollers that each opened a new ssh connection filled a host's sshd. | L-7.4 | Two long-lived forwards per Allocation in ssh mode, never one per request. |
| Loopback ports on a shared host were live service tunnels. | L-7.5 | Every loopback endpoint checks a credential or a key, and frps admits only through the plugin. |
| Fixed and job-ID-derived ports collided. | L-2.4 | The forwards' local ports come from the kernel; the engine uses a Unix socket. |
| Loopback relay ports clashed between jobs on a shared node. | L-7.7 | No serving port on the node; the forwards' ports are kernel-chosen and retried. |
| Workers had to listen on all interfaces without a key to join a pool. | L-4.9 | Engine listeners bind a Unix socket; only the tunnel carries outside traffic. |
| A job that hit its time limit left its endpoint file behind. | L-10.10 | Path state comes from frps's live list, never from a file. |
| A multi-homed node changed address while the old one still answered. | L-10.12 | Nothing routes by node address; frps routes by the Instance's UID. |
| A follow-on on a new node changed the URL. | L-1.22 | The front reaches every Instance by its UID. |
| The route URL was part of the identity. | L-8.5 | The transport is keyed by the Instance's UID, not by an address. |
| Addresses changed with every session restart. | L-14.1 | frpc and the agent reach the front by DNS name. |
| A balancer ejected backends for good. | L-4.14 | Two failed probes make an Instance ineligible; one pass brings it back. |
| Checks that cannot see what they check passed while wrong. | L-10.6 | The probe goes through the tunnel to the engine; frpc never runs without its certificate; the condition is Unknown when frps's list cannot be read. |
| Keyword sweeps missed many tunnels at the end. | L-15.2 | frps is in the launcher's record, and every tunnel is in frps's list. |
| A scheduled respawner revived killed relays. | L-15.1 | No respawners outside the controller and the launcher. |
| Kill by pattern hit the wrong processes. | L-11.2 | frps is stopped by its recorded process ID plus start time. |
| Recorded process IDs pointed at the wrong process. | L-11.3 | The start time is checked together with the process ID. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #6.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation; spike S5 facts carried in (HBEP-4, HBEP-6, HBEP-7 only).
- 2026-10-30: spike S1 step 0 (issue #18) carried in: outbound 443 from compute nodes.
- 2026-10-30: open question 8 decided for alpha by HBEP-11: the front is reached directly or through ssh on port 22.
- 2026-10-30: the allocation agent (HBEP-7) starts the tunnel agent and owns the Allocation's control connection; HBEP-7's alpha credential noted under open question 7.
- 2026-10-30: the agent endpoint listens on loopback, reached through the ssh forward or through the front's `/agent/v1/` pass-through (HBEP-7).
- 2026-10-30: the risk of vLLM's internal listeners on the node's address, from spike S2 phase B, noted with HBEP-5.
- 2026-10-31: implementable. Spike S1c (PR #78) and frp v0.71.1's source carried in. frp over `wss` with `tcpmux` routing by UID, admission by an frps server plugin, path state from frps's list, the front's deadlines and probe, S1's heartbeats, two ssh forwards in ssh mode with the owner's key restriction, and the owner's word on center consent. Every open question decided, not needed for alpha, or open for beta. HBEP-4, HBEP-5, HBEP-7 and HBEP-11 amended with it.

## Amendments

None.
