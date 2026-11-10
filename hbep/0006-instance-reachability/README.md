# HBEP-6: Instance reachability

## Summary

On batch providers, compute nodes are reachable only through login nodes, and on some clusters not at all. This HBEP decides how the front (HBEP-5) reaches an instance: the instance dials home. For alpha the tunnel is frp v0.71.1. Each Instance's frpc connects out over `wss` to the front's dial-home listener, which passes it to frps on the control-plane host. frps routes the front's requests by the Instance's UID. A server plugin in the lifecycle controller admits each tunnel with the Allocation's credential. The HBEP sets the wiring of the three dial-home modes (in host-dialed mode the control-plane host opens the connections, through the login node, for a host that compute nodes cannot reach), the heartbeats, the front's deadlines and end-to-end probe, path state and teardown. Port forwards are not used in alpha, and login-node relays only in host-dialed mode. Every path has one owner, a liveness signal and a defined teardown.

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
- Teardown leaves nothing behind. Every process the framework starts outside an allocation is in the inventory, and is removed by record, never by pattern. Host-dialed relays are in the controller's own records in alpha, and in the relay keeper's in beta, and stopped by PID.
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
- An operator or agent lists every path, with its owner, instance, state and last start and close times: the Instances' tunnel conditions, plus frps's list for anything that has no Instance.

### Notes, constraints and caveats

- **Compute nodes connect out on port 443.** Spike S1's step 0 and probe 2 (issue #18, 30 Oct 2026) measured it.
  - Short jobs on Helios CPU, Helios GH200 and Athena A100 all reached public HTTPS endpoints directly, with no proxy configured.
  - The development VM sits behind a NAT, and from those nodes only port 22 answered at its address. That sshd turned out to be a gateway's, not the VM's: its host key differs (issue #18, 31 Oct 2026; L-17.7). The tunnel still runs inside ssh, with a restricted key (ssh mode). The route is a port forward on the gateway, so the clusters reach the VM's sshd at the gateway's address and port (owner decision, 31 Oct 2026, [issue #18, comment 6546958511](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/18#issuecomment-6546958511)). So ssh mode runs over the configured ssh port, 22 by default.
  - Sano was not measured. It is not an alpha platform (HBEP-4).
  - Where dial home works stays a per-partition fact in the platform profile (HBEP-4).
- **The front needs an address that compute nodes reach** (HBEP-11, open question 8): directly on the dial-home port (direct mode), or through ssh on the configured ssh port, 22 by default, with a restricted key (ssh mode). Or it needs none: in host-dialed mode the control-plane host dials each job through the login node, and the job's connections run back over it (Dial-home modes; science-agent decision, 9 Nov 2026, on the owner's "we have to solve this, you should wait for nothing. just dial whatever you want from this host").
- **What the control-plane host reaches** (probes of 9 Nov 2026, from the alpha host, Helios's CPU nodes and Athena's A100 nodes, #145): the login nodes over ssh, and through them its own job, with `srun --overlap` steps whose standard input and output cross both ways. It can't open ssh on a compute node: after a public-key partial success, the node's sshd asks for a Kerberos ticket ("Permission denied (gssapi-keyex,gssapi-with-mic)"). A reverse forward on a login node binds its loopback only, so compute nodes can't reach it.
- **The center consents** to outbound tunnels from compute nodes (owner, 31 Oct 2026; open question 9).
- **Inside the instance,** the engine listens on a Unix socket in the job's private temporary directory (HBEP-7). frpc reaches it through that socket, with the Instance's own key on the serving path (HBEP-9). That key is not the Allocation's credential, which admits the tunnel. A key covers only part of the engine's HTTP API (Risks).
- **All tokens of every reply cross the path.** Its throughput and latency under many concurrent streams are part of serving capacity (Operational readiness 5).
- **Spike S1c (PR #78) is the main evidence** for the tunnel's numbers. It ran both ends on one VM, on loopback, behind a link emulator, at head `2f55545`. Its figures carry their conditions below.

### Risks and mitigations

- **Outbound is blocked or proxied.** It is a profile fact, measured per partition. Every alpha partition dials out (S1's step 0). A relay is built only for host-dialed mode, where nothing reaches the control-plane host (open question 3).
- **Streams on one tunnel block each other (head-of-line blocking).** frp multiplexes every stream of a tunnel over one connection (yamux, with up to 6 MiB of send credit per stream). In S1c this happened in every valid run, on every transport: on a 1 MB/s link, one 4 MiB reply held every other stream's tokens back for 4.47–4.54 s, and one slowly read reply for up to 7.4 s. With separate connections behind the same shared cap the delay stayed under 0.15 s. Mitigations:
  - the agent's control traffic has its own connection, never the token tunnel (Dial-home modes);
  - the tunnel carries every request HBEP-5 routes to an Instance: its listed endpoints and its passthrough of every other JSON `POST` under `/v1/` (HBEP-5, Contract), so any large non-streamed reply can stall the streams of its Instance. An Instance serves one model, and vLLM v0.32.0 serves `/v1/embeddings` only for a pooling model and the chat and completions routes only for a generative one: a server's tasks come from its one runner type (`vllm/v1/worker/gpu_model_runner.py:3158-3164`), the generation routes need `generate` (`vllm/entrypoints/launchers/api_server/routers.py:49-54`), and the embeddings route needs `embed` (`vllm/entrypoints/pooling/factories.py:132-135`; `pooling/embed/api_router.py:28-29`). So embeddings and token streams never share an Instance's tunnel. The risk that remains is any large non-streamed reply of a generative model, from a listed endpoint or the passthrough, which can stall its Instance's streams for seconds on a slow path. The path rate is measured in S1's one-hour runs (Graduation criteria);
  - after alpha, a bulk endpoint gets its own frpc proxy with `transport.bandwidthLimit`, or a separate frpc with `transport.tcpMux = false` (open question 12).
- **A short heartbeat timeout drops a healthy tunnel.** With a 6 s timeout, a slow reader queued more than 6 s of data in S1c, a heartbeat timed out, and all 16 streams were cut, in both passes. S1's set survived every run. Heartbeat timeouts are budgeted from the queueing delay (Heartbeats). yamux's ping timeout is fixed at 10 s, so on a slow path a queue of more than 10 s drops the tunnel at any setting.
- **The tunnel drops while the instance is healthy.** frpc and the ssh forwards reconnect on their own. Meanwhile the front holds the Instance as not eligible, not dead (HBEP-5), and the controller restarts nothing on that basis (HBEP-7). A host-dialed relay that ends for good is the exception: its Allocation is released (Dial-home modes, What ends a relay).
- **Requests hang in a dead tunnel before frps notices.** frps answers the front's `CONNECT` before it takes a work connection (frp facts), so a silent path still accepts requests. frps then closes each of them itself, after the front's write and before any response byte: a request that waits for a work connection at `userConnTimeout` (10 s), and the one that drew the spare work connection when the session is torn down (12–20 s at S1's settings, over 15 s at frp's defaults in S1c). The front returns that as 502 `upstream_closed` (HBEP-5, Errors), does not resend it, and its probe takes the Instance out of rotation (The front's half).
- **A lifecycle-controller outage blocks new logins.** The plugin handles `Login`, `NewProxy` and `CloseProxy` only, so frps consults it only when a session logs in or registers a proxy, and established tunnels survive a controller restart, except in host-dialed mode, where the relays are the controller's children (Dial-home modes). While the plugin is down, frps refuses every login and proxy (frp facts), so an frpc that reconnects meanwhile cannot log in. It keeps retrying (`loginFailExit = false`) and returns when the plugin is back. `Ping` is not subscribed (science-agent decision, 31 Oct 2026): frps would turn a failed `Ping` call into an error answer, on which frpc closes its session (frp facts), so a controller outage would cut every tunnel.
- **A slow plugin stalls a tunnel.** frps's plugin client has no timeout, and `NewProxy` runs on the session's read loop (frp facts). A plugin that waits holds that tunnel's control messages, and past 25 s frps drops it. So the plugin answers from in-memory state within 1 s, and refuses rather than waiting on the API server (Admission).
- **A false instance registers and receives prompts.** frps admits a tunnel only through the plugin: the login must carry a live Allocation's credential, and a proxy may claim only the UID of an Instance placed on that Allocation (Admission). The Instance takes traffic only after the controller's verification (HBEP-7, HBEP-9). One limit remains: frps attaches a work connection to a session by its run ID alone, and neither the plugin nor frps's empty token checks it (frp facts). frps draws run IDs at random. They appear in frps's log and the plugin's calls on the control-plane host, and frpc logs its own and prefixes every later line with it. Anyone who read that log could, in direct mode, attach a work connection through the public dial-home listener and receive requests, with the Instance's key in their headers. So frpc's log goes only to a file in the job's private temporary directory, never to the job's output files on shared storage (frpc in the job). A shared frps token with `auth.additionalScopes = ["NewWorkConns"]` would close this; it is not in alpha.
- **The tunnel's ping succeeds while the engine is dead.** The front's probe goes through the path to the engine.
- **An engine API key is taken for isolation.** A vLLM API key covers only part of the engine's HTTP API (vLLM's security documentation), and traffic between an engine's ranks is unauthenticated by default. So engine HTTP listeners bind to a Unix socket or loopback and are reached through frpc. A multi-node instance's rank traffic still crosses the cluster network unauthenticated. That is a known limitation, recorded in each platform profile (HBEP-4).
- **Engine internals listen off loopback.** In spike S2 phase B each vLLM worker had its API port on the IPv4 loopback address and six more TCP listeners on the node's cluster address, although `--host` and `VLLM_HOST_IP` were both set to that loopback address (HBEP-5, Risks). The serving path needs the Instance's key. The other listeners are engine internals, which no key covers, so an Instance on a shared cluster network exposes them unless the provider's network isolates it.
- **Beta: a relay-keeper restart cuts every host-dialed stream** on its host (Dial-home modes, The relay keeper), and so does the launcher's `upgrade`, which stops the whole supervisor group. Mitigation: the keeper is a small process with no network listener and a stable interface, so it restarts only with an upgrade or a crash, and the launcher restarts it at once. A controller-only restart that keeps the keeper is open for beta's upgrades.
- **Loopback is shared.** Other users' jobs on a shared compute node can reach the ssh forwards' local ports, and in host-dialed mode the agent's two listeners, and other local users on the control-plane host can reach frps and the plugin endpoint (L-7.5). The forwards lead only to endpoints that check the Allocation's credential or the Instance's key. The alpha host is single-user. Recorded as a known limit.
- **No end-to-end encryption from front to engine.** TLS ends at the front's dial-home listener; from frps to frpc's socket the bytes are plain inside the TLS hop (open question 2). Security is not the main concern for alpha (owner), so this is a recorded limit.
- **Paths outlive their owner.** frpc ends with its Instance and at the latest with the job. Owner references and garbage collection (HBEP-11) remove the front-side records.
- **Teardown kills the wrong process.** The launcher stops frps by its recorded process ID plus start time (HBEP-11). The agent stops frpc and its ssh forwards as its own children. No respawners outside the controller, the launcher and, in beta, the relay keeper, which starts relays only under a leader's unexpired set.
- **The front's address changes.** frpc and the agent reach the front by DNS name in direct mode, never by a stored address.

### Open questions

1. **Tunnel protocol.** **Decided** (science-agent decision, 31 Oct 2026): frp v0.71.1 (`fatedier/frp`), as two pinned release binaries, frps and frpc, with frpc's `wss` transport to the front's dial-home listener and `tcpmux` routing (`httpconnect`) by the Instance's UID. Source: spike S1c (PR #78). With no impairment, at 1 to 256 streams of 20 tokens/s, frp added 1–4 ms to the median time to first token over tcpmux, wss and ssh. At 256 streams wss cost the front 7.9–8.7% of a core per 1,000 tokens/s against 6.6–6.9% for frp's plain tcp (at 64 streams, 15–16% against 8.8–11.5%), and about 5% more payload, and it is ordinary HTTPS. frps v0.71.1 cannot serve wss itself, so the front's TLS terminator passes the websocket on (S1c's self-test; frp facts).
2. **Encryption.** **Decided for alpha** (science-agent decision, 31 Oct 2026, under the owner's "security is not the main concern"): TLS terminates at the front's dial-home listener, which frpc verifies against the launcher's certificate. In ssh mode that TLS runs inside ssh. Front to frps is loopback on the control-plane host. frpc reaches the engine through its Unix socket on the node, and the engine checks the Instance's key (HBEP-9). There is no end-to-end TLS from front to engine (Risks). Source: S1c's wss arm, where frpc verified the front's certificate and every request completed.
3. **Relay fallback form.** **Decided for alpha** (science-agent decision, 9 Nov 2026): host-dialed mode's relays, an `srun -u --overlap` step reached over ssh through the login node (Dial-home modes). It serves a control-plane host that compute nodes cannot reach. A partition without outbound needs nothing more, since host-dialed mode needs none. Source: the probe of 9 Nov 2026.
4. **Port forwards.** **Not needed for alpha:** an ssh port forward that an operator opens is never a routing path in alpha. A network forward on a gateway in front of the control-plane host, which ssh mode may cross, is not this (open question 8). Whether forwards ever become a supported path, with an owner and a teardown, stays the owner's decision after alpha.
5. **Multi-node instances.** **Not needed for alpha:** alpha Instances are single-node (HBEP-7, Provisioning).
6. **Heartbeats.** **Decided** (science-agent decision, 31 Oct 2026): S1's set, on both ends, as the tested starting point (Heartbeats). Source: S1c. frps noticed a silent path (a frozen path or a node crash) in 12–20 s, against 28–34 s at frp's defaults, and the set survived every head-of-line run. The tight set (2 s interval, 6 s timeout) noticed in 4–6 s but dropped a healthy tunnel. S1c ran on loopback, so the real path rate from compute node to front is unknown. It is measured in S1's one-hour runs, and is an alpha graduation criterion.
7. **Credential.** **Decided for alpha** (science-agent decision, 31 Oct 2026): HBEP-7's per-Allocation random 256-bit token, valid for the Allocation's life. frpc sends it in its login metadata, and the frps server plugin checks it (Admission). Source: frp v0.71.1's plugin interface (frp facts). **Not needed for alpha:** renewal over the tunnel, since the token lives no longer than its Allocation's lease.
8. **A front behind NAT,** as in a single-user setup. **Decided for alpha** by HBEP-11 (its open question 8): no rendezvous relay. The alpha host is reached directly on the dial-home port, or through ssh on the configured ssh port, 22 by default, with a restricted key. Source: S1's step 0, where the development VM's address, behind a NAT, answered the compute nodes on port 22 and nothing else. That sshd was a gateway's, as its host key showed (L-17.7). The route is a port forward on the gateway to the VM's sshd, reached at the gateway's address and port (owner decision, 31 Oct 2026, issue #18). **Amended, 9 Nov 2026:** the gateway's forward is not awaited. The alpha host runs in host-dialed mode (Dial-home modes; open question 3).
9. **Center consent.** **Decided** (owner, 31 Oct 2026): "The outbound tunnels are acceptable use, yes."
10. **Compute-node reachability.** **Decided for alpha's partitions** by S1's step 0 and probe 2 (issue #18): outbound 443 works on Helios CPU, Helios GH200 and Athena A100, and the home directory, which holds the ssh-mode key, is readable from their compute nodes (HBEP-4, open question 16). Inbound reachability is not needed, since Instances dial home. **Not needed for alpha:** Sano, which is not an alpha platform (HBEP-4).
11. **Many tunnels on one front.** **Partly answered: steady state only; the rest blocks beta** (spike S14, issue #313, PR #328; 10 Nov 2026). Setup: loopback through S1c's emulator with no impairment, on three 4-CPU hosted runners, with 1 to 300 frpc tunnels at 1 and 8 streams of 20 tokens/s, in direct and host-dialed mode.
   - **No steady-state tunnel-count knee through 300 tunnels under these conditions.** The measured stack's CPU follows `a × (tokens/s ÷ 1000) + b × tunnels + c`. The coefficients vary by runner: up to 44% on one CPU model in S14's first run, whose direct `a` reached 0.109 and whose data weren't kept. So the rates below carry that spread. From the 90 runs with committed data:
     - direct mode (frps plus the dial-home listener): a = 0.05–0.08, b = 0.0004–0.0006 (fitted), c = 0.016–0.022 cores. S1c's 7.9–8.7% at one tunnel agrees on the AMD runners; the Intel runner's `a` of 0.049 is lower.
     - host-dialed mode (plus the relay owner and its ssh processes, without ssh's encryption): a = 0.08–0.13, b = 0.0008–0.0016, c = 0.026–0.044.
     - The measured idle cost per tunnel is lower: 0.0002 cores in direct mode and 0.0003–0.0005 in host-dialed mode.
   - **Memory and descriptors are not limits at this scale.** At 300 tunnels × 8 streams, frps held about 250 MB and 2,700 descriptors (one per tunnel plus one per stream in flight), and the listener 60 MB and 2 descriptors per tunnel. Host-dialed mode adds 9 descriptors and two ssh processes per Allocation in the relays' host side: the controller in alpha, the relay keeper in beta.
   - **No tunnel dropped in steady state.** In the 90 runs with committed data, and in the first run's 90, whose data weren't kept, there was no frps drop, no frpc re-login, no relay restart and no failed probe, including runs where tokens queued for seconds on a saturated host. All 300 tunnels registered within 3 s of a fresh start.
   - **Latency bends with the host's CPU, not the tunnel count.** Tunnels add a few ms to the time to first token while the host has headroom; the tails grow once its CPUs near saturation.
   - **The steady-state per-front budget**, with `q = tokens/s ÷ 1000`, keeps `a × q + b × tunnels + c + R_front + R_ssh` under 70% of the cores left to these processes on the front's host (the alpha host also runs k3s and the controllers), both reserves growing with the rate:
     - R_front = s × a × q is the front's own proxy and control work, unmeasured: a chosen budgeting floor, with s = 0.45 in direct mode and 0.25 in host-dialed mode (the listener's measured share, rounded up);
     - R_ssh = 0.045 × q is ssh's encryption in host-dialed mode: a provisional assumption to validate, since S1c's lightly loaded sample gave about 0.14 × q and its cipher was not recorded.
     - That is an estimated 4,000–9,000 tokens/s per front core in direct mode at 100–300 tunnels, and about 3,200–3,900 at the first run's highest `a` (computed in this PR's review from S14's formula, not by S14). In host-dialed mode it's 1,000–4,000 (700–2,400 with R_ssh = 0.14 × q), measured with one Allocation per tunnel, more than HBEP-6's host-dialed capacity allows today. All of these are for 60-token streams with a new connection per request.
     - The link still bounds the same rate at payload bytes/s ÷ about 300 bytes per token (Operational readiness 5); in host-dialed mode ssh's framing adds to that.
     - **Open-files hard limits are per process,** each above its count plus a baseline:
       - frps needs one descriptor per tunnel plus one per stream in flight;
       - the front's process, which holds both the client listener and the dial-home listener, needs two per tunnel plus two per stream in flight, for the client's connection and the upstream one. That's about 5,400 at 300 tunnels × 8 streams, above a common 4,096;
       - the relays' host side needs 9 per host-dialed Allocation.
   - **Until the open items are measured, keep a front at or below 300 tunnels.** It's an operating limit; no code enforces it.
   - **Still open, and each could bound tunnels where steady state does not:**
     - a mass re-login of hundreds of frpc through the admission plugin after a front or frps restart;
     - many tunnels sharing a capped link, where heartbeats could starve while CPU is spare;
     - a dedicated front host near full CPU, since S14's runners saturated first;
     - ssh's encryption cost per relay;
     - the front's own proxy cost (R_front).
12. **Bulk endpoints off the token tunnel.** **Open (blocks beta):** embeddings, file transfers and large non-streamed replies get their own frpc proxy with `transport.bandwidthLimit`, or a separate frpc with `transport.tcpMux = false`. S1c tested neither.
13. **A relay keeper.** **Decided for beta** (science-agent decision, 10 Nov 2026): one `relaykeeper` process per control-plane host, supervised by the launcher beside frps. It holds every host-dialed relay, so a controller restart or hand-over cuts no serving stream (Dial-home modes, The relay keeper). Alpha keeps the relays in the controller's process, with the cuts the owner accepted ("Accept cuts for alpha", 9 Nov 2026).

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
| control-plane host | the plugin endpoint, in the lifecycle controller's process | admits logins and proxies, and triggers the reads of path state |
| job | the allocation agent | starts frpc, and in ssh mode its two ssh forwards; in host-dialed mode it holds the two channels' loopback listeners and their sessions |
| control-plane host | the relays (host-dialed mode): in alpha the batch Provider's controller's children; in beta the relay keeper's | two per running Allocation, one per channel; each dials only its channel's loopback port |
| control-plane host | the relay keeper (beta, host-dialed mode) | one per host, supervised by the launcher; holds every relay, takes its desired relays from the leading controllers, and reports how each ends |
| login node | an ssh session and an `srun` client per relay (host-dialed mode) | carry the relay's bytes; they compute nothing (HBEP-4, open question 14) |
| job | `relay <channel>`, the agent binary in an `srun --overlap` step (host-dialed mode) | hands the step's standard input and output to the agent |
| job | frpc | dials home over `wss` and serves one `tcpmux` proxy, named by the Instance's UID, from the engine's Unix socket |

**Loopback ports on the control-plane host,** by offset from the launcher's port base. `docs/development.md` (host layout) assigns base to base+7: the API server, its HTTPS port and the controller manager at base to base+2, the agent endpoint at base+3, the front's `/healthz` at base+4, the load balancer's reserved base+5, the lifecycle controller's `/healthz` at base+6 and the dial-home listener at base+7. The tunnel's ports take the next free offsets. Those marked "proposed" are proposals for the code agent, who owns `pkg/agentapi` and `internal/launcher`. With them the block grows to base+11, and `PortsFromBase`'s range check grows with it.

| Offset | Listener | Status |
|---|---|---|
| base+3 | the agent endpoint (HBEP-7) | `agentapi.AgentEndpointPortOffset`, on `main` |
| base+7 | the dial-home listener's default in ssh mode (HBEP-5) | `agentapi.DialHomePortOffset`, on `main` |
| base+8 | the frps plugin endpoint, plain HTTP | proposed |
| base+9 | frps's bind port | proposed |
| base+10 | frps's tcpmux `CONNECT` port | proposed |
| base+11 | frps's web server, for its API | proposed |

The plugin endpoint has its own port. It is not on base+3, because the restricted key opens base+3 to every job (Dial-home modes), and a job could then send the plugin forged calls.

### frps on the control-plane host

The launcher verifies frps's checksum against `internal/launcher/frp-pins.txt` (HBEP-15), writes its configuration file (mode 600, in the launcher's state directory), starts it, records it by process ID plus start time, and restarts it when it exits, as it does for k3s (HBEP-11).

```toml
bindAddr = "127.0.0.1"
bindPort = <base+9>
proxyBindAddr = "127.0.0.1"
tcpmuxHTTPConnectPort = <base+10>
webServer.addr = "127.0.0.1"
webServer.port = <base+11>
webServer.user = "hpcburst"
webServer.password = "<from the web password file>"
log.to = "<the launcher's state directory>/frps.log"
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatTimeout = 25
transport.tcpKeepalive = 30

[[httpPlugins]]
name = "hpcburst"
addr = "127.0.0.1:<base+8>"
path = "/frp/v1/handler"
ops = ["Login", "NewProxy", "CloseProxy"]
```

- **Loopback only.** frp's `bindAddr` defaults to every interface, and `proxyBindAddr` follows it, so both are set. The web server defaults to loopback. Every other listener (KCP, QUIC, HTTP and HTTPS vhosts, the ssh tunnel gateway) stays off at its default of port 0. The launcher's health check requires frps to hold exactly its three loopback ports, as it does for k3s (HBEP-11).
- **The web server** runs because frps keeps its proxy list only while it runs (frp facts). Its password is random, generated by the launcher into a file that both the launcher, writing frps's configuration, and the lifecycle controller, reading the list, read: a proposed `agentapi.Host` path such as `<state>/frps/web-password`, mode 600. It also serves frp's dashboard pages; nothing uses them, and they sit behind the password on loopback. Its unauthenticated `/healthz` is the launcher's liveness check.
- **Auth.** frps keeps frp's default `token` method with no token, and frpc sends none. frps accepts that, so its own check passes every login, and the plugin is the gate (Admission).
- **Everything else** stays at frp v0.71.1's defaults: `userConnTimeout` 10 s, `tcpmuxPassthrough` off.
- **Logs** go to a file in the launcher's state directory (`log.to`). frps logs proxy names (Instance UIDs) and run IDs, never the login metadata.

### frpc in the job

The Provider's controller stages frpc for the partition's CPU architecture under `stagingDir`, by its own digest, at `frpc/<digest>/frpc` (HBEP-4), together with the launcher's certificate, which is public (HBEP-4, Submission). The agent checks frpc's SHA-256 before it runs it. It writes frpc's configuration into the job's private temporary directory, mode 600, because it holds the credential. frpc reads it by path, so the credential is never on a command line. The agent starts frpc with `http_proxy` unset, because frp otherwise takes it as its proxy to frps (frp facts).

```toml
serverAddr = "<direct: the front's DNS name; ssh and host-dialed: 127.0.0.1>"
serverPort = <direct: the dial-home port; ssh: connection 2's local port; host-dialed: the agent's serving listener>
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
- **The server name** is a fixed synthetic name in the launcher's certificate (HBEP-11). `dial-home.hpcburst.invalid` is a proposal for the code agent; the `.invalid` top-level domain never resolves. It is the same in every mode, because in ssh and host-dialed modes frpc dials loopback.
- **`loginFailExit = false`.** frp's default makes frpc exit when its first login fails. With `false` it keeps retrying, like the ssh forwards. frpc is part of the agent's transport, not a part of the Instance: the agent restarts it with backoff if it exits, without asking the controller, as it keeps its ssh forwards (HBEP-7). A tunnel down is not a dead Instance.
- **`user` is not set,** so frps sees the proxy name unchanged (frp facts).
- **Logs.** frpc logs to a file in the job's private temporary directory (`log.to`), and its console output is discarded. frpc logs its run ID, which is enough to attach a work connection to its session (Risks), so the log never reaches the job's output files on shared storage.
- **The proxy** reaches the engine through frp's `unix_domain_socket` client plugin. A `tcpmux` proxy hands each work connection to its plugin, so no TCP listener serves the Instance on the node (frp facts). In beta, a router that listens on loopback (HBEP-5) is reached through `localIP` and `localPort` instead.
- **Heartbeats** are S1's set (Heartbeats). `poolCount = 0` is S1's tested value; frps still keeps one spare work connection (frp facts).

### Dial-home modes

**Direct mode.** frpc's `wss` connection goes to the front's dial-home listener, on the address compute nodes reach. The agent's control traffic uses its own HTTPS connection to `/agent/v1/` on the same listener, pinned as before (HBEP-7). frpc's connection carries only the token tunnel.

**Ssh mode.** The job runs two `ssh -N -L` connections over the configured ssh port, 22 by default, with the same restricted key, each in its own reconnect loop. The key's restriction is on the control-plane host's side, so it does not change with the port.
- **Connection 1** forwards to the agent endpoint's loopback port (base+3), for the agent's control traffic.
- **Connection 2** forwards to the dial-home listener's loopback port (base+7), for frpc's `wss`.
- **Local ends.** Each forward listens on the node's IPv4 loopback address, on a port the agent asks the kernel for once, at its start, never a fixed or derived port (L-2.4, L-7.7). The agent keeps that port for the Allocation's life, and each reconnect binds it again. The agent runs ssh with `ExitOnForwardFailure=yes`, so a failed bind shows at once. It also sets `ServerAliveInterval=10` and `ServerAliveCountMax=3`, and the pinned host key (HBEP-7).
- **frpc starts only once connection 2's forward listens.**
- **A port that cannot be bound again.** If another process took a forward's port, the agent takes a new one from the kernel and updates the forward's user. For connection 1 it points its control client at the new port. For connection 2 it rewrites frpc's configuration with the new `serverPort` and restarts frpc, since frpc keeps retrying the port it was started with.
- **Dead nodes.** A node that crashes sends no FIN, so its forwards' sshd sessions on the control-plane host stay until sshd notices. The key's options cannot set that, so the control-plane host's sshd sets `ClientAliveInterval 10` and `ClientAliveCountMax 3`, an operator setting. Otherwise the sessions linger until TCP keepalive ends them, a known limit; they carry nothing once frps has dropped the proxy.
- **The key's line** in `authorized_keys` on the control-plane host (owner decision, 31 Oct 2026), one key per cluster, used by S1's one-hour runs and by alpha:

  ```
  restrict,port-forwarding,permitopen="127.0.0.1:<agent endpoint port>",permitopen="127.0.0.1:<dial-home listener port>",permitlisten="127.0.0.1:1",command="/bin/false"
  ```

  The two ports are base+3 and base+7, so they are fixed for a given launcher base. A custom base needs new key lines.

**Host-dialed mode** (science-agent decision, 9 Nov 2026, on the owner's "we have to solve this, you should wait for nothing. just dial whatever you want from this host"). It's for a control-plane host that compute nodes cannot reach. The host dials each job through the login node, and the job's two connections run back over what the host opened. frp, the dial-home listener and its TLS, the server plugin and the agent's control protocol are unchanged.

The evidence is two probes from the alpha host on 9 Nov 2026 ([#145, comment 6747458195](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/145#issuecomment-6747458195), and the second probe's on the same issue, at 18:11–18:22 UTC):
- an `srun -u --overlap` step reached over ssh through the login node carries bytes both ways. All 256 byte values arrived intact each way on Athena's A100 nodes.
- **Throughput:** 48–85 MB/s each way, with a 2.3–2.5 ms median round trip. Those figures are from Helios's CPU nodes and Athena's A100 nodes. Helios's GH200 partition was down that day, so it's unmeasured, and it's an alpha precondition there (HBEP-4, open question 16).
- **A Unix socket that the batch step created** is reachable from an `--overlap` step under `/dev/shm`, under the batch step's `$TMPDIR` (the step's is the same), and under `$HOME`, on Athena.
- **No preamble:** nothing appeared on standard output before the bridge's own first line.

- **The mode's value** is `host-dialed`: a launcher setting, per host in alpha, written into `agent.json`'s `mode` key, as for direct and ssh. In this mode `agent.json` carries no `endpoint` and no `ssh`; the certificate pin and the server name are as today. The dial-home listener binds loopback at base+7, as in ssh mode (HBEP-5), and no address that compute nodes reach is needed (HBEP-11, open question 8).
- **Two relays per running Allocation,** one per channel, so control and the token tunnel still never share a connection (Risks):
  - `control` ends at the agent endpoint's loopback port (base+3);
  - `serving` ends at the dial-home listener's (base+7).
- **A relay's host side,** in alpha, is an `ssh` child of the process that leads `lifecycle`, where the batch Provider's controller runs (HBEP-11). The controller runs the relay's session end itself. In beta the relay keeper holds both instead (The relay keeper, below); the rest of this section applies to either, except where it names the controller.
  - **The ssh process** is its own connection, never the Provider's shared master. It uses the Provider's private `-F` configuration with its hardening (HBEP-4, Transport), plus `ControlMaster=no`, `ControlPath=none`, `-T`, `ServerAliveInterval=10` and `ServerAliveCountMax=3`. It's started from a thread that stays locked for good, with `Pdeathsig=SIGKILL` (as for the fake Slurm's processes, #274), so it dies with the controller process. It's stopped by PID.
  - **It runs** `srun -u --overlap --jobid=<job> --ntasks=1 <the job's agent binary> relay <channel> --dir <Allocation directory> --allocation <UID> --epoch <E> --seq <N>`.
    - `-u` is required: without it `srun` holds output that ends without a newline until the stream closes, and with it the task's stdout is a pty with `OPOST` cleared (Slurm 24.11.5 and 23.11.7, `src/slurmd/slurmstepd/io.c`), which carried all 256 byte values unchanged.
    - Alpha's Instances are single-node, so the step lands on the job's one node.
  - **Before each start,** the Provider's latest queue listing (one per poll interval, HBEP-4) must show the recorded job ID RUNNING, with its name equal to the Allocation's tag; otherwise the relay waits for the next listing (HBEP-4's absence grace), starting no `srun`. An `srun` refusal that the job doesn't exist or is completing ends the relay for good.
- **The wire:** `relay` first writes one preamble line, `hpcburst-relay/1 <Allocation UID>`. After it comes one multiplexed session: `fatedier/yamux` v0.2.0, the same version frp v0.71.1 pins, with a 10 s keepalive and a 10 s write timeout.
  - The host side requires the preamble within 30 s of the start. Anything else first is refused as named output ("unexpected output before the preamble"), counted and retried.
  - The host side dials only its channel's loopback port for each stream the job side opens, so a job can't reach any other port, the plugin endpoint included.
  - TLS stays end to end: frpc's `wss` to the dial-home listener, and the agent's HTTPS to `/agent/v1/`. Inside the serving relay, frpc's own single multiplexed connection is one stream.
- **The job side:**
  - **The agent's listeners:** the agent binds two loopback listeners at its start, one per channel, on ports it asks the kernel for, and keeps them for the Allocation's life. Nothing is handed between processes. frpc's `serverPort` is the serving listener's port, and the control client uses the control listener; neither changes when a relay restarts.
  - **The rendezvous:** the agent creates a Unix socket in a directory with a random name (never a predictable path) and mode 0700, under its `$TMPDIR` or `/dev/shm`. The socket's absolute path, at most 107 bytes, goes into `<Allocation directory>/relay-socket`: mode 600, written to a temporary file, then renamed. `relay` reads that file, connects, and checks that the agent's greeting names its `--allocation`. The agent accepts only a peer of its own user (`SO_PEERCRED`). A missing file, or a socket nobody answers, ends that relay as "agent not ready", which is retried.
  - **A connection accepted while its channel has no session** waits up to 30 s for one, then is closed. The control client's TLS handshake gives up after 10 s anyway. frpc and the control client retry, as they do today.
  - **Fencing:** a relay carries the epoch `E`, the `leaseTransitions` of the `provider-<name>` `Lease` when its controller took the lead, and a per-channel sequence `N`.
    - The agent keeps the session with the highest `(E, N)` on each channel, and refuses a lower one with the reason `superseded`.
    - A controller whose relay ends `superseded` doesn't start it again.
    - A copy that loses the lead stops all its relays by PID before it releases the `Lease`.

    So an unfenced hand-over (HBEP-11) can't make two copies take turns cutting each other. In beta the keeper issues `E` and `N`, and a hand-over replaces no relay (The relay keeper).
  - **`relay` exits** when its standard input closes or its socket closes.
- **The control-plane side:**
  - **When relays start and stop:** the controller (in beta, the keeper, on the controller's desired set) starts both relays once the Allocation's job runs. It keeps them level-triggered: a dropped relay is started again, and a controller that takes the lead starts the relays of every running Allocation. It stops them by PID when the job ends or the Allocation is deleted. `srun` ends a step with its job, so no relay outlives the job.
  - **Capacity:** in host-dialed mode a Provider has at most 2 Allocations, pending or running. That's a submit limit (`SubmitLimitReached`, HBEP-4), so the planner counts it, and the handover pair fits. The Provider's controller counts again, from a fresh read, just before it submits, and refuses a third with `SubmitLimitReached`; the race that remains is two Provider leaders in an unfenced hand-over (HBEP-11's accepted overlap), whose extra Allocation is brief and the planner's to release as surplus.
    - Each running Allocation's pair of relays is reserved together and held through a relay's backoff. So the 4 relays per login node that the probe held without refusal are never exceeded by one copy, and no Allocation is left with half a pair.
    - **Only one Provider per login host runs in host-dialed mode.** The login host is its `HostName` as `ssh -G` resolves it through the Provider's own `-F` file. The Provider's controller refuses a second Provider on that host: it submits nothing, and sets the Provider's `Ready` condition to False with reason `LoginHostInUse`, naming the login host and the Provider holding it. The planner then treats that Provider as unusable (`ProviderUnavailable`, HBEP-7), so no Allocation waits on it. A per-platform budget stays HBEP-4's open question 17.
  - **What ends a relay:**
    - **Retried**, with a backoff of 1 s doubling to 30 s with jitter (first starts jittered too, since all relays start together after a restart), reset once a relay has stayed up for 60 s:
      - ssh dropping, or the login node killing it;
      - a missing preamble or unexpected output;
      - "agent not ready".
    - **Final, with no restart:**
      - `superseded`;
      - the job listed as ended or completing, or `srun`'s refusal that it doesn't exist or is completing;
      - `srun`'s step-limit refusal;
      - more than 120 restarts of one channel within an hour, counting only restarts whose session came up and then dropped. Failures before a session comes up (the login node down for maintenance, ssh refused, "agent not ready", a missing preamble) are never capped: they keep retrying at the 30 s backoff cap, bounded by the step count below and by the job's end. A final end stays final.

      A final end other than `superseded` or the job's end raises a Warning event, reason `RelayFailed`, regarding the Allocation, naming the channel and the cause (untyped, like every Provider-controller event in alpha; HBEP-8, open question 14). The code adds the reason to HBEP-8's table, since a new reason isn't an amendment. Before it raises the event, the Provider's controller writes the Allocation's condition `RelayEndedForGood` True, reason `RelayFailed`, with the event's message (#304). The rules for that mark:
      - **When it's written:** only on an Allocation that still runs the relay's job (the same UID and job, `Running`, not completing and not being deleted), and only while this copy leads. A copy that loses the lead during the write raises no event.
      - **It's never cleared.**
      - **When the write fails,** the same process's next observation of the running Allocation sets it. In alpha a controller restart or hand-over before then forgets the failure (in beta the keeper keeps it, The relay keeper), and the new leader starts the relays again, so a lasting cause ends them for good once more.
      - **What follows:** the lifecycle controller ends the Allocation's Instances with reason `RelayEndedForGood` and a `failing` event, and releases the Allocation, whatever its retention policy (HBEP-7, step 5 and Retention). So its job and GPU are freed and the planner replaces the Instance. The release counts toward the class's `RestartLoop`, so a cause that keeps recurring pauses provisioning.
    - **Steps:** at the 30 s cap two relays make at most about 240 `srun` steps an hour, about 11,500 in 48 h, below the 40,000 `MaxStepCount` that both profiles record.
  - **A path that fails never restarts a healthy engine.** HBEP-7's rules apply, with one exception below:
    - an Instance first becomes `ready` only with `TunnelConnected` True, from frps;
    - relay restarts don't reset its start deadline;
    - a `ready` Instance whose path is down takes no new requests (the front's probe), and `AgentUnreachable` follows its own timeout.

    The exception is a relay that ends for good, other than `superseded` or by its job's end: one channel never returns, so the Instance can't serve, and its Allocation is released (What ends a relay). Retried ends and `superseded` restart no engine.
- **What a drop costs:** the streams on that session reset. A token stream in flight is cut and counted as cut, and a request already written is never replayed (HBEP-5). frpc logs in again once the relay is back.
- **A controller restart, in alpha, cuts the relays** (owner decision, 9 Nov 2026: "Accept cuts for alpha"). The relays are children of the controller process, so a restart cuts every stream in flight, counted as cuts and never replayed, and the new leader starts both relays of every running Allocation within the backoff.
- **The relay keeper (beta)** (open question 13). One process per control-plane host, `relaykeeper`, holds every host-dialed relay of that host, so the relays outlive a controller restart or a hand-over. Every controller copy that drives host-dialed relays runs on that host.
  - **Names** (science-agent ruling, 10 Nov 2026, on the code agent's plan for #343): the keeper is the `relaykeeper` subcommand of the controller binary (`hpcburst-controller relaykeeper`), so the launcher resolves and verifies one binary and both ends speak one protocol version. It's opted into with `hpcburst up --relay-keeper`, valid only with `--front-mode host-dialed`, and recorded in the launcher's state.
  - **Supervised like frps:** the launcher starts it under the controllers' supervisor, as a sibling of the controller processes and not a child, records it by process ID and start time, and `down` stops it by that record (HBEP-11). Its relays' `ssh` processes are its own children, started from a thread locked for good with `Pdeathsig=SIGKILL`, so they die with the keeper and never with a controller.
  - **What a controller restart then leaves standing:**
    - each relay's `ssh` process and its yamux session, held by the keeper;
    - every **serving** stream, whose host end is the front's dial-home listener at base+7, and which goes through frps; neither restarts with the controller.
    - **What still ends:** the **control** streams, whose host end is the agent endpoint at base+3, in the controller's process. A refused dial closes only that stream, never the session, and the agent re-registers on its next poll with the same `agentInstanceID` (HBEP-7, registration is soft state).
    - **The limit:** this covers a controller that exits or loses its lead. The launcher's `upgrade` stops the whole supervisor group, keeper included, so a planned upgrade still cuts every host-dialed stream (Risks).
  - **The keeper's state file** lives in the launcher's state directory, mode 600, and `down` keeps it. It holds:
    - the keeper's epoch base;
    - each Provider's highest accepted `Lease` epoch;
    - the final ends, kept after they're decided until their Allocation leaves a complete set, so a restart can't revive a channel;
    - the login-host claims (below), which survive a keeper restart.

    Every change is written and synced before it takes effect. The launcher's `restore` clears the accepted `Lease` epochs, since a restored store rolls `leaseTransitions` back.
  - **Fencing moves to the keeper.**
    - **The keeper issues each relay's `E` and `N`.** `E` is 2³¹ plus the keeper's start count, written and synced before any relay of that run starts. A Kubernetes `Lease`'s `leaseTransitions` is an `int32` and the job side's epoch an `int64`, so every keeper relay outranks every alpha relay, and a restarted keeper's relays outrank its former run's.
    - **The job side is unchanged:** `relay --epoch --seq`, and the agent keeping the highest `(E, N)` on each channel.
    - **A controller fences what it sends instead:** each desired set and each acknowledgement carries the sender's `provider-<name>` `Lease` epoch (`leaseTransitions` at its own acquisition write), and the keeper refuses one from an epoch lower than that Provider's highest accepted.
    - **A copy that loses the lead** stops sending sets and acknowledgements before it releases the `Lease`. A copy holds no relays, so a new leader **adopts** the keeper's relays and replaces none.
    - **In beta a `superseded` end is unexpected,** since no newer owner of a channel exists, so the keeper reports it as a final end needing the mark, like `RelayFailed`. The mark's message says "superseded, which is unexpected in beta", and `RelayFailed` is raised once.
  - **The interface:** a Unix socket at a short path the launcher chooses, its length checked against the 107-byte limit, mode 0600 and owned by the host's user. Over it:
    - **the leading controller of each Provider sends desired sets,** level-triggered. A set names the running Allocations, each with its UID, tag, job, Allocation directory, credentials generation and whether a channel has ended for good. For the Provider it gives its UID and creation time, the login host as configured (`HostName`, which the keeper resolves with `ssh -G`), `User` and `Port`, the paths of its key and known-hosts file, and the staged agent binary's path. It never carries a secret.
    - **A set is marked complete** once every Allocation of the Provider that's `Running` with a job ID has had an observation under this lead that reached a decision, keep or drop. Unresolved login-node paths, or a login-host claim not yet answered, is not a decision. Only a complete set stops a relay by leaving its Allocation out, so a new leader whose master connection hasn't resolved its paths yet (HBEP-4) can't cut relays.
    - **Each set carries the age at sending of the Provider's queue listing it rests on, and its validity** (the poll interval in force plus the renew deadline). The keeper computes the expiry on its own monotonic clock at receipt, and refuses a validity above 2 min. That's beta's form of the check before each start (above): the keeper starts or restarts a relay only under an unexpired set that shows its job `RUNNING` under its tag, and a retry counts as a start. Sets aren't persisted: after its own restart, the keeper starts nothing until a fresh set arrives.
    - **The keeper writes its own private `-F` file for each Provider,** under a stable name, from the paths in the set, with the hardening of HBEP-4 (Transport). A relay's identity is its Allocation UID, job, channel and credentials generation, never a file path. A new generation restarts the relay, as in alpha (PR #301), except a channel that has ended for good, which stays down. A new leader sending the same generation replaces nothing.
    - **The keeper reports** each relay's state, and each final end with its kind: an actionable failure, which needs `RelayEndedForGood`, or `superseded` in beta, likewise.
    - **The leader acknowledges a final end once it's decided:** the mark written, or the write refused with its reason (the Allocation ended, is being deleted, or has another UID or job). The keeper drops a decided end.
    - **The keeper never restarts a channel whose final end it holds,** decided or not, until that Allocation leaves a complete set. `RelayFailed` is raised once, when the mark is newly written. So in beta a failed write raises nothing until a write succeeds, whereas alpha raises the event after any attempt.

    This closes alpha's limit, where a restart before the mark was written forgot the failure (What ends a relay).
  - **No leader:** with no unexpired set for a Provider, the keeper keeps that Provider's relays, which end with their jobs through `srun` at the latest, and starts and restarts none. An Allocation deleted while no controller leads keeps its relays until its job ends, a stated limit. The keeper respawns nothing on its own (L-15.1).
  - **Capacity and the login host:** the keeper is the single owner of HBEP-4's 4-relays-per-login-host count and of the one-host-dialed-Provider-per-login-host rule, across leadership generations.
    - **A Provider's claim on a login host** lasts while the keeper holds any of its relays there, and survives a keeper restart. It lapses when its holder sends a complete set with no Allocation on that host, or names another host. It also lapses when the holder has held no relay there and sent no unexpired set for 5 min.
    - **A set from a second Provider for a held host** is refused with the holder's name, and that Provider's controller sets `Ready` False with `LoginHostInUse`.
    - **With no relays held,** the deterministic rule decides: the oldest Provider, then by name.
    - The 2-Allocations-per-Provider submit limit is unchanged.
  - **Stated limits:**
    - the restart cap's hour (What ends a relay) is kept in memory, so it starts again at a keeper restart;
    - a `Lease` deleted by hand, or a store rebuilt from scratch with the keeper's state file kept, lowers `leaseTransitions` below the keeper's fence. The keeper's refusal names the Provider and its highest epoch, and the runbook clears that fence as `restore` does. No tool does it.
  - **A keeper restart** cuts every host-dialed stream on the host, as an frps restart cuts every tunnel. The new keeper takes a higher `E`, and starts relays only under fresh sets (Risks).
  - **Signals:** the keeper signals only its own recorded children, each through a pidfd, as the 10 Nov incident's fix requires of the test harness (#308). Alpha's signal to a relay's process group is dropped, since a relay's `ssh` starts no children.
- **The login node:** each relay keeps an ssh session and an `srun` client there for its Allocation's life. They relay bytes and compute nothing (HBEP-4, open question 14, as amended the same day, with the owner's go). Model weights and images never cross a relay; staging stays in the job (HBEP-7).

**The dial-home listener's paths** (HBEP-5) in every mode: `/agent/v1/` goes to the agent endpoint. A `GET /~!frp` websocket upgrade goes to frps's bind port on loopback. frps matches the exact bytes `GET /~!frp` (frp facts), so the listener forwards the request line unchanged, never re-encoded. Every other path gets 404.

### Admission: the server plugin

The lifecycle controller's process serves frps's HTTP plugin on loopback at base+8, next to the agent API. frps posts `{"version", "op", "content"}` and reads `{"reject", "reject_reason", "unchange", "content"}` (frp facts). The plugin always answers `unchange: true` and never edits content.
- **`Login`.** The content is frpc's login message, with `metas` from frpc's `metadatas`. The plugin accepts only when `metas.token` equals the credential of an Allocation that exists and has not ended, compared in constant time against the files the controller keeps (HBEP-7). Anything else is refused, and frps answers frpc with a login error.
- **`NewProxy`.** The content carries `user.metas` from the session's login, so the plugin rechecks the token on every call and keeps no session state. It accepts only a proxy with:
  - `proxy_type` `tcpmux` and `multiplexer` `httpconnect`;
  - exactly one entry in `custom_domains`, equal to `proxy_name`;
  - that value the UID of an Instance placed on the token's Allocation, which has not ended;
  - no `subdomain`, `group`, `http_user` or `route_by_http_user`.

  Anything else is refused, and frps does not register the proxy. This is what the mitigation for "a false instance registers" rests on.
- **`CloseProxy`.** A notification: it carries the proxy name and the session's user info. The plugin takes it as a reason to re-read frps's list (Path state).
- **Answers within 1 s, from memory.** frps calls `NewProxy` on the session's own read loop, with no timeout on the plugin's HTTP client (frp facts), so a slow answer stalls that tunnel's control messages, and a stall past 25 s drops it. The plugin answers every call from in-memory state (the controller's caches) within 1 s, and refuses rather than waiting on the API server. frpc retries a refused login.
- **After a start,** the plugin refuses every `Login` until the controller's caches have synced. A login it accepted too early could be followed by a `NewProxy` it must refuse, and frpc retries a refused proxy only every 30 s, while it retries a refused login with a backoff of at most 20 s (frp facts).
- **Reject reasons** reach frpc and frps's log, so they name the rule that failed and never the token.
- **Fail closed.** A plugin that cannot be reached refuses every login and proxy (frp facts). frpc retries.
- **No `Ping`** (science-agent decision, 31 Oct 2026). A failed `Ping` call becomes an error answer to frpc, and frpc closes its session (frp facts). Subscribed, it would make every tunnel drop within one heartbeat interval whenever the plugin is down, as during a restart of the controller binary. Unsubscribed, a controller restart cuts no established tunnel in direct or ssh mode; only logins and proxy registrations wait for the plugin. Liveness rests on frps's own heartbeat timeout (Heartbeats) and the front's probe.

### Path state and the inventory

- **The condition.** The lifecycle controller records whether each Instance's tunnel is connected as an Instance condition, which it writes. An Instance first becomes `ready` only with it True, also while the condition is absent, so a tunnel that never comes up fails the Instance at `health.startTimeout`. An Instance restarted before it was ever `ready` is still at its first entry, so the gate applies. Its start deadline is `health.startTimeout` counted from its latest restart, and at that deadline it ends with `TunnelNotConnected` when the condition is present and not True; otherwise the restart ladder climbs as for any start that times out (HBEP-7, Instance start). The gate is on that first entry only: an Instance that returns to `ready` after a restart does so when its checks pass, and while its tunnel is down it is the healthy Instance with a dropped tunnel of Risks, not eligible at the front, with nothing restarted, ended or released on that basis (HBEP-7, Instance start; science-agent ruling, 1 Nov 2026, for #167). The reason names the tunnel, `TunnelNotConnected`, when the condition is present and not True; while it is absent, before the reader's first write, the reason is `StartTimeout` (HBEP-7). Proposed names, for the code agent, who owns `api/v1alpha1/conditions.go`:
  - type `TunnelConnected`;
  - status True with reason `ProxyOnline`: frps lists the Instance's proxy as online;
  - status False with reason `ProxyOffline`: frps lists it as offline, or not at all;
  - status Unknown with reason `ProxyListUnreadable`: frps's list could not be read. The condition is never left True on a list it could not read.
- **frps's list is the truth; the plugin's calls are triggers.** The plugin sees `NewProxy` before frps registers the proxy, and registration can still fail afterwards with no `CloseProxy`. `CloseProxy` is sent asynchronously and its errors are dropped, and on a reconnect, which keeps frpc's run ID, the new session's `NewProxy` can arrive before the old session's `CloseProxy` (frp facts). So the controller sets the condition only from frps's list. It reads the list about a second after each `NewProxy` and `CloseProxy`, and every 10 s.
- **The list** is `GET /api/proxy/tcpmux` on frps's web server, on the IPv4 loopback address at base+11, with the web server's password. Each entry has `name`, `status` (`online` or `offline`), `conf` (with the custom domains), `lastStartTime` and `lastCloseTime`. Offline entries stay in it, so only `online` counts.
- **Soft state.** Path state lives in memory and in the condition. After a restart the lifecycle controller rebuilds it from the list before it writes any condition.
- **The inventory** that the user story asks for is the Instances' `TunnelConnected` conditions, plus frps's own state of each proxy from the list: `status` (`online` or `offline`), `lastStartTime` and `lastCloseTime` (frp facts). An online proxy whose name is no current Instance's UID is an orphan once it has been listed so for `TunnelOrphanGrace` (proposed, 2 min). Its clock starts at the first successful read that lists it so; a successful read that shows it offline or absent restarts the clock; a read that fails neither starts nor restarts it, and the event waits for the next successful read that still lists it. The clock is in memory, so a controller restart starts it again. An orphan cannot come back online, since the plugin refuses a proxy for an ended Instance, so a proxy listed at both ends of a failed read was online throughout. The grace is well above a normal teardown: the agent learns that its Instance ended through the desired-state long poll (an answer within the server's 1 s recheck, or up to 30 s of backoff after a failed poll), stops frpc with TERM and a 5 s grace, frpc over `wss` exits on TERM at once (frp v0.71.1 installs a TERM handler only for kcp and quic), frps then sees the session close, or a silent path within its heartbeat timeout, and the reader runs at least every 10 s. At a job's end Slurm's TERM reaches frpc too, and `KillWait` only delays the KILL. An orphan gets an event from the controller (proposed reason `TunnelOrphaned`), under HBEP-8's hourly gate for repeating events, counted from the end of the grace (science-agent rulings, 1 Nov 2026, for #167, settled in PR #186's review).

### The front's half

- **The transport per Instance UID** (HBEP-5, Reachability). The front dials frps's tcpmux port on the IPv4 loopback address at base+10, sends `CONNECT <UID>:80 HTTP/1.1` with `Host: <UID>:80`, and reads the reply. On 200 the connection carries plain HTTP to the engine, and may be kept alive and reused under HBEP-5's `upstreamIdleTimeout`. On 404 frps has no proxy for that UID.
- **The set-up timeout** (`tunnelSetupTimeout`, 5 s, proposed). It runs from the dial until the 200 or the 404.
  - frps writes the 200 once the UID is routed, before it takes a work connection to frpc (frp facts). So the front never sees the connection to the Instance being established, and the timeout bounds only the front's exchange with frps on loopback.
  - A refused dial, a 404 or the timeout means that no byte of the request was written. HBEP-5's open question 5 then resends it once, and it counts toward the Instance's pause.
  - A frozen path still answers 200, so this timeout cannot catch it. frps closes such a request itself, within `userConnTimeout` (10 s) or at session teardown (12–20 s), after the write and before any response byte (frp facts). The client gets 502 `upstream_closed` with detail `tunnel` and `cutBy: tunnel`; it is not resent (HBEP-5, open question 5) and counts toward the pause. The probe then makes the Instance ineligible.
- **The streamed-headers deadline** (`streamHeadersTimeout`, 30 s, proposed). For a streamed request only, it runs from the end of writing the request until the response's status line and headers arrive.
  - vLLM v0.32.0 sends a stream's status and headers before the engine starts the request. The routes return a `StreamingResponse` around a generator that has not started (`vllm/entrypoints/openai/chat_completion/api_router.py:62-80`, `chat_completion/serving.py:408-409`; `completion/api_router.py:55-72`; `responses/api_router.py:65-75`). The first chunk waits for the engine's first output (`chat_completion/serving.py:527-537`).
  - So the deadline covers frpc, the socket, and vLLM's validation and rendering of the request, on a live tunnel; on a frozen one frps closes the request first. It never covers queueing, prefill or the first token, which HBEP-5 allows up to `idleTimeout` (a first token after 1,100 s is healthy).
  - On expiry the request has been written, so it is not resent (HBEP-5, open question 5). The client gets 504 `request_timeout` with detail `headers`, and it counts toward the pause.
- **No deadline before a non-streamed reply's first byte.** vLLM sends a non-streamed reply's headers only with its body (`chat_completion/api_router.py:69-73`), after the whole generation. In the undetected window frps closes such a request, as above, and the client gets 502 `upstream_closed`; otherwise it relies on the probe.
- **The probe.** Every 10 s (`probeInterval`, proposed) the front probes each Instance in phase `verifying`, `ready` or `draining`, end to end:
  - a new connection through the transport, then `GET /health`, with no key: vLLM v0.32.0 guards only paths under `/v1`, `/v2`, `/inference` and `/cohere` (`vllm/entrypoints/serve/middleware/authenticate.py:11`, `:59-60`);
  - it passes on a 200 within `probeTimeout` (10 s, proposed), which exceeds S1c's worst head-of-line delay of 7.4 s. vLLM answers 503 when its engine is dead (`vllm/entrypoints/serve/instrumentator/health.py:29-33`). Anything else fails, a set-up failure included.
- **What the probe decides.** An Instance's transport counts as registered (HBEP-5) from its first passing probe. Two consecutive failures (`probeFailures`, proposed) make it not registered, so the Instance is not eligible: HBEP-5's skip, which keeps its place in its group and its in-flight count. It is not a leave, and not a dead Instance. One passing probe makes it eligible again. Routing never rests on frps's view or on the condition alone.
- **Reporting.** When a `ready` Instance stops being eligible this way while its `TunnelConnected` is True, the front emits a `degraded` event (reason `PathProbeFailing`, distinct from HBEP-7's `ProbeFailing`). The controller restarts nothing on it; HBEP-7 decides restarts from its own health checks.

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
- **Host-dialed mode** adds the same ssh liveness on each relay, and the relay session's yamux keepalive every 10 s with a 10 s write timeout, inside frps's 12–20 s detection of a silent path.

### Local listeners

Engine and router listeners bind a Unix socket or loopback, never a cluster address, and are reached only through frpc. In alpha the engine listens on a Unix socket in the job's private temporary directory (HBEP-7), so no port can clash on a shared node (L-7.7). Rank traffic between the nodes of a multi-node instance is the engine's own and stays unauthenticated by default, a known limitation (Risks). Nothing is computed from job IDs or indices.

### Paths

| Path | Owner | Liveness | Teardown |
|---|---|---|---|
| Dial-home tunnel (frpc) | the Instance | frp's heartbeats, plus the front's probe through the path to the engine | frpc stops when the Instance ends, and at the latest the job's end closes its connection. frps drops the proxy on the close, or within 12–20 s if the path went silent. The condition follows frps's list |
| Control connection (the agent's HTTPS) | the Allocation | the agent's reports (HBEP-7) | ends with the agent, at the latest with the job |
| ssh forwards (ssh mode) | the Allocation | `ServerAliveInterval` and the traffic they carry | the agent's children; they end with the job, and their sshd sessions with them |
| Relays (host-dialed mode) | the Allocation | `ServerAliveInterval`, the session's keepalive, and the traffic they carry | stopped by PID with the Allocation, by the controller in alpha and by the relay keeper in beta; their `srun` steps end with the job at the latest |
| ssh port forward opened by an operator | never a routing path in alpha (open question 4) | | |

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
| frps closes a user connection that gets no work connection, at `userConnTimeout`; one that drew a work connection lives as long as that stream | `server/proxy/proxy.go:268-292`, `:147-199`; `server/control.go:619-631` |
| An unknown domain gets 404 | `pkg/util/vhost/vhost.go:184-188`; `pkg/util/tcpmux/httpconnect.go:99-106` |
| frps keeps one spare work connection, replacing each one it hands out | `server/control.go:634-635` |
| `bindAddr` defaults to every interface, and `proxyBindAddr` follows it; the tcpmux listener binds `proxyBindAddr` | `pkg/config/v1/server.go:110-114`; `server/service.go:199-211` |
| The web server defaults to loopback; frps keeps proxy statistics only while it runs | `pkg/config/v1/common.go:50-73`; `server/service.go:155-167` |
| `GET /api/proxy/{type}` lists the proxies of a type, online and offline, with `name`, `status`, `conf`, `lastStartTime` and `lastCloseTime`; the API sits behind the web server's auth, and `/healthz` before it | `server/api_router.go:27-49`; `server/http/controller.go:151-161`, `:234-257`; `server/http/model/types.go:100-115` |
| The same web server serves the dashboard pages | `server/api_router.go:62-70` |
| frps's websocket listener matches the plain bytes `GET /~!frp`; a TLS client hello goes to frp's own TLS listener | `server/service.go:296-301`, `:360-364`; `pkg/util/net/websocket.go:16` |
| frpc's `wss` always uses TLS; with no `trustedCaFile` it skips verification | `client/connector.go:183-206`, `:223-227`; `pkg/transport/tls.go:128-155` |
| `loginFailExit` defaults to true; a refused login is retried with a backoff of at most 20 s | `pkg/config/v1/client.go:85-88`; `client/service.go:275-276`, `:300-326` |
| A refused proxy is retried only after 30 s | `client/proxy/proxy_wrapper.go:48-50`, `:205-213` |
| frpc takes `http_proxy` from its environment as its proxy to frps when none is configured | `pkg/config/v1/client.go:152` |
| With no `user`, the proxy name is sent unchanged | `pkg/naming/names.go:5-11` |
| A `tcpmux` client proxy is a general TCP proxy, which hands each work connection to its client plugin; `unix_domain_socket` dials the configured socket | `client/proxy/general_tcp.go:23-33`; `client/proxy/proxy.go:110-118`, `:217-223`; `pkg/plugin/client/unix_domain_socket.go:37-69` |
| frpc's default transport settings | `pkg/config/v1/client.go:147-163` |

### Test plan

Against the launcher's frps, the front, the agent and an engine double on one host first, then on Helios and Athena.
- **Wiring, every mode.** An Instance's frpc logs in and registers its proxy. The front's `CONNECT <UID>:80` gets 200 and a reply from the engine double. In ssh mode both forwards carry their traffic, and the agent's control traffic never uses frpc's connection.
- **Host-dialed mode.** The fake Slurm's `srun --overlap` carries the relays.
  - **Channels stay apart:** frpc logs in and serves through the serving relay, and the agent's control traffic uses only the control relay.
  - **A relay killed mid-stream:** the stream is cut and counted, the relay starts again within the backoff, frpc logs in again, and the next request completes.
  - **A stream on either relay can reach only its channel's port.**
  - **A restart of the Provider's controller,** in alpha: its relays are stopped by PID, and the new leader starts both relays of every running Allocation. In beta the relay keeper's relays stay, and the new leader adopts them (the keeper's tests below).
  - **Two relays for one channel:** the agent keeps the newer.
  - **A fifth relay on one login node** waits.
  - **An `srun` command line without `-u`** fails a test of the argument list.
  - **The fake Slurm gains a streaming `srun -u` mode** (incremental standard input and output both ways, no output cap, no command time-out for a relay step), since today's fake returns output at exit.
  - **Fencing:** an old copy's relay that arrives after the new copy's, with a lower epoch, is refused as `superseded`, and isn't started again. In alpha, a copy that loses the lead stops its relays before it releases the `Lease`; in beta it stops sending sets and acknowledgements first.
  - **The rendezvous:** a batch step and a relay step with different `$TMPDIR` values still meet through `relay-socket`; a stale `relay-socket` (no listener) ends the relay as "agent not ready"; a peer of another user is refused.
  - **Capacity:** a third Allocation of a host-dialed Provider waits at submission (`SubmitLimitReached`); a pair is reserved whole, and neither relay of a running Allocation's pair is ever missing while the other runs.
  - **What ends a relay:**
    - a missing preamble, ssh drops and "agent not ready" are retried, and so are login-side kills up to the cap. A retried end, and a `superseded` one, keeps the Instance and its Allocation.
    - a final end other than `superseded` or the job's end (a step-limit refusal, more than 120 session drops in an hour) writes `RelayEndedForGood`, raises `RelayFailed`, ends the Instance with reason `RelayEndedForGood` and a `failing` event, and deletes the Allocation, whose job is cancelled. An empty Allocation kept warm is released too.
    - the job ending while a relay starts ends the relay with no mark, since the job's end releases nothing new.
    - the mark is refused on another UID or job, and on an Allocation that isn't running.
    - **Proves nothing here:** a test that calls the release directly, without a relay ending through the real relay owner.
  - **The nested stack under slow readers:** frpc's tunnel inside the serving relay, with a slow token reader, keeps the other streams moving within frp's window.
  - **The real platforms, at the first live run:** all 256 byte values cross each way, an `--overlap` step reaches the agent's socket, and the step lands on the batch host. The fake uses pipes, so none of this is shown by the fake.
  - **The relay keeper (beta):**
    - a controller restart and a leadership hand-over while streams run cut no serving stream, and the relays' `E` doesn't change;
    - an agent fenced at an alpha epoch accepts the keeper's first relays, at 2³¹ plus the start count;
    - a set or an acknowledgement from a lower `Lease` epoch is refused, including a stale controller that reconnects first after a keeper restart;
    - after `restore`, the real leader's lower epoch is accepted;
    - a new leader's incomplete set stops no relay;
    - an expired set starts and restarts nothing. That holds after a keeper restart, during a leader outage, and for an Allocation deleted during that outage;
    - a final end survives a controller restart, and the next leader writes `RelayEndedForGood` and acknowledges it;
    - a final end whose Allocation was deleted before the replay is acknowledged as refused, and dropped;
    - a crash after the mark but before the acknowledgement raises `RelayFailed` once;
    - a final-ended channel isn't restarted after a keeper restart;
    - a keeper restart's relays carry a higher `E`;
    - `down` leaves no keeper and no relay `ssh` behind, and keeps the state file;
    - a second Provider's set for a held login host is refused, and its `Ready` is `LoginHostInUse`;
    - the surplus third Allocation of a Provider-leader overlap waits for its relays under the 4-relay count.
    - **Proves nothing here:** a restart test with no stream running, or a keeper and a controller in one process.
  - **None of these proves anything here:** a relay test with both ends in one process and no `srun` standard input and output between them, or a count of relays with no byte crossing.
- **Loopback only.** frps holds exactly its three loopback ports, and `ss` shows no frps listener on another address. A positive control: started without `bindAddr`, frps listens on every interface, so the check can fail.
- **Plugin refusals** (alpha graduation): a login with a wrong token, an ended Allocation's token, or no token; a proxy whose domain is not one of the token's Allocation's Instances; a proxy of type `tcp`, or `tcpmux` with another multiplexer; two domains; a `subdomain`, a `group` or an `http_user`. Each is refused, and frps lists no proxy. A test that only shows a valid login accepted proves nothing.
- **Certificate.** frpc with the right certificate connects. frpc facing another certificate, and frpc with the right certificate but another `serverName`, are refused. The agent refuses to start frpc without the certificate file, and with a certificate whose pin differs from `agent.json`'s. A test with no `trustedCaFile` that "connects" proves nothing, since frpc then verifies nothing.
- **Key restriction.** With the key's line, the two forwards work. A forward to a third port, a remote forward and a command are refused. A key without the second `permitopen` gives an Instance that never becomes `ready`: it fails at `health.startTimeout` with `TunnelNotConnected` and is never counted as capacity (HBEP-7).
- **Port change.** Take connection 2's local port while the forward is down: the agent binds a new port, rewrites frpc's configuration, restarts frpc, and frpc logs in on the new port. The same for connection 1 moves the control client.
- **Path state.** The condition turns True after registration and False after frpc is killed. A `CloseProxy` dropped while the plugin endpoint is down is corrected by the next periodic read. A second registration of an Instance's UID while its proxy is up is refused by frps (a duplicate name): the existing proxy stays registered, and the Instance's condition stays True. With frps's web server stopped the condition is Unknown, never True. After a controller restart the conditions are rebuilt from frps's list.
- **The probe** (alpha graduation). Kill the engine while frpc stays up: two probes fail, the Instance stops being eligible while keeping its count, the `degraded` event appears, and no restart follows from the probe. Restart the engine: one passing probe makes it eligible again.
- **A frozen path.** Freeze the path (stop frpc with SIGSTOP), then send streamed and non-streamed requests: frps closes each within `userConnTimeout` (10 s) or at session teardown (12–20 s), before any response byte. Each gets 502 `upstream_closed` with detail `tunnel` and `cutBy: tunnel`, and none is resent.
- **Deadlines.** On a live tunnel, an engine double that reads the request and never answers gives a streamed request 504 `request_timeout` with detail `headers` at `streamHeadersTimeout` (30 s), and it is not resent. A `CONNECT` for an unknown UID gets 404 and is resent once to the next Instance. A first token after 1,100 s still completes (HBEP-5).
- **Heartbeats.** Freeze the path: frps drops the proxy within 12–20 s, and the condition turns False. A slow reader on the tunnel, as in S1c, drops no healthy tunnel.
- **Reconnects.** Restart frps: every frpc logs in again without help, and the probes pass again. Restart the front: the same.
- **Controller restart** (alpha graduation). Restart the lifecycle controller while streams run through established tunnels: in direct and ssh modes no tunnel drops and no stream is cut. In host-dialed mode, in alpha, the streams in flight are cut and counted as cut, no request already written is replayed, and both relays of every running Allocation are back within the backoff. In beta, with the relay keeper, no serving stream is cut and no relay is replaced, and the agent re-registers over the same control relay. An frpc that reconnects during the restart is refused, retries, and logs in once the plugin is back. A test that restarts the controller with no stream running proves nothing about cuts.
- **Plugin speed.** With the API server stopped, the plugin still answers every call within 1 s, from memory, and refuses what it cannot decide.
- **The orphan grace.** A proxy that stays online after its Instance ended raises no `TunnelOrphaned` at 1 min 59 s of listed orphan time and one at 2 min. A successful read that shows it offline or absent restarts the clock. A failed read between two that list it changes neither. An Instance that ends on a kept Allocation, whose agent stops frpc, raises none: the reader is fed frps's real list across the teardown, since a stub that lists an orphan from the start cannot show that a normal teardown raises nothing.
- **A restart before the first `ready`.** An Instance restarted before it was ever `ready`, whose tunnel is down, ends with `TunnelNotConnected` at `health.startTimeout` from its latest restart.
- **Teardown** (alpha graduation). End the allocation at its hard limit, and cancel one early: no online proxy for the ended Allocation's Instances in frps's list (offline entries may remain as history), no ssh process in the job's process tree or on the control-plane host, no relay's `srun` client on the login node and no relay step in the job, and no record left. Both cases end with a live node that sends a FIN, so also freeze the path with no FIN, through a relay on the path stopped with SIGSTOP as in S1c: frps drops the proxy within 12–20 s, and the forwards' sshd sessions end within sshd's client-alive window. frps itself is stopped only by its recorded process ID and start time.
- **One-hour runs** (alpha graduation). S1's one-hour runs pass with this wiring on Helios and on Athena, in the mode each uses, with the path rate measured.

### Compatibility and upgrades

- Clients see only the front, so a path change is invisible to them.
- **frps and frpc move together.** Both are pinned to one frp version in `internal/launcher/frp-pins.txt`. New Allocations get the current frpc. An frps upgrade waits until no running Allocation has another frpc version, or until that pair is tested.
- **The agent protocol** (`/agent/v1/`) is unchanged; frpc's tunnel is separate from it.
- **Stored state:** none of this HBEP's own, except the Allocation condition `RelayEndedForGood`, which is durable and never cleared (Dial-home modes, What ends a relay), and in beta the relay keeper's state file (The relay keeper). Path state is soft, rebuilt from frps's list; frps's state lives in memory and is rebuilt by reconnects.
- **From alpha's relays to the keeper (beta):** a host can switch with Allocations running. The keeper's `E` starts above every alpha epoch, so its relays supersede the controller's, and each running Allocation's streams are cut once at the switch. The reverse, back to in-controller relays, needs every host-dialed Allocation of the host drained first, since alpha's lower epochs would be refused as `superseded`. So `up` without `--relay-keeper` refuses while the launcher's record says the keeper ran and host-dialed Allocations are running.
- **Downgrade:** start the earlier frps with the earlier frpc pin, under the same rule.

### Security

- **The relay keeper (beta)** listens on no network port, only on a mode-0600 Unix socket of the host's user. It's sent no secret, only the paths of the Provider's ssh configuration and the agent binary, and it signals only its own recorded children, through a pidfd.
- **Credentials.** The Allocation's credential is in frpc's configuration file, mode 600, in the job's private temporary directory, read by path. It never appears in the batch script, on a command line, or in a log. frps and the plugin log neither the metadata nor the token.
- **The plugin endpoint** answers only frps's calls, on loopback. The restricted key does not open its port.
- **frps's web server** is on loopback, behind a password the launcher generates into its web password file and frps's configuration file, both mode 600.
- **Trust boundary:** an Instance can reach only the agent endpoint and the dial-home listener's two paths. The front reaches an Instance only through frps and its tunnel.
- **Prompts** cross center networks inside the dial-home listener's TLS, and in ssh mode inside ssh as well. In host-dialed mode they cross ssh to the login node, then Slurm's own step I/O on the cluster network, still inside that TLS. There is no end-to-end TLS from front to engine (Risks). Nothing on the path stores prompts or replies.

### Graduation criteria

- **Alpha:**
  - S1's one-hour runs pass with this wiring on Helios and on Athena, with the path rate from compute node to front measured, in the mode the alpha host uses: host-dialed, since 9 Nov 2026;
  - the plugin's refusals are tested: a wrong token, a domain that is not one of the Allocation's Instances, and a wrong proxy type;
  - a dead engine behind a live tunnel is caught by the probe;
  - teardown leaves no online frps proxy for the ended Allocation's Instances, offline history aside, and no ssh process, relay `srun` client or relay step behind;
  - a lifecycle-controller restart while serving cuts no established tunnel in direct or ssh mode; in host-dialed mode it cuts the streams in flight, counted and never replayed, and the relays return within the backoff (owner, 9 Nov 2026);
  - in host-dialed mode, on each alpha platform, a relay works on the partition the Instances use (HBEP-4, open question 16), Helios GH200 included.
- **Beta:**
  - many tunnels on one front are measured (open question 11): S14 measured steady state, and the five items it leaves open are measured too;
  - bulk endpoints run off the token tunnel (open question 12);
  - in host-dialed mode, the relay keeper's tests pass, and a controller restart while serving cuts no serving stream (open question 13).
- **Stable:** an frps and frpc version skew is tested, and end-to-end encryption is decided with HBEP-9.

## Operational readiness

1. **Working:**
   - component health: the launcher's check of frps (its process, its three loopback ports, `/healthz`), the plugin endpoint answering within 1 s, and each Instance's `TunnelConnected` condition with frps's last start and close times;
   - end to end: the front's probe through each tunnel to the engine;
   - not working: a probe that fails on a connected tunnel (the `degraded` event), or a condition that is False or Unknown for a running Instance.
   - in beta, the relay keeper: its pidfd and socket for the launcher; and as silent-failure signals, the serving controller's relay series (HBEP-8), refused sets, and `superseded` ends.
2. **Restart:**
   - frps: every tunnel drops and frpc logs in again on its own. Streams through it are cut. The controller's next list read shows the new state.
   - The lifecycle controller: established tunnels stay up in direct and ssh modes; in host-dialed mode, in alpha, its relays, and the streams on them, are cut and restarted within the backoff, and in beta the keeper keeps them, and only control streams end. While its plugin endpoint is down, logins and new proxies are refused, so an frpc that reconnects meanwhile retries until the plugin is back (Risks). After the restart the controller rebuilds path state from frps's list.
   - The relay keeper (beta): every host-dialed stream on the host is cut. The new keeper takes a higher `E` and starts relays once fresh sets arrive. The launcher's `upgrade` stops the whole supervisor group, so it cuts them too.
   - The front: its dial-home listener carries frpc's connections, so every tunnel drops and reconnects. Its probe state starts empty, and each Instance is eligible again at its first passing probe.
   - frpc or an ssh forward: the agent restarts it with backoff.
3. **Silent failure:**
   - a tunnel that is up with a dead engine behind it: the probe;
   - a frozen path that still answers `CONNECT`: frps closes the request within `userConnTimeout` or at session teardown, the front answers 502 `upstream_closed`, and the probe takes the Instance out;
   - a proxy that the plugin accepted but frps never registered, or a lost `CloseProxy`: the condition follows frps's list, read every 10 s;
   - a list that cannot be read: the condition says Unknown, never True;
   - frpc that does not verify the front's certificate: the agent refuses to start it without the certificate and its pin;
   - frps listening off loopback: the launcher's check of its ports;
   - a record left after its allocation ended: the inventory compared with frps's list and the provider's job list.
4. **Provider limits:** outbound 443 per partition (S1's step 0); in ssh mode, the configured ssh port to the control-plane host's sshd, 22 by default; loopback shared with other users' jobs on a node; lease ends, which end the tunnel. In host-dialed mode: the login node's process policy and sshd throttle, its availability (an outage or maintenance there cuts every path on that cluster at once), and each job's `MaxStepCount`.
5. **Capacity:** for budgeting a front, open question 11 is authoritative (S14). Below are S1c's figures, from one tunnel at up to 256 streams on loopback, with no reserves.
   - **Front CPU:** 7.9–8.7% of a core per 1,000 tokens/s over wss (frps plus the TLS terminator), 6.6–6.9% over frp's plain tcp, and 7.5–7.8% for the ssh path (frps plus sshd). The cost per token was higher at 64 streams (8.8–16%).
   - **Per front core, one tunnel, no reserves:** about 580–630 wss streams at 20 tokens/s (11,500–12,600 tokens/s), or 720–760 over tcp. This is extrapolated linearly from 36–39% of a core at 256 streams; the front near full CPU was not measured. S14's budget with reserves is lower (open question 11).
   - **Memory:** about 50 MB for frps at 256 streams, and 70 MB for frpc on the instance side.
   - **The link is the more likely limit:** about payload bytes/s ÷ (300 × tokens/s per stream) streams, since each 222-byte token chunk took 290–335 bytes of TCP payload. That is about 85 streams of 20 tokens/s at 512 kB/s.
   - **Overload:** every stream on the tunnel slows evenly, and time to first token grows to seconds; in S1c no stream failed.
   - **Many tunnels on one front:** S14's steady-state budget (open question 11). Its five open items block beta.
6. **Idle cost:**
   - per Instance: one frpc process, one `wss` connection, a yamux ping and an application heartbeat every 10 s, and one probe every 10 s;
   - per Allocation in ssh mode: two ssh connections with their keepalives;
   - per Allocation in host-dialed mode: two ssh connections, two `srun` clients on the login node and two `srun` steps in the job;
   - on the control-plane host: frps and the plugin endpoint, and in beta's host-dialed mode the relay keeper;
   - no grant hours beyond the Allocation's own, and nothing while the job waits in the queue.
7. **Rollback:** stop frps, or stop the Instances' frpc. Instances keep running but take no traffic until a path returns, and streams in flight are cut. frps keeps no stored state, so nothing needs cleaning beyond the launcher's record. In beta, stopping the relay keeper cuts every host-dialed stream; its state file must be kept, or a new keeper's epochs could fall below its old relays'.

## Drawbacks

- Every token makes an extra hop through the front's host.
- The front must be reachable from every center's compute network, except in host-dialed mode, which goes through the login node instead.
- All streams of an Instance share one connection, so a large reply delays the others (Risks).
- New tunnels cannot log in while the lifecycle controller is down (Risks).

## Alternatives

- **For the relay keeper: the launcher supervising each relay directly,** from a spool directory the controller writes. It needs no new process, but the launcher would then hold per-Allocation state, fencing and failure reports, which is a controller's job, and `up`'s supervisor would grow a second reconcile loop. Rejected.
- **For the relay keeper: a transient systemd user unit per relay.** Units would outlive the controller with no new code. Rejected: they'd need their own fencing and failure reports, and the framework's processes would then depend on the host's user manager, which the launcher's records and `down` don't cover.
- **Login-node relays everywhere.** They work without outbound, but they are subject to login throttles and login-node process policies, and each relay is one more process to own. Adopted only in host-dialed mode, where nothing else reaches the job, with 2 relays per Allocation and at most 4 per login node in alpha.
- **ssh into the job's compute node, with reverse forwards** (probe of 9 Nov 2026). It would need no `srun` client on the login node. Rejected: after a public-key partial success, the compute nodes' sshd asks for a Kerberos ticket, which the alpha host doesn't have.
- **A reverse forward on the login node** (probe of 9 Nov 2026). Rejected: it binds the login node's loopback only, so compute nodes can't reach it.
- **Waiting for the gateway's forwarded port** (direct and ssh modes). Kept as modes, no longer awaited for alpha (owner, 9 Nov 2026).
- **Port forwards from the front.** Simple for one instance. In bulk they became the unowned tunnels listed in Motivation.
- **Instances listening on cluster interfaces.** Reachable only inside the cluster, and exposed to every user on it.
- **A center-provided ingress.** Where a center offers one, it may be the best path. Whether any target center does is not known yet.
- **frp's plain tcp transport to a public port.** wss costs 15–30% more front CPU per token than plain tcp (S1c), but plain tcp is not HTTPS on 443, which is what compute nodes were measured to reach.
- **frp's Go packages inside our binaries.** Pinned release binaries keep frp's code unmodified and its version visible in one place (HBEP-15).
- **`tcpmuxPassthrough`.** It would pass the `CONNECT` to the Instance, so the 200 would come from the far end. The Instance side would then have to answer `CONNECT` itself, and the setting applies to every proxy. Not needed for alpha, given the streamed-headers deadline and the probe.
- **wstunnel** ([erebe/wstunnel](https://github.com/erebe/wstunnel)), a tunnel over websockets. frp's `wss` covers HTTPS, and frp has the server plugin that admission needs.

## Prior art and reuse

Dial home replaces www-2027's central relays and forward watchdogs, which kept hand-made ssh forwards alive, probed each path end to end, and kept a flapping path out of pools.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| frp v0.71.1 (frps, frpc) | [fatedier/frp](https://github.com/fatedier/frp), release `v0.71.1` | Apache-2.0 (its `LICENSE`, read 2026-10-31) | Dependency: two pinned release binaries, recorded in `internal/launcher/frp-pins.txt` (HBEP-15). frps runs on the control-plane host, started by the launcher; frpc runs in each job, staged by the Provider's controller. Its server plugin interface admits each tunnel. frp's Go packages are not imported |
| vLLM | [vllm-project/vllm](https://github.com/vllm-project/vllm) v0.32.0 | Apache-2.0 | Dependency (HBEP-9): the engine behind the tunnel; its unguarded `/health` is the probe's target |
| yamux | [hashicorp/yamux](https://github.com/hashicorp/yamux), or frp's fork [fatedier/yamux](https://github.com/fatedier/yamux) v0.2.0 (frp's `go.mod`, line 9, read 2026-11-09) | MPL-2.0 (both `LICENSE` files, read 2026-11-09) | Dependency in host-dialed mode: the multiplexed session over a relay's `srun` standard input and output |
| Headscale + tsnet | [juanfont/headscale](https://github.com/juanfont/headscale); [tsnet](https://pkg.go.dev/tailscale.com/tsnet) | BSD-3-Clause (both) | Optional, not in alpha: a front behind NAT without root (open question 8) |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| A per-host process that holds connections across restarts of its controller, fenced by the controller's lease epoch | New in this HBEP, after frps's supervision by the launcher (HBEP-11) and the alpha relays' epoch fencing (PR #301) | The relay keeper (beta), Dial-home modes |
| An SSH relay behind a forced-command key, with measured overhead | GWDG SAIA ([gwdg/saia-hpc](https://github.com/gwdg/saia-hpc)) | The restricted key of ssh mode; open question 3 |
| Dial home from jobs over outbound-only connections | OpenTela with libp2p ([eth-easl/opentela](https://github.com/eth-easl/opentela)); interLink's mesh ([interlink-hq/interLink](https://github.com/interlink-hq/interLink)) | Dial home, and its alternatives |
| Many tunnels on one front, in steady state | spike S14 (issue #313, PR #328) | Open question 11; Operational readiness 5 |
| frp's tunnel through a TLS terminator on 443, its heartbeat settings, capacity and head-of-line figures | spike S1 (issue #18) and spike S1c (PR #78), this project | Open questions 1 and 6, Heartbeats, Risks, Operational readiness 5 |
| An end-to-end probe that keeps a flapping path out of the pool and lets it back in | www-2027's forward watchdogs | The front's probe |

### Field lessons answered

Spike S1 (step 0 and probe 2, issue #18) and spike S1c (PR #78) measured the facts behind several answers below.

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| Compute-node listeners were reachable only through login nodes. | L-7.1 | Instances dial home on 443 or through ssh; nobody opens a port. |
| Hand-made tunnels and watchdogs multiplied, with clashing names and pid files. | L-7.2 | One tunnel per Instance, owned by it, named by its UID, and ending with it. |
| Parallel logins tripped a login node's connection limit. | L-7.3 | In direct and ssh modes, no login-node connections beyond the Provider's master: jobs dial the control-plane host. In host-dialed mode, at most 4 relays per login node, and jittered restarts. |
| Pollers that each opened a new ssh connection filled a host's sshd. | L-7.4 | Two long-lived forwards per Allocation in ssh mode, and two long-lived relays in host-dialed mode, never one per request. |
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
| An ssh banner was read as proof that the clusters reached the VM; the host key was a gateway's. | L-17.7 | In ssh mode the forwards use a pinned host key (HBEP-7, The allocation agent), and S1's kit checks the target's own key, not a banner, before any forward. |
| Checks that cannot see what they check passed while wrong. | L-10.6 | The probe goes through the tunnel to the engine; frpc never runs without its certificate; the condition is Unknown when frps's list cannot be read. |
| Keyword sweeps missed many tunnels at the end. | L-15.2 | frps is in the launcher's record, and every tunnel is in frps's list. |
| A scheduled respawner revived killed relays. | L-15.1 | No respawners outside the controller and the launcher; in beta the relay keeper starts relays only under a leader's unexpired set. |
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
- 2026-10-31: implementable. Spike S1c (PR #78) and frp v0.71.1's source carried in. frp over `wss` with `tcpmux` routing by UID, admission by an frps server plugin (`Login`, `NewProxy` and `CloseProxy`; `Ping` unsubscribed), path state from frps's list, the front's deadlines and probe, S1's heartbeats, two ssh forwards in ssh mode with the owner's key restriction, and the owner's word on center consent. Every open question decided, not needed for alpha, or open for beta. HBEP-4, HBEP-5, HBEP-7 and HBEP-11 amended with it.
- 2026-10-31: L-17.7 cited in Field lessons answered. The note on the development VM corrected: the ssh banner from the compute nodes was a gateway's sshd, as its host key showed (issue #18).
- 2026-10-31: amendment from the owner's decision on the dial-home route (issue #18): ssh mode runs over the configured ssh port, 22 by default; open questions 4 and 8 corrected.
- 2026-10-31: frpc's staging path stated as HBEP-4's, `frpc/<digest>/frpc` (wording only).
- 2026-11-01: amendment with HBEP-7's of the same day: the end reason of an Instance whose `TunnelConnected` is absent at `health.startTimeout`.
- 2026-11-01: erratum (with HBEP-15's move to implementable): frp's and k3s's checksums are recorded in the launcher's pins files, `internal/launcher/frp-pins.txt` and `internal/launcher/k3s-pins.txt`, not in `build/dependencies.yaml`, which never existed.
- 2026-11-01: the `TunnelConnected` gate applies to an Instance's first entry to `ready` only (ruled for #167, with HBEP-7's amendment of the same day).
- 2026-11-01: an orphan proxy must be listed so for `TunnelOrphanGrace` (2 min), a failed read skipped, so a normal teardown raises no event; a restart before the first `ready` is still gated, its deadline from its latest restart (ruled for #167, settled in PR #186).
- 2026-11-02: erratum: `PathProbeFailing` is no longer marked proposed, since the front emits it (PR #248).
- 2026-11-09: host-dialed mode. The control-plane host dials each job through the login node, with two `srun -u --overlap` relays per Allocation carrying multiplexed sessions, because compute nodes can't reach the alpha host and the gateway's forward isn't awaited (owner, 9 Nov 2026; the probe of that day).
- 2026-11-10: a relay that ends for good releases its Allocation (#304, PR #319).
- 2026-11-10: the relay keeper for beta's host-dialed mode (open question 13).
- 2026-11-10: open question 11 partly answered by spike S14 (steady state).
- 2026-11-10: the relay keeper's names, set fields, expiry, completeness and claim rules, from the code agent's plan (#343).

## Amendments

- **2026-10-31: the ssh port, and the route behind S1's step 0.** The owner's decision of 31 Oct 2026, recorded on [issue #18, comment 6546958511](https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/18#issuecomment-6546958511), with the review round of PR #96.
  - **The ssh port is configurable, 22 by default** (Notes; Dial-home modes; Operational readiness 4). The compute nodes reach the development VM's sshd through a port forward on the gateway in front of it, at the gateway's address and port. The key's restriction is on the VM's side and is unchanged. Before, ssh mode named port 22.
  - **Open question 8's source** is corrected. S1's step 0 saw port 22 answer at the VM's address, but that sshd was the gateway's (L-17.7), so it did not show that the VM accepted port 22. Before, the source said it did.
  - **Open question 4** now names an ssh port forward that an operator opens, so that it does not read as the gateway's network forward. The decision is unchanged.
- **2026-11-01: the end reason before the first read** (Path state and the inventory). Science-agent ruling of 1 Nov 2026, for #111, and the review round of PR #127. An Instance still becomes `ready` only with `TunnelConnected` True, also while the condition is absent. At `health.startTimeout` the reason is `TunnelNotConnected` when the condition is present and not True, and `StartTimeout` while it is absent, before the reader's first write. The reader writes it for every placed Instance (#90), so it is absent only until then. Before, the text said only that the reason names the tunnel. The names are on `main` (`api/v1alpha1/conditions.go`).
- **2026-11-01: the tunnel gate on first entry only** (science-agent ruling for #167, the final tunnel PR, from its Opus review). An Instance first becomes `ready` only with `TunnelConnected` True. After a restart it returns to `ready` when its checks pass; a tunnel that is down then makes it ineligible at the front, as for any healthy Instance whose tunnel drops, and nothing restarts, ends or is released on that basis. Before, the gate applied on every entry, so a tunnel outage after a restart climbed the restart ladder and released the Allocation, against this HBEP's own rule.
- **2026-11-01: the orphan grace, and a restart before ready** (Path state and the inventory; Test plan). Science-agent rulings for #167, the final tunnel PR, settled in PR #186's review. An online proxy naming no current Instance is an orphan only once it has been listed so for `TunnelOrphanGrace` (proposed, 2 min); a successful read showing it offline or absent restarts the clock, and a failed read is skipped. So a normal teardown, whose proxy goes offline within that time, raises no `TunnelOrphaned`. An Instance restarted before it was ever `ready` is still at its first entry: the tunnel gate applies, its deadline runs from its latest restart, and it ends `TunnelNotConnected` when its tunnel is down. Before, every proxy that outlived its Instance by one read was an orphan, and a restart before the first `ready` was not covered.
- **2026-11-02: erratum, `PathProbeFailing`** (The front's half). The front emits the reason on `main`, so its "proposed" mark is dropped, as in HBEP-8's Reasons table (PR #248). No behaviour changes.
- **2026-11-09: host-dialed mode** (Summary; Notes; Open questions 3 and 8; Components; Dial-home modes; Paths; Test plan; Graduation criteria; Alternatives; Prior art). An amendment, on the owner's "we have to solve this, you should wait for nothing. just dial whatever you want from this host" (9 Nov 2026). The same day's probe, from the alpha host to Helios and Athena `plgrid-now`, measured three things. An `srun --overlap` step reached over ssh through the login node carries bytes both ways at 48–85 MB/s with a 2.3 ms median round trip, given `srun -u`. ssh into a compute node needs a Kerberos ticket. A reverse forward on a login node binds its loopback only. So the batch Provider's controller now holds two relays per running Allocation, one per channel, each a yamux session over a step's standard input and output. The agent owns its loopback listeners, and frp, TLS, the plugin and the agent's protocol are unchanged. Settled in PR #298's review (Astra and a fresh Opus):
  - **The mode's value and listeners:** `host-dialed`, with the listener on loopback.
  - **Fencing:** an epoch and a sequence on every relay; a copy that loses the lead stops its relays before it releases.
  - **The rendezvous:** through `relay-socket` in the Allocation directory, with peer checks.
  - **The wire:** a preamble before a pinned yamux.
  - **Capacity:** a Provider limit of 2 Allocations, with relays reserved in whole pairs.
  - **What ends a relay:** retried or final, with `RelayFailed`, and never an engine restart.
  - **The owner's two decisions:** relays run without asking Cyfronet first, and a controller restart cuts host-dialed streams in alpha, with a relay keeper deferred to beta (open question 13).
  Before, a host that compute nodes could not reach was out of alpha, and alpha waited for the gateway's forward.
- **2026-11-09: erratum:** `RelayFailed` is an untyped Warning, not a `failing` event (#301).
- **2026-11-09: erratum:** a job missing from the listing is retried, not a final end (#301).
- **2026-11-09: erratum:** the Provider's controller checks the 2-Allocation cap again before it submits, and two Provider leaders in an unfenced hand-over remain the one race, its extra Allocation released as surplus (#301).
- **2026-11-09: erratum:** the restart cap counts only restarts whose session came up and then dropped; failures before a session comes up are never capped (#301).
- **2026-11-10: a relay that ends for good releases its Allocation** (Risks; Dial-home modes, What ends a relay and the no-restart rule; Test plan; Operational readiness). An amendment, the code built ahead of the text (#304, PR #319), settled in this PR's review (Astra and a fresh Opus). The Provider's controller marks the Allocation `RelayEndedForGood` before it raises `RelayFailed`, and the lifecycle controller ends its Instances with that reason and releases it, whatever its retention policy (owner's go for warm Allocations, 10 Nov 2026). Before, only the event was raised, and a ready Instance kept its job until its lease ended.
- **2026-11-10: the relay keeper for beta** (Open questions 13; Components; Risks; Dial-home modes, host-dialed mode; Paths; Test plan; Graduation criteria; Operational readiness; Security; Alternatives). An amendment.
  - **The keeper:** one launcher-supervised `relaykeeper` per host holds every host-dialed relay in beta, so a controller restart or hand-over cuts no serving stream, and only control streams end.
  - **Fencing:** the keeper issues the relays' epochs, and the `Lease` epoch fences the controllers' desired sets instead. A new leader adopts relays and never replaces them.
  - **Durable failures:** the keeper keeps unacknowledged final ends, which closes alpha's lost-failure limit.
  - **Capacity:** it owns the per-login-host relay count.

  - **Settled in this PR's review** (Astra and a fresh Opus):
    - the keeper's epochs start at 2³¹, above alpha's;
    - its state file is synced and survives `down`, and `restore` clears its fence;
    - sets are complete-or-not, carry their listing time and expire, and the keeper starts nothing without an unexpired one;
    - the keeper writes its own ssh configuration, and a relay's identity is its credentials generation;
    - acknowledgements are fenced, and a decided end may also be a refused write;
    - final-ended channels are never restarted;
    - the login-host claim lasts while relays are held;
    - the upgrade limit is stated.

  Alpha's in-controller relays are unchanged. Before, open question 13 was open, and a controller restart cut host-dialed streams in every stage.
- **2026-11-10: many tunnels on one front, steady state** (Open questions 11; Graduation criteria; Operational readiness 5; Prior art). An amendment from spike S14 (issue #313, PR #328), its proposed text as written.
  - **What S14 found:** no tunnel-count knee in steady state through 300 tunnels. The measured stack's CPU is linear in tokens per second, with small per-tunnel terms, in direct and host-dialed mode: frps and the dial-home listener, plus the relays' host side in host-dialed mode.
  - **Settled in this PR's review** (Astra and a fresh Opus):
    - per-process descriptor budgets, the front's process included;
    - the runner spread and the unkept first run;
    - the relays' host side named for alpha and beta;
    - the cores defined;
    - Operational readiness 5 deferring to open question 11.
  - **The budget:** a per-front budget in CPU per token, with reserves.
  - **Until five open items are measured,** a front stays at or below 300 tunnels.

  Before, open question 11 was open with no measurement beyond S1c's one frpc.
- **2026-11-10: the relay keeper's details** (Dial-home modes, The relay keeper; Compatibility and upgrades). An amendment from the science agent's rulings on the code agent's stage-0 plan for #343.
  - **Names:** `hpcburst-controller relaykeeper`, and `hpcburst up --relay-keeper`.
  - **The set:** its fields, an expiry computed on the keeper's monotonic clock from the listing's age and the validity, and when it counts as complete.
  - **Final ends:** decided ones are kept until their Allocation leaves a complete set; a `superseded` end in beta is marked and raises `RelayFailed` once; a generation change leaves a final channel down; and `RelayFailed` is raised only when the mark is newly written.
  - **Login-host claims** survive a keeper restart, with their lapse rules.
  - **The switch back:** `up` refuses it while host-dialed Allocations run.
  - **Two stated limits.**

  Before, PR #339 left these unspecified.
