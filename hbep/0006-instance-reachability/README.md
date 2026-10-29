# HBEP-6: Instance reachability

## Summary

On batch providers, compute nodes are reachable only through login nodes, and on some clusters not at all. This HBEP decides how the front (HBEP-5) reaches an instance: the instance dials home. It opens an outbound tunnel to the front, and the instance owns that tunnel. The HBEP also covers the alternatives, login-node relays and port forwards, and where they still apply. It requires every path to have one owner, a liveness signal and a defined teardown.

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
- Only the path carries traffic from outside an allocation. Engine HTTP listeners bind to loopback and are reached through the tunnel agent.

### Non-goals

- One tunnel technology for every provider. Profiles may differ (HBEP-4).
- Instance health and restarts (HBEP-7). A path that is down is not a dead instance.
- Traffic inside a multi-node instance. Its ranks use the cluster fabric, unauthenticated by default: a known limitation (Risks).
- Reachability of non-LLM services. These are out of scope for now.

## Proposal

### User stories

- An instance starts on Helios. Its tunnel agent connects out to the front and registers with its allocation's credential. Once the controller marks the instance ready, the front routes to it. Nobody opens a port or a login session.
- On a cluster whose compute nodes cannot connect out, the platform profile says so. The framework then uses a login-node relay, which it owns and records.
- The allocation reaches its hard end. The tunnel closes with it, the front marks the path gone, and nothing is left on login nodes.
- An operator or agent lists every path, with its owner, instance, kind and last heartbeat.

### Notes, constraints and caveats

- Helios compute nodes can make outbound connections, so dial home works there. Whether it works on Athena and on other clusters is a per-partition fact in the platform profile (HBEP-4): outbound allowed or not, through which protocols and ports, directly or through a proxy.
- The front needs an address that compute nodes can reach. Where it runs is HBEP-11's decision.
- A center may need to consent to tunnels from compute nodes. The fix owner is then the center.
- Inside the instance, the engine and its router listen on loopback or a Unix socket, and the tunnel agent connects to them locally. On a shared node, other users' jobs can reach loopback too, so the local listener uses the Instance's own key, which the front sends (HBEP-9), or a socket protected by file permissions. That key is not the allocation's credential, which registers the Instance. A key covers only part of the engine's HTTP API (Risks).
- All tokens of every reply cross the path. Its throughput and latency under many concurrent streams are part of serving capacity.

### Risks and mitigations

- **Outbound is blocked or proxied.** It is a profile flag, measured per partition, with the relay as fallback.
- **The tunnel slows streaming.** Many streams multiplexed over one TCP connection, as ssh forwards are, block each other on loss, and ssh's channel windows can cap throughput. Throughput and time to first token through the path are measured before alpha.
- **The tunnel drops while the instance is healthy.** The agent reconnects with backoff. Meanwhile the front holds the instance as unreachable, not dead, and the controller checks the allocation through the provider (HBEP-7).
- **A false instance registers and receives prompts.** Each allocation gets its own credential, and registration is bound to the provider's identity for the job. The instance takes traffic only after the controller's verification (HBEP-7, HBEP-9).
- **The tunnel's ping succeeds while the engine is dead.** The liveness probe goes through the path to the engine.
- **An engine API key is taken for isolation.** A vLLM API key covers only part of the engine's HTTP API (vLLM's security documentation), and traffic between an engine's ranks is unauthenticated by default. So engine HTTP listeners bind to loopback and are reached through the tunnel agent. A multi-node instance's rank traffic still crosses the cluster network unauthenticated. That is a known limitation, recorded in each platform profile (HBEP-4).
- **Relays trip login throttles.** One pooled connection per login node, with the login rate limit as a profile parameter.
- **A center's login-node policy kills relays.** Relays are supervised. A relay death is an event (HBEP-8), and the instance stays unrouted until its path returns.
- **Paths outlive their owner.** Owner references and garbage collection (HBEP-11) remove the front-side records. A relay outside the allocation carries the allocation's end time and exits at that time on its own.
- **Teardown kills the wrong process.** Kill by recorded process ID plus start time, never by pattern. No respawners outside the controller.
- **The front's address changes.** Tunnel agents reconnect to the front by DNS name, never by a stored address.

### Open questions

1. **Tunnel protocol.** Candidates: an ssh reverse forward to an endpoint the framework runs; an HTTP/2 or WebSocket reverse tunnel over HTTPS; userspace WireGuard. Decided by a spike on Helios: which egress works, throughput, and time to first token under many concurrent streams.
2. **Encryption.** Where TLS terminates. Should front and engine be encrypted end to end inside the tunnel? Decided with HBEP-9.
3. **Relay fallback form.** An ssh forward from the front through the login node, or a process on the login node? Who supervises it? Is its owner the Allocation or the Instance? Spike on a cluster without outbound.
4. **Port forwards.** Are they a supported path, or only an operator's debugging tool? If supported, with which owner and teardown? Owner decision.
5. **Multi-node instances.** Candidate: only the entry rank dials home. Pooling workers across Instances is not part of this design (HBEP-3, open question 11), so no traffic between Instances needs a path.
6. **Heartbeats.** Interval and miss count, a trade of detection time against connections and log volume. Measurement.
7. **Credential.** Its format, its lifetime against the lease length, and renewal over the tunnel. Decided with HBEP-9.
8. **A front behind NAT,** as in a single-user setup: does it need a rendezvous relay? Decided with HBEP-11.
9. **Center consent.** Ask Cyfronet, and each other center, whether outbound tunnels from compute nodes are acceptable use. Owner.

## Design details

**Dial home** (decided). An instance's launcher starts a tunnel agent inside the allocation. The agent opens an outbound connection to the front, presents the allocation's credential and registers the instance. The instance owns the tunnel: it starts with the instance and ends with it.

**Local listeners** (proposed; follows from the Goals). The engine's and router's HTTP listeners bind loopback or a Unix socket, on a port the operating system chooses, and report it to the agent; clients reach them through the agent. Rank traffic between the nodes of a multi-node instance is the engine's own and stays unauthenticated by default, a known limitation (Risks). Nothing is computed from job IDs or indices.

**Paths:**

| Path | Owner | Liveness | Teardown |
|---|---|---|---|
| Dial-home tunnel | the instance | heartbeat, plus a probe through the path to the engine | closes with the instance, at the latest at its allocation's end; the front expires its record |
| Login-node relay | candidate: the Allocation (Open question 3) | candidate: heartbeat through the relay, plus the same engine probe | candidate: exits on its own at the allocation's end; if the allocation ends early, the controller kills it by recorded process ID and start time |
| Port forward | candidate: the operator who opens it, for debugging only (Open question 4) | none: never used for routing | candidate: closed by its owner, and listed in the inventory until closed |

Protocol, credential format and heartbeat timing: TBD (Open questions 1, 6 and 7).

### Test plan

- A spike per provider records whether outbound works, from which partitions, and over which protocols.
- Throughput and time to first token through the path, at the instance's session count, compared with a direct connection inside the cluster.
- Kill the tunnel agent while the engine serves: the front stops routing within the heartbeat window, and routing resumes after reconnect, without a restart.
- Kill the engine while the tunnel stays up: the end-to-end probe fails, and the path is reported as up with its engine down.
- End the allocation at its hard limit: no record or process remains, on the front or on any login node.
- Register with a stolen or expired credential, and with a valid credential but a foreign job: both are refused.
- Two instances on one shared node: no port clash, and neither can reach the other's key-protected endpoints without the key.

### Compatibility and upgrades

- Clients see only the front, so a path change is invisible to them.
- Tunnel agent and front versions must be compatible across an upgrade: TBD with Open question 1.
- Stored state: registrations only, in the control plane, re-created by reconnect.

### Security

- Allocation credentials are written to a file readable only by the job's user. They never appear in the batch script's text, on a command line, or in logs.
- Trust boundary: an instance can reach only the front's registration and data endpoints. The front reaches an instance only through its path.
- Prompts cross center networks. Encryption: TBD (Open question 2).

### Graduation criteria

TBD. Candidate for alpha: dial home on Helios passes the test plan.

## Operational readiness

1. **Working:** each path's last heartbeat, plus a periodic probe through the path to the engine. A probe failure on a live heartbeat says the engine, not the path, is down.
2. **Restart:** the tunnel agent reconnects and re-registers. A restarted front rebuilds its registrations from reconnects and the control plane.
3. **Silent failure:** a path that is up with nothing behind it (the end-to-end probe), or a record left after its allocation ended (the inventory compared with the provider's job list).
4. **Provider limits:** outbound rules, login throttles, login-node process policies, and lease ends.
5. **Capacity:** path bandwidth and connection counts on the front. TBD after the spike.
6. **Idle cost:** one connection and periodic heartbeats per instance, plus one pooled login connection per relay cluster.
7. **Rollback:** close the paths. Instances keep running but take no traffic until a path returns.

## Drawbacks

- Every token makes an extra hop through the front's network.
- The front must be reachable from every center's compute network.
- Each relay cluster keeps a second, more fragile path kind alive.

## Alternatives

- **Login-node relays everywhere.** They work without outbound, but they are subject to login throttles and login-node process policies, and each relay is one more process to own.
- **Port forwards from the front.** Simple for one instance. In bulk they became the unowned tunnels listed in Motivation.
- **Instances listening on cluster interfaces.** Reachable only inside the cluster, and exposed to every user on it.
- **A center-provided ingress.** Where a center offers one, it may be the best path. Whether any target center does is not known yet.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #6.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).

## Amendments

None.
