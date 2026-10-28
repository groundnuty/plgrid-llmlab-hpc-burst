# HBEP-5: LLMLab-compatible front

## Summary

The front is the one endpoint a user points their client at. It speaks PLGrid LLMLab's OpenAI-compatible contract, so a client written for LLMLab changes only its base URL. Behind it, the front routes each request by the naming levels of HBEP-3, to LLMLab or to the framework's own instances on batch providers. This HBEP decides which parts of LLMLab's contract the front copies and which it does not. It also covers cache affinity across two levels, draining before a lease ends, and the rules for time-outs, retries, caps, attribution headers and authentication.

## Motivation

This HBEP serves stage 5 of the user story (HBEP-2): serve through one endpoint that aggregates LLMLab and our own deployments. It also carries stage 6 (know what answered) on every response. And it is where stage 3's handovers become invisible to clients.

Problems a front must not repeat:
- A router's default total request deadline cut long streaming replies with no log line. Every reply longer than the deadline failed.
- A managed gateway ends any reply that sends no byte for a fixed time, so slow non-streaming requests fail.
- A gateway that injects prompt text, or allowlists request fields, makes its deployment differ from a direct one with the same checkpoint.
- Pure load balancing lost prefix-cache hits on long multi-turn sessions and cut throughput two to three times. Pure affinity piled sessions on one worker while others idled.
- A balancer ejected a backend after a few failures and never re-admitted it. Its upstream time-out was below the client's.
- Requests stayed on a saturated worker until they timed out, and no response said which worker answered.
- Only global caps existed, so one server ran out of KV cache while others idled.
- A router's admin API, which adds and removes workers, had no authentication.
- An instance that reached its lease's hard end took its in-flight requests with it, unless someone had removed it from the pool by hand.

### Goals

- A client written for LLMLab works against the front with no change beyond its base URL (and its key, if Open question 8 requires one).
- Every response, streamed or not, success or error, names what produced it: for our instances, the deployment and instance; for a managed provider, what that provider exposes; for an error the front raises before it chooses an Instance, the front itself, with no deployment or Instance (HBEP-9).
- A conversation stays where its cache is warm, and moves only when load or a lease end requires it.
- No hop between client and engine imposes a total deadline below the client's.
- No request starts on an instance after its drain begins, so a lease end the front could see coming cuts nothing.
- Admin operations require a key.

### Non-goals

- Copying LLMLab's quirks (see Notes).
- Rewriting prompts, completions or tool calls. Per-model parser settings belong to the serving spec, so they are part of the deployment ID (HBEP-3).
- Starting, verifying or releasing instances (HBEP-7). The front routes only to instances the controller has marked ready and whose path is up (HBEP-6). For a managed provider, its catalog says what is ready.
- Opening paths to instances (HBEP-6).
- Per-user billing and quotas (stage 7).

## Proposal

### User stories

- A researcher's agent calls LLMLab's `example-27b`. They change the base URL to the front. Requests go to LLMLab while their own deployment queues, then spread over both once it serves.
- A researcher who needs one fixed configuration sends `example-27b@helios/<deployment-id>`. Through a handover within Helios, every response still names that deployment ID; only the Instance changes.
- An agent adds a deployment as a routing target with an admin key. A client key cannot.
- A reply streams for over an hour, or spends many minutes in prefill before its first byte. It completes.
- A user who sent plain `example-27b` reads the response headers to learn whether LLMLab or their own instance answered.

### Notes, constraints and caveats

The front copies LLMLab's contract:
- the OpenAI-compatible endpoints LLMLab serves, with their request and response shapes;
- LLMLab's model names, as the `model` level;
- `/v1/models`;
- streaming;
- usage reporting;
- the error codes LLMLab returns for an inactive model.

It deliberately does not copy:
- the no-byte cut (LLMLab ends a reply that sends nothing for 180 s);
- prompt text injected by the gateway;
- the request-field allowlist. Fields the front does not know pass through to the engine.

Routes to LLMLab still pass through LLMLab's own cut and injection. The front cannot remove them; it can only report which provider answered.

Keys are required on the front, at least for admin operations such as adding and removing models.

### Risks and mitigations

- **A hop's deadline cuts long replies.** Every hop's total deadline is either absent or above the client's. A test streams past each known default, including vllm-router's `--request-timeout-secs`.
- **An idle time-out cuts a slow first token.** Idle time-outs on every hop exceed the deployment's measured worst time to first token.
- **Keep-alive mismatch.** An engine's short keep-alive dropped connections that a router reused. On each link, the side that reuses connections must have the shorter idle time-out.
- **Affinity piles return.** Bounded loads skip any target above its bound. The load measure must count cut and abandoned requests, because a counter that misses them lets piles re-form.
- **Mixed serving under `model`.** LLMLab and a direct deployment of the same checkpoint answer differently. Attribution headers show which answered. Pinned names exclude the other.
- **A saturated instance holds requests.** Per-instance caps; requests over the cap wait at the front, not on the instance.
- **Retries multiply harm.** A request that crashes an engine, sent again, can crash the next one. Retries need a budget and a guard (Open questions).
- **Ejected backends never return.** Membership comes from the controller's ready set (HBEP-7), read by watch. Failure counters may pause a target but never remove it for good.
- **The drain window is wrong.** Too short cuts in-flight requests at the lease end; too long wastes lease time. Candidate: the front's maximum request duration plus a margin, as a provider-profile parameter (HBEP-4).
- **The admin surface is exposed.** All admin goes through the authenticated front. Instance-side admin endpoints are reachable only inside the instance (HBEP-6).
- **A stream is cut and looks complete.** The front counts a stream that ends without a finish reason as truncated, and reports it (HBEP-8).
- **The front is a single point of failure.** Open question 11.

### Open questions

1. **Conversation key.** Which session headers the front honours, and which part of the opening it hashes: the system prompt plus first user message, or a fixed token prefix. Decided by measuring prefix-cache hit rates on a synthetic multi-turn workload.
2. **The inner router.** Each Instance has its own router, and pooling workers across Instances is not part of this design (HBEP-3, open question 11). Is that router vllm-router, our own, or a component from the evaluated projects (Gateway API Inference Extension, llm-d, the vLLM production stack)? Candidate: the front passes its conversation key downstream as a session header, so a hashing router gets one even when the client sends none. Decided by that evaluation.
3. **Load measure and bound.** In-flight requests, sessions, or KV use; KV percentage does not compare across engines and attention backends. And the bound factor. Measurement.
4. **Choice under `model`.** Weight by capacity, prefer our own, or spill to LLMLab only at our caps. Whether managed providers are in `model` by default, and whether a conversation may move between providers. Owner decision.
5. **Retries.** Candidate: retry only before the first byte reaches the client, to a different instance, within a per-deployment budget. A request during which two instances have died is never sent again. Spike with fault injection.
6. **Caps.** In-flight caps per deployment, per instance and per worker, from measured capacity, updated when instances join. Over the cap: a bounded wait, then 429 or 503 with `Retry-After`. LLMLab's behaviour decides the code.
7. **Attribution headers.** Names and fields; how the worker is named when the inner router does not report it; and what a managed provider's replies carry, since its revisions and settings cannot be read. Does `model@llmlab/<deployment-id>` exist? HBEP-9 owns the fields and HBEP-3 the names; this HBEP carries them.
8. **Authentication.** Whether inference also needs a key. Whether the front forwards a client's own LLMLab key or uses one it holds. Owner decision, with HBEP-9.
9. **Errors and listing.** LLMLab returns alike 400s for unknown, inactive and blocked models. Keep the status codes and add a reason field to the body? Should `/v1/models` list every naming level and show state (ready, draining)? Decided by testing LLMLab clients.
10. **Mid-stream failure and keep-alive.** Close with an error event, or resume elsewhere? Send comment lines during long prefill? Spike with common OpenAI clients.
11. **Front replicas.** How affinity state survives a front restart, and how streams survive a replica's loss. With HBEP-11.

## Design details

**Contract.** As listed in Notes. Extensions are additive: response headers, and extra fields in error bodies. Nothing an LLMLab client sends is rejected for being unknown.

**Routing.** A request's `model` field is resolved by the naming levels (HBEP-3):
- `model` matches any ready instance of any deployment of that model, LLMLab included;
- `model@provider` matches those on that provider;
- `model@provider/<deployment-id>` matches the instances of that deployment only.

A name with no ready instance returns the inactive-model error. It never widens to a broader level. The error's attribution names the front as the emitting component, with the deployment and Instance fields absent (HBEP-9).

**Two-level affinity.** The front picks the deployment and the Instance by a conversation key: a client session header, or a hash of the conversation's opening. It uses consistent hashing with bounded loads: a key maps to a point on a ring of eligible targets, each target one Instance, and a target above its bound passes the key to the next one on the ring. So the front always knows which Instance answered. Inside the Instance, its own router picks a worker (HBEP-3); how it picks is Open questions 2 and 3.

**Draining.** Drain rule (the same text in HBEP-3, HBEP-5 and HBEP-7): The framework cannot move a lease end, so an Instance starts draining at the latest at its hard end minus the drain window, whether or not its replacement is ready. Make-before-break is the goal: the controller plans the replacement to be ready before that drain start, and the old Instance never drains before its replacement is ready unless that deadline forces it. If the replacement is late, the old Instance still drains on time, capacity drops, and an event is emitted (HBEP-8).

The front enforces the deadline itself. It reads each Instance's lease end from the control plane, and from the lease end minus the drain window it sends the Instance no new requests, from new conversations or old ones, even if the controller has not marked it `draining`. A drain before that deadline, once the replacement is ready, is the controller's decision (HBEP-7). Keys move to the next target on the ring, and in-flight requests finish.

**Time-outs.** No hop sets a total deadline below the client's. The front's maximum request duration is a published setting, at or above the longest deadline a supported client sets. Inner hops have none, or a longer one. Every cut is logged with the hop that made it.

**Attribution and keys.** Every response from an Instance names its deployment and Instance. An error the front raises before it chooses an Instance names the front, with no deployment or Instance (fields per HBEP-9). Admin operations need a key. Retries, caps and header names: TBD (Open questions 5–7).

### Test plan

- An unmodified OpenAI client, configured as for LLMLab, runs: chat, streaming, usage, tool calls, `/v1/models`, unknown and inactive models.
- Unknown request fields reach the engine unchanged. Prompt token counts match a direct call to the same instance, proving no injected text.
- A stream outlives every known default deadline. A reply sends no byte for longer than LLMLab's cut. Both complete.
- Affinity under a skewed key distribution: no target exceeds its bound. Prefix-cache hit rate is compared with round-robin.
- An Instance with a near lease end and no ready replacement stops receiving requests at its lease end minus the drain window, although the controller has not marked it `draining`. No request is cut at the end, and an event reports the lost capacity.
- An Instance that the controller marks `draining` earlier, once its replacement is ready, receives no new request after that.
- Attribution headers appear on successes, errors, streams and LLMLab-served replies. An unknown model, a failed authentication and a pinned name with no ready Instance each return an error naming the front, with no deployment or Instance.
- An admin call without a key is refused.
- Fault injection: kill an instance mid-stream; saturate one instance; flap one until the controller marks it ready again.

### Compatibility and upgrades

- Clients written for LLMLab are the compatibility target; extensions are additive.
- Stored state: none of its own. Membership and lease ends come from the control plane.
- Mixed versions during an upgrade, and downgrade: TBD with Open question 11.

### Security

- Client and admin keys live in the control plane's secret store (HBEP-11) and are never logged.
- The front is the only component reachable from outside. Instances are reachable only through their paths (HBEP-6).
- Prompts and responses: TBD. Candidate: the front stores no bodies, and logs request IDs, names, sizes, timings and status only.

### Graduation criteria

TBD. Candidate for alpha: the test plan passes against LLMLab and one batch provider.

## Operational readiness

1. **Working:** the front's health endpoint plus the age of its membership view; end to end, a synthetic request per naming level returns the expected attribution headers. A failed synthetic request, or a response without headers, says it is not working.
2. **Restart:** it re-reads the ready set and lease ends from the control plane. Streams through it are cut: TBD (Open question 11).
3. **Silent failure:** a cut stream that looks complete (counted by finish reason), or a request answered by a wider level than asked (seen in the headers).
4. **Provider limits:** lease ends (draining), and LLMLab's no-byte cut and model flapping on routes to LLMLab.
5. **Capacity:** TBD (Open question 6).
6. **Idle cost:** one small process, plus one path per instance (HBEP-6).
7. **Rollback:** TBD.

## Drawbacks

- One more hop: added latency and one more place to fail.
- The front must follow LLMLab's contract as it changes.
- Two affinity levels mean two sets of parameters to tune.

## Alternatives

- **One URL per instance, chosen by the client.** It breaks at every handover and gives no attribution.
- **A stock router at the edge.** It has no naming levels, no LLMLab route and no lease-end drain, and its default deadline cuts long streams.
- **Pure load balancing or pure session affinity.** Both failed on long multi-turn sessions (Motivation).
- **Gateway API Inference Extension or llm-d.** Both are built around Kubernetes pods. To be evaluated later; either may become a component.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #5.

## Amendments

None.
