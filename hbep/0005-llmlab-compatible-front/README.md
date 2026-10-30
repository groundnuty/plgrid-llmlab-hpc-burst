# HBEP-5: LLMLab-compatible front

## Summary

The front is the one endpoint a user points their client at. It speaks PLGrid LLMLab's OpenAI-compatible contract, so a client written for LLMLab changes only its base URL. Behind it, the front routes each request by the naming levels of HBEP-3, to LLMLab or to the framework's own instances on batch providers. Plain `model` reaches only the providers the operator attached to it. This HBEP decides which parts of LLMLab's contract the front copies and which it does not. It decides the conversation key, the bounded-load choice of an Instance and its default bound, what the front needs inside an Instance (one worker in alpha, a router in front of several in beta), draining before a lease ends, and the rules for time-outs, retries, caps, errors, attribution headers, usage records and authentication.

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

- A client written for LLMLab works against the front with no change beyond its base URL. For that, the operator may set the front's client key to the user's LLMLab key.
- Every response, streamed or not, success or error, names what produced it: for our instances, the deployment and instance; for a managed provider, what that provider exposes; for an error the front raises before it chooses an Instance, the front itself, with no deployment or Instance (HBEP-9).
- A conversation stays where its cache is warm, and moves only when load or a lease end requires it.
- No hop between client and engine imposes a total deadline below the front's maximum request duration.
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

- A researcher's agent calls LLMLab's `example-27b`. The researcher attaches the LLMLab provider to `example-27b`, one setup step, and changes the agent's base URL to the front. Requests go to LLMLab while their own deployment queues. Once it serves, they go to it, and spill to LLMLab when its Instances are full.
- A researcher who needs one fixed configuration sends `example-27b@helios-a/<deployment-id>`, where `helios-a` is the name the operator gave its provider on Helios. Through a handover within that provider and accelerator class, every response still names that deployment ID; only the Instance changes.
- An agent attaches a provider to a model name, or adds a deployment as a routing target, with an admin key. A client key cannot.
- A reply streams for over an hour, or spends many minutes in prefill before its first byte. It completes.
- A user who sent plain `example-27b` reads the response headers to learn whether LLMLab or their own instance answered.

### Notes, constraints and caveats

The front copies LLMLab's contract:
- the OpenAI-compatible endpoints LLMLab serves, with their request and response shapes;
- LLMLab's model names, as the `model` level;
- `/v1/models`;
- streaming;
- usage reporting;
- the error codes LLMLab returns for an inactive model: 503 (L-6.5).

It deliberately does not copy:
- the no-byte cut (LLMLab ends a reply that sends nothing for 180 s, with a 504; L-6.1);
- prompt text injected by the gateway;
- the request-field allowlist. Fields the front does not know pass through to the engine.

Routes to LLMLab still pass through LLMLab's own cut and injection. The front cannot remove them; it can only report which provider answered.

**Plain `model`.** It resolves only over the providers the operator attached to that model name, by name or by label selector (HBEP-3). No provider is attached by default, LLMLab included: LLMLab is a platform of the Cyfronet organization, and a provider on it is a provider like any other. A user switching from LLMLab who wants requests to keep reaching it attaches the LLMLab provider explicitly, as one setup step.

**Routing policy is the ModelDeployments** (open question 4). The front needs no kind of its own.
- A model name's routing targets are the ModelDeployments whose `spec.model` is that name. A provider is attached to the name when such a ModelDeployment allows it, by name or by label selector (`spec.providers`, HBEP-11).
- Attaching the LLMLab provider is creating a ModelDeployment with a managed source (`spec.source.managed`: the platform and the catalog name) that allows that provider. The lifecycle controller starts nothing for it (HBEP-4); the front routes to the catalog entry while the Provider's status lists it.
- An admin operation is a write to these objects through the Kubernetes API, under the writer's own identity and RBAC (HBEP-11). That identity's credential is the admin key. The front's client key reaches no admin operation, and the front has no admin endpoint in alpha.

**Keys.**
- Clients send the front's client key. The operator sets it to any value. It may equal the user's LLMLab key, so that switching is only a change of base URL.
- For an LLMLab provider, the front calls LLMLab with that provider's configured upstream key, which may be the same key.
- Toward an Instance, the front uses that Instance's own key, generated when the Instance starts and kept in its identity package (HBEP-9). The front reads it from the mode-600 file the lifecycle controller keeps per Instance UID (HBEP-7).
- Admin operations, such as attaching providers and adding and removing models, need an admin key: a Kubernetes identity with the right role (above).

### Risks and mitigations

- **A hop's deadline cuts long replies.** Every hop's total deadline is either absent or above the front's maximum request duration. The router that beta adds, vllm-router, defaults `--request-timeout-secs` to 1,800 s and cut a stream at 1,800.0 s after 1,794 tokens in S2 phase A; at 10,800 s the same stream ran 7,300 tokens in 7,324 s. A test streams past each known default.
- **An idle time-out cuts a slow first token.** Idle time-outs reset on every byte and exceed the deployment's worst time to first token. S2 phase A passed a first token at 1,100 s through both routers; the front's idle time-out is 30 min.
- **Keep-alive mismatch.** An engine's short keep-alive dropped connections that a router reused (L-2.5). On each link, the side that reuses connections has the shorter idle time-out (Time-outs and keep-alive).
- **Affinity piles return.** Bounded loads skip any target above its bound, and the cap stops a pile at full membership (S10b, S10c). The load measure must count cut and abandoned requests, and stop counting requests that no longer compete. The once-guarded release and the drain release do both (S10).
- **Mixed serving under `model`.** LLMLab and a direct deployment of the same checkpoint answer differently. It happens only when the operator attached both providers to the name, and in alpha LLMLab takes only what our own Instances cannot (open question 4). Attribution headers show which answered. Pinned names exclude the other.
- **A saturated instance holds requests.** Per-Instance caps at full membership; requests over every cap wait at the front, not on the instance (Load bound and cap).
- **Retries multiply harm.** A request that crashes an engine, sent again, can crash the next one. In alpha the front sends a request again only when no byte of it reached an upstream (open question 5).
- **Ejected backends never return.** Membership comes from the controller's ready set (HBEP-7), read by watch. Failure counters pause a target for a fixed time and never remove it for good.
- **The drain window is wrong.** Too short cuts in-flight requests at the lease end; too long wastes lease time. The window is the profile's `drainWindow` (HBEP-4), read by the front and the lifecycle controller (HBEP-7). This HBEP sets it to the maximum request duration plus a margin: 2 h 10 min by default (open question 12). A 48 h lease then drains for its last 4.5%.
- **The admin surface is exposed.** All admin goes through the Kubernetes API with RBAC. Instance-side admin endpoints are reachable only inside the instance (HBEP-6).
- **vLLM workers listen off loopback** (a risk for HBEP-6). In S2 phase B each worker had its API port on 127.0.0.1 and six more TCP listeners on the node's cluster address, although `--host 127.0.0.1` and `VLLM_HOST_IP=127.0.0.1` were set. The serving port needs the per-Instance key. The other listeners are engine internals, which no key covers; an Instance on a shared cluster network exposes them unless the provider's network isolates it.
- **Beta: the router's admin API is open on loopback** (L-4.9). On a shared node another user's job can reach vllm-router's add and remove calls. Mitigation: the allocation agent compares the router's worker list with its own at every report and restores it, the same check that re-registers workers after a router restart (Inside an Instance). A difference raises an event.
- **Beta: the router re-serialises the body.** vllm-router 0.1.16 parses a chat request into a typed struct that keeps unknown fields (`#[serde(flatten)] other`, `src/protocols/spec.rs:550` at commit db02f35) and sends it again as JSON. Unknown fields survive, but not their bytes, so the test plan checks fields and values at the engine, and prompt token counts.
- **A stream is cut and looks complete.** The front never sends `[DONE]` for a stream it did not see finish. It counts a stream that ends without a finish reason as truncated, and records it (HBEP-8).
- **The front is a single point of failure.** One front in alpha (open question 11).

### Open questions

1. **Conversation key.**
   - **Decided** (S10, PR #34): the client's session header when it sends one, else the hash of the conversation's opening, the system prompt plus the first user message. In S10's tight regime (working set 1.04 times the cluster's KV cache, means of seeds 1–3) CHWBL at c = 2.0 reached 82.5% prefix-cache hit rate on the opening hash and 82.1% on header-then-opening, against round robin's 43.4%. The header alone, with no fallback, reached 46.2–47.5%. Keys over the start of the request (KubeAI's first 100 characters, a fixed 2,048-token prefix, the system prompt alone) put 15–36% of conversations on one key, and KubeAI's default reached 45.8% at 0.88 times round robin's throughput.
   - **Decided:** the honoured session header is `x-session-id`, the one S2 phases A and B ran through both routers. More names are a setting (`sessionHeaders`). The exact bytes hashed are under Conversation key.
2. **The inner router.**
   - **Decided for alpha** (HBEP-7, its alpha scope): an Instance has one worker and no router. The front sends requests through the Instance's serving path, which the allocation agent forwards to the engine's Unix socket (HBEP-7). The front still sets `x-session-id` on every request to an Instance, so the header is in place when routers arrive. The worker's health is the Instance's health.
   - **Decided, as the provisional default for beta's Instances with several workers** (S2 phase B, PR #55): vllm-router 0.1.16 with `consistent_hash` on `x-session-id`. With each worker's cache capped, it reached a hit rate of 0.902 in both runs, against round robin's 0.635 and 0.654, with a reuse bound of 0.905–0.906. Every session stayed on one worker, and all four workers got sessions. llm-d's shipped scorers sent each agent type to one worker: the busiest worker took 2.17 times the mean, and one worker took none. vllm-router's `cache_aware` scored 0.642, because 0.1.16 routes a chat request on `session_params.session_id` or nothing (`src/protocols/spec.rs:563-571`).
   - Three conditions hold for it (Inside an Instance): the front sets `x-session-id` on every request; `--request-timeout-secs` is above the front's maximum request duration; and the allocation agent re-registers workers after every router restart, because vllm-router forgets them (L-4.8; S2 phase A).
   - **Open (blocks beta):** S2 phase B ran four workers sharing one GPU, which keeps the hit-rate comparison but understates time to first token and the cost of an unbalanced router. Spike S2c (issue #62), one worker per GPU, validates hit rate, balance across workers and latency. Until then the default is provisional.
   - **Not needed for alpha:** llm-d's router is the candidate for multi-node Instances, with a scorer weighting that does not let a shared system prompt decide the worker (S2 phase B).
3. **Load measure and bound.**
   - **Decided** (S10, S10b PR #50, S10c PR #53): in-flight requests, counted from dispatch to completion, under KubeAI's reading of the bound. An Instance takes a request while its load before it is at most c × (T + 1) / n, with T the requests in flight and n the members.
   - **Decided:** the default is S10c's option 5, chosen by the HBEP-5 owner on 30 Oct 2026 (PR #53 comment). While at least the desired number of Instances is ready, c = 1.25 with a hard per-Instance cap; while fewer are ready, c = 2.0 with no cap. Option 5 peaked at 46, 48 and 48 in flight in the tight, scarce and half-prefill regimes against an engine limit of 48, with 0 s above it. c = 2.0 alone peaked at 75, 69 and 76 (S10b). After an unreplaced drain option 5 held 62.5% and 57.8% hit rate in the 0–5 and 5–15 min windows, against 22.3% and 17.5% for c = 2.0 with a cap that stays on. The same switch with the cap kept fell to 37.5% and 21.9%, which is why the cap drops with the bound. The cap moved 9 and 156 of 62,534 and 76,946 placements at full membership.
   - S10b and S10c switch at "ready ≥ desired". This HBEP keeps that reading, so the overlap of a handover, when both Instances serve, keeps the tight bound.
   - c is a per-deployment parameter (`spec.routing.loadBound`).
4. **Choice under `model`.**
   - **Decided for alpha, a science-agent proposal of 30 Oct 2026 that needs the owner's word before merge (HBEP-1):** routing policy is the ModelDeployments of the name (Notes). Our own deployments come first. An attached managed provider takes a request only when no Instance of ours can: none is routable, or every one is at its cap. A conversation then moves to the managed provider and returns at its next turn once its home has room. Among several of our own ModelDeployments, the key's home on one ring over all their routable Instances names the ModelDeployment (Request path). Source of the spill rule: HBEP-7, open question 10 (owner, 29 Oct 2026, PR #14): no spill to a provider that is not attached.
   - **Open (blocks beta):** weighting by measured capacity, and whether LLMLab should share load while our Instances have room.
5. **Retries.**
   - **Decided for alpha** (science-agent decision, 30 Oct 2026; the guard HBEP-7's poison-request risk relies on): the front sends a request again only when no byte of it was written to the upstream: a refused or timed-out connection, or a reused idle connection closed before the write. It sends it once more, to the next Instance on the ring, or on a fresh connection to the same one. A request that reached an engine is never sent again; the client sees its error.
   - **Open (blocks beta):** retrying before the first byte reaches the client, to another Instance, within a per-deployment budget, never after two Instances have died under one request. A spike with fault injection decides.
6. **Caps.**
   - **Decided** (S10c, open question 3): one cap per Instance, in force only at full membership. Its value is the engine's maximum concurrent sequences times the Instance's workers (Load bound and cap). Over the cap, a request goes to the next Instance on the ring. When every candidate is at its cap, it waits at the front in a FIFO for at most `queueTimeout`, then gets 503 with `Retry-After` (Errors).
   - **Not needed for alpha:** per-deployment and per-worker caps. Workers are the inner router's and the engine's.
   - **Open (blocks beta):** the status LLMLab returns when its shared capacity binds (L-6.3). Alpha returns 503.
7. **Attribution headers.**
   - **Decided for alpha, a proposal that needs the owner's word before merge** (HBEP-9 leaves header names to the owner, since they are hard to change): the `x-hpcburst-*` headers under Attribution headers, and the usage log as the attribution log. No body field carries attribution; the engine fingerprint stays in `system_fingerprint` (HBEP-9).
   - **Not needed for alpha:** naming the worker. An alpha Instance has one; in beta vllm-router does not report which worker answered, and the Instance is the unit HBEP-3 attributes to.
   - **Decided:** the pinned form exists for a managed provider (HBEP-3, open question 2; HBEP-4): `model@<provider>/<deployment-id>` with the catalog entry's managed ID resolves while the Provider's status lists that entry.
8. **Authentication.** **Decided** (owner decision): clients send the front's client key, which the operator sets and may set to the user's LLMLab key. The front calls LLMLab with the provider's configured upstream key, and an Instance with that Instance's own key (Notes). Admin is the Kubernetes API's (Notes).
9. **Errors and listing.**
   - **Decided for alpha** (science-agent decision, 30 Oct 2026, from L-6.5): keep LLMLab's status codes, 503 for an inactive model and 400 for a malformed request, and add a `code` and a `detail` to the error body that tell the causes apart (Errors).
   - **Decided for alpha:** `/v1/models` lists each plain model name that has a ModelDeployment, with an additive `routable` field.
   - **Not needed for alpha:** listing the qualified levels and their states.
10. **Mid-stream failure and keep-alive.**
    - **Decided for alpha** (science-agent decision, 30 Oct 2026, from L-6.7): a stream that fails after its headers were sent ends with one error event and no `[DONE]`. Nothing resumes it elsewhere, since no engine can continue another's generation.
    - **Not needed for alpha:** comment lines during long prefill. No hop between client and engine in alpha has an idle time-out below 30 min, and LLMLab's 180 s cut sits upstream of the front, where comments from the front cannot reset it.
11. **Front replicas.** **Not needed for alpha:** alpha has one user and one front (HBEP-11, open question 8). A restart cuts the streams through it and rebuilds the rest from the control plane (Operational readiness 2). **Open (blocks beta):** replicas for the team option, with HBEP-11's open questions 3 and 6.
12. **The drain window and the maximum request duration.**
    - **Decided** (science-agent decision, 30 Oct 2026): `maxRequestDuration` is 2 h by default, the deadline www-2027 raised its router to (L-4.1) and the stream length S2 phase A carried with no router and through each configured router. `drainWindow` must be at least `maxRequestDuration` plus `drainMargin` (10 min), so its default is 2 h 10 min. In beta, vllm-router's `--request-timeout-secs` is `maxRequestDuration` plus 1 h, 10,800 s at the defaults, the value S2 ran.
    - HBEP-4's default of 30 min was a placeholder "until this HBEP sets it"; it changes to 2 h 10 min. A platform that needs a shorter window lowers both values.
13. **Reachability.**
    - **Decided:** Instance addresses come from the inventory (HBEP-11). In alpha the front reaches an Instance through the dial-home path HBEP-11 requires, direct or through ssh on port 22. Its dial-home listener also passes `/agent/v1/` through to the lifecycle controller (HBEP-7, HBEP-11).
    - **Not needed for alpha from this HBEP's side:** HBEP-6's tunnel protocol, encryption inside the tunnel, relays, port forwards and heartbeat timing. The front needs one seam: an HTTP transport per Instance UID, from that Instance's registration (Reachability). The Instance route runs end to end only once HBEP-6 picks its protocol for alpha.

## Design details

### Contract

As listed in Notes. Extensions are additive: response headers, extra fields in error bodies, and the `routable` field in `/v1/models`. Nothing an LLMLab client sends is rejected for being unknown.
- **Endpoints.** Every `POST` under `/v1/` with a JSON body is routed by its `model` field: `/v1/chat/completions`, `/v1/completions`, `/v1/embeddings` and any other OpenAI path the target serves. `GET /v1/models` is answered by the front.
- **The one change to a request body.** The front replaces the value of the top-level `model` field, in place, with the name the target serves: the ModelDeployment's `spec.model` for an Instance, the name its worker serves (`--served-model-name`; HBEP-7's qualification checks the served model names), and the catalog name for a managed entry. Every other byte of the body is passed unchanged. Responses pass unchanged.
- **Request headers.** The front removes the client's `Authorization` and hop-by-hop headers, adds the target's key, and toward an Instance sets `x-session-id` (Conversation key). Other headers pass unchanged.

### Listeners

- **The client listener** (`clientListen`) serves the contract. It never serves `/agent/v1/`, and answers 404 there.
- **The dial-home listener** (`dialHomeListen`) is the dial-home port of HBEP-11's open question 8. It presents the certificate the launcher created for the lifecycle controller's agent endpoint, so the agent's pin holds on both paths (HBEP-7, HBEP-11).
  - **`/agent/v1/` passthrough.** In direct mode the front passes every request under `/agent/v1/` through, unchanged, to the lifecycle controller's loopback endpoint, and streams the answer back, long polls included. The controller authenticates the agent by the Allocation's credential (HBEP-7); the front adds nothing. In ssh mode the agent reaches that endpoint through its forward, and the passthrough is unused.
  - Every other path on the dial-home listener belongs to HBEP-6's tunnel.

### Request path

1. **Authenticate.** The `Authorization: Bearer` key must equal one of the client keys in `clientKeysFile` (mode 600, HBEP-11 Credentials). Each entry has an ID, which usage records carry; the key itself is never logged. Otherwise 401.
2. **Parse the name** by HBEP-3's grammar: split at the last `@`; `model`, `model@<provider>` or `model@<provider>/<deployment-id>`. A grammar error, an unknown provider or a malformed ID is a 400 and never falls back to a plain model (HBEP-3).
3. **Resolve the candidates.** The ModelDeployments whose `spec.model` is the model name, in the namespaces the front watches. Then:
   - `model`: every routable target of those ModelDeployments;
   - `model@<provider>`: those on that provider only;
   - `model@<provider>/<deployment-id>`: those on that provider whose deployment ID has that display form. For a managed entry that is its managed ID.
   - A routable target of ours is an Instance with phase `ready`, the condition `AgentUnreachable` not true, not paused, before its drain start, with its transport registered (Reachability). A routable managed target is a catalog entry the Provider's status lists now, under a Provider whose `CredentialsRejected` condition is not true (HBEP-4).
   - No candidate: 503 `model_inactive` (Errors). It never widens to a broader level.
4. **Compute the conversation key** (Conversation key).
5. **Choose the ModelDeployment.**
   - Our own first. With one candidate ModelDeployment of ours, it is chosen. With several, the front places all their routable Instances on one ring, KubeAI's (256 points per Instance, xxhash of the Instance's name and the point's index), and the key's home on it names the ModelDeployment. That ring holds no counts; it only picks the deployment.
   - If the chosen ModelDeployment has no Instance below its cap while its cap is in force, the walk continues to the next ModelDeployment on the ring.
   - If none of ours can take the request, an attached managed target takes it: the key's home on a ring of the managed targets, with no bound.
   - If no managed target is attached, the request waits in the chosen ModelDeployment's front queue (Load bound and cap).
6. **Choose the Instance** with the ModelDeployment's balancer (Load bound and cap). A qualified or pinned name filters its walk.
7. **Send.** The `model` field is rewritten and the headers set. Toward an Instance the front adds `Authorization: Bearer <Instance key>` and `x-session-id: <conversation key>`. Toward a managed provider it adds the provider's upstream key and the provider's base URL, both from the Provider's credentials file (HBEP-4), and sets no `x-session-id` of its own.
8. **Return.** Status and headers come from the target, plus the attribution headers. A stream passes through event by event. The front parses each event only to read the first token's time, the finish reason and `usage`, and to see `[DONE]`.
9. **Record** one usage record (Usage records), and release the request's reservation exactly once.

### Conversation key

- **Session header.** When the request carries a non-empty header named in `sessionHeaders` (default `x-session-id`), the key is its value, unchanged.
- **Opening hash**, for a request with a `messages` array and no session header:
  - take the messages from the first up to and including the first one whose `role` is `user`, or every message when none is;
  - reduce each to an object with exactly its `role` and its `content`, as sent: a string, an array of parts, or `null`;
  - serialise that array by RFC 8785 (JCS), the scheme HBEP-3 uses, and hash the UTF-8 bytes with SHA-256;
  - the key is `o1-` followed by the 64 lowercase hex characters. `o1` names this rule, so a later rule is visible in logs.

  So several system or developer messages all count, and a conversation with no system message keys on its first user message. Tools, names and tool calls are not hashed. A client that rewrites its system prompt mid-conversation moves once (S10, Still unknown).
- **No key.** A request with neither, such as `/v1/completions` or `/v1/embeddings` without the header, gets a fresh key per request: `r-` and 32 random hex characters. It has no affinity, as S10's `header>none` key, and is still counted.
- **One key, both levels.** The same string is the key on the front's ring and the `x-session-id` the front sends to the Instance. The front sets it on every request to an Instance, in alpha too, so the header is in place when beta's routers route on it with `consistent_hash`. Without it, vllm-router hashed every chat request to one worker in S2 phase A (0, 0 and 9 of 9 sessions).

### Load bound and cap

- **One balancer per ModelDeployment**, over its routable Instances: KubeAI's CHWBL group, copied unchanged under `third_party/forked/kubeai/` (commit `89dfff46`; `ORIGIN.md` there). The load is in-flight requests from dispatch to completion, waits at the Instance included.
- **The front's wrapper** around the group is product code, rewritten from S10's `spikes/s10/front`, S10b's ring replica and capped pick, and S10c's adaptive cap, with their tests ported. Spike code is not shipped (HBEP-1). It does four things:
  - **Serialises** selection and reservation with one mutex per ModelDeployment. Upstream checks and reserves under a read lock, so two selectors can pass on the same loads. S10's hook-forced test: loads 25, 18, 18 and 18 give a bound of 25, both selectors pass at 25, and the second reservation finds 26 against 25.3125 (L-4.3).
  - **Releases each request exactly once,** with a once-guard. Upstream's release decrements on every call; two calls left the total at −1 in S10's fork test.
  - **Releases at a leave.** When an Instance leaves the routable set, by drain start, `draining`, a pause, `AgentUnreachable` or deletion, the front removes it from the group and releases its open reservations at once. Their later completions call the guarded release and change nothing. Without this release, placements broke the bound in all 12 of S10's drain and handover runs at c = 1.25 in the tight regime, up to 75 in one run, and in 10 of 12 at c = 2.0.
  - **Applies the adaptive bound and cap** (open question 3), from the ModelDeployment's ready count against `spec.instances`, recomputed at every membership change:
    - ready ≥ desired: c = `loadBound.tight` (1.25), and the cap in force;
    - ready < desired: c = `loadBound.loose` (2.0), and no cap.
- **The cap's value** per Instance: `spec.routing.instanceCap` when set; otherwise `max-num-seqs` from the serving spec, else from the Instance's latest verification record (resolved settings, HBEP-7), times the Instance's workers. With neither, there is no cap for that Instance, and the front emits a `degraded` event with reason `CapUnknown` on its ModelDeployment.
- **The capped pick** (S10b): KubeAI's pick at c; if the choice already holds its cap, its reservation is released and the request goes to the next Instance on the ring that is below its cap. The spill is reserved through the group, so it counts in the bound's mean. When every Instance is at its cap, nothing is reserved and the request waits in the ModelDeployment's FIFO at the front. A completion, a join, or the cap going off places the oldest one. After `queueTimeout` it gets 503 `capacity`.
- **When the cap returns** with an Instance above it, nothing is cut. That Instance gets no new request until it is below the cap (S10c).
- **Filters.** For `model@<provider>` and pinned names, the walk skips Instances that do not match, and T and n stay those of the whole group. The walk uses the ring replica, checked against the fork's own picks as S10b's tests do.
- **Pauses.** A target whose connections fail `pauseAfterFailures` times in a row (3) leaves the routable set for `pauseDuration` (30 s), then returns. Nothing removes a target for good except the control plane (L-4.14).

### Inside an Instance

HBEP-7 runs the Instance's processes; this HBEP sets what the front needs from them.

**Alpha: one worker, no router** (HBEP-7's alpha scope; open question 2).
- The front sends each request through the Instance's serving path, which HBEP-6's transport reaches and the allocation agent forwards to the engine's Unix socket (HBEP-7).
- The engine checks the Instance key, which the agent passes to it through the environment (HBEP-7). The socket sits in the job's private temporary directory, so other users' jobs on the node cannot reach it.
- The request carries `x-session-id` (Conversation key). The single worker ignores it.
- The worker's health is the Instance's health. A refused connection or an error from the path counts toward the Instance's pause (Load bound and cap); HBEP-7's probe and restart ladder handle the worker.

**Beta: several workers behind a router.** The provisional default is vllm-router 0.1.16, `--policy consistent_hash`, until spike S2c (issue #62) validates it (open question 2).
- The router listens on loopback only, checked from `/proc` after start, as S2 phase B's job did. It is the Instance's entry point.
- The workers behind it get `VLLM_HTTP_TIMEOUT_KEEP_ALIVE=120`. vLLM v0.32.0 defaults that keep-alive to 5 s (`vllm/envs.py`), and the router keeps idle connections for 50 s (S2 phase A), so the worker must hold them longer (L-2.5). The framework sets it outside the serving spec, as it sets the listen address, so it is not a deployment-ID input (HBEP-3).
- The router copies every request header but `Content-Type`, `Content-Length` and trace headers to the worker (`src/routers/http/router.rs` at db02f35), so the worker still checks the Instance key the front sent.
- **The three conditions:**
  1. the front sets `x-session-id` on every request to an Instance (Conversation key);
  2. `--request-timeout-secs` is above `maxRequestDuration`: `maxRequestDuration` plus 1 h, 10,800 s at the defaults. The router's default 1,800 s cut a stream at 1,800.0 s with no log line (S2 phase A, L-4.1);
  3. the allocation agent re-registers the workers after every router restart, and compares the router's worker list with its own at every report. vllm-router keeps its workers in memory only: in S2 phase A a removed worker was back after a restart (L-4.8).
- **The router's health is the Instance's health** (S2 phase A). For llm-d's EPP, TERM let the open stream finish (150 of 150 tokens) while new requests got HTTP 500, and TERM with KILL after 10 s cut the stream at 40 s. So a router is stopped only once its Instance is out of the routable set and has no request in flight; KILL then follows the polite stop (L-3.3). S2 did not test vllm-router under TERM.
- llm-d's router is the candidate for multi-node Instances (open question 2).

### Draining

Drain rule (the same text in HBEP-3, HBEP-5 and HBEP-7): The framework cannot move a lease end, so an Instance starts draining at the latest at its hard end minus the drain window, whether or not its replacement is ready. Make-before-break is the goal: the controller plans the replacement to be ready before that drain start, and the old Instance never drains before its replacement is ready unless that deadline forces it. If the replacement is late, the old Instance still drains on time, capacity drops, and an event is emitted (HBEP-8).

The front enforces the deadline itself. It reads each Instance's lease end from the control plane (`status.leaseEnd`, HBEP-11), and from the lease end minus the drain window it sends the Instance no new requests, from new conversations or old ones, even if the controller has not marked it `draining`. A drain before that deadline, once the replacement is ready, is the controller's decision (HBEP-7). Keys move to the next target on the ring, and in-flight requests finish.
- **The window** is the `drainWindow` of the PlatformProfile of the Instance's Provider. A later lease end, once observed, moves the drain start; nothing plans for one (HBEP-3).
- **The clock** is the front host's, in UTC.
- **The release.** At drain start the Instance leaves its balancer group, and its open requests are released from the count (Load bound and cap).
- **The end.** A request still running when the lease ends is cut. The front ends that stream with an error event and records it as cut by the lease end.
- **The check.** At start, and at every PlatformProfile change, the front checks that each profile with a batch Provider has `drainWindow` ≥ `maxRequestDuration` + `drainMargin`. A profile that fails gets a `degraded` event naming it, with reason `DrainWindowShort`, and its Instances' long requests can be cut at the lease end.

### The LLMLab route

- **Target.** A managed ModelDeployment's catalog entry, under an attached managed Provider. Its base URL and upstream key come from the Provider's credentials file (HBEP-4).
- **Request.** `model` is rewritten to the catalog name, the client's key is replaced by the upstream key, and everything else passes unchanged. The front adds no `x-session-id` and no other field.
- **Responses and errors** pass through unchanged, status and body. Attribution names the Provider and the entry's managed ID, with emitter `provider`. So LLMLab's 504 after 180 s with no byte (L-6.1), its 503 for an inactive model and its 400s (L-6.5), and a 401 for an expired key (L-6.4) reach the client as LLMLab sent them. A 401 or 403 also sets nothing in the front; the Provider's controller sets `CredentialsRejected` from its own catalog read (HBEP-4).
- **Limits.** No ring bound and no cap: LLMLab's capacity cannot be read. Its requests are counted in flight for the usage records only. No retry, beyond a connection that no byte reached (open question 5).
- **Stalls** (L-6.2) and the no-byte cut stay LLMLab's. In alpha they reach only the requests our own Instances could not take (open question 4).

### Errors

Every error the front raises has the body `{"error": {"message": <text>, "type": "hpcburst_error", "code": <code>, "param": <field or null>, "detail": <detail or null>}}` and the attribution headers. An error from a target passes through unchanged.

| Case | Status | `code` | `detail` | Names |
|---|---|---|---|---|
| No client key, or a wrong one | 401 | `invalid_api_key` | | the front |
| Body is not JSON, or has no string `model` | 400 | `invalid_request` | | the front |
| The name breaks HBEP-3's grammar, or names an unknown provider | 400 | `invalid_model_name` | `grammar` or `unknown_provider` | the front |
| No ModelDeployment has the model name, so no provider is attached | 503, `Retry-After: 30` | `model_inactive` | `not_attached` | the front |
| Attached, with no routable target at that level | 503, `Retry-After: 30` | `model_inactive` | `no_ready_target` | the front |
| Every candidate at its cap past `queueTimeout` | 503, `Retry-After: 10` | `capacity` | | the front, and the ModelDeployment |
| No byte reached the target, after the one resend | 502 | `upstream_unreachable` | | the front, and the target it tried |
| `maxRequestDuration` reached before the first byte | 504 | `request_timeout` | | the front, and the target |
| A stream fails after its headers were sent | one `data:` event with this body, then the end of the stream, never `[DONE]` | `stream_cut` | `upstream`, `deadline`, `idle` or `lease_end` | the target, as in the headers already sent |

An unknown model and an unattached one look alike, as on LLMLab, where unknown and inactive models both fail (L-6.5); `detail` tells them apart.

### Attribution headers

The proposal of open question 7, on every response, streamed or not, success or error:

| Header | Value | When |
|---|---|---|
| `x-hpcburst-request-id` | the front's request ID | always |
| `x-hpcburst-emitter` | `front`, `instance` or `provider` | always |
| `x-hpcburst-provider` | the Provider's name | once a target is chosen |
| `x-hpcburst-deployment-id` | the display form, 12 hex characters | once a target is chosen; a managed entry's managed ID |
| `x-hpcburst-instance` | the Instance's namespace and name, as `<namespace>/<name>` | an Instance of ours |
| `x-hpcburst-instance-uid` | the Instance's UID, which labels its verification records (HBEP-11) | an Instance of ours |

An error the front raises before it chooses a target carries only the first two, with emitter `front`. A response without the headers is a failure of the front (Operational readiness 3). Each response's usage record holds the same facts, keyed by request ID, so dropped headers lose nothing (HBEP-9).

### Usage records

One record per request, written when the request ends (HBEP-8).
- **Fields:** `counter`; `requestID`; `startTime` and `endTime` (the request's own times, UTC, millisecond RFC 3339); `clientKeyID`; `path`; `requestedModel`, as sent; `level` (`model`, `provider` or `pinned`); `provider`; `modelDeployment`; `deploymentID`; `instance` and `instanceUID`; `emitter`; `status`; `stream`; `finish` (the finish reason, or `truncated`, `cut` or `error`); `cutBy` (the hop that cut it, when one did); `promptTokens`, `cachedTokens` and `completionTokens`; `countsSource`; `firstTokenMs`; `durationMs`.
- **Counts** come from the response's `usage` when it carries one (`countsSource: engine`). A stream that the client did not ask to include usage carries none, so the counts are unknown (null), and the record holds the front's count of content events instead (`countsSource: front_events`). Nothing is added to the request to get counts.
- **Sink in alpha.** HBEP-11 has no durable log in alpha, so the front appends records to a JSON Lines file in `usageDir` on its host. It syncs each record to disk before the next, as HBEP-8 asks of its log (S8: `sync_interval: always`), and writes the records waiting behind a sync together in one write and one sync. So records are batched only when they arrive faster than the disk syncs.
- **Counter.** Each record carries a monotonic counter, persisted apart from the log. The front reserves counters in blocks of 1,000: it syncs the block's end to a counter file before it uses the first one. After a crash it starts at the persisted end, so lost records show as a gap (HBEP-8).
- **Privacy.** No prompt, response or key is in a record; `clientKeyID` names the key (HBEP-8, HBEP-9).

### Time-outs and keep-alive

| Hop | Total deadline | Idle time-out | Connection reuse |
|---|---|---|---|
| client → front | `maxRequestDuration` (2 h), set by the front | none shorter than the total | the front keeps idle client connections `clientKeepAlive` (120 s), longer than clients' pools |
| front → Instance (alpha: its engine, through the path) | `maxRequestDuration` | `idleTimeout` (30 min), reset by every byte | the front drops idle upstream connections after `upstreamIdleTimeout` (4 s), below vLLM's 5 s default; a reused connection closed before the write is resent (open question 5) |
| beta: router → worker | `--request-timeout-secs` = `maxRequestDuration` + 1 h (10,800 s) | none (S2 phase A) | the router keeps idle connections 50 s; the worker's `VLLM_HTTP_TIMEOUT_KEEP_ALIVE` is 120 s |
| front → LLMLab | `maxRequestDuration` | `idleTimeout`; LLMLab's own cut is 180 s with no byte | `upstreamIdleTimeout` |

- Connection set-up toward any upstream has `connectTimeout` (10 s).
- Every cut is logged, and recorded in the usage record's `cutBy`, with the hop that made it: `front_deadline`, `front_idle`, `upstream`, `lease_end` or `client`.

### Parameters

Front settings, in the front's configuration file, which the launcher passes (HBEP-11). A change needs a restart.

| Setting | Default | Meaning |
|---|---|---|
| `clientListen` | `127.0.0.1:8000` | the client listener |
| `dialHomeListen` | none; required for direct mode | the dial-home port (HBEP-11, open question 8) |
| `clientKeysFile` | required | mode-600 file of client keys with their IDs |
| `sessionHeaders` | `[x-session-id]` | the session headers honoured, in order |
| `maxRequestDuration` | 2 h | the front's total deadline per request (open question 12) |
| `drainMargin` | 10 min | `drainWindow` must be at least `maxRequestDuration` plus this |
| `idleTimeout` | 30 min | no byte from the upstream for this long cuts the request |
| `connectTimeout` | 10 s | connection set-up to an upstream |
| `upstreamIdleTimeout` | 4 s | idle upstream connections are dropped after this |
| `clientKeepAlive` | 120 s | idle client connections are kept this long |
| `queueTimeout` | 60 s | the longest wait at the front when every candidate is at its cap |
| `pauseAfterFailures`, `pauseDuration` | 3, 30 s | consecutive connection failures that pause a target, and for how long |
| `usageDir` | required | where usage records and the counter file live |

Per deployment, in the ModelDeployment's `spec.routing` (an amendment to HBEP-11's API):

| Field | Default | Meaning |
|---|---|---|
| `loadBound.tight` | `"1.25"` | c while at least the desired Instances are ready; the cap is in force |
| `loadBound.loose` | `"2.0"` | c while fewer are ready; no cap |
| `instanceCap` | from `max-num-seqs` times workers | the per-Instance cap at full membership |

Elsewhere: `drainWindow` in the PlatformProfile (HBEP-4), 2 h 10 min by this HBEP; in beta, the router's and workers' launch settings under Inside an Instance.

### Reachability

- **Addresses** come from the inventory: each Instance's UID, `status.url` and lease end (HBEP-11).
- **The seam.** The front sends requests to an Instance through an HTTP transport that HBEP-6's dial-home tunnel provides, keyed by the Instance's UID from its registration. In alpha the tunnel arrives on the dial-home listener, directly or through ssh on port 22 (HBEP-11, open question 8). An Instance with no registered transport is not routable. How the transport carries bytes is HBEP-6's (open question 13).

### Test plan

Against HBEP-4's managed fake, S4's fake Slurm and an engine double, then against LLMLab and one batch provider.
- **Contract.** An unmodified OpenAI client, configured as for LLMLab, runs chat, streaming, usage, tool calls, `/v1/models`, unknown and inactive models.
- **No change but `model`.** The front's outgoing body differs from the client's only in `model`'s value, byte for byte. Unknown request fields reach the engine with their values. Prompt token counts match a direct call to the same Instance, so no text was injected. In beta the check at the engine compares fields, not bytes, because the router re-serialises the body (Risks).
- **Deadlines.** A 7,300-token stream at one token per second completes through the front, as in S2 phase A. A first token after 1,100 s completes. A positive control: with `maxRequestDuration` at 60 s, the front cuts a 150 s stream at about 60 s and records `cutBy: front_deadline`, so the test can see a cut.
- **Beta, the router.** The same 7,300-token stream completes through vllm-router at `--request-timeout-secs` 10,800, and the router's 1,800 s default is refused at launch. After a router restart, the agent re-registers its workers before the Instance is routable again. S2c's measurements are the router's acceptance (open question 2).
- **Conversation key.** Golden vectors: fixed `messages` arrays with hand-written JCS bytes and their SHA-256 from `sha256sum`, outside our code, for one system message, two system or developer messages, no system message, content as an array of parts, and a user message after an assistant one. The same opening with different later turns gives the same key; a different first user message gives another.
- **`x-session-id`.** Every request reaching an engine carries it, with the client's header value or the opening key. A request without the header and without `messages` carries an `r-` key.
- **Concurrency.** S10's hook-forced interleaving, ported: the serialised wrapper holds the bound, and the same scenario on the plain group breaks it, so the test can fail. A timing-based test proves nothing here, since a lucky schedule passes.
- **Release.** Two releases of one request leave the counts unchanged. A drain releases its open requests at once, and their completions change nothing. The check is that KubeAI's total equals the members' at every placement. Zero placements above the bound proves nothing alone: with consistent counts it follows from the arithmetic (S10).
- **Bound and cap.** With a cap of 2 on three Instances (S10c's hand-worked case): below full membership no request spills, at full membership one spills, and when all are full requests wait at the front and are placed by a completion. A run where the cap never binds does not count as a pass. When a replacement joins, the cap returns, the Instance above it is not cut, and it gets nothing until it is below the cap. During a handover's overlap, ready above desired keeps c = 1.25 and the cap.
- **Drain.** An Instance with a near lease end and no ready replacement stops receiving requests at its lease end minus the drain window, although the controller has not marked it `draining`. No request is cut at the end, and an event reports the lost capacity. An Instance that the controller marks `draining` earlier, once its replacement is ready, receives no new request after that.
- **Drain-window check.** A profile with `drainWindow` below `maxRequestDuration` plus `drainMargin` raises `DrainWindowShort`.
- **Choice under `model`.** With our deployment ready, LLMLab gets nothing; with every Instance at its cap, the next request goes to LLMLab; with no Instance ready, everything does. A conversation spilled to LLMLab returns home at its next turn once there is room.
- **Errors.** Each row of the Errors table, with its status, `code`, `detail` and headers. An unknown model, a failed authentication and a pinned name with no ready Instance each name the front, with no deployment or Instance.
- **Attribution.** The headers appear on successes, errors, streams and LLMLab-served replies. A stream whose headers are dropped is still attributed by its usage record.
- **Usage.** Kill the front between records: the counter shows the gap. A stream without `include_usage` gets null counts and its event count, marked `front_events`.
- **Keys.** With no ModelDeployment of a model name, plain `model` returns 503 `model_inactive`, although LLMLab serves that model. Once a managed ModelDeployment attaches the LLMLab provider, requests reach LLMLab. A client configured with its LLMLab key and only the front's base URL works when the operator set the front's client key to that key. A request to an Instance without that Instance's key is refused by its engine. A client key cannot write a ModelDeployment.
- **Passthrough.** In direct mode an agent registers, long-polls and reports through the dial-home listener, which presents the pinned certificate. The client listener answers 404 under `/agent/v1/`.
- **Keep-alive.** Idle the front's connection to an Instance past 5 s and send again: no request fails (L-2.5). In beta the same holds for the router-to-worker link.
- **Fault injection.** Kill an Instance mid-stream: the client gets one error event and no `[DONE]`. Refuse an Instance's connections three times: it is paused, then returns after 30 s. Saturate one Instance; flap one until the controller marks it ready again.

### Compatibility and upgrades

- Clients written for LLMLab are the compatibility target; extensions are additive. A client that should keep reaching LLMLab needs the LLMLab provider attached to its model names.
- Stored state: the usage records and the counter file only. Membership and lease ends come from the control plane.
- The front, the controllers and the CRDs are upgraded together on the single-user host (HBEP-11). The front ignores status fields it does not know, so a downgrade can meet them.
- The opening hash is versioned by its `o1-` prefix. A changed rule gets a new prefix, and moves each conversation once.
- Downgrade: start the earlier front. The usage file stays; records keep their fields.

### Security

- Client keys live in a mode-600 file on the front's host. Upstream keys are in the Providers' credentials files (HBEP-4, HBEP-11). Instance keys are in the controller's mode-600 files (HBEP-7). None is logged or put on a command line.
- The front is the only component reachable from outside. Instances are reachable only through their paths (HBEP-6). The dial-home listener serves the tunnel and `/agent/v1/` only.
- Admin is the Kubernetes API's, under RBAC (HBEP-11).
- Prompts and responses: the front stores no bodies. It logs request IDs, names, sizes, timings and status only, and the usage records hold counts and identities.
- Known limits: vLLM's internal listeners on the node's address, and in beta vllm-router's admin API on loopback (Risks).

### Graduation criteria

- **Alpha:**
  - the Test plan passes against HBEP-4's managed fake and S4's fake Slurm with an engine double;
  - on one batch provider, through the front: a 2 h stream completes, one handover cuts no request (with HBEP-7), and every response carries its attribution;
  - against LLMLab: chat and streaming through an attached managed provider, and the inactive-model error without one.
- **Beta:**
  - spike S2c (issue #62, one worker per GPU) validates the router default's hit rate, balance across workers and latency, or replaces it, and Instances with several workers run behind it with its three conditions (open question 2; HBEP-7's beta criteria);
  - retries before the first byte, with a budget and a guard, pass a fault-injection spike (open question 5);
  - LLMLab's overload status is read and matched (open question 6);
  - usage records go to HBEP-8's log;
  - the Test plan passes on two batch providers;
  - front replicas are decided for the team option (open question 11).
- **Stable:** the attribution headers and the opening hash's rule are frozen, with golden vectors in CI.

## Operational readiness

1. **Working:** the front's health endpoint, plus the age of its membership view and of each watch. End to end, a synthetic request per naming level returns the expected attribution headers, and its usage record appears. A failed synthetic request, a response without headers, or a stale watch says it is not working.
2. **Restart:** it re-reads the ready set, lease ends, ModelDeployments, Providers and PlatformProfiles from the control plane, and rebuilds every ring from them. Balancer counts start at zero, which is consistent, since the restart cut every stream through it. The usage counter resumes at the persisted end. Instance transports come back as the tunnels reconnect (HBEP-6).
3. **Silent failure:**
   - a cut stream that looks complete: counted by finish reason, never closed with `[DONE]`;
   - a request answered by a wider level than asked: seen in the headers;
   - counts that drift from the members': the wrapper compares KubeAI's total with its own at each placement and emits a `degraded` event on a difference;
   - in beta, a router that lost its workers: the agent's comparison of worker lists;
   - a response without attribution headers: the synthetic request.
4. **Provider limits:** lease ends (draining), and LLMLab's no-byte cut, stalls, shared capacity and model flapping on routes to LLMLab.
5. **Capacity:** at full membership, requests over every cap wait at the front up to `queueTimeout`, then get 503 with `Retry-After`. Below full membership, nothing holds the engine limit: S10c's peak after an unreplaced drain was 84–96 in flight against 48, and the engine queues the rest. The front's own limit is open connections and the path's bandwidth (HBEP-6).
6. **Idle cost:** one small process with its watches, one connection per Instance's transport (HBEP-6), and no grant hours.
7. **Rollback:** stop the front, and clients lose the endpoint. Instances keep running until their leases end, and the usage file stays. A front of the earlier version reads the same control-plane objects. In-flight requests through a stopped front are cut.

## Drawbacks

- One more hop: added latency and one more place to fail.
- The front must follow LLMLab's contract as it changes.
- In beta, two affinity levels mean two sets of parameters to tune.
- A 2 h 10 min drain window spends the end of every lease on draining.
- In alpha LLMLab serves only overflow, so its capacity is idle while our Instances have room.

## Alternatives

- **One URL per instance, chosen by the client.** It breaks at every handover and gives no attribution.
- **A stock router at the edge.** It has no naming levels, no LLMLab route and no lease-end drain, and its default deadline cuts long streams.
- **Pure load balancing or pure session affinity.** Both failed on long multi-turn sessions (Motivation). In S10's tight regime round robin reached 43.4% hit rate; pure affinity reached 83.4% at 1.63 times round robin's throughput, against 2.15 times for CHWBL at c = 2.0.
- **Gateway API Inference Extension or llm-d.** Both are built around Kubernetes pods. The evaluation (28 Oct 2026) found that the Inference Extension routes only to pods. llm-d's router can run without Kubernetes, through file discovery, so it stays the candidate for multi-node Instances; its shipped scorers piled sessions by system prompt in S2 phase B. Neither offers the naming levels, the bounded-load affinity or the drain before a lease ends, so the front core is ours.
- **vllm-router's `cache_aware` inside an Instance.** It cannot see a chat prompt in 0.1.16 and behaved like round robin (0.642 against 0.635 in S2 phase B).
- **A sixth kind for routing policy.** It would amend HBEP-3's five kinds. The ModelDeployment already holds a model name, a provider selection by name or label, and a managed source.

## Prior art and reuse

The front grows from www-2027's hand-tuned per-stack routers: affinity and policy switches, a load report written for the router, a hand-run join script that re-added workers within milliseconds of a router restart, and worker removals scheduled by hand before each job ended.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| KubeAI CHWBL balancer | [balance_chwbl.go](https://raw.githubusercontent.com/kubeai-project/kubeai/main/internal/loadbalancer/balance_chwbl.go) in [kubeai-project/kubeai](https://github.com/kubeai-project/kubeai), commit `89dfff46` | Apache-2.0 | Copied code, unchanged, under `third_party/forked/kubeai/` (its `ORIGIN.md`), because it sits under Go `internal/`. The front's wrapper around it is ours (S10, S10b, S10c) |
| xxhash | [cespare/xxhash](https://github.com/cespare/xxhash) v1.1.0 | MIT | Dependency of the copied balancer and of the ring replica |
| vllm-router | [vllm-project/router](https://github.com/vllm-project/router) 0.1.16, PyPI aarch64 and x86-64 wheels | Apache-2.0 | Dependency in beta: the provisional router in front of an Instance's several workers, `consistent_hash` on `x-session-id` (S2 phases A and B), until S2c validates it |
| vLLM | [vllm-project/vllm](https://github.com/vllm-project/vllm) | Apache-2.0 | Dependency (HBEP-9): the engine the front reaches; in beta its `VLLM_HTTP_TIMEOUT_KEEP_ALIVE` is set by the launch |
| llm-d router endpoint picker + Envoy | [llm-d/llm-d-router](https://github.com/llm-d/llm-d-router); [Envoy](https://github.com/envoyproxy/envoy) | llm-d router Apache-2.0; Envoy Apache-2.0 | Candidate after alpha: the router for multi-node Instances, in file-discovery mode (S2) |
| Agent Router (optional) | [theagentrouter/agent-router](https://github.com/theagentrouter/agent-router) | Apache-2.0 | Optional edge for a team (keys, token limits, metrics), decided by spike S6, not scheduled |
| llm-d-inference-sim | [llm-d/llm-d-inference-sim](https://github.com/llm-d/llm-d-inference-sim) v1.0.0 | Apache-2.0 | Test dependency: the GPU-free engine double S2 phase A used |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| Drop an expiring job from routing once another job is ready | GWDG SAIA ([services.py](https://raw.githubusercontent.com/gwdg/saia-hpc/main/scheduler_core/services.py)) | Draining, made lease-aware |
| Affinity key as an HMAC over user, model and session; a per-user `cache_salt` | ALCF FIRST (Minerva) ([argonne-lcf/FIRST](https://github.com/argonne-lcf/FIRST)) | The conversation key; alpha has one user, so no user enters it |
| Consistent hashing with bounded loads | KubeAI's CHWBL balancer | The ring on a conversation key, and its bound |
| Session headers to honour; SSE comment keepalives during long prefill | LiteLLM ([BerriAI/litellm](https://github.com/BerriAI/litellm)); NVIDIA Dynamo ([session IDs](https://docs.nvidia.com/dynamo/v1.6.1/agents/session-i-ds.md)) | `sessionHeaders`; keepalives are not needed for alpha (open question 10) |
| A canonical JSON serialisation for hashing | RFC 8785, JSON Canonicalization Scheme, as HBEP-3 uses it | The opening hash |
| Endpoint-picker protocol (`x-gateway-destination-endpoint`, fallback list, served-endpoint feedback); the dropped-reason header | Gateway API Inference Extension ([kubernetes-sigs/gateway-api-inference-extension](https://github.com/kubernetes-sigs/gateway-api-inference-extension)); llm-d | The seam to an llm-d router after alpha; `detail` in error bodies |
| Measure front capacity in concurrent streams | ExaServe ([arXiv 2609.10812](https://arxiv.org/abs/2609.10812)) | Operational readiness 5; replicas after alpha (open question 11) |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| A router's default total deadline cut long streams without a log line. | L-4.1 | `maxRequestDuration` of 2 h, with no hop's deadline below it; in beta the router's timeout is set above it. |
| A managed gateway cuts replies that send no byte for a fixed time. | L-6.1 | The front does not copy the cut; routes to LLMLab still pass through it, and carry only overflow in alpha. |
| A gateway that injects prompt text or filters fields changes the deployment. | L-6.6 | The front changes only `model`'s value and passes unknown fields through. |
| No routing policy fit long multi-turn sessions. | L-4.3 | Consistent hashing with bounded loads on a conversation key, with a serialised, once-released count. |
| Sessions piled onto one worker while others idled. | L-4.4 | Bounded loads, a cap at full membership, and a count that releases drained requests. |
| Requests stayed on a saturated worker, and nothing said which one answered. | L-4.6 | Per-Instance caps; every response names its Instance. |
| Only global caps existed. | L-4.7 | A cap per Instance (open question 6). |
| Servers ran out of KV cache with no per-server concurrency cap. | L-3.12 | Requests over every Instance's cap wait at the front. |
| Workers added to a router were lost when it restarted. | L-4.8 | The front's membership comes from the ready set, read by watch. In beta the allocation agent re-registers workers after every router restart and checks the list at every report. |
| Pool membership went stale. | L-4.11 | Membership comes from the controller's ready set, read by watch. |
| A router's admin API had no authentication. | L-4.9 | Admin is the Kubernetes API's. Alpha has no router; in beta its admin API stays on loopback, and the agent restores its worker list. |
| Workers were removed by hand before each job ended. | L-4.10 | The front stops sending to an Instance at its lease end minus the drain window. |
| A balancer ejected backends for good, with a time-out below the client's. | L-4.14 | A pause lasts 30 s; no deadline below the front's maximum. |
| An engine's keep-alive dropped connections a router reused. | L-2.5 | The side that reuses connections has the shorter idle time-out: the front's 4 s; in beta, workers keep 120 s for the router. |
| A SIGTERMed router can hang in graceful shutdown. | L-3.3 | In beta a router is stopped only once its Instance is unrouted and empty, then killed after a grace. |
| The router has no model-name option. | L-4.13 | The front rewrites `model` to the name the workers serve. |
| Some streamed tool-call arguments arrived cut. | L-6.7 | A stream that ends without a finish reason counts as truncated, and gets an error event, never `[DONE]`. |
| Errors for unknown, inactive and blocked models looked alike. | L-6.5 | LLMLab's status codes are kept; `code` and `detail` tell the causes apart. |
| A managed gateway's shared capacity binds under load. | L-6.3 | LLMLab takes only overflow in alpha. |
| A follow-on on a new node changed the URL. | L-1.22 | Clients keep the front's base URL; handovers happen behind it. |

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #5.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).
- 2026-10-30: Prior art and reuse filled from the state-of-the-art evaluation.
- 2026-10-30: the drain window is the profile's `drainWindow` (HBEP-4), read by both the front and the lifecycle controller (HBEP-7).
- 2026-10-30: implementable. Spikes S10 (PR #34), S10b (PR #50), S10c (PR #53) and S2 phases A (PR #44) and B (PR #55) carried in, with the owner's choice of the default load bound (PR #53). The request path, the conversation key, the balancer and its wrapper, one worker per alpha Instance (HBEP-7) and the provisional router for beta, draining, the LLMLab route, errors, attribution headers, usage records, time-outs and parameters written for alpha. Every open question decided, not needed for alpha, or open for beta.

## Amendments

None.
