# HBEP-2: Scope and user story

## Summary

This HBEP fixes what the framework is for. It defines the user story that every other HBEP cites by stage number, the actors in it, what is in and out of scope, and the dimensions used to classify problems and proposals. It is written for authors and reviewers of the other HBEPs.

## Motivation

All stages (0–7). Without one story, proposals optimise for different users and the design grows in every direction at once. A shared stage number lets each HBEP say which step of the user's path it serves, and lets a reviewer reject work that serves none.

### Goals

- One user story, in stages, that every HBEP names in its Motivation.
- A map from each stage to the HBEPs that own it.
- A written scope, so out-of-scope work is refused by pointing at this text.
- Shared dimensions for classifying problems, risks and known issues.

### Non-goals

- Designing any component. Each stage's design belongs to the HBEPs mapped to it below.
- Store copies and other non-LLM services, for now. Running a non-LLM service inside a GPU job is parked as an idea.
- Any design that depends on a center pushing the priority of our jobs. The framework's lever is hedging (HBEP-3 defines it, HBEP-7 builds it).

## Proposal

### User stories

- **Stage 0.** A researcher sends requests to PLGrid LLMLab through its OpenAI-compatible API, with an OpenAI client and a key.
- **Stage 1.** The researcher's runs start to fail or crawl: shared capacity binds, the gateway stalls periodically, models go inactive without warning, and the researcher cannot see why.
- **Stage 2.** The researcher holds grants on HPC platforms such as Cyfronet Helios or Athena; each grant is a provider (HBEP-3). They ask for `example-27b`, four instances, until Friday. The framework submits the jobs, sets up the environment and starts the engine. It may start a few more instances than asked, qualify each, and keep the best.
- **Stage 3.** Every job ends at a hard time limit, and some fail earlier. The framework restarts failed instances and hands over to a successor before each lease ends.
- **Stage 4.** The researcher, or an agent acting for them, sees each instance's health, performance and usage while it runs.
- **Stage 5.** The client changes only its base URL. The operator attaches the providers that serve `example-27b`, the LLMLab provider among them if requests should keep reaching LLMLab, as one setup step. The name is then served by LLMLab and by the researcher's own instances, behind one endpoint.
- **Stage 6.** A reviewer asks which configuration produced a result. Every response names the deployment and instance that produced it.
- **Stage 7.** The work ends. Usage is accounted against each grant, every allocation is released, at the latest when its retention policy ends, and nothing is left running.
- **An agent as operator.** An agent acting for the researcher runs stages 2–7 through the control plane, using primitives built for agents (HBEP-10), and asks a person before irreversible actions.
- **An agent coordinating an experiment.** It needs throughput across several models at once, so it holds capacity for them. It over-allocates, qualifies each Instance against the platform profile, and keeps the best (HBEP-7). As one model's work finishes, it repurposes that model's Allocations to the models still running, instead of queueing new jobs. Its retention policy keeps everything until the grant's end or the deadline. Other operators choose other policies: keep until a date, or release when idle.

### Notes, constraints and caveats

**Actors.**
- **User:** a researcher, or an agent acting for one, who sends requests through an OpenAI-compatible client.
- **Operator:** the person or agent that runs the control plane and the front: declares deployments, attaches providers to model names, sets retention policies, watches deployments, and repurposes and releases Allocations. Often the user. Coordination between operators stays with agents; the framework gives them primitives (HBEP-10).
- **Organization:** a computing center or company that runs platforms (HBEP-3), such as Cyfronet. It acts only through its own channels, such as an admin request, and on its own schedule. The HBEPs also call it a center.
- **Upstream:** the engine and router projects whose bugs the framework works around but does not fix.

**Scope.**
- In scope: serving LLMs behind one LLMLab-compatible front (HBEP-5), on managed and batch providers (HBEP-3, HBEP-4), operated through a Kubernetes-API control plane (HBEP-11).
- Static providers (always-on machines) come later.
- The user holds the grants. The framework uses them; it does not obtain them.

**Principles taken from the owner's decisions.**
- LLMLab will improve but keeps the same kinds of characteristics. Every contingency therefore stays, as a platform-profile parameter with thresholds, enabled by default and cheap when idle (HBEP-4).
- The client changes nothing beyond its base URL.
- The framework copies LLMLab's contract, not its quirks (HBEP-5).
- Proposals come first. Existing projects are evaluated afterwards, and may change the proposals or become components (see Alternatives).

### Risks and mitigations

- **Scope creep into a general HPC job manager.** Each HBEP names the stage it serves; work that serves no stage is out.
- **A story fitted to one center.** Only stage 0 is specific to LLMLab. Provider kinds, platform profiles and provider settings (HBEP-3, HBEP-4) carry each platform's and each grant's specifics as data.
- **Out-of-scope services come back as hidden requirements.** A model's clients may depend on a non-LLM service on the same cluster, and its placement may then constrain where models run. Such a dependency is raised on this HBEP's issue, not built into a provider.
- **Clients that rely on LLMLab's quirks behave differently behind the front.** HBEP-5 lists which behaviours are copied and which are not.
- **A design that waits on center staff.** Admin requests are slow and unavailable outside support hours. No stage may depend on one; the framework hedges instead.
- **Misweighted problems.** An engineering lesson treated as a story driver bloats the design; a story driver filed as a profile entry hides a design gap. The reviewer of each HBEP PR checks the Weight of the problems it cites.

### Open questions

1. **Adjacent concerns.** A client harness's own semantics (claims, retries, replay checks), result checking, data-release hygiene and review capacity look outside the framework. Proposed out of scope; the owner decides on this HBEP's issue.
2. **Client-side machines as a provider.** The Platform dimension lists them, while static providers come later. Are they a static provider, and when are they in scope? Owner discussion.
3. **Single user or team first.** Is sharing one front and its deployments among several users in scope from the start? Decided with HBEP-9 and HBEP-11.
4. **The API key.** Resolved (owner decision): the operator sets the front's client key, and may set it to the user's LLMLab key, so the client changes only its base URL (HBEP-5).
5. **A public problem catalogue.** Should the repository keep a neutral catalogue of problems, classified by the dimensions below, as input to test plans and the known-issues register (HBEP-4)? Owner decision.
6. **The parked non-LLM idea.** What would bring a non-LLM service inside a GPU job back into scope? Owner decision, when a concrete case appears.
7. **Reaching `implemented`.** A scope HBEP has no stages; what marks it implemented? Owner decision.

## Design details

### Stages and owning HBEPs

| Stage | The user's step | What goes wrong | Owning HBEPs |
|---|---|---|---|
| 0 use LLMLab | sends requests to LLMLab | nothing yet | HBEP-4 (LLMLab as a managed provider), HBEP-5 |
| 1 hit the limit | finds LLMLab cannot carry the load | shared capacity, stalls, inactive models, no visibility | HBEP-4, HBEP-8 |
| 2 get resources | runs the same models on HPC systems | submission rules, environment, start-up, slow or broken nodes | HBEP-4, HBEP-7, HBEP-11 |
| 3 keep it running | keeps instances up | failures, restarts, time limits, handovers | HBEP-7 |
| 4 watch it | sees health, performance, usage | silent failures, missing signals | HBEP-8 |
| 5 one endpoint | uses one base URL for everything | reachability, routing, naming | HBEP-3, HBEP-5, HBEP-6 |
| 6 know what answered | attributes each response | identity, reproducibility | HBEP-3, HBEP-9 |
| 7 pay and give back | accounts for and releases resources | cost, release, cleanup | HBEP-7, HBEP-8, HBEP-10 |

HBEP-10 (agent primitives) and HBEP-11 (control plane) serve every stage. The stages are a path, not a sequence in time: stages 3–7 run together once instances serve.

### Classification dimensions

Every problem, risk and known-issue entry can be tagged on four dimensions.
- **Stage:** 0–7 above.
- **Platform:** a hierarchy of organization → platform → partition or node type (HBEP-3), with the provider added when a problem belongs to one grant. Examples: LLMLab (managed, at Cyfronet); Cyfronet Helios (GH200, arm64, with CPU partitions); Cyfronet Athena (A100, x86); a small institutional cluster with two GPU nodes; client-side machines; "any".
- **Weight:** a story driver (changes what the software does), an engineering lesson (a test case or a platform-profile entry), or out of scope for now.
- **Fix owner:** the framework, the center (an admin request) or upstream (an engine or router bug). Upstream bugs go to the known-issues register with versions, pins and a regression test (HBEP-4).

### Test plan

- The reviewer of each HBEP PR checks that its Motivation names a stage from this HBEP, or says N/A with a reason.
- Open: an end-to-end story test that walks stages 0–7 against simulated providers, as the framework's top-level acceptance test. Decided once HBEP-4 and HBEP-7 define their test doubles.

### Compatibility and upgrades

N/A: no software. Renumbering stages would break every citation, so stages may be added at the end but not renumbered.

### Security

N/A: no component. Security scope per stage belongs to the owning HBEPs, mainly HBEP-5 and HBEP-9.

### Graduation criteria

N/A: a scope HBEP has no stages of its own.

## Operational readiness

1. **Working:** N/A; no running component. Each owning HBEP answers for its stage.
2. **Restart:** N/A.
3. **Silent failure:** N/A.
4. **Provider limits:** N/A; recorded per provider in HBEP-4.
5. **Capacity and exhaustion:** N/A.
6. **Idle cost:** none. The principle that contingencies are cheap when idle is enforced in HBEP-4.
7. **Rollback:** N/A; a scope change is an amendment to this HBEP.

## Drawbacks

- A story centred on one researcher may underweight teams sharing deployments (open question 3).
- Numbered stages suggest a linear order, while stages 3–7 overlap in practice.

## Alternatives

- **Scope by components** (front, controller, providers). Rejected: it says what to build, not for whom, and gives no test for scope.
- **Scope by a list of past problems.** Rejected: problems are evidence with different weights, not requirements. They feed Motivation, Risks and test plans instead.
- **Evaluate existing projects first:** interLink and other virtual-kubelet offloading, Slinky / slurm-bridge, KubeAI, the vLLM production stack, the Gateway API Inference Extension, llm-d. Deferred by decision: proposals first, then evaluation, which may change the proposals or turn these projects into components.

## Implementation history

- 2026-10-28: proposed (provisional), tracking issue #2.
- 2026-10-28: owner decisions folded in (naming hierarchy, providers behind model names, reassignable allocations, qualification, retention, keys).

## Amendments

None.
