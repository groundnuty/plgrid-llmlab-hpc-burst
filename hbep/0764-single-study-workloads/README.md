# HBEP-764: A single study with several workloads

## Summary

This HBEP tells the story of one study that serves several workloads through the front. Some of its workloads have long contexts and reasoning, and others have short prompts. The study starts on LLMLab and bursts onto HPC as it grows. The HBEP names what the study declares, what the framework derives from that, and the requirements that later amendments must meet. It refines HBEP-2's stages 2 to 5 for this case. It designs no mechanism and changes no decided behaviour: the amendments to HBEP-3, HBEP-5, HBEP-7 and HBEP-8 that build it come after, and each cites this one. It's written for the authors and reviewers of those amendments, and for the owner, who decides its open questions.

## Motivation

HBEP-2's stages 2 to 5 (deploy, keep serving, watch, one endpoint). Its story asks for `example-27b`, four instances, and treats every request to that name alike. A real study runs several workloads at once, and they differ by an order of magnitude in the context memory each session holds. So the number of sessions an Instance can serve well depends on three things: the model, the deployment (the GPU and the memory it leaves for the cache), and the workload's shape.

A measured example (#735, 18 Nov 2026): `Qwen/Qwen2.5-7B-Instruct` on one Sano A100 80 GB reported a KV cache of 1,065,312 tokens. That's about 32 sessions at 32,768 tokens each, or about 260 at 4,096.

Today the framework doesn't use the shape for capacity:
- the front caps each Instance at a fixed number of concurrent requests: `routing.instanceCap` when set, else the engine's `--max-num-seqs` times its workers, 256 by default (HBEP-5, Load bound and cap; HBEP-7, Launch);
- the deployment's `workload` block, which holds the shape, drives only warm-up and qualification (HBEP-7).

A long-context workload therefore runs out of cache well below the cap (L-3.12), while a short one may be held below what it could use. The front also can't tell one workload's request from another's, so it can't keep them on deployments sized for them.

The owner raised this on 18 Nov 2026, asking for a proper end-to-end stress test and for the session limit to follow the model, the deployment and the study.

### Goals

- One story, in steps, of a single study with several workloads, refining HBEP-2's stages, for the mechanism amendments to cite.
- What the study declares, and what the framework derives from it.
- The requirements the mechanism must meet, each mapped to the HBEP that will own it.
- The open questions, written down before the amendments start.
- The end-to-end stress test that checks the result on Sano.

### Non-goals

- Designing the mechanism. The amendments do that.
- Several studies sharing a front, its deployments or LLMLab's capacity. Owner, 18 Nov 2026: "let's now think of the single study use case". The structure shouldn't rule it out later.
- A standing reserve of HPC capacity. Owner, 18 Nov 2026: capacity comes on need, pre-staged or dynamic, never as a small deployment left running.
- Knowing LLMLab's capacity, or who else is using it. LLMLab is best effort (owner, 18 Nov 2026).

## Proposal

### User stories

These are steps W0 to W7. Each refines a stage of HBEP-2.

- **W0, declare (refines stage 2).** The study declares its workloads by name, for example `analysis` (long contexts, reasoning, long outputs) and `labelling` (short prompts, many sessions). For each one it gives:
  - the model it uses;
  - its shape: typical and longest context, and typical and longest output;
  - whether it reasons;
  - the sessions it expects in parallel, and when.

  The framework provides the definition format, and the study fills it in. Owner, 18 Nov 2026: the study provides the information, and "our responsibility [is] to provide them with the structure … that they can define … their expected workloads and their characteristics".
- **W1, start on LLMLab (refines stages 0 and 5).** Each request names its workload. Most studies start on LLMLab, "because it's there" (owner, 18 Nov 2026), so the front sends each workload's requests there first.
- **W2, burst (refines stage 2).** The study asks for HPC capacity for a workload, in one of two ways.
  - **Pre-staged:** the study knows what it will need. The framework works out how many Instances a workload needs on a cluster, from the cluster's measured cache for that model on that GPU and from the workload's shape, and queues them ahead of need.
  - **Dynamic:** the framework adds Instances as demand grows, at the cost of the queue wait. This comes later. Raising a deployment's count can't release anything, so it needs no approval (HBEP-10, The capacity guard).
- **W3, serve (refines stage 3).** Each session stays on its home Instance among its own workload's Instances, so its cache stays warm. An Instance takes a new session only while its context memory has room for it, each session counted by its own workload's shape. Workloads may share an Instance or keep their own.
- **W4, order (refines stage 5).** Each workload says which comes first, LLMLab or our own Instances, and the other takes the overflow. Early in a study LLMLab comes first; once many of our Instances run, ours may. Owner, 18 Nov 2026: "this should be something that the work[load] can control".
- **W5, full (refines stage 5).** When every route of a workload is full, its requests wait at the front, and then get the front's capacity answer (HBEP-5, Errors).
- **W6, watch (refines stage 4).** For each workload and each route, LLMLab's included, the study and its agents see sessions in flight, sessions waiting, time to first token and the speed per session.
- **W7, end (refines stage 7).** A workload's capacity is released as its work ends, under HBEP-7 and HBEP-10's rules.

### Notes, constraints and caveats

**Actors.**
- **The study:** HBEP-2's user or operator, or an agent acting for one, running one coordinated piece of work.
- **A workload:** a named class of the study's requests with one declared shape and one model.
- **A route:** where a workload's request can go: LLMLab, or one of our Instances.

**Constraints from the owner (18 Nov 2026).**
- One study, one front.
- Workloads are named, and the study uses them by name as it wants. Its clients can set a header.
- LLMLab is out of our control and shared, so routing to it is best effort.
- No standing HPC reserve. An Instance takes minutes to hours to get through a queue, and then minutes to start: 142 s to ready for the 7B model on Sano (#735). So with LLMLab first and nothing pre-staged, the overflow has nowhere to go until Instances start.

**Kept from HBEP-2.** The client still changes only its base URL. A request that names no workload goes to the study's default workload, which is today's behaviour.

**What doesn't change now.** HBEP-5's recorded decision stands until an amendment changes it: LLMLab takes overflow only (owner, 30 Oct and 17 Nov 2026). A per-workload order (W4) will amend it for a study that sets one.

### Requirements

These are for the amendments. Each names the HBEP expected to own it, and each is a proposal until its amendment is reviewed.

| | Requirement | Owner HBEP |
|---|---|---|
| R1 | **Workload definitions:** named per study, with the shape, the model, the expected sessions and their window, and the route order. Built from the ModelDeployment's `workload` block, not a second place to declare the same thing. | HBEP-3, HBEP-7 |
| R2 | **Naming a workload in a request:** a header, and a form of the model name for clients that can't set one. A request naming none gets the default workload. | HBEP-3, HBEP-5 |
| R3 | **Pinning within a workload:** the conversation key (HBEP-5) placed on a ring of that workload's routes only. | HBEP-5 |
| R4 | **Capacity per Instance follows the workload,** within the engine's own limit, with a set `instanceCap` still able to cap it. The proposal: a budget in context tokens, taken from the engine's own report at start, with each session charged its workload's expected tokens. | HBEP-5, HBEP-7 |
| R5 | **Route order per workload,** LLMLab first or ours first, replacing the fixed order for a study that sets one. | HBEP-5 |
| R6 | **Planning for pre-staging:** the Instances a workload needs on a cluster, from that cluster's measured cache for the model and GPU and from the workload's shape. | HBEP-4, HBEP-7 |
| R7 | **Signals per workload and per route:** sessions in flight and waiting, time to first token, speed per session. | HBEP-8 |
| R8 | **Later, awareness of speed:** a route counts as full once its speed per session falls below what the workload needs, starting with LLMLab, whose capacity we can't see. | HBEP-5, HBEP-8 |

### Risks and mitigations

- **A declared shape is wrong.** A session counted below its real use overfills an Instance, and the engine then preempts and recomputes rather than failing. The signals per workload (R7) show the gap, and the study corrects its declaration. Whether the engine's measured cache use should correct the count is open question 3.
- **A workload's home Instance fills.** A spill moves a session and loses its cache, as it does today. The cap stops a pile, and the spilled session returns at its next turn once its home has room (HBEP-5).
- **Workloads that share an Instance crowd each other out.** A long workload takes the cache a short one needed. The count per session is by workload, and the study can keep workloads apart.
- **LLMLab first with nothing pre-staged.** The overflow waits at the front, and then gets the capacity answer. The study chooses pre-staging when that matters.
- **Too much design for one study.** The amendments build pre-staged capacity first, and dynamic capacity and awareness of speed later.

### Open questions

1. **Where workloads live:** a new resource, or fields on the ModelDeployment (R1). Owner and HBEP-3.
2. **The name form** for a workload in a model name (R2). HBEP-3 splits a name at its last `@`, so the new form must parse without ambiguity.
3. **What a session is charged** (R4): the typical shape or the longest; whether prefix-cache sharing among a workload's sessions, such as a common system prompt, lowers it; and whether the engine's measured cache use corrects it. The owner suspects that for LLMLab the binding limit is the number of sessions served efficiently rather than the cache size (18 Nov 2026), which awareness of speed (R8) is meant to find.
4. **Workloads mixed on one Instance,** or one workload per deployment first. The lean: design the count for mixing, and deploy one workload per deployment first.
5. **Where a cluster's measured cache lives** for planning (R6): an `expectedPerformance` entry, or a field of its own.
6. **The dynamic mode's trigger:** sessions waiting at the front, LLMLab's speed per session falling, or both.
7. **The LLMLab route's model:** LLMLab's current model under the study's name, read from its model list, and whether our pinned engine can serve the same model. If it can't, raising the engine version is the owner's decision.

## Design details

### Steps and owning HBEPs

| Step | HBEP-2 stage | HBEPs |
|---|---|---|
| W0 declare | 2 | HBEP-3, HBEP-7 |
| W1 start on LLMLab | 0, 5 | HBEP-5 |
| W2 burst | 2 | HBEP-4, HBEP-7, HBEP-10 |
| W3 serve | 3 | HBEP-5, HBEP-7 |
| W4 order | 5 | HBEP-5 |
| W5 full | 5 | HBEP-5 |
| W6 watch | 4 | HBEP-8 |
| W7 end | 7 | HBEP-7, HBEP-10 |

### Test plan

**The end-to-end stress test**, on Sano, after #735's serve and handover rows:
- two Instances of one model, and two declared workloads, one with long contexts and one short;
- sessions of each workload ramped up until the counts bind.

At each step it checks, per workload:
- that sessions stay pinned to their home Instance;
- that each Instance fills to its count and then spills to the next;
- the route order;
- overflow to LLMLab, kept small, since LLMLab is shared;
- waiting at the front once every route is full.

It records time to first token, speed per session, and the engine's preemptions.

**A baseline row first**, on today's code, with a hand-set `instanceCap`: it shows the routing at full load before any amendment, and gives the numbers the count of R4 is checked against.

### Compatibility and upgrades

A study that declares no workloads behaves as today: one default workload, the fixed route order, and the cap as HBEP-5 gives it.

### Security

Nothing new. A workload name is client input, checked as a model name is (HBEP-3).

### Graduation criteria

None of its own. Like HBEP-2, this HBEP has no stages. It becomes `implementable` once the owner accepts its story and requirements, and each requirement then graduates with the amendment that builds it.

## Operational readiness

The operator reads each workload's signals per route (R7) to see whether its declared shape holds, and corrects the declaration rather than the cap.

## Drawbacks

- A study declares more before it starts.
- A wrong shape misleads planning until the signals show it.

## Alternatives

- **A fixed cap per Instance, set by hand per study** from a measured run. It's simple and works today, but it's static, ignores the workload, and is retuned for every cluster and model.
- **One deployment per workload, each with a hand-set cap,** on today's design. Under plain `model`, every request goes to one ring over all the deployments of the name (HBEP-5, Choice under `model`). A client can name one deployment, `model@<provider>/<deployment-id>`, but that name stops resolving when its ID changes, and it never spills (HBEP-3, open question 12).
- **Routing aware of the cache inside the engine only.** It doesn't span Instances or LLMLab, and the front still needs a count to know when a route is full.

## Prior art and reuse

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| KubeAI CHWBL balancer | [kubeai-project/kubeai](https://github.com/kubeai-project/kubeai), as HBEP-5 records it | Apache-2.0 | Unchanged: the ring a workload's sessions are pinned on (R3) |
| vLLM | [vllm-project/vllm](https://github.com/vllm-project/vllm) | Apache-2.0 | The engine's own report of its KV cache at start, the budget R4 proposes |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| Consistent hashing with bounded loads on a conversation key | KubeAI's CHWBL balancer, through HBEP-5 | Pinning within a workload (R3) |
| A workload declared by its shape | HBEP-7's `workload` block | The workload definitions (R1) |

### Field lessons answered

| Problem | Lesson | Answered by |
|---|---|---|
| Servers ran out of KV cache with no per-server concurrency cap. | L-3.12 | R4: the count per Instance follows the workload's shape |
| A managed gateway's shared capacity binds under load. | L-6.3 | R5 and R8: the route order per workload, and later awareness of speed |
| Pure load balancing lost prefix-cache hits on long multi-turn sessions, and pure affinity piled sessions on one worker (HBEP-5, Motivation). | HBEP-5 | R3: pinning within a workload, under HBEP-5's bound and cap |

## Implementation history

- 2026-11-18: proposed, from the owner's discussion with the science agent on 18 Nov 2026 (issue #764).

## Amendments

None yet.
