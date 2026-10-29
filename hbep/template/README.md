# HBEP-N: Title

<!--
Copy this folder to hbep/NNNN-short-title/: NNNN is the tracking issue number, zero-padded to four digits.
Everywhere else write the number unpadded (HBEP-N, `hbep: N`).
Keep metadata.yaml in sync with this text; the status lives there.
A provisional HBEP may leave Design details and Operational readiness as "TBD".
"N/A" is allowed anywhere, with a one-line reason.
-->

## Summary

One paragraph: what this proposal adds or decides, and for whom.

## Motivation

The problem, stated in general terms. Name the stage of the user story it serves, as defined in the scope HBEP (HBEP-2), or "N/A" for a process HBEP.

### Goals

### Non-goals

## Proposal

### User stories

### Notes, constraints and caveats

### Risks and mitigations

### Open questions

Each with how it will be answered (discussion, a spike, a measurement).

## Design details

### Test plan

Which tests prove it works, including tests for the failure modes in Risks.

### Compatibility and upgrades

- API compatibility with earlier versions, and with clients written for LLMLab.
- Migration of stored state.
- Behaviour when components run different versions during an upgrade.
- How to downgrade.

### Security

- Credentials: where they live and who can read them. Never on command lines or in logs.
- Trust boundaries: who can call what, across which networks.
- Prompts and responses: what is stored or logged, where, and for how long.

### Graduation criteria

What makes it alpha, beta and stable.

## Operational readiness

Answer each question.
1. How does an operator or agent know it is working? Name a component health signal and an end-to-end signal (a request served correctly), and which signal says it is not working.
2. What happens when its process or controller restarts mid-operation? What state does it re-read?
3. How could it report success while failing, and which check would show the difference?
4. Which provider limits does it depend on (time limits, queueing, reachability, quotas)?
5. How does it behave at capacity and when a resource runs out (queues full, KV cache full, quota used up)?
6. What does it cost when it is enabled but idle, and while waiting in a provider queue? Count compute, memory, grant hours and connections.
7. How is it switched off or rolled back, including requests in flight, running jobs and stored state?

## Drawbacks

## Alternatives

## Prior art and reuse

What this HBEP reuses or borrows, and where it came from. Use "None" for an empty table.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|

## Implementation history

- YYYY-MM-DD: proposed (provisional).

## Amendments

None.
