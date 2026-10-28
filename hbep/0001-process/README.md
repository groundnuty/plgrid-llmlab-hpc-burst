# HBEP-1: Enhancement proposal process

## Summary

Design decisions in this repository are made as hpc-burst enhancement proposals (HBEPs). They are modelled on Kubernetes Enhancement Proposals (KEPs), trimmed for a small team.
- Each HBEP has a tracking issue, and the issue number is the HBEP number.
- Its text is added and changed only by pull request.
- It moves through a fixed set of statuses, and the repository owner approves every change of status.
- After acceptance it can still change, through errata, amendments or a replacing HBEP.

## Motivation

This framework is designed by one owner and several software agents working in parallel. Without a fixed process:
- decisions end up scattered across chats and issue threads;
- nobody can tell which design is approved and which is still an idea;
- agents build from drafts;
- a later change silently rewrites what was agreed.

### Goals

- One place and one form for each design decision.
- An approval that is on the record: who approved what, and when.
- A status that an agent can read from a file before it starts building.
- Traceability from proposal to code: implementation PRs cite the HBEP they implement.
- A defined way to correct or change an accepted proposal without losing what was agreed before.

### Non-goals

- Kubernetes' organisational layers: SIGs, release teams, a separate production-readiness approver. One owner approves.
- Tracking bugs and small changes. Those stay ordinary issues and PRs; an HBEP is for decisions that shape the design.
- Freezing proposals once accepted. An HBEP describes what is built, so it follows the code.

## Proposal

### Lifecycle

| Step | Where | Gate |
|---|---|---|
| 1. Idea | An issue titled `HBEP: <title>` with the `hbep` label: a paragraph on the problem. Discussion of the idea stays on this issue. | none |
| 2. Provisional | A PR adding `hbep/NNNN-short-title/` copied from `hbep/template/`, with `status: provisional`. | the approver's approval: worth exploring |
| 3. Design | Further PRs fill in the design, the test plan and the readiness answers. Spikes answer open questions. | review (see Roles) |
| 4. Implementable | The PR that sets `status: implementable`. | **the approver's approval.** Implementation may start only from here. |
| 5. Build | Code PRs cite `HBEP-N` and the section they implement. A deviation found while building is written into the HBEP in the same PR or before it. | per PR |
| 6. Implemented | The PR that sets `status: implemented` and records the stage reached. The tracking issue is closed. | the approver's approval |

One PR may add several provisional HBEPs together. A PR that sets `implementable` or `implemented` changes one HBEP only.

### Statuses

- **provisional**: agreed to be worth exploring. The design may be incomplete. Only spikes may be built.
- **implementable**: the design is approved and implementation may start.
- **implemented**: built. The `stage` field records its maturity: alpha, beta or stable, with criteria in the HBEP's Graduation criteria. A process HBEP has stage `none`.
- **deferred**: paused; it can return to provisional.
- **rejected**: the approver decided against it.
- **withdrawn**: the author dropped it.
- **replaced**: superseded by another HBEP, named in `superseded-by`.

A rejected, withdrawn or replaced HBEP stays in the repository, because it records why something was not done.

### Roles

- **Author:** anyone, including an agent. Writes the HBEP and keeps it current while it is being built.
- **Reviewer:** at least one, who did not write the HBEP: a person, or an agent working from a fresh context that never saw the draft. A reviewer checks the text section by section against the template and the tracking issue.
- **Approver:** the repository owner (`approvers` in `metadata.yaml`). The approver's formal GitHub approval on a PR is the decision for any status change.

The author merges a PR once the approval it needs is given.

### Numbering and layout

- The HBEP number is the tracking issue's number. Agents drafting at the same time therefore never take the same number; the sequence has gaps where issues are not HBEPs.
- The folder is `hbep/NNNN-short-title/`, with NNNN zero-padded to four digits. It holds `README.md` (the text, from `hbep/template/README.md`) and `metadata.yaml` (number, title, status, stage, authors, approvers, tracking issue, dates, replaces, superseded-by, see-also).
- In text, write `HBEP-N` without padding.
- `hbep/README.md` lists every HBEP with its status. Each PR that adds an HBEP or changes a status updates that list.

### Changes after an HBEP is implementable

- **While provisional,** an HBEP is edited freely by PR.
- **Erratum:** a typo, an unclear sentence, a broken link. A PR with one review; no approval round.
- **Amendment:** a small change of behaviour or detail that keeps the HBEP's goals. A PR that adds a dated entry to the Amendments section and edits the text, with the approver's approval.
- **Replacement:** a different design, or a change to the goals. A new HBEP with `replaces: [N]`. The old one gets `status: replaced` and `superseded-by: [M]`.

Git history is the full record of every change.

### Discussion

- Discussion of the idea, and of decisions, goes on the tracking issue, which outlives the PRs.
- PR review comments are for wording and for the design as written.
- A decision reached on the issue is written into the HBEP by the next PR.

### Content rules

This repository is public.
- An HBEP describes the framework in general terms.
- It contains no private data from the projects that motivated it: no project or study names, host names, job IDs, account or grant names, private datasets or internal addresses.
- A problem that motivated a design is described in the HBEP's own words.

### How agents use HBEPs

- Before building, an agent reads the HBEP's `metadata.yaml`.
- It builds only from `implementable` or `implemented` HBEPs, and only spikes from `provisional` ones.
- An agent may author and review HBEPs. It never approves, and never merges a status change without the approver's approval.

### Notes, constraints and caveats

- **Bootstrap.** HBEP-1 defines the process, so it cannot go through its own provisional step. It is proposed directly as `implementable`; the approver's approval of the PR that adds it is the gate. It becomes `implemented` once the first HBEPs after it have gone through the lifecycle.
- The trimmed template keeps the KEP sections that carry decisions:
  - summary, motivation, goals and non-goals;
  - user stories;
  - risks;
  - design;
  - test plan;
  - graduation;
  - drawbacks and alternatives;
  - history.
- It replaces KEP's production-readiness questionnaire with six operational-readiness questions written for this framework: health signals, restart behaviour, silent failure, provider limits, idle cost, rollback.

### Risks and mitigations

- **Process overhead slows small decisions.** Only decisions that shape the design need an HBEP. Errata skip the approval round. Provisional HBEPs may be short.
- **Text and code drift apart.** A deviation found while building is written into the HBEP in the same PR or before it. Reviewers of code PRs check the cited section.
- **Two agents edit the same HBEP at once.** Edits go through PRs, so they conflict visibly instead of overwriting each other. The author resolves the conflict.

### Open questions

- A script that checks `metadata.yaml` fields and regenerates the index, run in CI. To be proposed once there is code to run CI on.

## Design details

### Test plan

Manual for now: the reviewer of each HBEP PR checks that
- the folder name matches the issue number;
- `metadata.yaml` has every field of the template;
- the index lists the HBEP with the right status.

The check script from the open questions automates this later.

### Graduation criteria

N/A: a process HBEP has no stages. It is `implemented` once three HBEPs after it have reached `implementable` through it.

## Operational readiness

1. **Working:** every HBEP in `hbep/` has a matching tracking issue, a valid `metadata.yaml` and a row in the index. A status change without an approved PR is the signal that the process is not followed.
2. **Restart:** N/A. There is no running process; state is the files in git.
3. **Silent failure:** an index row that disagrees with `metadata.yaml`. The reviewer compares them until the check script exists.
4. **Provider limits:** N/A.
5. **Idle cost:** none.
6. **Rollback:** a later HBEP can amend or replace this one by the same process.

## Drawbacks

- More ceremony than writing decisions in issues.
- Gaps in the numbering, because numbers come from issues.

## Alternatives

- **Python PEPs and Rust RFCs:** a proposal is effectively frozen once accepted, and changes need a new proposal. Rejected, because this framework's designs will change as providers and engines change; a frozen text would soon describe something that no longer exists.
- **Architecture decision records:** short, immutable notes, one per decision. Lighter, but they have no status lifecycle and no place for user stories, test plans or readiness questions.
- **Issues only:** no single current text, and no file an agent can read to learn what is approved.
- **Sequential numbers claimed in the index:** numbers without gaps, but two agents drafting at once can claim the same number.

## Implementation history

- 2026-10-28: proposed as implementable (bootstrap), tracking issue #1.

## Amendments

None.
