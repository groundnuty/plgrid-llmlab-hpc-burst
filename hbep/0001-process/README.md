# HBEP-1: Enhancement proposal process

## Summary

Design decisions in this repository are made as hpc-burst enhancement proposals (HBEPs). They are modelled on Kubernetes Enhancement Proposals (KEPs), trimmed for a small team.
- Each HBEP has a tracking issue, and the issue number is the HBEP number.
- Its text is added and changed only by pull request.
- It moves through a fixed set of statuses, and the repository owner approves every change of status.
- Agents build only from what is merged on `main`.
- After acceptance it can still change, through errata, amendments or a replacing HBEP.

## Motivation

This framework is designed by one owner and several software agents working in parallel. Without a fixed process:
- decisions end up scattered across chats and issue threads;
- nobody can tell which design is approved and which is still an idea;
- agents build from drafts;
- a later change silently rewrites what was agreed.

### Goals

- One place and one form for each design decision.
- An approval on the record: which approver approved which revision, and when.
- A status that an agent can read from a file on `main` before it starts building.
- Traceability from proposal to code: implementation PRs cite the HBEP they implement.
- A defined way to correct or change an accepted proposal without losing what was agreed before.

### Non-goals

- Kubernetes' organisational layers: SIGs, release teams, a separate production-readiness approver. One owner approves.
- Tracking bugs and small changes. Those stay ordinary issues and PRs. An HBEP is for decisions that shape the design.
- Freezing proposals once accepted. An HBEP describes what is built, so it follows the code.

## Proposal

### Lifecycle

| Step | Where | Gate |
|---|---|---|
| 1. Idea | An issue titled `HBEP: <title>` with the `hbep` label: a paragraph on the problem. Discussion of the idea stays on this issue. | none |
| 2. Provisional | A PR adding `hbep/NNNN-short-title/` copied from `hbep/template/`, with `status: provisional`. | approval: worth exploring |
| 3. Design | Further PRs fill in the design, the test plan and the readiness answers. Spikes answer open questions. | review (see Roles) |
| 4. Implementable | The PR that sets `status: implementable`. | **approval.** Implementation may start once it is merged. |
| 5. Build | Code PRs cite `HBEP-N` and the section they implement. | per PR (see Building from an HBEP) |
| 6. Implemented | The PR that sets `status: implemented` and `stage: alpha` (or `none` for an HBEP without stages). | approval |
| 7. Graduation | One PR per stage promotion (alpha → beta → stable), with evidence against the HBEP's Graduation criteria. | approval |

- One PR may add several provisional HBEPs together.
- Any other status or stage change is one HBEP per PR. The one exception is the replacement transaction below, which changes two.
- "Approval" always means the approval defined in Roles and approval.

### Statuses and transitions

- **provisional**: agreed to be worth exploring. The design may be incomplete. Only spikes may be built.
- **implementable**: the design is approved, and implementation may start once this status is on `main`.
- **implemented**: built. `stage` records the maturity reached: alpha, beta or stable, with criteria in the HBEP's Graduation criteria. An HBEP without stages, such as a process HBEP, has stage `none`.
- **deferred**: paused. Nothing new may be built from it.
- **rejected**: the approver decided against it.
- **withdrawn**: the author dropped it.
- **replaced**: superseded by another HBEP, named in `superseded-by`.

Allowed transitions. Every one goes through a PR with approval, including a withdrawal the author asks for:

| From | To |
|---|---|
| provisional | implementable, deferred, rejected, withdrawn |
| implementable | implemented, deferred, withdrawn, replaced |
| implemented | replaced |
| deferred | provisional, rejected, withdrawn |

- **Terminal statuses.** Rejected, withdrawn and replaced are terminal. Reviving such an idea takes a new HBEP, which lists the old one in `see-also`.
- **Abandoning a design.** An implementable HBEP that will never be built is moved to deferred or withdrawn, so it stops granting permission to build.
- **Removing what an implemented HBEP built** takes a replacing HBEP that says so.
- **The records stay.** HBEPs in terminal statuses remain in the repository, because they record why something was not done.
- **The tracking issue** closes when the HBEP reaches a terminal status, or reaches the final stage its Graduation criteria name (status `implemented` for an HBEP without stages). It stays open while deferred.

### Roles and approval

- **Author:** anyone, including an agent. Writes the HBEP and keeps it current while it is being built.
- **Reviewer:** at least one, who did not write the HBEP. Either a person, or an agent working from a fresh context that never saw the draft. A reviewer checks the text section by section against the template, the tracking issue, and anything merged on `main` since the PR was opened.
- **Approver:** an account listed in `approvers` in the HBEP's `metadata.yaml`. For now that is only the repository owner.

What counts as approval:
- **An approving GitHub review by an approver, on the PR's current head commit.**
  - An approval from any other account does not count, including another agent's bot.
  - Any push after the approval needs a new approval.
- **A direct approval recorded by the author.**
  - The approver may approve in a direct session with the author, such as a chat.
  - Before merging, the author records it as a PR comment. The comment quotes the approver's words and gives the time and the approved head commit.
  - It counts like an approving review on that commit, and any push after it needs a new approval.
  - Only an author who received the approval directly may record it. An approval relayed through another agent does not count.
- **A PR opened by the approver** cannot be approved by them, since GitHub forbids approving your own PR. The approver's own merge of it is the decision. Agents never merge a PR the approver opened.
- **Changing `approvers`**, `CODEOWNERS` or this HBEP needs approval like any status change.

Enforcement:
- `.github/CODEOWNERS` assigns `hbep/` and itself to the approver.
- Branch protection on `main` should require a pull request. Requiring code-owner review would block merges on a recorded direct approval, so it stays off unless the approver chooses to give every approval on GitHub.
- The author checks before merging that the current head commit has an approver's APPROVED review, or a recorded direct approval naming that commit (`gh pr view <N> --json reviews,headRefOid,comments`).
- The merge must name the approved commit (`gh pr merge --match-head-commit <sha>`), so a push in between makes the merge fail.

The author merges a PR once the approval it needs is in place.

### Numbering and layout

- The HBEP number N is the tracking issue's number. Agents drafting at the same time therefore never take the same number; the sequence has gaps where issues are not HBEPs.
- The folder is `hbep/NNNN-short-title/`, with N zero-padded to four digits. It holds:
  - `README.md`: the text, from `hbep/template/README.md`;
  - `metadata.yaml`: the number, title, status, stage, authors, approvers, tracking issue, dates, replaces, superseded-by and see-also.
- Everywhere else N is written unpadded: the title `HBEP-N`, the field `hbep: N` and the issue URL.
- `hbep/README.md` lists every HBEP with its status and stage. Each PR that adds an HBEP or changes a status or stage updates that list.

### Building from an HBEP

- **The authoritative text** is the HBEP as merged on `main`. A status or text on a PR branch, or in an unmerged change, grants nothing.
- **Code may be built** from an HBEP whose merged status is `implementable` or `implemented`. From a merged `provisional` HBEP, only spikes may be built. Spike code is not shipped.
- **A deviation found while building:**
  - If it is an erratum, it may go in the same PR as the code.
  - If it is an amendment, it is merged first, with approval. The code that depends on it is built after.
- **An agent may author and review HBEPs.** It never approves, never merges a PR that lacks the approval it needs, and never merges a PR the approver opened. It records a direct approval only when it received it itself.

### Changes after an HBEP is implementable

- **While provisional,** an HBEP is edited freely by PR, with review; approval is needed only for status changes.
- **Erratum:** a change that keeps the meaning. Examples: a typo, a broken link, formatting, or a rewording that the author and the reviewer agree changes no behaviour. A PR with one review; no approval round.
  - A change that chooses between behaviours an unclear sentence allowed is not an erratum. It is an amendment.
- **Amendment:** a change of behaviour or detail that keeps the HBEP's goals. A PR that edits the text and adds a dated entry to the Amendments section, with approval.
- **Replacement:** a different design, or a change to the goals.
  - A new HBEP goes through the lifecycle, with `replaces: [N]` from its first PR.
  - The old HBEP keeps its status, and stays the basis for building, until the successor becomes implementable.
  - The PR that makes the successor implementable also:
    - sets the old HBEP to `status: replaced` with `superseded-by: [M]`;
    - updates both index rows.
  - If the successor is rejected or withdrawn, the old HBEP is unchanged.

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

### Notes, constraints and caveats

- **Bootstrap.** HBEP-1 defines the process, so it cannot go through its own provisional step.
  - It is proposed directly as `implementable`. The approver's approval of the PR that adds it is the gate.
  - It becomes `implemented` once three HBEPs after it have been merged as `implementable` through it.
- **What the template keeps from KEPs:** the sections that carry decisions.
  - summary, motivation, goals and non-goals;
  - user stories;
  - risks;
  - design, with compatibility and security;
  - test plan;
  - graduation;
  - drawbacks and alternatives;
  - history.
- **What the template adds to KEPs:** a "Prior art and reuse" section.
  - KEPs leave prior art to Motivation and Alternatives.
  - This framework is composed from community software, so each HBEP states what it reuses (with licence), which designs and papers it borrows, and which field lessons it answers.
- **What the template changes:** it replaces KEP's production-readiness questionnaire with seven operational-readiness questions written for this framework:
  - health and end-to-end signals;
  - restart behaviour;
  - silent failure;
  - provider limits;
  - capacity and exhaustion;
  - idle cost;
  - rollback.
- **Stage.** A KEP's `stage` records the stage it targets in a release. An HBEP's `stage` records the stage it has reached.

### Risks and mitigations

- **Process overhead slows small decisions.** Only decisions that shape the design need an HBEP. Errata skip the approval round. Provisional HBEPs may be short.
- **Text and code drift apart.** Building follows the merged text. Amendments are merged before the code that needs them. Reviewers of code PRs check the cited section.
- **Two agents change the same HBEP at once.** Their edits to different paragraphs can merge cleanly and still contradict each other. So before merging, the author updates the branch from `main` and re-reads every section changed on `main` since the PR opened, and the reviewer checks the PR against those changes too.
- **An approval is applied to a different revision than the one approved.** Approval counts only on the current head commit, and stale approvals are dismissed.

### Open questions

- A script that checks `metadata.yaml` fields, allowed transitions and the index, and verifies the approving account and commit, run in CI. To be proposed once there is code to run CI on.

## Design details

### Test plan

Manual until the check script exists. The reviewer of each HBEP PR checks that:
- the folder name matches the issue number;
- `metadata.yaml` has every field of the template;
- the status change is an allowed transition;
- the index lists the HBEP with the right status and stage.

The author checks the approval as described in Roles and approval.

### Compatibility and upgrades

N/A: a process, not software. A later process HBEP amends or replaces this one by the same process.

### Security

Approval is the only control: who may change what is designed and built. It is tied to an approver's account and to the exact commit approved, enforced by CODEOWNERS and branch protection.

### Graduation criteria

N/A: a process HBEP has no stages. It is `implemented` once three HBEPs after it have been merged as `implementable` through it.

## Operational readiness

1. **Working:** every HBEP in `hbep/` has a matching tracking issue, a valid `metadata.yaml` and a row in the index, and every status change on `main` came through a PR with approval. A status change without one is the signal that the process is not followed.
2. **Restart:** N/A. There is no running process; state is the files in git.
3. **Silent failure:** an index row that disagrees with `metadata.yaml`, or a merge whose approval named an older commit. The reviewer and author checks catch these until the check script exists.
4. **Provider limits:** N/A.
5. **Capacity and exhaustion:** N/A.
6. **Idle cost:** none.
7. **Rollback:** a later HBEP can amend or replace this one by the same process.

## Drawbacks

- More ceremony than writing decisions in issues.
- Gaps in the numbering, because numbers come from issues.
- One approver is a bottleneck. Adding approvers is a change to this HBEP.

## Alternatives

- **Python PEPs and Rust RFCs.**
  - Both allow changes to accepted proposals: Rust RFCs through minor amendments, PEPs through approved changes, and process PEPs are updated continually.
  - But in both, substantial changes usually go to a new proposal, and the accepted text serves mainly as a record of the decision.
  - KEPs instead keep the text current with the implementation. We follow KEPs, because this framework's designs will change as providers and engines change.
- **Architecture decision records.**
  - They have statuses (proposed, accepted, superseded) and are lighter.
  - But they are short records of one decision, not usually edited after acceptance, and they have no place for user stories, test plans or readiness questions.
- **Issues only.** There is no single current text, and no file on `main` an agent can read to learn what is approved.
- **Sequential numbers claimed in the index.** Numbers without gaps, but two agents drafting at once can claim the same number.

## Implementation history

- 2026-10-28: proposed as implementable (bootstrap), tracking issue #1. Revised after a fresh-context review.

## Amendments

- **2026-10-29: direct approval.**
  - The approver directed that a chat approval counts.
  - The author records it on the PR and merges with the approved commit named.
  - Branch protection requires a PR but not code-owner review.
- **2026-10-29: prior art and reuse.** The template gains a "Prior art and reuse" section, and HBEP-15 is to define the catalogue of field lessons and the dependency policy.
