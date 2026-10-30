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
- **Reviewer:** someone who did not write the HBEP. A reviewer checks the text section by section against the template, the tracking issue, and anything merged on `main` since the PR was opened.
- **Approver:** an account listed in `approvers` in the HBEP's `metadata.yaml`. For now that is only the repository owner.

Identity on GitHub:
- Agents have no GitHub accounts of their own. They post through the owner's login.
- Every text an agent posts begins with its handle, for example `@hpc-burst-science-agent to @hpc-burst-code-agent:`. That first line is the only attribution, and it tells an agent's PR from one the owner opened.
- So a GitHub review, approving or not, shows the owner's account whoever wrote it. Agents never submit an approving GitHub review.

The review round, for every PR an agent opens on `hbep/`:
- **Reviewers:** two frontier models from different vendors, each working from a fresh context that never saw the draft, launched at the same time. For now these are Codex `gpt-6-astra` and Gemini argon, at high or extra-high effort. A fresh Claude Opus agent stands in when one of them is unavailable.
- **Merging the findings:** the author takes the union of both reviews. A finding raised by one model alone counts in full, because different models catch different things.
- **Acting:** the author acts on the major findings and records in the PR what it changed and what it left, with reasons.
- **One round only:** there is no re-review after the fixes.

What counts as approval:
- **The completed review round, by default.**
  - For a PR an agent opened, the review round recorded on the PR is the approval. That includes status changes.
  - It covers the commit that holds the fixes. Any later push needs a note on the PR saying what changed.
- **The approver's word, when the author still has doubts.** The author asks the approver first, and merges only on their word, when the PR:
  - settles an open question or decision that is the approver's to take (goals, scope, an irreversible or costly choice, an access change);
  - goes against a recorded decision of the approver.
- **How the approver's word is given:** on GitHub, or in a direct session with the author, such as a chat.
  - The author records a direct approval as a PR comment, quoting the approver's words with the time and the approved head commit.
  - Only an author who received it directly may record it. An approval relayed through another agent does not count.
- **A PR the approver opened** (no agent handle line) is merged by the approver. Agents never merge it.
- **Changing this HBEP** follows the same rules as any amendment, with one addition: the author asks the approver.

Enforcement:
- Branch protection on `main` requires a pull request. Code-owner review is not required: every PR is opened through the owner's login, and GitHub does not let an author approve their own PR. `.github/CODEOWNERS` records the owner of `hbep/`.
- Before merging, the author checks that the PR records the review round, or the approver's word where one was needed.
- The merge names the reviewed commit (`gh pr merge --merge --match-head-commit <sha>`), so a push in between makes the merge fail.

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
- **An agent may author and review HBEPs, and merges its own HBEP PRs once their approval is in place** (Roles and approval). It never submits an approving GitHub review, never merges a PR the approver opened, and records a direct approval only when it received it itself.

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
- **A change is merged that was not the one reviewed.** The merge is pinned to the reviewed commit, and a later push must be noted on the PR.

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

Approval is the only control on what is designed and built. Because agents post through the owner's login, the control is procedural, not account-based:
- the review round recorded on the PR;
- the approver's recorded word where the author has doubts;
- a merge pinned to the reviewed commit.

The owner sees every change in git history and in the agents' reports.

### Graduation criteria

N/A: a process HBEP has no stages. It is `implemented` once three HBEPs after it have been merged as `implementable` through it.

## Operational readiness

1. **Working:** every HBEP in `hbep/` has a matching tracking issue, a valid `metadata.yaml` and a row in the index, and every status change on `main` came through a PR with approval. A status change without one is the signal that the process is not followed.
2. **Restart:** N/A. There is no running process; state is the files in git.
3. **Silent failure:** an index row that disagrees with `metadata.yaml`, or a merge with no review round or needed approval recorded on its PR. The reviewer and author checks catch these until the check script exists.
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

- **2026-10-30: approval by review round.**
  - Owner's direction, 29–30 Oct: a chat approval counts, and agents' PRs, status changes included, merge after one review round by two frontier models launched in parallel. Their findings are merged as a union, and the author acts on the major ones.
  - The approver is asked only when the author has doubts.
  - Agents post through the owner's login with a handle line, and never submit approving GitHub reviews.
  - Branch protection requires a PR, but not code-owner review.
- **2026-10-29: prior art and reuse.** The template gains a "Prior art and reuse" section, and HBEP-15 is to define the catalogue of field lessons and the dependency policy.
