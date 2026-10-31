# HBEP-15: Dependencies and provenance

## Summary

This HBEP decides where the repository records the origin of each part of the framework, and how those records are checked. It follows Kubernetes practice, trimmed for a small team.
- Go modules are recorded in `go.mod` and `go.sum`. Components outside Go (engines, routers, images, binaries) are recorded in `build/dependencies.yaml`.
- Every licence of what the framework distributes must be on an allowlist taken from the CNCF Allowlist License Policy. Licence texts of what the framework ships are kept under `LICENSES/`.
- Code copied from another project lives under `third_party/forked/<project>/`, with its original licence and a note of its origin and changes.
- Every dependency change is reviewed by a named dependency reviewer against a written checklist.
- Each release publishes an SPDX SBOM.
- Field lessons live in a public catalogue, [`docs/lessons/README.md`](../../docs/lessons/README.md), with stable IDs `L-<section>.<item>`. HBEPs cite lessons by these IDs.

It is written for authors and reviewers of HBEPs and code PRs.

## Motivation

N/A for the user story: this is a repository policy and serves no single stage of HBEP-2.

Problems this addresses:
- The framework is composed from community software (HBEP-2). Without a record, nobody can say which projects it ships, under which licences, or whether those licences allow shipping them together.
- Code copied from another project loses its licence notice and origin unless both travel with it. One copy is already a candidate: a load-balancing algorithm that sits in a Go `internal/` package and cannot be imported.
- The repository had no licence until 31 Oct 2026, so nothing could be checked against one. It is now Apache-2.0 (open question 1).
- HBEP-1's template asks each HBEP to cite field lessons by ID from a catalogue this HBEP defines. Until the catalogue exists, HBEPs write `pending`.
- The lessons that shaped the design were recorded privately, with host names, job IDs and study details. HBEPs need a public, neutral form they can cite (HBEP-1, Content rules).
- Something referenced by a mutable name drifts under it. A checkpoint served without a pinned revision changed its tokenizer and template (L-8.3). Dependencies need the same pinning.

### Goals

- One fixed record per kind of origin, readable from `main`.
- Every dependency's licence checked against a written allowlist, by a check that compares with the allowlist itself.
- Copied code that carries its upstream licence, URL, commit and list of changes.
- Every dependency change reviewed by a named reviewer against a written checklist.
- Each release describing its contents in a machine-readable SBOM.
- Field lessons with stable IDs that HBEPs, tests and the known-issues register (HBEP-4) can cite.

### Non-goals

- Legal analysis beyond the allowlist. Anything off the list goes to the approver as an exception.
- Supply-chain hardening beyond checksums and review: signing, provenance attestations, reproducible builds. Security is not the main concern; the limits are listed under Security.
- The identity of checkpoints and serving configurations (HBEP-3, HBEP-9).
- Classifying lessons by HBEP-2's dimensions (open question 6).
- Choosing the implementation language. Go is assumed, because the community components evaluated for the control plane, the tunnel and the event layer (controller-runtime, crossplane-runtime, frp, NATS) are Go. For another language, its lock file takes the place of `go.mod` and `go.sum`.

## Proposal

### User stories

- An author adds controller-runtime. The PR changes `go.mod` and `go.sum`. CI finds its licence, Apache-2.0, on the allowlist. A dependency reviewer runs the checklist, and the adopting HBEP lists it under "Community software reused".
- An author needs KubeAI's bounded-load balancer, which sits under Go `internal/` and cannot be imported. They copy it to `third_party/forked/kubeai/` with its `LICENSE` and an `ORIGIN.md` naming the upstream URL, commit and changes. Each changed file says it was changed.
- The Instance launcher pins a vLLM version and a router image. Neither is a Go module, so both are entries in `build/dependencies.yaml`, with licence and upstream URL. A check fails if the launcher's pinned version drifts from the entry.
- A dependency update changes its licence to one off the allowlist. The licence check fails. The PR proceeds only with the approver's word, recorded as an exception.
- A reviewer of HBEP-5 reads L-4.1 in its "Field lessons answered" table and finds the lesson in the catalogue.
- An operator of the framework meets a new failure. The lesson is added as the next item of its section, in general terms.
- A user downloads a release, reads its SBOM to see which components it contains, and finds their licences in the release's `LICENSES/` tree and licence report.

### Notes, constraints and caveats

- **Kubernetes is the model.** Its repository has a `LICENSES/` tree, a `third_party/forked/` folder, and `dep-approvers` and `dep-reviewers` aliases in `OWNERS_ALIASES`. Its contributor guide gives the dependency review checklist (Prior art and reuse lists the sources).
- **Kubernetes' licence script does not check the CNCF allowlist.** Its `hack/verify-licenses.sh` refers to the CNCF list in comments and messages, but it compares each package's licence with every non-deprecated SPDX identifier. So it catches unknown licences, not licences off the allowlist. In Kubernetes the allowlist is applied by the dependency approvers in review. This HBEP checks the allowlist in CI as well. That is new here, not Kubernetes practice.
- **CNCF's automatic approval does not cover modified copies.** It requires a component to be stored unmodified in a designated third-party folder, or fetched at installation or build time. A changed copy under `third_party/forked/` falls outside it, so copies get their own rule (Copied code).
- **`go.mod` sees only Go.** go-licenses states that it cannot check non-Go code inside a dependency. Engines, routers, images and binaries are recorded separately, in `build/dependencies.yaml`, as Kubernetes does for its own external versions.
- **Agents have no GitHub accounts** (HBEP-1), so the dependency reviewers are named as a role, not as a GitHub team (open question 2).
- **The catalogue's numbering is inherited.** The lessons were first kept in a private problem list. The catalogue keeps its section and item numbers, so IDs cited from that list stay valid. Its header names the research project the lessons come from, as HBEP-1's content-rules amendment of 2026-10-30 allows.
- **Relation to HBEP-1.** HBEP-1's content-rules amendment of 2026-10-30 lets HBEPs name the research project and cite lessons by catalogue ID, and drops `pending`: a lesson the catalogue lacks is added to it first. That amendment and this HBEP merge together. HBEP-2's open question 5, on a public problem catalogue, is answered here.

### Risks and mitigations

- **A licence check that passes while wrong.** A check that only validates SPDX identifiers, as Kubernetes' does, passes every known licence. Mitigation: compare with the allowlist file, and keep a fixture whose licence is off the list, which must fail (Test plan).
- **An unknown licence passes as "no licence found".** go-licenses can fail to classify a licence. Mitigation: an unknown licence fails the check.
- **Components outside Go escape the record.** Mitigation: every image, binary or top-level package version that the launcher or deployment files pin must match an entry in `build/dependencies.yaml`; an image's transitive contents are in its SBOM.
- **A copy loses its origin.** Mitigation: `ORIGIN.md` has required fields, checked in CI; resyncing with upstream is a PR that updates it.
- **Lesson IDs change and citations break.** Mitigation: IDs are only appended; CI checks that every ID on `main` still exists and that every ID an HBEP cites exists.
- **A lesson leaks private data.** Mitigation: the catalogue's content rules, a CI grep for generic forbidden shapes, a private pre-push scan for named patterns, and the reviewer's check.
- **Review overhead.** Mitigation: only licence exceptions and non-Apache modified copies need the approver's word; routine updates need the checklist alone.

### Open questions

1. **The project's licence.** Decided: Apache-2.0 (owner decision, 31 Oct 2026). `LICENSE` at the root holds the standard text, unchanged. Reasons: the CNCF policy is written relative to Apache-2.0, every component named in this HBEP is under it, and spike S12 (PR #70) found all 47 modules in the `hpcburst` binary under Apache-2.0, BSD-3-Clause, MIT or ISC. The project has no `NOTICE` of its own, as Kubernetes has none; a copied project's `NOTICE` travels with its copy (Copied code).
2. **Who the dependency reviewers are.** Candidate: the role is filled by the code pipeline's fresh-context reviewers, with the checklist below; the approver decides exceptions. Whether `.github/CODEOWNERS` also lists the owner for `go.mod`, `go.sum`, `build/dependencies.yaml`, `third_party/` and `LICENSES/` needs the approver's word, as every CODEOWNERS change does (HBEP-1). Owner decision.
3. **Components fetched at run time.** The launcher installs some components inside jobs on the cluster, so the framework never redistributes them. Do they need an allowlisted licence? CNCF's condition 2 covers components fetched at installation or build time. Candidate: yes, the same allowlist, because the framework chooses and installs them.
4. **SBOM tool.** Decided: syft, from spike S12 (PR #70). The format is SPDX 2.3. On the `hpcburst` binary, kubernetes-sigs/bom and syft were equal: both SBOMs validated, and each listed exactly the 47 modules plus the main module that `go version -m` reports, with none missing, extra or listed twice. They differ on job images, per each tool's documentation only, since S12 scanned no image: syft documents OCI layouts, OCI archives, docker archives and SIF files, which Helios runs; bom's documentation mentions registry images and docker archives only. The first release that publishes a job image scans one with syft before relying on it; if that fails, this question reopens. Neither SBOM carries licences, so the licence check stays a separate step.
5. **The format of `build/dependencies.yaml`.** zeitgeist checks Kubernetes' file. Can its entries carry licence and upstream fields, or do those go in a parallel list? Decided when the first non-Go component is added.
6. **Classifying lessons.** Tag each lesson with HBEP-2's dimensions (stage, platform, weight, fix owner)? Useful for test plans and the known-issues register. Decided once HBEP-2 resolves its open questions.
7. **Naming the source project.** Resolved by HBEP-1's content-rules amendment of 2026-10-30: the research project and its tools may be named; no other project or study, and no study's results or data.
8. **Vendoring.** Kubernetes vendors its modules. Candidate: no `vendor/` directory; `go.sum` and the Go checksum database are enough at this size. Revisit if builds must run offline on a cluster.

## Design details

### Where each origin is recorded

| Origin | Record | Checked by |
|---|---|---|
| Go library | `go.mod`, `go.sum` | `go mod verify`; the licence check |
| Component outside Go that the framework pins directly: engine, router, tunnel, metrics server, container image digest, top-level package of a job image | `build/dependencies.yaml`: name, version or digest, upstream URL, SPDX licence, where it is pinned, the HBEP that adopted it. Transitive contents of an image are listed by its SBOM, not here | a drift check (zeitgeist, open question 5); the licence check |
| Code copied from another project | `third_party/forked/<project>/` | the copy check |
| Licence texts of what the framework ships | `LICENSES/` | regenerate and diff |
| A design or paper borrowed | the adopting HBEP's "Designs and published work borrowed" table | HBEP review |
| A field lesson | `docs/lessons/README.md`, cited by ID in the HBEP's "Field lessons answered" table | the lesson ID check |
| A release's contents | an SPDX SBOM attached to the release | SBOM validation |

### Dependencies

- A Go module enters only through `go.mod` and `go.sum`, as in Kubernetes. Prefer tagged releases. A module pinned to a commit needs a trailing comment naming the release it corresponds to, and a tracking issue to move to a tag, as Kubernetes asks.
- A component outside Go that the framework pins directly gets an entry in `build/dependencies.yaml` before anything pins it. Images are pinned by digest, packages by version in a lock file. The packages an image pulls in transitively are listed by the image's SBOM.
- No `vendor/` directory for now (open question 8).
- **frp's rows** come from HBEP-6. `build/dependencies.yaml` does not exist yet (k3s's checksums are still in the launcher's own pins file), so the PR that adds frp creates it, in the format open question 5 decides then, and fills in each binary's SHA-256 from the release:

  | Name | Version | Upstream URL | SPDX licence | Where it is pinned | Adopted by |
  |---|---|---|---|---|---|
  | frps | v0.71.1, the release binary for the control-plane host's architecture | https://github.com/fatedier/frp | Apache-2.0 | the launcher's checksum check before it starts frps (HBEP-11) | HBEP-6 |
  | frpc | v0.71.1, the `linux_amd64` and `linux_arm64` release binaries | https://github.com/fatedier/frp | Apache-2.0 | the Provider's controller's staging at `frpc/<digest>/frpc`, and the digest in `agent.json` that the agent checks (HBEP-4, HBEP-7) | HBEP-6 |

### Licence allowlist

- **Allowed:** Apache-2.0, plus the licences of the CNCF allowlist, as listed on 2026-10-30: 0BSD, BSD-2-Clause, BSD-2-Clause-FreeBSD, BSD-3-Clause, MIT, MIT-0, ISC, OpenSSL, OpenSSL-standalone, PSF-2.0, Python-2.0, Python-2.0.1, PostgreSQL, SSLeay-standalone, UPL-1.0, X11, Zlib, and the Google patent licence for Go.
- **The CNCF conditions, as adopted here:**
  - (1) the licence is on the list: checked in CI, though that check is disabled since 31 Oct 2026 by the owner's decision (`if: false` in `.github/workflows/ci.yml`, PR #83; the scripts stay in `hack/`; Implementation history);
  - (2) unmodified in a third-party folder, or fetched at installation or build time: Go modules and `build/dependencies.yaml` entries meet it; modified copies follow Copied code;
  - (3) substantial use outside the project (a standard library, or created at least 12 months ago with at least 10 stars or forks): a checklist item for the reviewer, not a CI check.
- **Exceptions:** anything else, an unknown licence included, needs the approver's word. It is recorded in `hack/license-exceptions.yaml` with the package, licence, reason, PR and date.
- **The machine-readable list** is `hack/allowed-licenses.txt`. It changes only by amendment of this HBEP.
- **The check:** `go-licenses check ./... --allowed_licenses=<the list>` for Go code, and a lookup of each `build/dependencies.yaml` licence for the rest. Both fail on an unknown licence.
  - **One run per module,** because `./...` does not reach a nested module. The modules are every `go.mod` outside `spikes/`, found by a search of the whole tree, not a fixed list: today the root, `third_party/forked/kubeai` and `third_party/forked/kcp`.
  - **No empty run.** A run over a module with no buildable package exits 0 and checks nothing (spike S12), so each run asserts that it checked at least one package. A module may be listed as a known zero only with a recorded reason. Today that is `third_party/forked/kcp`: its one file is behind the `s3embedded` build tag, no binary links it, and the copied-code check still covers it.
  - **Spike modules are outside the check, the allowlist and the dependency review.** The framework does not distribute them, and the root `LICENSE` covers them as repository content. A dependency that product code adopts from a spike enters through a `go.mod` outside `spikes/`, and is checked and reviewed then. Checking spikes would also need a copy of `LICENSE` in each spike module, because go-licenses v2.0.1 looks for a licence file only up to the package's own module directory (`licenses/library.go`, line 221, which calls `FindCandidates(pkg.pkgDir, pkg.moduleDir)`; `licenses/find.go`, "Find will not search out of the rootDir"). The allowlist applies to what the framework itself distributes: its Go binaries, copied code and the entries of `build/dependencies.yaml`. A job image's transitive packages are recorded in its SBOM and not checked against the allowlist, because GPU vendor runtime wheels (licence undeclared, or proprietary) and MPL-2.0 packages are part of every engine image. Whether the framework publishes engine images, and so redistributes them, is open question 3. A licence that changes in an update is flagged in the PR, as Kubernetes asks.

### LICENSES/

- The project's own licence is `LICENSE` at the root (open question 1).
- `LICENSES/` holds a copy of the licence of everything the framework ships, by path, as in Kubernetes:
  - `LICENSES/go/<module path>/LICENSE` for Go modules;
  - `LICENSES/third_party/forked/<project>/LICENSE` for copies;
  - `LICENSES/components/<name>/LICENSE` for `build/dependencies.yaml` entries the framework redistributes, such as images it publishes.
  - A `NOTICE` file is copied beside the licence where upstream has one.
- Why: Apache-2.0 section 4 requires giving recipients a copy of the licence and the NOTICE attributions, and a Go binary contains its dependencies.
- Generated by a script (`go-licenses save` for Go modules), never edited by hand. CI regenerates the tree and diffs it, as Kubernetes' `verify-vendor-licenses.sh` does.

### Copied code

- Copied code goes under `third_party/forked/<project>/`, never into the framework's own packages.
- The folder holds:
  - the copied files;
  - the upstream `LICENSE`, and `NOTICE` or `PATENTS` where present, unchanged;
  - `ORIGIN.md`: upstream URL, commit or tag, SPDX licence, date copied, why it was copied rather than imported, and a list of changes.
- Each modified file carries a line saying it was changed. Apache-2.0 section 4(b) requires this for Apache-licensed code; here it applies to every copy.
- A copy under Apache-2.0 needs the dependency checklist. A modified copy under any other licence also needs the approver's word, because CNCF's automatic approval covers only unmodified copies.
- Resyncing with upstream is a PR that updates `ORIGIN.md`.

### Dependency review

- **Scope:** any PR that changes a `go.mod` or `go.sum` outside `spikes/`, `build/dependencies.yaml`, `third_party/`, `LICENSES/`, the allowlist or the exceptions.
- **The reviewer** is a named dependency reviewer (open question 2). It records the checklist answers in a PR comment.
- **The checklist,** from Kubernetes' guide to dependency changes:
  - Is the dependency maintained?
  - Does it bring value? Could this be done without it?
  - Is it the original project, or a fork?
  - Does the project already have a dependency that does something similar?
  - Is its licence on the allowlist? Did the licence change in this update?
  - Do the `go.mod` changes match what the PR claims?
  - Is there a tagged release to use instead of a commit?
  - Does the imported code have `init()` functions or other side effects at import?
  - Ours: does the adopting HBEP list it under "Community software reused"? Does it meet CNCF condition 3 (substantial use)?
- **The approver's word** is needed only for a licence exception and for a modified copy under a licence other than Apache-2.0.

### SBOM

- Each release publishes an SPDX 2.3 SBOM, generated in CI from the tagged commit and attached to the release. It covers the Go modules of every binary and the components of every published image.
- Kubernetes does the same: it publishes an SPDX 2.3 document per release at `sbom.k8s.io`, generated by its `bom` tool.
- Tool: open question 4.

### Lessons catalogue

- **Place:** one file, `docs/lessons/README.md`, in sections by area.
- **IDs:** `L-<section>.<item>`.
  - The first version keeps the numbering of the private problem list it was rewritten from, so that IDs cited from that list stay valid.
  - IDs are never renumbered, merged or reused. A new lesson takes the next free item number in its section; a new area takes the next section number.
  - A lesson found to be wrong stays, tagged `[withdrawn]`, with one line saying why.
- **Entry:** a list item that begins with the bold ID and a one-line title, then a status tag, then one to three lines on what went wrong and what it taught.
- **Status tags:** `[fixed]`, `[worked around]` or `[open]`, with an optional short qualifier. They record the state where the lesson was learned. `[out of scope]` and `[non-LLM service; parked]` mark lessons the framework does not address (HBEP-2).
- **Citing:** an HBEP cites lessons by ID in its "Field lessons answered" table. Which HBEP answers a lesson is recorded there, not in the catalogue. A lesson an HBEP needs but the catalogue lacks is added to the catalogue first, in the same PR or an earlier one.
- **Content rules:** HBEP-1's Content rules, plus: no host or node names, addresses, ports or job IDs; no user, account or grant names; no private paths; no results or data of any study; and nothing about another team's service beyond what the lesson needs. Platform, organisation and public software names are allowed.
- **Changes:** ordinary PRs, reviewed for the content rules and ID stability. A change to these rules is an amendment of this HBEP.
- **Lessons from the framework's own operation** join the same catalogue.

### Test plan

- **The licence check fails on a fixture module whose licence is off the allowlist,** and on one with an unknown licence. Both fixtures' licences are written by hand. "The check passes on the current tree" proves nothing: a check that compares with SPDX identifiers, as Kubernetes' does, passes too.
- **The dependency record check** fails when the launcher pins an image or version that `build/dependencies.yaml` lacks.
- **The copy check** fails for a `third_party/forked/` folder without `LICENSE`, or with an `ORIGIN.md` missing a field.
- **The `LICENSES/` check** fails when a dependency is added without regenerating.
- **The SBOM** validates as SPDX 2.3, and lists every module the Go toolchain reports for the released binaries. The expected list comes from the toolchain, not from the SBOM tool.
- **The lesson ID check:**
  - IDs are unique and well formed;
  - every ID on `main` still exists in the PR;
  - every ID cited under `hbep/` exists;
  - a committed CI grep for generic shapes only (IPv4 addresses, host-name-like tokens, long digit runs such as job IDs, absolute home or scratch paths) finds nothing in `docs/lessons/`. Named host and node patterns never enter the repository; they live in a CI secret or the private pre-push scan.
- **The first catalogue:** the author lists the private list's item numbers independently, diffs them against the catalogue's IDs, and records the result on the PR. Counting entries is not enough, because a count produced in the same pass that wrote the catalogue would agree with it anyway.

### Compatibility and upgrades

- No API. Lesson IDs are a public interface: they are only appended, as HBEP-2's stages are.
- A change of the project's licence changes the basis of the allowlist (open question 1).
- Downgrade: N/A; every record is a file in git.

### Security

- `go.sum` checksums and the Go checksum database pin module contents; `go mod verify` runs in CI.
- The checklist asks about side effects at import.
- The catalogue's content rules keep private infrastructure data out of a public repository.
- Accepted limits: no signing, attestations or reproducible builds for now. Components fetched at run time inside jobs are pinned, but not verified beyond their pins.

### Graduation criteria

N/A: a repository policy has no stages. It is `implemented` once the licence, dependency record, copy, `LICENSES/` and lesson ID checks run in CI on `main`, and the first release carries an SBOM.

## Operational readiness

1. **Working:** the checks pass on `main`. End to end: a release whose SBOM lists every module and component (identity), and whose licence report and `LICENSES/` give each of them a licence on the allowlist or in the exceptions (licence). The SBOM carries no licences (spike S12). The failure signal is a dependency missing from `LICENSES/`, from the licence report or from the SBOM.
2. **Restart:** N/A; all state is files in git.
3. **Silent failure:** a licence check that compares with the wrong list; a lesson check that counts entries but not IDs. The fixture tests and the ID diff show the difference.
4. **Provider limits:** N/A.
5. **Capacity and exhaustion:** N/A.
6. **Idle cost:** CI minutes only.
7. **Rollback:** revert the PR that added a dependency or copy. A change to this policy is an amendment.

## Drawbacks

- Files and checks to maintain before the framework has much code.
- The approver is a bottleneck for licence exceptions.
- The inherited numbering mixes areas and keeps parked and out-of-scope sections.
- The CI allowlist check duplicates part of what the reviewer checks.

## Alternatives

- **REUSE's `LICENSES/`:** one file per SPDX licence identifier, and nothing else in the folder. It suits a project whose own files carry several licences, but it does not record which dependency uses which licence. The two layouts cannot share one folder, so this HBEP follows Kubernetes'.
- **Vendoring every module,** as Kubernetes does. Offline builds and a visible diff of every change, at the cost of large PRs. Not now (open question 8).
- **Review alone, with no CI allowlist check,** as Kubernetes does in practice. Rejected: agents open most PRs here, and a mechanical check costs little.
- **CycloneDX instead of SPDX.** Both are standard. SPDX is chosen because Kubernetes uses it; a later amendment can add a CycloneDX copy if users need one.
- **Lessons inline in each HBEP.** Rejected: a lesson cited by several HBEPs would be copied and drift.
- **A fresh numbering (L-1, L-2, …).** Cleaner, but IDs already cited from the private list would break.
- **Lessons as GitHub issues.** Stable numbers, but they would mix with tracking issues, and their text would not be reviewed by PR.

## Prior art and reuse

All sources were read on 2026-10-30.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| go-licenses | google/go-licenses, https://github.com/google/go-licenses | Apache-2.0 | CI tool: `check --allowed_licenses`, `report`, `save` |
| bom | kubernetes-sigs/bom, https://github.com/kubernetes-sigs/bom | Apache-2.0 | compared in spike S12, not adopted (open question 4) |
| syft | anchore/syft, https://github.com/anchore/syft | Apache-2.0 | CI tool: the release SBOM (open question 4, decided) |
| zeitgeist | kubernetes-sigs/zeitgeist, https://github.com/kubernetes-sigs/zeitgeist | Apache-2.0 | CI tool, candidate check of `build/dependencies.yaml` (open question 5) |

Licences are each repository's GitHub licence field.

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| A `LICENSES/` tree holding each dependency's licence by path, generated by a script and verified by regenerating; approved by `dep-approvers` | Kubernetes: https://github.com/kubernetes/kubernetes/tree/master/LICENSES, its `OWNERS`, `hack/update-vendor-licenses.sh` and `hack/verify-vendor-licenses.sh` | LICENSES/ |
| Copied code under `third_party/forked/<project>/` beside its upstream `LICENSE` (and `PATENTS`); approved by `dep-approvers` | Kubernetes: https://github.com/kubernetes/kubernetes/tree/master/third_party/forked (for example `golang/` and `cadvisor/`) and `third_party/OWNERS` | Copied code; `ORIGIN.md` is added here |
| Named dependency reviewers | Kubernetes: `dep-approvers` and `dep-reviewers` in https://github.com/kubernetes/kubernetes/blob/master/OWNERS_ALIASES | Dependency review, as a role (open question 2) |
| The dependency review checklist; tagged versions over commits; flag licence changes | Kubernetes contributor guide: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/vendor.md | Dependencies; Dependency review |
| The licence allowlist and its three conditions | CNCF Allowlist License Policy: https://github.com/cncf/foundation/blob/main/policies-guidance/allowed-third-party-license-policy.md | Licence allowlist |
| A licence scan in CI with go-licenses; its gap (it compares with SPDX identifiers, not the allowlist) | Kubernetes: https://github.com/kubernetes/kubernetes/blob/master/hack/verify-licenses.sh | The licence check, which compares with the allowlist |
| Versions of non-Go dependencies in one file, checked against where they are pinned | Kubernetes: https://github.com/kubernetes/kubernetes/blob/master/build/dependencies.yaml, checked by zeitgeist in `hack/verify-external-dependencies-version.sh` | `build/dependencies.yaml` |
| An SPDX 2.3 SBOM per release | Kubernetes: https://kubernetes.io/releases/download/ and https://sbom.k8s.io/v1.37.2/release (created by `bom`) | SBOM |
| Copies of the licence and NOTICE for recipients; notices on modified files | Apache License 2.0, section 4: https://www.apache.org/licenses/LICENSE-2.0 | LICENSES/; Copied code |
| One licence file per SPDX identifier in `LICENSES/` | REUSE Specification 3.3: https://reuse.software/spec-3.3/ | Alternatives (not adopted) |
| Identifiers that are appended, never renumbered | HBEP-2's rule for its stages | Lesson IDs |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| A checkpoint served without a pinned revision changed under its name | L-8.3 | Every dependency is pinned by checksum, digest or locked version, never by a mutable name |
| An environment build broke on packaging details of its dependencies | L-2.11 | Components outside Go are recorded with exact versions and checked where they are pinned |
| Checks that cannot see what they check passed while wrong | L-10.6 | The licence check must fail on a hand-written off-list fixture; the catalogue's IDs are diffed against an independent list |
| A public release made neutral by renaming strings broke strings that tools parse | L-16.4 | The catalogue is rewritten by hand in general terms, never produced by renaming |

## Implementation history

- 2026-10-30: proposed (provisional), tracking issue #15, together with the first lessons catalogue.
- 2026-10-31: the project's licence decided (Apache-2.0, owner) and added at the root; the SBOM tool decided (syft, spike S12); the licence check runs once per module outside `spikes/`, with no empty run; spike modules are outside the check, the allowlist and the dependency review.
- 2026-10-31: frp's rows, frps and frpc v0.71.1, recorded from HBEP-6 becoming implementable; the PR that adds frp creates `build/dependencies.yaml`.
- 2026-10-31: the CI licence check is disabled by the owner's decision; the scripts stay in `hack/`.

## Amendments

None.
