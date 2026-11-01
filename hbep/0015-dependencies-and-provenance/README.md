# HBEP-15: Dependencies and provenance

## Summary

This HBEP decides where the repository records the origin and version of each part of the framework, and how a pinned version is changed. It follows Kubernetes practice, trimmed for a small team.
- Go modules are recorded in `go.mod` and `go.sum`.
- A binary the framework downloads and runs is pinned by sha256 in one file that the code itself reads, such as `internal/launcher/k3s-pins.txt`. The engine image is pinned by digest in each ModelDeployment. There is no `build/dependencies.yaml`.
- Code copied or patched from another project lives under `third_party/forked/<project>/`, with upstream's `LICENSE` and an `ORIGIN.md` that names its origin and every change.
- Every HBEP names what it reuses, and where it came from, in its "Prior art and reuse" section.
- Field lessons live in a public catalogue, [`docs/lessons/README.md`](../../docs/lessons/README.md), with stable IDs `L-<section>.<item>`. HBEPs cite lessons by these IDs.
- "Pinned versions" lists each pin that a design decision rests on, with its reason and what triggers a bump.
- A bump is one PR. It re-checks every HBEP citation of the bumped source, and the live tests under the changed code are run again after it merges.

It is written for authors and reviewers of HBEPs and code PRs.

## Motivation

N/A for the user story: this is a repository policy and serves no single stage of HBEP-2.

Problems this addresses:
- The framework is composed from community software (HBEP-2). Without a record, nobody can say which projects it uses, at which versions, or where a copied file came from.
- Code copied from another project loses its origin unless the origin travels with it. One copy is in use: KubeAI's load balancer, which sits in a Go `internal/` package and cannot be imported (HBEP-5).
- Something referenced by a mutable name drifts under it. A checkpoint served without a pinned revision changed its tokenizer and template (L-8.3). Dependencies need the same pinning.
- HBEPs 3 to 8 and 11 cite upstream source by file and line at a pinned version. A bump moves those lines, and a citation of the old version then describes code the framework no longer runs.
- Some pins are held back on purpose. controller-runtime stays at v0.23.1 for alpha (Pinned versions). Without a written reason and trigger, a routine bump undoes such a decision, or nobody knows when to move.
- HBEP-1's template asks each HBEP to cite field lessons by ID from a catalogue this HBEP defines.
- The lessons that shaped the design were recorded privately, with host names, job IDs and study details. HBEPs need a public, neutral form they can cite (HBEP-1, Content rules).

### Goals

- One fixed record per kind of origin, readable from `main`.
- Copied code that carries its upstream URL, commit, `LICENSE` and list of changes.
- Every pin that a design decision rests on, listed with its reason and its trigger for a bump.
- A bump that re-checks what the HBEPs cite from the old version, and that re-runs the live tests it affects.
- Field lessons with stable IDs that HBEPs, tests and the known-issues register (HBEP-4) can cite.

### Non-goals

- Licence checking is out of scope, by the owner's decision of 31 Oct 2026: no licence allowlist, check, review, report or `LICENSES/` tree. The project itself is Apache-2.0 (`LICENSE` at the root).
- A release SBOM. `go.mod` lists the source's modules, and `go version -m` lists a binary's. If one is needed later, spike S12 compared two tools for it.
- Supply-chain hardening beyond checksums: signing, provenance attestations, reproducible builds. Security is not the main concern; the limits are listed under Security.
- The identity of checkpoints and serving configurations (HBEP-3, HBEP-9).
- Classifying lessons by HBEP-2's dimensions (open question 6).
- Choosing the implementation language. Go is assumed, because the community components evaluated for the control plane, the tunnel and the event layer (controller-runtime, crossplane-runtime, frp, NATS) are Go. For another language, its lock file takes the place of `go.mod` and `go.sum`.

## Proposal

### User stories

- An author adds a Go module. The PR changes `go.mod` and `go.sum`, and the adopting HBEP lists the module under "Community software reused", with its origin.
- An author needs KubeAI's bounded-load balancer, which sits under Go `internal/` and cannot be imported. They copy it to `third_party/forked/kubeai/` with its `LICENSE` and an `ORIGIN.md` naming the upstream URL, commit and changes. Each changed file says it was changed.
- A spike needs an upstream binary to bind loopback only, and upstream has no option for it. The patch goes under `third_party/forked/<project>/` with upstream's `LICENSE` and an `ORIGIN.md`, and the build applies it to the commit `ORIGIN.md` names.
- The launcher starts k3s and frps. Their sha256 sums are in pins files the launcher embeds, and it checks each binary against them before every start. A fetch script that downloads them reads the same file, or a test shows that its own copy agrees.
- After alpha, an author bumps controller-runtime. The worked example under "Upgrading a pinned dependency" shows the PR.
- A reviewer of HBEP-5 reads L-4.1 in its "Field lessons answered" table and finds the lesson in the catalogue.
- An operator of the framework meets a new failure. The lesson is added as the next item of its section, in general terms.

### Notes, constraints and caveats

- **Kubernetes is the model, trimmed.** This HBEP takes its `third_party/forked/` folder and its preference for tagged versions over commits. Kubernetes keeps its non-Go versions in `build/dependencies.yaml`; here each pin already has one file that the code reads (Alternatives).
- **`go.mod` sees only Go.** Engines, binaries and images are recorded separately (Where each origin is recorded).
- **The engine is chosen per deployment.** A served ModelDeployment names `spec.source.served.engine.image` and `spec.source.served.engine.imageDigest` (HBEP-3's `engine.image` and `engine.imageDigest`); a managed one has no engine image. The schema requires the digest, so every launch pulls `image@digest` (HBEP-7). The framework pins no engine image of its own. The engine version its code and HBEPs are written against is vLLM v0.32.0 (Pinned versions).
- **Software the site provides is not pinned by us.** Apptainer and Slurm come with the platform. The platform profile records the measured version as a fact (HBEP-4).
- **The catalogue's numbering is inherited.** The lessons were first kept in a private problem list. The catalogue keeps its section and item numbers, so IDs cited from that list stay valid. Its header names the research project the lessons come from, as HBEP-1's content-rules amendment of 2026-10-30 allows.
- **Relation to HBEP-1.** HBEP-1's content-rules amendment of 2026-10-30 lets HBEPs name the research project and cite lessons by catalogue ID, and drops `pending`: a lesson the catalogue lacks is added to it first. HBEP-2's open question 5, on a public problem catalogue, is answered here.

### Risks and mitigations

- **A copy loses its origin.** Mitigation: `ORIGIN.md` has required fields, checked in CI by the copy check (planned; Test plan); resyncing with upstream is a PR that updates it.
- **Two files pin the same binary and drift apart.** Mitigation: one file per pin wherever the code can read it, as `hack/fetch-k3s.sh` reads `k3s-pins.txt`. Where a script keeps its own copy, as `hack/fetch-frp.sh` does, a test compares the two and reads the script as text.
- **A bump leaves HBEP citations at lines the framework no longer runs.** Mitigation: the bump PR re-checks each one (Upgrading a pinned dependency).
- **A bump passes the unit tests and fails on a real cluster or engine.** The doubles were written from a reading of the old version's source. Mitigation: the live tests under the changed code are run again after the bump merges.
- **A pin held back for a reason is bumped as routine.** Mitigation: Pinned versions records the reason and the trigger, and a bump PR cites the row.
- **A lesson's ID changes and citations break.** Mitigation: IDs are only appended; the lesson ID check (planned; Test plan) makes sure every ID on `main` still exists and every ID an HBEP cites exists.
- **A lesson leaks private data.** Mitigation: the catalogue's content rules, a CI grep for generic forbidden shapes (planned; Test plan), a private pre-push scan for named patterns, and the reviewer's check.

### Open questions

Open questions 1 to 5 were removed with the licence policy and `build/dependencies.yaml` (Implementation history). The others keep their numbers.

6. **Classifying lessons.** Tag each lesson with HBEP-2's dimensions (stage, platform, weight, fix owner)? Useful for test plans and the known-issues register. Decided once HBEP-2 resolves its open questions.
7. **Naming the source project.** Resolved by HBEP-1's content-rules amendment of 2026-10-30: the research project and its tools may be named; no other project or study, and no study's results or data.
8. **Vendoring.** Decided for alpha, as `main` stands: no `vendor/` directory; `go.sum` and the Go checksum database are enough at this size. Revisit if builds must run offline on a cluster.

## Design details

### Where each origin is recorded

| Origin | Record | Checked by |
|---|---|---|
| Go module | `go.mod`, `go.sum` | the `go` command, against `go.sum` and the Go checksum database, on every download; `go mod tidy -diff` in CI |
| Binary the framework downloads and runs | the pins file the code reads: `internal/launcher/k3s-pins.txt` (k3s), `internal/launcher/frp-pins.txt` (frps, frpc), `hack/test-binaries.sh` (the test host's kube-apiserver, kube-controller-manager and etcd) | the launcher's sha256 check before every start (HBEP-11); the fetch scripts' check before install; `TestFrpPinsAgreeWithTheFetchScript` |
| Engine image | each served ModelDeployment's `spec.source.served.engine.image` and `spec.source.served.engine.imageDigest` | admission, which requires the digest; the agent pulls `image@digest` (HBEP-7) |
| Software the site provides | the platform profile's facts (`profiles/*.yaml`), as measured | HBEP-4's profile checks |
| Code copied or patched from another project | `third_party/forked/<project>/` | the copy check |
| Community software an HBEP adopts | its "Community software reused" table | HBEP review |
| A design or paper borrowed | the adopting HBEP's "Designs and published work borrowed" table | HBEP review |
| A field lesson | `docs/lessons/README.md`, cited by ID in the HBEP's "Field lessons answered" table | the lesson ID check |
| A pin that a design decision rests on | Pinned versions, below | the bump PR's review |

- **`build/dependencies.yaml` does not exist on `main`, and this HBEP does not create it.** The pins files above already hold each binary's sha256, and the launcher embeds them. A central list would be a second copy to keep in step.
- HBEP-4, HBEP-6 and HBEP-11 named `build/dependencies.yaml` as the record of frp's and k3s's checksums. They now name the pins files, by erratum in the same PR as this HBEP's move to implementable.

### Dependencies

- A Go module enters only through `go.mod` and `go.sum`, as in Kubernetes. Prefer tagged releases.
  - A direct requirement pinned to a commit of a project that tags releases needs a trailing comment naming the release it corresponds to, and a tracking issue to move to a tag, as Kubernetes asks.
  - A project that publishes no tags, such as `k8s.io/utils`, has only pseudo-versions; that needs no comment.
- A binary the framework downloads or runs is pinned by sha256 in one file the code reads, before anything runs it. A script that does not read that file keeps its own copy, and a test compares the two.
- An image is pinned by digest, never by tag.
- No `vendor/` directory (open question 8).
- **Adding a dependency.** The PR description answers these questions, from Kubernetes' guide to dependency changes. The PR's ordinary reviewers check the answers.
  - Is the dependency maintained?
  - Does it bring value? Could this be done without it?
  - Is it the original project, or a fork?
  - Does the project already have a dependency that does something similar?
  - Do the `go.mod` changes match what the PR claims?
  - Is there a tagged release to use instead of a commit?
  - Does the imported code have `init()` functions or other side effects at import?
  - Ours: does the adopting HBEP list it under "Community software reused"?
- **The licence jobs are removed.** The two disabled jobs, `licence-modules` and `licences` in `.github/workflows/ci.yml`, and the licence scripts and fixtures in `hack/` (`hack/licence*`, `hack/test_licence*.py`, `hack/test-licence-fixtures.sh`, `hack/allowed-licenses.txt`, `hack/known-zero-modules.txt`) were removed in #150.

### Pinned versions

The pins that a design decision rests on, as on `main` on 2026-11-01. The file in "Recorded in" is the record. `docs/development.md`, "Pinned versions", lists the pins of the build and the tests, the test host's binaries included.

| Dependency | Pin | Recorded in | Why this version | Bump when |
|---|---|---|---|---|
| controller-runtime (`sigs.k8s.io/controller-runtime`) | v0.23.1 | `go.mod` | The version crossplane-runtime v2.4.3 requires ([its `go.mod`](https://github.com/crossplane/crossplane-runtime/blob/v2.4.3/go.mod), accessed 2026-11-01). Held there for alpha (science-agent ruling, 1 Nov 2026). Its priority queue can deadlock on shutdown (issue #136). The fix, [kubernetes-sigs/controller-runtime PR #3540](https://github.com/kubernetes-sigs/controller-runtime/pull/3540) (merged 10 Jul 2026), was first released in v0.25.0. No 0.23.x or 0.24.x release carries it: the last of each, v0.23.3 and v0.24.1, came out before the fix merged ([releases](https://github.com/kubernetes-sigs/controller-runtime/releases), accessed 2026-11-01). Our controllers set `UsePriorityQueue` false and use client-go's work queue (PR #137). | After alpha, as one bump with `k8s.io/*` and crossplane-runtime (the worked example below) |
| `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go` | v0.35.3 (`k8s.io/apiextensions-apiserver` v0.35.0) | `go.mod` | The versions crossplane-runtime v2.4.3 requires. HBEP-8 cites their v0.35.3 source by file and line. | With controller-runtime: [v0.25.0's `go.mod`](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.0/go.mod) requires v0.37.0 (accessed 2026-11-01) |
| crossplane-runtime (`github.com/crossplane/crossplane-runtime/v2`) | v2.4.3 | `go.mod` | The Allocation carries its managed-resource fields, declared with crossplane's types and JSON names, so a change upstream changes our API (HBEP-11, Risks). HBEP-8 and HBEP-11 cite its v2.4.3 source. | With controller-runtime, once a crossplane-runtime release builds on controller-runtime v0.25 (its `main` did on 1 Nov 2026, unreleased); or with a new version of the Allocation's API |
| k3s | v1.36.5+k3s1, the default; v1.35.9+k3s1, the source of the minor-upgrade test; each for amd64 and arm64 | `internal/launcher/k3s-pins.txt` (sha256 per release asset), embedded by the launcher; the default is `DefaultVersion` in `internal/launcher/pins.go`. `hack/fetch-k3s.sh` reads the same file. The launcher records which release last wrote the store, and `hpcburst up` starts only that release on it (`internal/launcher/storeversion.go`) | The releases spike S3 measured on the single-user host, upgrade included (HBEP-11). HBEP-8 cites v1.36.5+k3s1's source. | A fix or feature we need. Upgrades go one minor version at a time with a backup first (HBEP-11), so the old default becomes the upgrade test's source |
| frp (frps, frpc) | v0.71.1; amd64 and arm64 in the pins file, linux_amd64 only in the fetch script | `internal/launcher/frp-pins.txt` (the tarball's, frps's and frpc's sha256 per architecture), embedded by the launcher and read by the controller for `agent.json`'s frpc digest; `hack/fetch-frp.sh` keeps its own amd64 copy, which `TestFrpPinsAgreeWithTheFetchScript` (`internal/launcher/frps_test.go`) compares | HBEP-6's tunnel, as spikes S1 and S1c measured it | frps and frpc move together. An frps upgrade waits until no running Allocation has another frpc version, or until that pair is tested (HBEP-6) |
| vLLM | v0.32.0. The real-engine test runs `docker.io/vllm/vllm-openai` at the v0.32.0 tag's multi-arch index, digest `sha256:befccf0e…` | Per deployment: each served ModelDeployment's `spec.source.served.engine.imageDigest`. The version the framework is written against: `internal/agent/enginedouble/config.go` (`Version`), `cmd/hb-agent/realengine_plan_test.go` (image, digest and version), `spikes/s94/README.md` | The engine of spikes S2b and S94. HBEPs 3, 4, 5, 6, 7 and 8 cite its v0.32.0 source by file and line, and the engine double and the CRD descriptions follow the same source. | An engine fix or feature a deployment needs. The bump also re-checks the engine double and the CRD descriptions, and re-runs the real-engine test |
| Apptainer | Not pinned by us: 1.5.4 on Helios GH200 nodes, 1.4.5 on Athena A100 nodes (spike S1 probe 2, 30 Oct 2026) | `profiles/helios.yaml` and `profiles/athena.yaml`, `facts.containers.apptainer_compute_nodes` | The site provides it (HBEP-7). Athena's cannot run a SIF, so its profile sets `imageFormat: sandbox` | Not by us. When a site upgrades it, the fact is measured again and the live tests on that platform are run again |

### Upgrading a pinned dependency

- **One PR per bump.** Modules that must move together are one bump; the PR names each module it moves, and why.
- **Citations.** The PR re-checks every HBEP citation of a bumped module's source (file and line) against the new version.
  - Find them by the old version string under `hbep/`, not only by URL: some citations name a file and line with no link.
  - A citation whose cited behaviour is unchanged gets the new version and line. That is an erratum of its HBEP, and it goes in the bump PR (HBEP-1).
  - A citation whose behaviour changed is an amendment of its HBEP. The amendment merges first, with approval, and the bump follows it (HBEP-1, Building from an HBEP).
  - The PR lists each citation with the line it now cites. A grep that finds no old version string shows only that the citations were renamed, not that they were read again.
- **The record.** The bump PR changes the file in "Recorded in", and its row in `docs/development.md` where it has one. The bump PR always updates the row's Pin cell here, as an erratum, so the table matches its file the moment the bump merges. A new row, or a changed reason or trigger, is an amendment of this HBEP, merged before the bump, and that amendment changes only the reason and trigger, never the Pin cell (HBEP-1).
- **Live re-run after merge.** After the bump merges, every live test that runs the changed code is run again on `main`: tests that need a real cluster, GPU or engine and do not run in CI, such as the real-engine run on GH200 (issue #94) and the Slurm provider's conformance runs (`internal/providerctl/conformance`, run with `HPCBURST_CONFORMANCE=1`; HBEP-4, Graduation criteria; results so far on issue #93). This is the post-merge live-rerun rule, as applied in issue #94 on 1 Nov 2026. The bump is done once the re-runs are posted on its issue.

**Worked example: controller-runtime after alpha.**
1. **The bump.** controller-runtime goes from v0.23.1 to a v0.25 release, the first line with PR #3540's fix. v0.25 requires `k8s.io/api`, `k8s.io/apimachinery` and `k8s.io/client-go` v0.37. crossplane-runtime v2.4.3 was built against controller-runtime v0.23.1 and `k8s.io` v0.35.3, so it moves too, to a release built on controller-runtime v0.25. All three are one bump, in one PR, with the modules they require moving with them, such as `k8s.io/apiextensions-apiserver` and crossplane's `apis` module; the PR names each module it moves.
2. **The citations.** The PR searches `hbep/` for `v0.35.3`, `v0.23.1` and `v2.4.3`. Among the hits are HBEP-8's citations of client-go's `tools/events` and `tools/record`, api's `events/v1/types.go`, apimachinery's `pkg/watch/mux.go`, controller-runtime's `pkg/cluster/cluster.go` and crossplane-runtime's `pkg/event/event.go`, and HBEP-11's of crossplane-runtime's `pkg/resource/enqueue_handlers.go`. Each is read against the new version, and listed with its new line.
3. **The workaround stays.** Our controllers keep `UsePriorityQueue` false. Turning the priority queue back on is a change of behaviour, decided on its own.
4. **The record.** The three rows above get a new reason and trigger, so they change by an amendment of this HBEP, merged before the bump. A citation from step 2 whose behaviour changed is amended in its own HBEP before the bump too. The bump PR then carries `go.mod`, `go.sum`, `docs/development.md`, the citations that still hold, and the three rows' Pin cells here.
5. **After merge.** The live tests that run the controllers are run again: the Slurm provider's conformance runs (`internal/providerctl/conformance`, run with `HPCBURST_CONFORMANCE=1`; HBEP-4, Graduation criteria; results so far on issue #93), and the one-host alpha rehearsal (issue #146) once it exists.

### Copied code

- Copied code goes under `third_party/forked/<project>/`, never into the framework's own packages.
- The folder holds:
  - the copied files, or a patch that the build applies to the upstream commit `ORIGIN.md` names;
  - the upstream `LICENSE`, and `NOTICE` or `PATENTS` where present, unchanged;
  - `ORIGIN.md`: upstream URL, commit or tag, SPDX licence, date copied, why it was copied rather than imported, and a list of changes.
- Each modified file carries a line saying it was changed. A patch adds that line to each file it changes.
- Resyncing with upstream is a PR that updates `ORIGIN.md`.
- On `main` on 2026-11-01:
  - `kubeai`: KubeAI's balancer, a Go module that the root `go.mod` replaces in (HBEP-5);
  - `kcp`: kcp's start sequence for spike S3, built only behind the `s3embedded` build tag;
  - `llm-d-inference-sim` and `llm-d-router`: loopback-bind patches for spike S2.

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

- **The copy check** fails for a `third_party/forked/` folder without `ORIGIN.md`, or with an `ORIGIN.md` missing a field. It is planned and does not run in CI yet. The four `ORIGIN.md` files' field names were aligned in #150 (`Upstream URL`, `Commit or tag`, `SPDX licence`, `Changes`, and `Why copied rather than imported`), with no recorded value changed.
- **The pin drift test,** `TestFrpPinsAgreeWithTheFetchScript`, runs today. It compares `hack/fetch-frp.sh`'s version and three sums with `frp-pins.txt`'s amd64 row, and it reads the script as text. A test that computed the script's values from the pins file would agree with it by construction. k3s needs no such test, because its fetch script reads the pins file.
- **The launcher's checksum checks** have their own tests in `internal/launcher` (for example `TestResolveBinary` and `TestResolveFrpsChecksTheBytesItOpened`).
- **The lesson ID check** is planned and does not run in CI yet. It checks that:
  - IDs are unique and well formed;
  - every ID on `main` still exists in the PR;
  - every ID cited under `hbep/` exists;
  - a committed CI grep for generic shapes only (IPv4 addresses, host-name-like tokens, long digit runs such as job IDs, absolute home or scratch paths) finds nothing in `docs/lessons/`. Named host and node patterns never enter the repository; they live in a CI secret or the private pre-push scan.
- **A bump PR** lists each re-checked citation with its new line (Upgrading a pinned dependency), and its live re-runs are posted on its issue.

### Compatibility and upgrades

- No API. Lesson IDs are a public interface: they are only appended, as HBEP-2's stages are.
- Upgrades of pinned dependencies follow "Upgrading a pinned dependency".
- Downgrade: revert the bump's PR. A k3s downgrade also restores the store's backup (HBEP-11). Every record is a file in git.

### Security

- `go.sum` checksums and the Go checksum database pin module contents; the `go` command checks them on every download.
- Every binary the framework downloads is checked against its sha256 pin before it runs. Images are pulled by digest.
- The dependency questions ask about side effects at import.
- The catalogue's content rules keep private infrastructure data out of a public repository.
- Accepted limits: no signing, attestations or reproducible builds for now. Software the site provides is not verified by us.

### Graduation criteria

N/A: a repository policy has no stages. It is `implemented` once the copy check and the lesson ID check run in CI on `main`, and every row of Pinned versions matches its file.

## Operational readiness

1. **Working:** the checks pass on `main`. End to end: the launcher starts only binaries that match their pins, and every row of Pinned versions matches its file. The failure signals are a binary the launcher refuses, a failing drift test, and an HBEP that cites a version no file pins.
2. **Restart:** N/A; all state is files in git.
3. **Silent failure:** a drift test whose expected values come from the file it checks; a lesson check that counts entries but not IDs; a bump whose citations were renamed but not read again. The drift test reads the script as text, the ID check compares IDs, and the bump PR lists each citation with its new line.
4. **Provider limits:** a live re-run needs an allocation on the platform, and waits in its queue (HBEP-4).
5. **Capacity and exhaustion:** N/A.
6. **Idle cost:** CI minutes; each bump also costs the GPU hours of its live re-runs.
7. **Rollback:** revert the PR that added a dependency, copy or bump; a k3s downgrade restores the store's backup (HBEP-11). A change to this policy is an amendment.

## Drawbacks

- The pins sit in several files rather than one list. The table here and `docs/development.md`'s list repeat them, and can lag.
- A bump costs a citation re-check and live re-runs, so bumps are rare and pins age.
- The inherited numbering mixes areas and keeps parked and out-of-scope sections.

## Alternatives

- **One file for every non-Go version,** as Kubernetes' `build/dependencies.yaml`, checked by zeitgeist. Not adopted: each pin already has one file that the code reads, so a central file would be a second copy that needs its own drift check.
- **Vendoring every module,** as Kubernetes does. Offline builds and a visible diff of every change, at the cost of large PRs. Not now (open question 8).
- **Automatic bump PRs on every upstream release.** Not adopted: each bump moves cited source lines and needs live re-runs, so the HBEPs would trail the code.
- **Lessons inline in each HBEP.** Rejected: a lesson cited by several HBEPs would be copied and drift.
- **A fresh numbering (L-1, L-2, …).** Cleaner, but IDs already cited from the private list would break.
- **Lessons as GitHub issues.** Stable numbers, but they would mix with tracking issues, and their text would not be reviewed by PR.

## Prior art and reuse

Sources were read on 2026-10-30, except where another date is given.

### Community software reused

| Component | Origin (project, URL) | Licence | How used (dependency, copied code, optional) |
|---|---|---|---|
| Go modules and the Go checksum database | golang/go, https://go.dev/ref/mod | BSD-3-Clause (GitHub licence field, read 2026-11-01) | The record and checksums of Go dependencies (`go.mod`, `go.sum`) |

### Designs and published work borrowed

| Idea | Origin (project, paper or document) | Where it lands here |
|---|---|---|
| Copied code under `third_party/forked/<project>/` beside its upstream `LICENSE` (and `PATENTS`) | Kubernetes: https://github.com/kubernetes/kubernetes/tree/master/third_party/forked (for example `golang/` and `cadvisor/`) | Copied code; `ORIGIN.md` is added here |
| Tagged versions over commits; questions to ask of a new dependency | Kubernetes contributor guide: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/vendor.md | Dependencies |
| Versions of non-Go dependencies in one file, checked against where they are pinned | Kubernetes: https://github.com/kubernetes/kubernetes/blob/master/build/dependencies.yaml, checked by zeitgeist in `hack/verify-external-dependencies-version.sh` | Alternatives (not adopted) |
| Notices on modified files | Apache License 2.0, section 4(b): https://www.apache.org/licenses/LICENSE-2.0 | Copied code |
| Identifiers that are appended, never renumbered | HBEP-2's rule for its stages | Lesson IDs |
| Pins listed with a reason and a trigger; a bump that re-checks cited source and re-runs live tests after merge | New: this project (science agent, 1 Nov 2026; the live re-run as applied in issue #94) | Pinned versions; Upgrading a pinned dependency |

### Field lessons answered

| Lesson | Lesson ID (from the lessons catalogue, HBEP-15) | How this HBEP answers it |
|---|---|---|
| A checkpoint served without a pinned revision changed under its name | L-8.3 | Every dependency is pinned by checksum, digest or locked version, never by a mutable name |
| An environment build broke on packaging details of its dependencies | L-2.11 | Binaries outside Go are pinned by exact version and sha256, and checked before they run |
| Checks that cannot see what they check passed while wrong | L-10.6 | The drift test reads the fetch script as text, not values computed from the pins file; a bump PR lists each citation it read again, because a grep for the old version shows only renaming |
| A public release made neutral by renaming strings broke strings that tools parse | L-16.4 | The catalogue is rewritten by hand in general terms, never produced by renaming |

## Implementation history

- 2026-10-30: proposed (provisional), tracking issue #15, together with the first lessons catalogue.
- 2026-10-31: the project's licence decided (Apache-2.0, owner) and added at the root; the SBOM tool decided (syft, spike S12); the licence check runs once per module outside `spikes/`, with no empty run; spike modules are outside the check, the allowlist and the dependency review.
- 2026-10-31: frp's rows, frps and frpc v0.71.1, recorded from HBEP-6 becoming implementable; the PR that adds frp creates `build/dependencies.yaml`.
- 2026-10-31: the CI licence check is disabled by the owner's decision; the scripts stay in `hack/`.
- 2026-11-01: implementable. The licence policy is removed (Non-goals): the allowlist, `LICENSES/`, the licence check, the dependency reviewer, the release SBOM, and open questions 1 to 5. `build/dependencies.yaml` is dropped: the pins files the code reads are the record. Added: Pinned versions, with controller-runtime held at v0.23.1 for alpha (science-agent ruling, 1 Nov 2026), and Upgrading a pinned dependency.
- 2026-11-01: erratum: the licence jobs and the scripts in `hack/` are recorded as removed in #150, where the text said they were kept but unused.
- 2026-11-01: erratum: the Test plan records that #150 aligned the four `ORIGIN.md` files' field names, where the text said the copy check's PR would.

## Amendments

None.
