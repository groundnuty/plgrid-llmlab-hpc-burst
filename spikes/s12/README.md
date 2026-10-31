# Spike S12: SBOM tool and licence check

Answers HBEP-15's open question 4 (which SBOM tool) and shows what its licence check finds on the module today. Issue #67.

## Result in short

- **Recommended tool: syft.** On the `hpcburst` binary, kubernetes-sigs/bom and syft are equal. Both write a valid SPDX 2.3 document that lists exactly the modules `go version -m` reports: none missing, none extra, every version equal. They differ on job images: syft documents OCI layouts, OCI archives, docker archives and Singularity/SIF files; bom documents registry images and docker archives only.
- **Neither tool replaces `go-licenses`.** An SBOM made from a binary carries no licence for any module (only the Go standard library has one). The licence check is a separate step.
- **The licence check passes on every dependency and fails on the repository's own packages.** All 47 modules in the binary use Apache-2.0, BSD-3-Clause, MIT or ISC, all on the allowlist. `go-licenses check ./...` still exits 1, because the repository has no `LICENSE` (HBEP-15 open question 1), so its 8 own packages are "unknown". Until a `LICENSE` lands, the CI step has to pass `--ignore` for the module itself.
- **`./...` does not cover `third_party/forked/kubeai` or `third_party/forked/kcp`, nor the spikes.** Each is its own module and needs its own run. A run over a module with no buildable package exits 0 and checks nothing (kcp today), so CI must not trust an exit status alone.

## Method

1. Built `hpcburst` with `go build -trimpath ./cmd/hpcburst` (Go 1.27.3, linux/amd64). The branch holds main (f880e38) plus `spikes/s12/` only, so the Go code is main's. The tree was clean at build time.
2. Took the reference with `go version -m hpcburst`: 47 `dep` lines, the `mod` line, the toolchain version. Saved as `results/go-version-m.txt`. It comes from the toolchain, not from either tool.
3. Generated an SPDX 2.3 JSON SBOM with each tool from pinned release binaries, sha256 checked against the release's checksum file.
4. Validated each with two validators, each with a negative control (a copy with a required field removed must be rejected).
5. `compare.py` compared each SBOM's Go modules (path and version, case-sensitive) with the reference. Its own tests (`test_compare.py`) use hand-written inputs and cover a missing module, a wrong version, a missing standard library, an extra module and a replaced module.
6. Ran `go-licenses check` and `report` with HBEP-15's allowlist, with a negative control (a licence taken off the list must fail) and hand-written licence fixtures.

Everything is reproduced by one command, which exits 0 only when both SBOMs validate, match the reference, and the licence check passes for everything except the repository's own packages:

    spikes/s12/reproduce.sh <empty work dir>      # about 500 MB, one minute
    CHECK_SPIKES=1 spikes/s12/reproduce.sh <dir>  # also checks each spikes/* module
    python3 -m unittest -v test_compare           # in spikes/s12; tests of compare.py

Its output is committed under `results/`.

## Versions

| Tool | Version | Release date | sha256 of the downloaded file | Licence |
|---|---|---|---|---|
| kubernetes-sigs/bom | v0.8.0 (`bom-amd64-linux`) | 2026-09-22 | `cafd00b75a8df860fec54d19b9e24fb48556c0848d0abf9e40cd2eabb11a8710` | Apache-2.0 |
| anchore/syft | v1.55.0 (`syft_1.55.0_linux_amd64.tar.gz`) | 2026-10-19 | `bb65249542db5fedeeb4d3f9b018b75e7264dbfbbb10486f044581a92f90490d` | Apache-2.0 |
| google/go-licenses | v2.0.1, `go install .../v2@v2.0.1` (checked by the Go checksum database) | 2025-09-08 | n/a | Apache-2.0 |
| spdx/tools-python (`pyspdxtools`) | 0.8.5 | 2026-03-13 | n/a (PyPI) | Apache-2.0 |
| check-jsonschema | 0.39.0 | n/a | n/a (PyPI) | GitHub's licence field says NOASSERTION; not checked further |
| SPDX 2.3 JSON schema | spdx-spec tag v2.3 | n/a | `239208b7ac287b3cf5d9a9af23f9d69863971102a5e1587a27a398b43490b89b` | n/a |

Licences are each repository's GitHub licence field, read 2026-10-31. Each sha256 above is the value in the release's own checksum file, and the script compares the download with both the pin and the release file. The sigstore bundles that the releases also carry were not verified.

Environment: 6-core VM, shared with other agents. The load was 1.3 to 4.1 during the runs, except that the first manual bom and syft runs (0.5 s and 2.7 s, nice 10) started at a load of 6.3, above the 4.0 limit of the spike rules. The build added about 600 MB to the shared Go build cache (`-trimpath` changes the cache keys), which briefly took free space on `/` below 3 GB.

## SBOMs of the binary

`results/bom-binary.spdx.json` (45 KB) and `results/syft-binary.spdx.json` (79 KB).

| | bom v0.8.0 | syft v1.55.0 |
|---|---|---|
| `spdxVersion` | SPDX-2.3 | SPDX-2.3 |
| `pyspdxtools` 0.8.5 | valid | valid |
| SPDX 2.3 JSON schema | valid | valid |
| Packages | 49 (main module, standard library, 47 modules) | 50 (the same 49, plus a FILE entry for the binary) |
| Names `github.com/groundnuty/plgrid-llmlab-hpc-burst` itself | yes, version from the build info | yes, version from the build info |
| Names the Go standard library | yes, `stdlib` 1.27.3 | yes, `stdlib` go1.27.3 |
| Module checksums | none | SHA256 of 48 packages, the `go.sum` hash decoded to hex |
| Download locations | proxy.golang.org URL for each of the 47 modules | NOASSERTION |
| Other references | purl | purl and CPE guesses (84 CPE entries) |
| Licences | `licenseDeclared` only for the standard library (BSD-3-Clause) | NOASSERTION for every module, BSD-3-Clause for the standard library |

The negative controls: `pyspdxtools` rejects a copy without `documentNamespace`, a copy without `spdxVersion`, a package with an invalid SPDX identifier, and a relationship to a missing element (exit 1 each time). **The official SPDX 2.3 JSON schema is the weaker validator:** it rejects the missing `spdxVersion` but accepts the missing `documentNamespace` and the invalid identifier. A pipeline that validates with the schema alone would pass broken documents. Use `pyspdxtools`.

### Comparison with `go version -m`

Reference: the standard library (go1.27.3), the main module, and 47 dependency modules, 49 entries. `results/compare-binary.txt`.

| | bom | syft |
|---|---|---|
| Reference entries present at the reference version | 49 of 49 | 49 of 49 |
| Modules missing | none | none |
| Modules with a different version | none | none |
| Extra Go packages the toolchain did not report | none | none |
| Replaced modules | none in this binary (the `replace` case is covered by the unit test only) | same |

The comparison checks names and versions, not that the other fields of a package are right. Two things it does not exercise on real data: this binary has no module path with an upper-case letter (the purl spec lower-cases Go paths; the unit test covers the matching), and no module is replaced.

**One difference found by accident:** the first manual run built from a tree with untracked files, so Go stamped the main module's version `...+dirty`. bom then wrote the main module with an empty `versionInfo` and a purl without a version, and the comparison failed for bom alone; syft kept the full version. A release built from a clean tagged commit has no `+dirty`, so this does not arise in CI, but it is the reason `reproduce.sh` warns on a dirty tree.

### Source-tree SBOMs (a different population, for context)

`bom generate --dirs` and `syft scan dir:` on the root module's source. `results/compare-source.txt`. Against the binary reference they differ, as they should: bom lists 22 extra modules and syft 50 (modules in `go.mod` and `go.sum` that the binary does not link, such as the code generators and test libraries); the standard library is the `go` directive of `go.mod` (bom reports 1.26.0, syft does not name it). So a source SBOM overstates what a release contains, and HBEP-15's test ("lists every module the toolchain reports for the released binaries") is met only by the binary mode. In the same mode, bom run online recorded a declared licence for 70 of 71 packages, and syft filled `licenseConcluded` for many (34 Apache-2.0, 22 BSD-3-Clause, 15 MIT among its 99 packages); these runs were done by hand and are not part of the script.

## Licence check

Allowlist: HBEP-15's list as of 2026-10-30, copied by hand into `allowed-licenses.txt`. "The Google patent licence for Go" has no SPDX identifier and go-licenses has no name for it, so it cannot be encoded; the `golang.org/x` modules are reported as BSD-3-Clause.

`go-licenses report ./...` has 65 rows (it reports per package, not per module): `results/go-licenses-report.csv`, with versions from a custom template. Every one of the 47 modules in the binary has a row at the same version as `go version -m`. Three more modules (crossplane `apis/v2`, `apiextensions-apiserver`, controller-runtime) appear because other packages of the root module import them.

| Licence (per module in the binary) | Modules |
|---|---|
| Apache-2.0 only | 24 |
| BSD-3-Clause only | 12 |
| MIT only | 5 |
| ISC only | 1 (`github.com/davecgh/go-spew`) |
| Apache-2.0 and BSD-3-Clause (a vendored `third_party` folder inside the module) | 4 (`k8s.io/apimachinery`, `k8s.io/kube-openapi`, `k8s.io/utils`, `sigs.k8s.io/json`) |
| Apache-2.0, BSD-3-Clause and MIT | 1 (`sigs.k8s.io/yaml`) |

`results/go-licenses-check.txt`:

| Run | Exit | Finding |
|---|---|---|
| `check ./...` with the allowlist | 1 | 8 packages, all of the repository's own module: "Did not find license". No `LICENSE` exists, so they are unknown. |
| the same with `--ignore github.com/groundnuty/plgrid-llmlab-hpc-burst` | 0 | no module is outside the allowlist or unknown |
| the same with `--include_tests` | 0 | modules that only tests import are allowed too |
| negative control: BSD-3-Clause taken off the list | 1 | names the 17 BSD-3-Clause packages. The check does compare with the list it is given. |

**Every module whose licence is outside the allowlist or unknown: none**, except the repository's own packages, which are unknown only because there is no `LICENSE`. `--ignore` skips a package's own licence but, per `check --help`, still checks its dependencies.

The default `check` (no `--allowed_licenses`) fails on the same 8 packages; it treats "unknown" and "forbidden" as disallowed (run by hand, not in `results/`).

**Fixtures** (`licence-fixtures.sh`, `results/licence-fixtures.txt`): HBEP-15's test plan asks for a check that fails on an off-list licence and on an unknown one, with hand-written licence texts. A fixture module with a hand-written BSD-4-Clause text is rejected ("Not allowed license 'BSD-4-Clause'"); one with an invented text is rejected ("Did not find license"); a control with a hand-written MIT text passes and appears in `report` as MIT, so the control does look at the module.

**go-licenses does check all licences found for a package:** `sigs.k8s.io/yaml` carries Apache-2.0, BSD-3-Clause and MIT texts, and the negative control flagged it for BSD-3-Clause. A package licensed "A or B" with both texts shipped would need both on the list. Not exercised here.

### Separate modules: `third_party/forked/*` and the spikes

`go list ./...` at the root matches 0 packages under `spikes/` or `third_party/`, so `go-licenses ./...` at the root covers none of them. `results/go-licenses-modules.txt`:

| Module | go-licenses run inside it |
|---|---|
| `third_party/forked/kubeai` | 7 rows, exit 0: its dependencies are all allowed, and its own `LICENSE` (Apache-2.0) is found |
| `third_party/forked/kcp` | **0 rows, exit 0**: its only Go file has the build tag `s3embedded`, so no package matches and nothing is checked. `go list ./...` says "matched no packages". The module also lists no dependency, so it cannot build today. |
| `third_party/forked/llm-d-inference-sim`, `llm-d-router` | no Go code (a `LICENSE`, `ORIGIN.md` and a patch), so go-licenses has nothing to read; HBEP-15's copy check covers them |
| `spikes/s2`, `s2b`, `s3`, `s4`, `s8`, `s10`, `s10b`, `s10c` | each exits 1 for its own packages only (no `LICENSE`); no third-party module is off the list or unknown |

So the shipped copies need their own runs, one per `go.mod` under `third_party/`. The spikes are not shipped; they need runs only if HBEP-15 wants them checked, and the CI step below skips them. A root `LICENSE` may not reach packages in nested modules (go-licenses looks for the licence file above a package, and where it stops was not tested).

## Job images

Not run (disk). From each tool's documentation:

| | bom | syft |
|---|---|---|
| Registry images | yes: `--image` (README, "Process a Container Image") | yes: `registry:` and the Docker, Podman and containerd daemons |
| `docker save` tarballs | yes: `--image-archive` | yes: `docker-archive:` |
| OCI archive (tar) and OCI layout directory | not mentioned in the README or in `bom generate --help`; `--archive` takes a tar or tar.gz as a package | yes: `oci-archive:`, `oci-dir:` |
| Singularity/SIF (the Apptainer images of the cluster providers) | not mentioned | yes: `singularity:` |
| Go binaries inside an image | yes, listed with their modules (README) | yes (Go binary cataloger) |

Sources, read 2026-10-31: https://github.com/kubernetes-sigs/bom/blob/v0.8.0/README.md and `bom generate --help` of v0.8.0; https://oss.anchore.com/docs/guides/sbom/scan-targets/, https://github.com/anchore/syft/blob/v1.55.0/README.md and `syft scan --help` of v1.55.0 (the help text lists `oci-archive:`, `oci-dir:` and `singularity:`). None of the image modes was run.

## Recommendation

**Use syft for the release SBOM, `pyspdxtools` to validate it, and `go-licenses` for the licence check.**

- On Go binaries the two tools are equal, so the choice rests on the other cases. HBEP-15's SBOM section wants one document covering "the Go modules of every binary and the components of every published image"; syft reads OCI layouts, OCI archives and SIF without a registry, which suits a CI job and the cluster images; bom documents registry images and docker tarballs.
- Release cadence differs: syft published four releases from 2026-09-17 to 2026-10-19; bom published v0.8.0 on 2026-09-22 after v0.7.1 on 2025-09-26.
- Against syft: bom is the tool behind Kubernetes' release SBOMs (HBEP-15 takes Kubernetes as the model) and writes smaller documents with download locations. Its main-module version is lost on a dirty tree. If the framework only publishes binaries, either is fine.
- The SBOM tool does not give licences from a binary. Licence evidence comes from `go-licenses` and `LICENSES/`, as HBEP-15 designs. HBEP-15 Operational readiness 1 ("a release whose SBOM lists every module and component, each with a licence on the allowlist or in the exceptions") reads as if the SBOM carried the licence; it does not, in binary mode.

### Proposed CI job

Not applied: main is protected and renaming a job blocks every merge. This is a new job, so existing check names do not change. The action SHAs are the ones `ci.yml` already pins. In the real PR, `compare.py` and the allowlist move to `hack/` (HBEP-15 names `hack/allowed-licenses.txt`), and the `--ignore` line goes once the repository has a `LICENSE`.

```yaml
  sbom:
    name: sbom and licences
    runs-on: ubuntu-24.04
    timeout-minutes: 20
    env:
      SYFT_VERSION: "1.55.0"
      SYFT_SHA256: bb65249542db5fedeeb4d3f9b018b75e7264dbfbbb10486f044581a92f90490d
      GO_LICENSES_VERSION: v2.0.1
      SPDX_TOOLS_VERSION: "0.8.5"
    steps:
      - name: Check out the repository
        uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          persist-credentials: false
      - name: Set up Go from go.mod
        uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0
        with:
          go-version-file: go.mod
          cache: false
      - name: Build hpcburst (a clean checkout, so the version has no +dirty)
        run: |
          mkdir -p "$RUNNER_TEMP/sbom"
          go build -trimpath -o "$RUNNER_TEMP/sbom/hpcburst" ./cmd/hpcburst
          (cd "$RUNNER_TEMP/sbom" && go version -m hpcburst > go-version-m.txt)
      - name: Install syft, checking its sha256
        run: |
          cd "$RUNNER_TEMP/sbom"
          curl -fsSL -o syft.tar.gz "https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/syft_${SYFT_VERSION}_linux_amd64.tar.gz"
          echo "${SYFT_SHA256}  syft.tar.gz" | sha256sum -c -
          tar -xzf syft.tar.gz syft
      - name: Generate the SPDX 2.3 SBOM
        run: |
          cd "$RUNNER_TEMP/sbom"
          SYFT_CHECK_FOR_APP_UPDATE=false ./syft scan file:hpcburst --source-name hpcburst -o spdx-json@2.3=hpcburst.spdx.json
      - name: Validate the SBOM
        run: |
          python3 -m venv "$RUNNER_TEMP/spdx-venv"
          "$RUNNER_TEMP/spdx-venv/bin/pip" install --quiet "spdx-tools==${SPDX_TOOLS_VERSION}"
          "$RUNNER_TEMP/spdx-venv/bin/pyspdxtools" -i "$RUNNER_TEMP/sbom/hpcburst.spdx.json"
      - name: Compare the SBOM with go version -m
        run: |
          python3 -I hack/compare-sbom.py --reference "$RUNNER_TEMP/sbom/go-version-m.txt" \
            --sbom syft="$RUNNER_TEMP/sbom/hpcburst.spdx.json" --fail-on-extra
      - name: Licence check
        run: |
          go install "github.com/google/go-licenses/v2@${GO_LICENSES_VERSION}"
          gl="$(go env GOPATH)/bin/go-licenses"
          list=$(grep -v '^#' hack/allowed-licenses.txt | paste -sd, -)
          # A check over zero packages exits 0, so require rows at the root.
          rows=$("$gl" report ./... 2>/dev/null | wc -l)
          [ "$rows" -gt 0 ] || { echo "::error::go-licenses saw no package"; exit 1; }
          # Drop --ignore once the repository has a LICENSE (HBEP-15 open question 1).
          "$gl" check ./... --ignore github.com/groundnuty/plgrid-llmlab-hpc-burst --allowed_licenses="$list"
          # Separate modules are not covered by ./...: one run per go.mod under third_party/.
          # (third_party/forked/kcp matches no package today and checks nothing.)
          for d in $(find third_party -name go.mod -printf '%h\n'); do
            (cd "$d" && "$gl" check ./... --allowed_licenses="$list")
          done
```

Notes on the job. Attaching the SBOM to a release is a step of the release workflow, outside this job. The HBEP-15 fixture test (`licence-fixtures.sh`) belongs in the same PR as a separate step. The job was not run on GitHub Actions: the pieces were run on this VM by `reproduce.sh`, with Go 1.27.3 where CI will use the 1.26.0 that `go.mod` names, and a few details (`python3 -m venv` and `pip` on the runner) are untested.

## What this means for HBEP-15

- Open question 4: syft, with the reasons above. The HBEP's statement that the SBOM is "generated in CI from the tagged commit" fits: a clean tagged checkout gives a clean version.
- Test plan, SBOM item: met by `compare.py` against `go version -m`, for the binary mode. Say "binary mode" in the HBEP, since a source SBOM lists unlinked modules.
- Test plan, licence item: confirmed that go-licenses fails on an off-list and on an unknown licence (the fixtures).
- Licence allowlist: add that "Google patent licence for Go" cannot be passed to go-licenses; it needs a note or an exception, not a list entry.
- The check cannot pass for the repository's own packages until a `LICENSE` exists (open question 1). It is the one failure today.
- Operational readiness 1 reads as if the SBOM carries licences. It does not in binary mode; the text should point at `LICENSES/` and the go-licenses report.
- The licence check needs a per-`go.mod` loop, and a guard against a run that sees no package.

## Unknowns

- Job images: no image was scanned. The table above rests on documentation, and the claim "bom does not read OCI layouts or SIF" rests on the absence of any mention in its README and help.
- Only `linux/amd64` and one binary. The controller binary is not on main yet; a binary with a `replace` directive or an upper-case module path was not tested on real data (only the unit tests cover them).
- The sigstore signatures of the releases were not verified (no cosign on the VM); the sha256 values come from the same release page as the files.
- Where go-licenses stops searching upward for a licence file, and so whether a root `LICENSE` will cover packages in the nested modules of `spikes/`.
- Whether go-licenses accepts "A or B" licence expressions in one file: it reports every licence text it finds, and each must be on the list.
- The CI job as a whole was not run on GitHub Actions, and not with Go 1.26.0, the version `go.mod` names. Nothing in the results should depend on the Go version, but only 1.27.3 was run.
- Reproducibility of the documents: each contains a creation time and a random namespace, so two runs differ byte for byte. The module lists are stable.

## Reused

| Component | Origin | Licence | Use |
|---|---|---|---|
| bom | https://github.com/kubernetes-sigs/bom | Apache-2.0 | compared, not adopted |
| syft | https://github.com/anchore/syft | Apache-2.0 | recommended |
| go-licenses | https://github.com/google/go-licenses | Apache-2.0 | recommended for the licence check |
| spdx-tools (`pyspdxtools`) | https://github.com/spdx/tools-python | Apache-2.0 | validator |
| check-jsonschema, SPDX 2.3 JSON schema | https://github.com/python-jsonschema/check-jsonschema, https://github.com/spdx/spdx-spec | see table above | second validator |
| The comparison idea (an SBOM against `go version -m`) | HBEP-15, Test plan | n/a | `compare.py` is new code |

## Files

- `reproduce.sh`: the whole run. `compare.py` and `test_compare.py`: the comparison and its tests. `licence-fixtures.sh`: the hand-written licence fixtures. `allowed-licenses.txt`: the allowlist as transcribed.
- `results/`: both SBOMs, the reference, the comparison output (binary and source), the validator output, the go-licenses report and check output, the nested-module output, the fixture output.
