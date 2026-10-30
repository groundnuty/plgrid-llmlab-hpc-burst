# Spike S7: checkpoint hashing on parallel filesystems

Status: done. The time budget below was committed first (commit "docs: record the S7 verification time budget before measuring"), before the hashing job was submitted.

## Question

HBEP-9 makes the checkpoint an input to the deployment ID and lists "file hashes against the revision" as the lean start-up verification. Two things are unknown:

1. How long does hashing a staged checkpoint take on a Helios compute node, reading from the cluster's Lustre scratch, and how many bytes does it actually read?
2. Do per-file SHA-256 hashes computed from the staged files equal the `lfs.sha256` that the Hugging Face Hub publishes for the same revision?

The decision: rehash every checkpoint file at every Instance start, or rehash once at staging and run a cheap check (size and mtime) at start.

## Time budget (set before any measurement)

Set on 2026-10-30, before the hashing job was submitted:

- **Verification at Instance start must add at most 5 minutes of wall time for any checkpoint up to 170 GB**, measured on a 4-CPU Helios CPU allocation reading from Lustre scratch.
- Reason: Instances run inside time-limited Allocations (HBEP-9, Operational readiness 4), and the GPU is held, and billed, while verification runs. Five minutes is about 2% of a 4-hour Allocation.
- Decision rule:
  - if a full rehash of the largest staged checkpoint fits the budget, recommend the full rehash at every start (no stored state to go stale, and it catches an edit that keeps size and mtime);
  - if it does not fit, recommend rehash at staging plus a cheap check at start, and state what the cheap check misses.

## Method

1. **Find the checkpoints.** A previous project staged Hugging Face checkpoints in a Hugging Face cache on the cluster's Lustre scratch. Each snapshot directory is named by the commit it was downloaded at, and the cache's `refs/main` holds the same commit. The revision is therefore known, not guessed. Two checkpoints of different size were chosen from the five staged:
   - `RedHatAI/gemma-4-31B-it-FP8-block` at `d7242548c457ab4b45bd3adb8937f2659af4739d`: 18 files, 33.3 GB, three of them LFS (two safetensors shards of 26.9 and 6.4 GB).
   - `deepseek-ai/DeepSeek-V4-Flash-0731` at `7872f01b1d1fe23eabc4c98b48bffcef5a386062`: 74 files, 166.9 GB, 48 safetensors shards of 1.1 to 3.7 GB.
   - A third staged checkpoint, `Qwen/Qwen3-Coder-30B-A3B-Instruct` at `b2cff646eb4bb1d68355c01b18ae02e7cf42d120`, supplies four shards for the single-stream read-size sweep.
2. **Reference hashes.** `fetch_ref.py` fetches the Hub's listing for the exact revision on the agents VM (never on the cluster): size, git blob id (`oid`) and, for LFS files, `lfs.oid`, the SHA-256 of the content.
3. **Hashing job.** `hash_job.sh` runs as one Helios CPU job (4 CPUs, one node, `--no-requeue`, one-hour limit; the job ran twice, for variance). `hash_tree.py` uses nothing newer than Python 3.6, the login node's Python. On the compute nodes the host Python is 3.9, so the job ran on 3.9; 3.6 was exercised only on the login node, on a 3-byte file:
   - 4 worker processes, each reading whole files with `readinto()` into one reused 8 MiB buffer and updating SHA-256 (and the git blob SHA-1 for files up to 64 MiB, which covers every non-LFS file);
   - files are taken largest first; each file is evicted from the page cache with `posix_fadvise(DONTNEED)` after it is hashed, so the model_signing pass that follows is not served from the cache;
   - per file: wall time, process CPU time, and the difference in `/proc/self/io` `rchar` and `read_bytes` before and after (the read of `/proc/self/io` itself is subtracted from `rchar`);
   - symlinks (a cache snapshot is made of them) are followed, and results are keyed by the path inside the snapshot. The name of the blob a symlink points to is never used: in a Hugging Face cache it equals the Hub's hash, so using it would compare the Hub with itself.
4. **model_signing.** `ms_manifest.py` builds a file-mode manifest with model-signing 1.1.1 on a `uv`-managed Python 3.11 kept in the job's scratch directory (no root; the cluster's `Python/3.11.5` module is built for the compute nodes' newer glibc and does not start on the login node; it was not tried on a compute node). It passes the explicit list of files and compares the digests with the plain pass.
5. **Single-stream sweep, cheap-check probe, CPU ceiling.** One process reads one ~4 GB shard with a 1, 8 and 64 MiB buffer, and two further times with the cache warm; a stat-only pass over both checkpoints before and after hashing; a 4 KiB file changed in place four ways on Lustre; SHA-256 of zeroed memory with no I/O.
6. **Compare.** `compare.py` checks every file's SHA-256 against `lfs.sha256` and, for non-LFS files, the git blob SHA-1 against the Hub's `oid`. `summarize_run.py` produces the tables below.

Run the tests (no network, no cluster):

```
python3 -m unittest discover -s spikes/s7 -v
```

Expected values in the tests are published vectors (SHA-256 of "abc" and of the empty string, the git blob ids of the empty file and of "hello\n") and the output of coreutils `sha256sum` and `git hash-object`, which are independent implementations. A test hashes a snapshot whose blob is named with 64 zeros and checks that the result is the hash of the content, not of the name.

## Provenance of reused components

| Component | Origin | Licence | Version | Use |
|---|---|---|---|---|
| model-signing | [sigstore/model-transparency](https://github.com/sigstore/model-transparency) | Apache-2.0 | 1.1.1 | dependency of `ms_manifest.py`, run on the cluster; not copied into the repository |
| Hugging Face Hub tree API | [huggingface.co](https://huggingface.co/docs/hub/api) | service | queried on 2026-10-30 | the reference hashes |
| CPython 3.11 build (uv-managed) | [astral-sh/uv](https://github.com/astral-sh/uv) (MIT or Apache-2.0), [astral-sh/python-build-standalone](https://github.com/astral-sh/python-build-standalone) (MPL-2.0), CPython (PSF-2.0) | as listed | 3.11.16 | interpreter for model-signing on the cluster; not copied into the repository |
| coreutils `sha256sum`, git `hash-object` | GNU, git | GPL | system | independent references in the tests only |

The hashing code (`hash_tree.py`, `compare.py`, `fetch_ref.py`, `ms_manifest.py`, `probe_cheap_check.py`, `summarize_run.py`) is new.

## Environment

| Item | Value |
|---|---|
| Compute node | 2 x AMD EPYC 9654 (192 cores, SHA extensions present), EL9 kernel 5.14, host Python 3.9.25. The job was pinned to 4 cores and had the partition default of 1536 MB per CPU (6 GB); peak resident memory of the run-1 job was 2.3 GB |
| Neighbours | The node was shared. Load average at job start: 166 (run 1) and 176 (run 2) on 192 cores |
| Filesystem | the staged checkpoints sit on the cluster's Lustre scratch. Client read-ahead: at most 1024 MB, 256 MB per file, 64 MB for whole-file reads |
| Login node | EL8, host Python 3.6.8; used only for submit, poll, copy and `ls` |
| Agents VM | 6 cores, 31 GB. Load average 4.4 when the work started. It ran the Hub fetch, the unit tests and the analysis; no measurement ran on it |
| SHA-256 in memory | 1820 MB/s on one core (both runs), 8 MiB buffer, no I/O |
| Software | Python 3.9 (compute node host) for `hash_tree.py`; uv-managed Python 3.11.16 and model-signing 1.1.1 for the manifest pass |

Raw outputs of both runs (paths relative to the checkpoint, sizes, hashes, times and counters; no host names, inode numbers or process ids) are in `results/run1/` and `results/run2/`. The Hub listings used as the reference are in each run's `hub/`.

## Reproduce

```
# on the agents VM: reference hashes
python3 fetch_ref.py RedHatAI/gemma-4-31B-it-FP8-block d7242548c457ab4b45bd3adb8937f2659af4739d ref/gemma.json
python3 fetch_ref.py deepseek-ai/DeepSeek-V4-Flash-0731 7872f01b1d1fe23eabc4c98b48bffcef5a386062 ref/deepseek.json

# on the cluster login node (the account and paths are the submitter's own)
sbatch --account <account> --output <out-dir>.slurm \
  --export=ALL,HF_HUB_CACHE=<hub cache dir>,S7_DIR=<this directory>,S7_OUT=<new out dir>,S7_PY311=<python 3.11>,S7_MS_SITE=<model-signing install dir> \
  hash_job.sh

# on the agents VM, after copying <out dir> back
python3 summarize_run.py <out dir> ref [<publish dir>]
```

`hash_job.sh` refuses to start if a required variable or path is missing, and refuses an existing output directory.

## Results

### Do the staged files equal the Hub's files? Yes, all of them

| Checkpoint (public repo @ revision) | Hub files | staged files | SHA-256 equals `lfs.sha256` | git blob SHA-1 equals `oid` (non-LFS files) | mismatches | not staged | not on the Hub |
|---|---|---|---|---|---|---|---|
| `RedHatAI/gemma-4-31B-it-FP8-block` @ `d7242548c457` | 18 | 18 | 3 of 3 (33.3 GB) | 15 of 15 | 0 | 0 | 0 |
| `deepseek-ai/DeepSeek-V4-Flash-0731` @ `7872f01b1d1f` | 74 | 74 | 48 of 48 (166.9 GB) | 26 of 26 | 0 | 0 | 0 |

Both runs gave the same result. The reference is the Hub's published hash, not a hash computed by any tool of ours. The comparison can fail: changing one hex digit of one LFS hash and one git oid in the DeepSeek listing gives exactly two mismatches (`config.json` and one shard), and the unit tests cover a changed file, a size mismatch, a missing file and an extra file.

The Hub publishes `lfs.sha256` only for LFS files. The files that matter most for identity in HBEP-9, `config.json`, the tokenizer files and the chat template, are not LFS files; the Hub gives them a git blob SHA-1 (`sha1("blob <size>\0" + content)`), which `hash_tree.py` computes for every file up to 64 MiB. Without that comparison 26 of 74 DeepSeek files and 15 of 18 Gemma files would have no reference hash.

### Time: well inside the budget in both runs

Four processes, 8 MiB reads, each file evicted from the page cache after hashing. Budget: 300 s for up to 170 GB.

| Checkpoint | Run | Wall s | MB/s | CPU s | share of budget |
|---|---|---|---|---|---|
| DeepSeek, 166.9 GB, 74 files | 1 | 46.5 | 3586 | 171.8 | 16% |
| DeepSeek | 2 | 77.2 | 2162 | 262.4 | 26% |
| Gemma, 33.3 GB, 18 files | 1 | 22.5 | 1480 | 27.9 | 8% |
| Gemma | 2 | 32.6 | 1022 | 41.6 | 11% |

- The Gemma pass is bound by its 26.9 GB shard: that one file took 22.5 s (run 1) and 32.5 s (run 2) in a single process, while the other three processes sat idle (CPU busy 31 to 32%). Spreading a single file over several processes would shorten it, but that needs shard-mode hashing, whose digests are not the Hub's file hashes.
- The DeepSeek pass keeps four CPUs 85 to 92% busy, so it is CPU-bound at the 4-CPU allocation: per-process speed 0.47 to 1.0 GB/s while the same CPU hashes 1.8 GB/s from memory. Part of the gap is the kernel copying from Lustre; part is the loaded node. Run 2 was 1.5 to 1.7 times slower than run 1 with the same code, the same files and the same CPU count; the cause is not known (neighbour load was 176 against 166).
- Single process, one ~4 GB shard (the model_signing default read is 1 MiB):

| Read size | Run 1: s, MB/s | Run 2: s, MB/s |
|---|---|---|
| 1 MiB | 4.0, 1001 | 5.2, 776 |
| 8 MiB | 4.1, 988 | 5.7, 701 |
| 64 MiB | 4.6, 864 | 7.3, 548 |
| 8 MiB, not evicted before (cold) | 3.1, 1304 | 4.1, 983 |
| 8 MiB, repeated at once (page cache) | 2.6, 1552 | 3.0, 1332 |

  Each size used a different shard of about 4 GB, and the fourth shard, read the same way as the 8 MiB one, ran 30% faster in run 1, so differences under about 30% are inside the shard-to-shard spread. Reads of 1 and 8 MiB are within 10% of each other; 64 MiB was 12 to 22% slower than 8 MiB. There is no sign of a gain from larger reads. A single process would take about 170 to 240 s for 167 GB from these rates (extrapolated, not measured), so one CPU is inside the budget with little margin, and four are comfortable.
- model_signing in file mode (4 threads, 8 MiB chunks, same files, run right after the plain pass):

| Checkpoint | Run | Wall s | MB/s | Digests equal to our SHA-256 |
|---|---|---|---|---|
| DeepSeek | 1 | 40.9 | 4082 | 73 of 73 |
| DeepSeek | 2 | 55.0 | 3033 | 73 of 73 |
| Gemma | 1 | 20.0 | 1667 | 17 of 17 |
| Gemma | 2 | 23.8 | 1400 | 17 of 17 |

  Its manifest holds one SHA-256 per file, equal to the content hash, so for the LFS files it equals the Hub's `lfs.sha256`. It is as fast as the plain pass. It has 73 and 17 entries, one fewer than the files: it drops `.gitattributes`.
- Listing the files and checking size, mtime, ctime and inode takes 2 ms for 74 files (both runs, before and after hashing); the values did not change between the two passes.

### Bytes actually read

- **`rchar`** (bytes returned by `read()`), with the measurement's own reads of `/proc/self/io` subtracted: equal to the file sizes to within 1 byte in the plain passes and the sweeps, and to within 0.01% in the model_signing passes (those include the library's own reads).
- **`/proc/self/io` `read_bytes` is not a usable witness on Lustre.** It reported 0.77 GB for the 33.3 GB Gemma pass and 17.7 GB for the 166.9 GB DeepSeek pass (2% and 11% of the bytes; the same values in both runs), and 0 for a read served from the page cache. It counts only the part of the reads that went through the synchronous block path. A `read_bytes` far below the file size does not show that less was read, and a `read_bytes` equal to the file size would not have shown that nothing extra was read.
- **Lustre read RPCs** are the better witness: `osc.*.rpc_stats` is readable by users (`osc.*.stats` is root-only) and counts RPC sizes in power-of-two buckets, node-wide. Over each pass the RPC byte count had an upper bound of 1.0002 times the checkpoint bytes in run 1 and 1.0018 to 1.0040 times in run 2, and a lower bound of 0.50 times (the bucket width). So there was no over-read of the kind in L-12.5, which read up to 190 times the file size: any such over-read would show as a multiple. The counters are node-wide and include other jobs' reads, so they bound our reads from above. The 4 GB sweep reads, including the 64 MiB ones, had an upper bound within 0.03% of the file size in run 1.

### What a cheap check at start can see

One 4 KiB file changed in place on Lustre, four ways (`probe_cheap_check.py`):

| Change | size | mtime | ctime | inode | content |
|---|---|---|---|---|---|
| flip a byte at once | same | run 1: +1 s; run 2: **unchanged** | run 1: changed; run 2: **unchanged** | same | changed |
| flip a byte 2.5 s later | same | changed | changed | same | changed |
| flip a byte, then set mtime back (what `cp -p` and `rsync -t` do) | same | **unchanged** | changed | same | changed |
| write a new file, copy the old mtime onto it, rename over the old one | same | **unchanged** | changed | **changed** | changed |

- A same-size edit leaves the size alone, so size alone sees none of these.
- mtime and ctime have one-second resolution on this Lustre (the observed steps were whole seconds: 1.000, 2.000 and 3.000 s). In run 2 an edit made in the same second as the previous write left both unchanged. A stat taken less than a second after a file's last write is therefore not a safe record (git calls this a racy entry). A record taken after hashing finished is at least that old for every file that matters.
- mtime misses an edit followed by a restored mtime; ctime does not, because a user cannot set it. ctime also changes on a harmless chmod or rename, so it can give false alarms, and a copy to another path has other ctimes than the original.

### Findings about the tools (they matter for HBEP-9)

- **Hub listing truncation.** The Hub's tree endpoint returns 50 entries unless `limit` is given. The first fetch of the DeepSeek listing showed 47 of 74 files, and `lfs` for 24 of 48 shards, without any error. `fetch_ref.py` requests `limit=1000`, follows the `Link` header and refuses a branch name as revision. Any verification built on the listing must do the same, otherwise half the shards are "not on the Hub".
- **model_signing 1.1.1 refuses symlinks.** A Hugging Face cache snapshot is made of symlinks. `use_file_serialization(allow_symlinks=True)` does not help: `Config.hash()` overwrites that setting with the Config's own flag. Only `Config.set_allow_symlinks(True)` works; without it every call raises `ValueError`.
- **model_signing ignores `.git*` paths by default** (`.git/`, `.gitattributes`, `.github/`, `.gitignore`). A file the Hub lists as part of the revision is not in the manifest. Harmless for identity, but "manifest equals Hub file list" is false by one file.
- **model_signing imports pyOpenSSL without declaring it.** `import model_signing` fails with `No module named 'OpenSSL'` in an environment built from the package's own requirements; the dependency tree for hashing alone is 36 packages including model-signing (about 50 MB of wheels, cryptography, protobuf and pydantic among them) and needs Python 3.9 or newer. `hash_tree.py` uses only the standard library and runs at the same speed.
- **model_signing default thread count** is the executor default, which is derived from the node's 192 cores, not the 4 allocated. We passed 4. `chunk_size=0` makes it `read()` each whole file in one call, the cap-sized read of L-12.5; the default is 1 MiB.
- **A cache blob's name is the Hub's hash.** In the staged Hugging Face cache, the blob behind each LFS snapshot entry is named with the file's SHA-256 (checked: 3 of 3 for Gemma against `lfs.sha256`), and a non-LFS blob with its git oid. Reading the name instead of the content would pass any verification. `hash_tree.py` hashes the bytes; a unit test uses a blob named with 64 zeros.
- **Revision.** Each cache snapshot directory is named by the commit it was downloaded at, and `refs/main` holds the same commit. The match of every file against that revision's hashes confirms it independently.

## Recommendation

**Rehash every file of the checkpoint at every Instance start. Do not add a rehash-at-staging-plus-cheap-check design on account of cost.**

Against the budget set before measuring (at most 5 minutes up to 170 GB on 4 CPUs): the largest staged checkpoint, 166.9 GB, took 46.5 s and 77.2 s in two runs, 16% and 26% of the budget. The 33.3 GB checkpoint took 22.5 s and 32.6 s. The decision rule committed with the budget gives the full rehash. The extra mechanism of a staging-time record plus a size-and-mtime check would also be weaker: it does not see an in-place edit that keeps the size and restores the mtime, and it does not see one made in the same second as the recorded stat.

Details of the recommendation:

1. Hash with at least 2, preferably 4, parallel readers over the files, 8 MiB reads from one reused buffer, never a read sized by a cap. Wall time is then set by the largest single file: about 1 GB/s per reader, so a 27 GB shard needs 22 to 33 s.
2. The reference for the identity record is one SHA-256 per file for every file the engine loads, over an explicit list of files. At staging, compare each file with the Hub's `lfs.sha256`, and for non-LFS files with the git oid; record our own SHA-256 for every file, because the Hub gives no SHA-256 for non-LFS files.
3. model_signing's file mode produces the same per-file SHA-256 and is as fast, so it is acceptable if signing is wanted later. It is not needed for hashing. If HBEP-9 names it, it must name the three settings above (symlinks, ignored git paths, thread count) and the undeclared pyOpenSSL dependency.
4. Keep the cheap check only as a fallback if a larger checkpoint or a slower filesystem breaks the budget. Then record size, mtime, ctime and inode at least two seconds after the last write, rehash when any differs, and say plainly that an edit restoring size and mtime and made in the same second as the record is not seen. For scale, at the slower run's 2.16 GB/s the 5-minute budget is reached at about 650 GB (extrapolated). The unmeasured 328 GB checkpoint staged on the cluster would take about 150 s at that rate.

## What this means for HBEP-9

- Open question "which checks verification runs at start": "file hashes against the revision" is affordable at every start on Helios CPU nodes: about 0.4 to 0.5 minutes for 33 GB and 0.8 to 1.3 minutes for 167 GB. Operational readiness item 4 ("TBD in minutes per checkpoint size") can say so, with the caveats below.
- The identity record should hold a SHA-256 per file for all loaded files, not only the weights, with the Hub's `lfs.sha256` or `oid` used at staging to confirm the revision.
- The Hub reference must be fetched with pagination handled and a full commit id as revision.
- Bytes read must be measured by `rchar` and the Lustre RPC counters, not `read_bytes`.
- The test plan's "one tokenizer file changed" case is detected by the full rehash at any size we measured.

## Deviations from the issue

- No download: two of the five checkpoints already staged were used. Nothing was moved or deleted; every file was opened read-only.
- **Sano was skipped.** No checkpoint is staged there: a search to depth 6 under the home directory found no `*.safetensors` and no model index, and `/mnt`, `/scratch*` and `/net` held no Hugging Face cache. Sano's shared filesystem is CephFS, where `fincore` reads zero (L-12.5), so a Sano measurement also needs a different cache check.
- The job ran twice. The second run was added to show that the timing varies between runs.
- `hash_tree.py` ran on the compute nodes' Python 3.9, not on 3.6 (the login node's). It avoids 3.7+ features and compiles and runs on 3.6 on the login node.

## What is still unknown

- Timing on the Helios GH200 nodes (aarch64 CPUs, a different SHA-256 implementation, probably a different Lustre client path) and on Athena. The numbers above are for x86 CPU nodes. Athena was not used (GPU-only).
- Whether a cold start of a checkpoint nobody has read for days is slower. The files were staged on 22 and 23 October and when they were last read is unknown; the client cache was evicted after each file, but the state of the Lustre servers' caches is unknown. Two runs on loaded nodes differ by a factor of 1.7.
- Several Instances hashing the same or different checkpoints at once on one filesystem. Not tested.
- Whether hashing overlaps with weight loading to cost less than the sum. Not tested.
- Checkpoints larger than 167 GB, and a single file larger than 27 GB.
- Whether the page-cache eviction worked as intended: the model_signing pass read the same `read_bytes` as the plain pass, which is consistent with it, not proof.
- The same-second hole of the cheap check was seen in one of two probe runs; its frequency was not measured.
