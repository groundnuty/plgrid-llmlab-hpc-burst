# Golden vectors for the deployment ID, scheme v1

Each directory under `vectors/` is one vector. The expected values were computed outside the Go package, so they are not circular (HBEP-3, Test plan): the package never produced them.

| File | What it is | Who made it |
|---|---|---|
| `input.json` | the structured input handed to `Compute` or `ComputeManaged` | written by hand |
| `expected-object.json` | the `v1` object, from HBEP-3's rules, keys in any order | written by hand |
| `canonical.json` | the RFC 8785 bytes of the object, no trailing newline | `gen_vectors.py` |
| `sha256.txt` | their SHA-256, in `sha256sum -c` format | `gen_vectors.py` |
| `tokenizer_config.json` (vector 06) | a checkpoint file the template is taken from | written by hand |

Python does only the canonicalisation and the hash. It never sees the flag table or `input.json`, so a classification or mapping bug in the package shows up as a mismatch with the hand-written object.

## Tools and versions

- `gen_vectors.py`: Python 3.12.14 with `rfc8785` 0.1.4 (PyPI; Trail of Bits, Apache-2.0; https://github.com/trailofbits/rfc8785.py), and `hashlib`.
- `crosscheck.mjs`: Node v26.10.0 with `canonicalize` 5.1.0 (npm; Samuel Erdtman, Apache-2.0; https://github.com/erdtman/canonicalize). A second JCS implementation, written independently of the first. Its output matches `canonical.json` byte for byte for all seven vectors.
- `sha256sum` (coreutils): `cd vectors/<name> && sha256sum -c sha256.txt` is `OK` for all seven.
- Both libraries were run once, before the vectors, on two samples from the RFC: the seven-key sorting sample of section 3.2.3, where both give the RFC's property order, and the string member of the section 3.2.2 sample, where both give the bytes of the section 3.2.4 hex dump that belong to it (`e2 82 ac 24 5c 75 30 30 30 66 ...`). Those throwaway snippets are not kept; `jcs_test.go` holds the same two samples for the Go serialiser.

Neither tool is a dependency of the Go module. To regenerate:

```
python3 -m venv /tmp/v && /tmp/v/bin/pip install rfc8785==0.1.4
/tmp/v/bin/python3 gen_vectors.py          # rewrites canonical.json and sha256.txt
# in a scratch directory outside the repository:
cp crosscheck.mjs /tmp/cc/ && cd /tmp/cc && npm install canonicalize@5.1.0
node crosscheck.mjs <path to this directory>/vectors
```

`gen_vectors.py` also checks that vector 06's `chat_template_sha256` is the SHA-256 of the `default` template in its `tokenizer_config.json`, taken with Python's own JSON and hashing.

## The vectors

1. `01-hbep-example`: HBEP-3's worked example. `canonical.json` has the 694 bytes HBEP-3 prints and hashes to `0e1dd69efe8a57c3eadd5e0293fd8c0f0cc185629e79297b6530de361b887852`. The Go tests also extract the bytes from HBEP-3's text and compare.
2. `02-managed-with-revision`: a catalog entry with a revision.
3. `03-managed-no-revision-unicode`: a catalog entry without a revision (the key is omitted), with a non-ASCII name that includes a supplementary-plane character.
4. `04-flags-env-repeat-json`:
   - non-input flags among the inputs (`port`, `max-num-seqs`, `gpu-memory-utilization`, `host`, `enable-prefix-caching`, `served-model-name`) and non-input variables (`HF_HOME`, `CUDA_VISIBLE_DEVICES`, `VLLM_LOGGING_LEVEL`, `VLLM_CACHE_ROOT`), none of which reach the object;
   - a repeated unknown flag, interleaved with others and repeating a value, so it maps to an array in the order given;
   - a boolean `"false"`;
   - a JSON-valued flag (`hf-overrides`), kept as the string given, which needs quote escapes;
   - an unknown flag and an unknown variable, which are inputs; empty values; a non-empty `env`;
   - no chat template, so the hash of the empty string.
5. `05-unicode-order`: the case where JCS differs from a bytewise sort.
   - Flag names U+20AC, U+00F6, U+FB33 and U+1F600 (the RFC 8785 section 3.2.3 set), and variable names with U+1F600 and U+FF5E. JCS compares UTF-16 code units, so the supplementary character U+1F600 (surrogate pair D83D DE00) sorts before U+FB33 and U+FF5E; sorted bytewise or by code point it would come after them.
   - File paths with U+FB33 and U+1F600. The `files` array is sorted bytewise by path, as HBEP-3 says, so here the order is the reverse of the one for keys. The Go test `TestVector05SeparatesTheTwoOrders` checks that this vector tells the two orders apart.
   - Values with every escape class: quote, backslash, `/`, `<>&`, U+2028, DEL, U+0001, `\b`, `\t`, `\n`, `\f`, `\r`. Paths with a space, quote, backslash, newline, tab and U+2028.
6. `06-template-from-tokenizer-config`: the chat template comes from the `default` entry of a list-form `chat_template` in `tokenizer_config.json`, and contains a newline and a non-ASCII character. The hash is the SHA-256 of that text.
7. `07-bytewise-paths-no-gpu`: ten ASCII and Latin-1 paths given in shuffled order, where bytewise order is not the order a human or a locale sort would give (`10.bin` before `2.bin`, `B/c` and `Z.json` before `_x` and `a-b`, `a-b` before `a.b` before `a/b`, a non-ASCII name last); an accelerator `none`; only non-input flags and variables, so `flags` and `env` are `{}`; no chat template.

HBEP-3's Test plan lists a catalog entry, a repeated flag, a boolean `"false"`, a JSON-valued flag, a non-empty `env` and a template from `tokenizer_config.json`; vectors 2, 3, 4 and 6 cover them. The `crosscheck.mjs` and `gen_vectors.py` scripts exist so the files can be re-derived by anyone without the Go code.
