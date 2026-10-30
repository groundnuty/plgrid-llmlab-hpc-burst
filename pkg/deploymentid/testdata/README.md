# Golden vectors for the deployment ID, scheme v1

Each directory under `vectors/` is one vector. The expected values come from outside the Go package, so they are not circular (HBEP-3, Test plan): the package never produced them.

| File | What it is | Who made it |
|---|---|---|
| `input.json` | the structured input handed to `Compute` or `ComputeManaged` | written by hand |
| `expected-object.json` | the `v1` object, from HBEP-3's rules, keys in any order | written by hand |
| `canonical.json` | the RFC 8785 bytes of the object, no trailing newline. The fixture. | see below |
| `sha256.txt` | their SHA-256, in `sha256sum -c` format | `sha256sum canonical.json` |
| `tokenizer_config.json` (vector 06) | a checkpoint file the template is taken from | written by hand |

`canonical.json` is the authoritative fixture and nothing in this repository writes it. HBEP-3 (Test plan) asks for objects written by hand, canonical bytes from an independent RFC 8785 implementation checked in as the authoritative file, the same bytes from a second independent implementation, and `sha256sum` for the hash:
- vectors 02, 03, 04, 06 and 07 were first emitted by the `rfc8785` package from their hand-written `expected-object.json` (commit 6e73334) and are frozen as committed;
- vector 01 is the byte string HBEP-3 prints, written by hand, and a Go test extracts it from HBEP-3's text and compares;
- vector 05 was rewritten for ASCII keys by editing its `env` and `flags` members of `canonical.json` by hand; `rfc8785` and `canonicalize` confirm the bytes;
- vector 04's repeated flag was changed from `lora-modules`, which HBEP-3 now refuses, to `middleware`, the same way.

The two scripts below are verifiers. They read the committed files and exit non-zero on a mismatch; neither writes anything. A change to a vector is made by editing `canonical.json` and `sha256.txt` and running them.

## Verifiers and versions

- `verify_vectors.py`: Python 3.15.0 with `rfc8785` 0.1.4 (PyPI; Trail of Bits, Apache-2.0; https://github.com/trailofbits/rfc8785.py), and `hashlib`. For each vector it canonicalises `expected-object.json` and requires the committed `canonical.json` bytes and `sha256.txt` line, an ASCII key everywhere, and, for vector 06, that `chat_template_sha256` is the SHA-256 of the `default` template in its `tokenizer_config.json`, taken with Python's own JSON and hashing.
- `crosscheck.mjs`: Node v26.10.0 with `canonicalize` 5.1.0 (npm; Samuel Erdtman, Apache-2.0; https://github.com/erdtman/canonicalize). A second JCS implementation, written independently of the first. It requires the same bytes for all seven vectors.
- `sha256sum` (coreutils): `cd vectors/<name> && sha256sum -c sha256.txt` is `OK` for all seven.
- Before the vectors were first written, both libraries were run once on two samples from the RFC: the seven-key sorting sample of section 3.2.3, where both give the RFC's property order, and the string member of the section 3.2.2 sample, where both give the bytes of the section 3.2.4 hex dump that belong to it (`e2 82 ac 24 5c 75 30 30 30 66 ...`). Those throwaway snippets are not kept; `jcs_test.go` holds the same two samples for the Go serialiser.

Neither tool is a dependency of the Go module. To verify:

```
python3 -m venv /tmp/v && /tmp/v/bin/pip install rfc8785==0.1.4
/tmp/v/bin/python3 -I verify_vectors.py            # exit 0, "match" for each vector
# in a scratch directory outside the repository:
cp crosscheck.mjs /tmp/cc/ && cd /tmp/cc && npm install canonicalize@5.1.0
node crosscheck.mjs <path to this directory>/vectors
for d in vectors/*/; do (cd $d && sha256sum -c sha256.txt); done
```

Python sees neither the flag table nor `input.json`, so a classification or mapping bug in the package shows up as a mismatch with the hand-written object.

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
5. `05-unicode-order`: non-ASCII text in the values and paths, and ASCII keys. HBEP-3 makes every key ASCII, so a key can no longer tell UTF-16 order from bytewise order; `jcs_test.go` does that for the serialiser, with the RFC 8785 section 3.2.3 sample.
   - Flag keys `1st-flag`, `a-b`, `a0`, `ab`, `dtype`, `x` and variable keys `A1`, `AB`, `ASCII_ENV`, `_LEAD`: ASCII order that a case-folding or punctuation-skipping sort would not give (`a-b` before `a0` before `ab`; `_LEAD` after `ASCII_ENV`, since `_` is 0x5F).
   - This is HBEP-3's vector for a value with non-ASCII characters: they are literal UTF-8 in `canonical.json`, never a `\u` escape, and `TestVector05Orders` checks that.
   - Values with every escape class: quote, backslash, `/`, `<>&`, U+2028, DEL, U+0001, `\b`, `\t`, `\n`, `\f`, `\r`, and non-ASCII text, a supplementary-plane character among it.
   - File paths with U+FB33 and U+1F600, a space, quote, backslash, newline, tab and U+2028. The `files` array is sorted bytewise by path, as HBEP-3 says, so U+FB33 comes before U+1F600; sorted by UTF-16 code units it would be the reverse. `TestVector05Orders` checks that the vector tells the two apart.
6. `06-template-from-tokenizer-config`: the chat template comes from the `default` entry of a list-form `chat_template` in `tokenizer_config.json`, and contains a newline and a non-ASCII character. The hash is the SHA-256 of that text.
7. `07-bytewise-paths-no-gpu`: ten ASCII and Latin-1 paths given in shuffled order, where bytewise order is not the order a human or a locale sort would give (`10.bin` before `2.bin`, `B/c` and `Z.json` before `_x` and `a-b`, `a-b` before `a.b` before `a/b`, a non-ASCII name last); an accelerator `none`; only non-input flags and variables, so `flags` and `env` are `{}`; no chat template.

HBEP-3's Test plan lists a catalog entry, a repeated flag, a boolean `"false"`, a JSON-valued flag, a non-empty `env` and a template from `tokenizer_config.json`; vectors 2, 3, 4 and 6 cover them. The `crosscheck.mjs` and `verify_vectors.py` scripts exist so anyone can check the files without the Go code.
