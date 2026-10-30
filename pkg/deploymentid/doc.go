// Package deploymentid computes the deployment ID of HBEP-3, scheme v1.
//
// A deployment ID names what a ModelDeployment serves. It is the SHA-256 of an
// RFC 8785 (JCS) serialisation of a canonical object, and its display name is
// the first 12 lowercase hex characters of that hash.
//
//   - Compute builds the object of a served deployment from a ServingSpec, an
//     Accelerator class and the checkpoint's per-file hash list.
//   - ComputeManaged builds the object of a managed catalog entry.
//   - ResolveChatTemplate and ChatTemplateSHA256 give the spec's
//     chat_template_sha256.
//   - The flags and environment variables that are inputs are decided by a
//     per-engine classification table held as data (table.go). A name the table
//     does not list is an input; a key, a short option, or a setting the
//     framework passes itself is refused with an error.
//
// # What a caller must do first
//
// The library hashes what it is given. It does not fill in defaults or fetch
// anything, so a caller:
//   - applies the API schema's defaults to the spec first: a field left out
//     and the same field set to its default must reach Compute as the same
//     value (HBEP-3). The engine's own defaults are not filled in;
//   - resolves the chat template (ResolveChatTemplate) and passes its hash.
//     No template is the empty string and its hash, e3b0c442...b855, never an
//     empty chat_template_sha256. An unset optional template field is a nil
//     TemplateSources.Spec, not a pointer to "";
//   - gives the checkpoint's full per-file list, with the revision resolved to
//     a commit.
//
// # Inputs refused beyond HBEP-3's table
//
// Compute returns an error, and never a default or a repaired value, for:
//   - an engine other than "vllm" (ErrUnsupportedEngine);
//   - an empty file list, a path that is not a clean relative path with '/'
//     separators (no empty, "." or ".." segment), or a path listed twice;
//   - an environment variable listed twice, whatever its class;
//   - a checkpoint revision that is not 40 or 64 lowercase hex characters;
//   - a template hash, file hash or image digest that is not lowercase hex of
//     the right length, and any string that is not valid UTF-8;
//   - a flag name outside [a-z0-9][a-z0-9-]* once underscores are read as
//     dashes, and an environment variable name outside [A-Z_][A-Z0-9_]*. Both
//     are ASCII, so every key of the object is ASCII and JCS key order is
//     bytewise order;
//   - flag spellings that vLLM resolves to another flag (flagname.go): a
//     dotted sub-key, a prefix of a refused flag, "no-" and a listed boolean,
//     and the short options such as tp, pp and dp.
//
// # Versioning
//
// Compute and ComputeManaged are scheme v1 and stay so: a later scheme gets
// new functions beside them, so that stored v1 IDs can be recomputed. Changing
// a table row, the object or the serialisation is a new scheme. The table
// covers vLLM v0.32.0 (vllmV0320); the engine name selects it and the engine
// version is hashed, not checked against it.
//
// The package uses only the standard library. Its RFC 8785 serialiser covers
// objects, arrays and strings, the only value types the scheme has.
package deploymentid
