// Package deploymentid computes the deployment ID of HBEP-3, scheme v1.
//
// A deployment ID names what a ModelDeployment serves. It is the SHA-256 of an
// RFC 8785 (JCS) serialisation of a canonical object, and its display name is
// the first 12 lowercase hex characters of that hash.
//
//   - Compute builds the object of a served deployment from a ServingSpec, an
//     Accelerator class and the checkpoint's per-file hash list.
//   - ComputeManaged builds the object of a managed catalog entry.
//   - The flags and environment variables that are inputs are decided by a
//     per-engine classification table held as data (table.go). A name the table
//     does not list is an input; a key, a short alias, or a setting the
//     framework passes itself is refused with an error.
//
// The package uses only the standard library. Its RFC 8785 serialiser covers
// objects, arrays and strings, the only value types the scheme has.
package deploymentid
