// Package slurm is the Slurm provider plugin's platform side (HBEP-4): the
// Slurm dialect over the command line, provider.Batch and the platform-profile
// reader, and the ssh transport with one multiplexed connection per login node
// and credential.
//
// Layers:
//   - Runner runs one command line on the login node. SSH is the production
//     Runner; package fakeslurm has an in-process fake Slurm that implements
//     it for tests.
//   - The dialect (dialect.go, parse.go) builds HBEP-4's fixed command
//     templates and reads squeue and sacct JSON of data_parser v0.0.40 and
//     v0.0.42, checking every reply's version against the profile.
//   - Batch implements provider.Batch over a Runner for one Provider, with
//     one cached queue listing per Provider (HBEP-4, Transport, Poll rate) and
//     the login-node paths resolved once per connection.
//   - ProfileReader implements provider.ProfileReader.
//
// Origin: the dialect, the outcome classification and the record merging are
// ported from spike S4 (spikes/s4/internal/slurm), rewritten for the contracts
// in pkg/provider; the JSON field tables follow S7's real captures
// (spikes/s7/slurm-json-real.md). New code otherwise.
package slurm
