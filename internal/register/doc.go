// Package register loads and checks the known-issues register, register/known-issues.yaml
// (HBEP-4, Known-issues register; beta). The register holds the upstream bugs the framework works
// around, each tied to versions, with a workaround that is plain configuration and one regression
// test. No controller reads it: a CI test does (TestKnownIssuesRegister), through Load and Check.
//
// Provenance: new code. The YAML is read by sigs.k8s.io/yaml (Apache-2.0,
// https://github.com/kubernetes-sigs/yaml), strictly, as the repository's other manifests are.
package register
