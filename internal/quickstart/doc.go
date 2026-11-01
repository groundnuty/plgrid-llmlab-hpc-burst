// Package quickstart holds the tests that keep docs/quickstart.md true (issue #162): the file
// samples it shows parse with the code that reads them, the shell snippets that create credentials
// produce files that code accepts, the paths and flags it names exist, and every manifest in
// examples/ is mentioned. The manifests themselves are admitted against the real CRDs by
// api/v1alpha1/e2e (examples_test.go). The package has no non-test code.
package quickstart
