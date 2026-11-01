// Package quickstart holds the tests that keep docs/quickstart.md true (issue #162): the file
// samples it shows parse with the code that reads them, the shell snippets that create credentials
// produce files that code accepts, the paths it names exist, every manifest in examples/ is
// mentioned, and its commands are accepted by the real programs: the hpcburst binary (built from
// cmd/hpcburst and asked with -h) and the hack/ scripts (run under an environment that stops them
// after their arguments). The directories it sets, and passes to every command that takes one,
// are compared with what the launcher resolves. The manifests themselves are admitted against the
// real CRDs by api/v1alpha1/e2e (examples_test.go). The package has no non-test code.
package quickstart
