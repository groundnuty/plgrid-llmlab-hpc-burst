package livechecks

// The fixture's stand-ins for the alpha rows (issue #194): the package their
// tests live in, the three binaries and the agent that make up the stack, and the
// shared package the stack imports, so that --plan can show a change in a stack
// package covering the rows. The accounting rows' package, the conformance
// package, is already in the fixture.

func alphaFixtureFiles() map[string]string {
	return map[string]string{
		"internal/alpha/alpha.go":             "package alpha\n",
		"cmd/hpcburst/main.go":                "package main\n\nimport _ \"example.test/fx/internal/stack\"\n\nfunc main() {}\n",
		"cmd/hpcburst-controller/main.go":     "package main\n\nimport _ \"example.test/fx/internal/stack\"\n\nfunc main() {}\n",
		"cmd/hpcburst-front/main.go":          "package main\n\nimport _ \"example.test/fx/internal/stack\"\n\nfunc main() {}\n",
		"internal/stack/stack.go":             "package stack\n",
		"examples/provider-athena-a100.yaml":  "kind: Provider\n",
		"examples/provider-helios-gh200.yaml": "kind: Provider\n",
		"hack/build-agents.sh":                "#!/bin/bash\n" + alphaBuildStub + "\n",
		"hack/fetch-frp.sh":                   "#!/bin/bash\n" + alphaFetchStub + "\n",
	}
}

// The two scripts prepare_alpha runs: they record that they ran, with their arguments, and make what the run later looks for.
const alphaBuildStub = `echo "build-agents $*" >> "$STUB_STATE/events"
mkdir -p "$1" && echo agent > "$1/hb-agent-x86_64" && echo agent > "$1/hb-agent-aarch64"`

const alphaFetchStub = `echo "fetch-frp $*" >> "$STUB_STATE/events"
mkdir -p "$2" && echo frpc > "$2/frpc-x86_64" && echo frpc > "$2/frpc-aarch64"`
