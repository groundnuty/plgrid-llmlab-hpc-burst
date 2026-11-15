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
		"hack/fetch-envoy.sh":                 "#!/bin/bash\n" + alphaEnvoyStub + "\n",
		// The plgrid-now rows of issue #376: row A's inputs, and the pre-staging row's helper package and build script.
		"profiles/athena-now.yaml":                             "name: athena-now\n",
		"examples/modeldeployment-qwen25-instruct.yaml":        "kind: ModelDeployment\n",
		"internal/providerctl/conformance/stagehelper/main.go": "package main\n\nfunc main() {}\n",
		"hack/build-stage-helper.sh":                           "#!/bin/bash\n" + stageBuildStub + "\n",
		// The bulk row's test shim (issue #521, option (A)), which its build line names beside the stack's binaries.
		"internal/agent/enginedouble/shim/main.go": "package main\n\nfunc main() {}\n",
	}
}

// The pre-staging row's build script (prepare_stage): it records that it ran, with its arguments, and makes the helper.
const stageBuildStub = `echo "build-stage-helper $*" >> "$STUB_STATE/events"
echo helper > "$1"`

// The two scripts prepare_alpha runs: they record that they ran, with their arguments, and make what the run later looks for.
const alphaBuildStub = `echo "build-agents $*" >> "$STUB_STATE/events"
mkdir -p "$1" && echo agent > "$1/hb-agent-x86_64" && echo agent > "$1/hb-agent-aarch64"`

// The router row's one more (issue #521): hack/fetch-envoy.sh --agent-builds DIR puts envoy-<cpuArch> beside the agent builds.
const alphaEnvoyStub = `echo "fetch-envoy $*" >> "$STUB_STATE/events"
mkdir -p "$2" && echo envoy > "$2/envoy-x86_64" && echo envoy > "$2/envoy-aarch64"`

const alphaFetchStub = `echo "fetch-frp $*" >> "$STUB_STATE/events"
mkdir -p "$2" && echo frpc > "$2/frpc-x86_64" && echo frpc > "$2/frpc-aarch64"`
