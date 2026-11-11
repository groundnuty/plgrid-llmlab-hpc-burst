// Command stagehelper stages a model's sandbox image and checkpoint into a
// Provider's persistent staging directory, in the place and by the code the
// allocation agent (cmd/hb-agent) stages in, so that the agent, started
// afterwards, finds both warm and does no work (issue #376). It runs as an
// Allocation's agent build, which the Provider's controller places by digest
// and the job's bootstrap runs as
//
//	stagehelper <allocation directory>
//
// It reads agent.json and the credential from that directory and the job's
// environment as the agent does (agent.LoadJob), finds Apptainer as the agent
// does (HPCBURST_APPTAINER, else PATH), and makes its run directory under the
// job's TMPDIR. Then it calls the real stager, stage.Stager, with the agent's
// configuration: Image for the image in the sandbox format, and Checkpoint for
// the checkpoint with the file list the lifecycle controller records in the
// identity record (pkg/hubfiles: Client.Files, then Inputs), without Restage
// or RehashOnly, as the agent's first start calls it.
//
// Its inputs that agent.json does not hold are compiled in, by whoever builds
// it:
//
//	go build -ldflags "-X main.image=<repository> -X main.imageDigest=<sha256:...> -X main.checkpointRepo=<owner/name> -X main.checkpointRev=<40 hex> -X main.loadFormat=<format>" ./internal/providerctl/conformance/stagehelper
//
// loadFormat is the launch's --load-format. A launch that names none sends the
// agent's request with "" (vLLM's default, auto), which the stager treats as
// "auto", so build with -X main.loadFormat=auto for it: an empty value is
// refused, to catch a build that forgot the flag.
//
// It leaves one line of fixed words and numbers in <allocation directory>/
// stage-result, written as stage-result.part and renamed:
//
//	outcome=ok image_seconds=N image_built=true|false checkpoint_seconds=N files=N mismatched=N
//	outcome=error step=image|files|checkpoint|config
//
// An error line names the step only; the error itself goes to standard error.
// The line holds no path, host, URL, token, account or job ID. A file that is
// present and does not match is reported (mismatched), never replaced, as at
// the agent's first start.
//
// It exits 0 once the line is written, 2 on a usage error, and 1 only when the
// line cannot be written.
//
// It serves a public checkpoint: there is no Hub token. Its only signal
// handling is the agent's: SIGTERM and SIGINT cancel the staging, which ends
// Apptainer's process group (package stage).
//
// Provenance: new, for the alpha rows' live steps; it reuses internal/agent,
// internal/agent/stage and pkg/hubfiles, and copies two small functions of
// cmd/hb-agent (lookTool, hubEndpoint), whose package main cannot be imported.
package main

import (
	"fmt"
	"os"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// Compiled in with -ldflags -X (see the package comment). They are variables,
// not constants, because -X sets only a package-level string variable that has
// no initializer.
var (
	// image is the engine image's repository, without a tag or digest.
	image string
	// imageDigest is the image's digest, sha256:<64 hex>.
	imageDigest string
	// checkpointRepo is the checkpoint's Hub repository, <owner>/<name>.
	checkpointRepo string
	// checkpointRev is the checkpoint's full commit id.
	checkpointRev string
	// loadFormat is the launch's --load-format; "auto" for a launch with none.
	loadFormat string
)

func main() {
	// As the agent does at its start: no process the helper starts, Apptainer
	// above all, inherits a descriptor the helper was started with.
	if err := agent.CloseInheritedFDs(); err != nil {
		fmt.Fprintln(os.Stderr, "stagehelper: cannot mark inherited file descriptors close-on-exec:", err)
	}
	os.Exit(run(os.Args[1:], os.Getenv, os.Stderr))
}

// compiled is the inputs the build compiled in.
func compiled() inputs {
	return inputs{image: image, imageDigest: imageDigest, checkpointRepo: checkpointRepo, checkpointRev: checkpointRev, loadFormat: loadFormat}
}
