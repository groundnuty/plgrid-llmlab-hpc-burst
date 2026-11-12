package launcher

import (
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/envoypin"
)

// EnvoyDigests returns the sha256 of the pinned Envoy of the agent's version for each architecture,
// keyed by cpuArch (x86_64, aarch64): the digests by which the Provider's controller stages Envoy's
// builds when Instances run behind a router, and which the launcher checks those builds against
// (#454). They come from internal/agent/envoypin, the pins the agent checks Envoy against before it
// runs it, so nothing holds a second copy. A pinned architecture with no cpuArch name is an error.
func EnvoyDigests() (map[string]string, error) {
	pins, err := envoypin.Pins()
	if err != nil {
		return nil, err
	}
	return envoyDigests(pins, envoypin.Version)
}

func envoyDigests(pins []envoypin.Pin, version string) (map[string]string, error) {
	out := map[string]string{}
	names := cpuArchOf()
	for _, p := range pins {
		if p.Version != version {
			continue
		}
		arch, ok := names[p.Arch]
		if !ok {
			return nil, fmt.Errorf("envoy-pins.txt pins Envoy %s for %s, which has no cpuArch name", p.Version, p.Arch)
		}
		out[arch] = p.SHA256
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("Envoy %s is not pinned for any architecture", version)
	}
	return out, nil
}
