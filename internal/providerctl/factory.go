package providerctl

import (
	"fmt"
	"io"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
)

// SSHPlatformOptions configure the production Platform: a slurm.Batch over an
// ssh transport.
type SSHPlatformOptions struct {
	// ControlDir holds the ssh masters' sockets: a short directory, mode 700,
	// for example under $XDG_RUNTIME_DIR or /tmp.
	ControlDir string
	// Binary is the ssh client; "" means ssh on PATH.
	Binary string
	// HostLock, when set, serializes the ssh calls to the host through a lock
	// file; tests against shared hosts set it.
	HostLock string
	// MaxConcurrent is the most commands at once on the connection; zero
	// means four (HBEP-4).
	MaxConcurrent int
	// Clock and the listing's poll intervals.
	Clock                      slurm.Clock
	PollInterval, BusyInterval time.Duration
}

// SSHPlatform returns a PlatformFactory that makes, per Provider, one ssh
// transport (one master per login node and credential) and a slurm.Batch over
// it.
func SSHPlatform(o SSHPlatformOptions) PlatformFactory {
	return func(v slurm.Validated) (Platform, io.Closer, error) {
		t, err := sshrun.New(sshrun.Options{
			Name: v.Settings.Provider, Config: v.SSH, ControlDir: o.ControlDir, Binary: o.Binary,
			HostLock: o.HostLock, MaxConcurrent: o.MaxConcurrent,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("the ssh transport: %w", err)
		}
		b := slurm.NewBatch(t, v.Profile, v.Settings, slurm.Options{Clock: o.Clock, PollInterval: o.PollInterval, BusyInterval: o.BusyInterval})
		return b, t, nil
	}
}
