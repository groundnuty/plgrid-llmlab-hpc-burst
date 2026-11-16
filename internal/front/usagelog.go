package front

import (
	"fmt"
	"os"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage/forward"
)

// newUsageForwarder makes the usage forwarder of a front started with the durable log (beta; HBEP-8,
// The durable log, Who writes): Options.EventLogTokenFile names the token file the launcher wrote,
// and the log is at Options.EventLogAddr, else the launcher's address for it. It returns nil, nil
// for a front started without the log, which keeps the usage files alone. A token file that cannot be
// read, or does not hold a token, is an error: the launcher writes it before it starts the front.
func newUsageForwarder(o Options) (*forward.Forwarder, error) {
	if o.EventLogTokenFile == "" {
		return nil, nil
	}
	b, err := os.ReadFile(o.EventLogTokenFile)
	if err != nil {
		return nil, fmt.Errorf("the durable log's token file: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if !eventlog.ValidToken(token) {
		return nil, fmt.Errorf("the durable log's token file %s does not hold a token (%d lower-case hex digits)", o.EventLogTokenFile, 2*eventlog.TokenBytes)
	}
	addr := o.EventLogAddr
	if addr == "" {
		addr = o.Host.EventLogAddr()
	}
	return forward.New(forward.Options{Addr: addr, Token: token, Dir: o.Config.UsageDir, Now: o.Clock.Now, Logger: o.Log}), nil
}
