package launcher

// When the durable log is ready for the others (HBEP-8, The durable log: the launcher starts it
// "before the controllers and the front", so that they can connect; issue #669).
//
// nats-server starts its monitoring port and restores JetStream's streams from the store before it
// starts accepting clients (Server.Start: StartMonitoring, EnableJetStream, then AcceptLoop). After a
// restart on an existing store the streams are therefore listed on the monitoring port while the
// client port is still closed, so "both streams are listed" is not "a client can connect". The
// barrier asks for what the others need: a client with the log's token connects.

import (
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// eventLogConnectTimeout bounds one connection attempt of the readiness check. The check is asked
// again every poll, and EventLogStartWait bounds the whole wait.
const eventLogConnectTimeout = time.Second

// eventLogAcceptsClient is the start barrier's readiness for the log: both streams are listed on the
// monitoring port, and a client with the token from the token file connects to the client address.
// It reads the token file at each ask, as a client does, and always closes the connection it opens.
// A log that is not up, one whose client port is not yet open, and a token the log refuses are all
// not ready.
func eventLogAcceptsClient(h agentapi.Host) bool {
	return eventLogStreamsExist(h.EventLogMonitorAddr()) && eventLogTokenConnects(h)
}

// eventLogTokenConnects makes one token-authenticated connection to the log's client address and
// closes it. It is true only when the log accepted the client.
func eventLogTokenConnects(h agentapi.Host) bool {
	b, err := os.ReadFile(h.EventLogTokenFile())
	if err != nil {
		return false
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return false
	}
	nc, err := nats.Connect("nats://"+h.EventLogAddr(), nats.Token(token), nats.NoReconnect(),
		nats.Timeout(eventLogConnectTimeout), nats.Name("hpcburst-supervisor"))
	if err != nil {
		return false
	}
	nc.Close()
	return true
}
