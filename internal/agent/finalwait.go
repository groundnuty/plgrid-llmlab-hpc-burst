package agent

import (
	"sync/atomic"
	"time"
)

// What the agent logs when the last report meets a periodic report still in
// flight (issue #220). The tests match on these.
const (
	// logWaitingForSend is logged (Info) when the last report waits for a
	// periodic send in flight.
	logWaitingForSend = "a periodic report is in flight; the last report waits for it"
	// logOpenSendAtLast is logged (Warn) when the wait reached its bound with
	// the send still open: that send can still reach the endpoint after the
	// last report (issue #221 tracks the full fix, a sequence number).
	logOpenSendAtLast = "sending the last report with a periodic send still open"
)

// finalWaitOverride, in nanoseconds, replaces finalReportTimeout as the bound
// of the wait for a periodic send in flight, so that a test need not wait 30 s
// for it. Production never sets it; 0 means finalReportTimeout.
var finalWaitOverride atomic.Int64

// finalWaitBound is how long the last report waits for a periodic send in
// flight.
func finalWaitBound() time.Duration {
	if d := finalWaitOverride.Load(); d > 0 {
		return time.Duration(d)
	}
	return finalReportTimeout
}
