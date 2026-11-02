package providerctl

import (
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// limitEventEvery is the most often the SubmissionRejected event of a full
// submit limit is emitted for one Allocation while the refusal lasts. A full slot can last hours and
// each attempt, one every Rejected backoff, is refused again, so the event is a
// repeating condition: emitted when it starts, then at most once an hour,
// never on every attempt (HBEP-8, Repeating conditions).
const limitEventEvery = time.Hour

// limitEventDue reports whether to emit the SubmissionRejected event of a full
// submit limit for the Allocation now, and records the emission if so. The first refusal of a run
// emits; later ones emit once limitEventEvery has passed since the last. A
// refusal for any other reason, a refusal before any call, an Unknown outcome
// or an accepted job ends the run (the callers delete the entry), so the next
// full slot emits again. An operator's abandon of an Unknown attempt needs no
// delete of its own: the Unknown outcome already ended the run.
func (s *shared) limitEventDue(uid types.UID, now time.Time) bool {
	if last, ok := s.limitEvents.Load(uid); ok {
		if t, ok := last.(time.Time); ok && now.Sub(t) < limitEventEvery {
			return false
		}
	}
	s.limitEvents.Store(uid, now)
	return true
}
