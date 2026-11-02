package lifecycle

// EventRecorder stands for client-go's recorder: it is no sink here, so a call
// of it is accepted only inside a listed wrapper.
type EventRecorder interface {
	Eventf(regarding, related Object, eventtype, reason, action, note string, args ...any)
}

// KubeRecorder is a wrapper, as the real ones are.
type KubeRecorder struct{ R EventRecorder }

// Event forwards its arguments: regarding, the reason (code) and the type (typ).
func (k KubeRecorder) Event(regarding, related Object, typ, code, message string) {
	k.R.Eventf(regarding, related, "Normal", code, typ, message)
}
