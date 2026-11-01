package launcher

// DefaultEventTTL is how long the launcher has the API server keep an Event: seven days, where the
// API server's own default is one hour (HBEP-8, Retention; HBEP-11, Single-user host). up passes
// it as --kube-apiserver-arg=event-ttl=168h, and records it in the host's record (State.EventTTL),
// from which status reports it.
const DefaultEventTTL = "168h"

// apiserverEventTTL is the API server's own default, which an Event gets when k3s runs without the
// argument: kube-apiserver's --event-ttl (HBEP-8, Retention). A record without State.EventTTL, from
// a host that an earlier release started, stands for it.
const apiserverEventTTL = "1h"

// eventTTLKey is the name of the API server flag, as k3s's --kube-apiserver-arg takes it.
const eventTTLKey = "event-ttl"

// eventTTLValue is the event-ttl this start passes to k3s, and so the value the record keeps: the
// same call feeds k3sArgs and State.EventTTL, so what is recorded cannot differ from what was
// passed. Empty means the argument is left out (only tests do that).
func (s launchSpec) eventTTLValue() string {
	switch {
	case s.omitEventTTL:
		return ""
	case s.eventTTL != "":
		return s.eventTTL
	}
	return DefaultEventTTL
}
