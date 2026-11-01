package frpsplugin

// inFlight is the number of calls the handler is handling now: the slots taken. It is for tests
// only, so that they wait on this state and not on timing.
func (h *Handler) inFlight() int { return len(h.slots) }
