package agentapi

// KeepsReport says whether a Backend keeps a report counted seq from the agent
// instance it holds the count held for: the Seq of the last report it kept from
// that instance, 0 while it has kept none (HBEP-7, Report order). It keeps a
// numbered report only if it is newer than the held count. An older one, or a
// resend of the held one, is answered as accepted without being kept, so the
// agent never retries it. A report counted 0 comes from an agent built before
// the count existed, which counts from 1 when it has one: it is kept, in arrival
// order, as before. A Backend raises the held count only to the Seq of a report
// it kept, so a report it refused is kept when the agent sends it again.
func KeepsReport(seq, held uint64) bool { return seq == 0 || seq > held }
