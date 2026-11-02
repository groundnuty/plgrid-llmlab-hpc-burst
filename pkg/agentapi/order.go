package agentapi

import "fmt"

// ReportIsNewer says whether a report counted seq is newer than the newest a
// Backend holds for the same agent instance ID: held, which is 0 while it holds
// none (HBEP-7, Report order). A Backend keeps a report only if it is, and
// answers an older one, or a repeat of the same number, as accepted without
// keeping it: the agent never retries it. The held count is raised only when a
// report is kept, so a report that was refused and is tried again, with its
// number, is not taken for an old one.
func ReportIsNewer(seq, held uint64) bool { return seq > held }

// CheckSeq refuses a report that carries no count. Counts start at 1; a report
// without one would never be newer than the 0 a Backend starts from, and would
// be dropped without a word.
func CheckSeq(seq uint64) error {
	if seq == 0 {
		return fmt.Errorf("the report carries no seq: an agent counts its reports from 1")
	}
	return nil
}
