package lifecycle

// What the lifecycle controller's logged notes may copy (HBEP-8, Allocation
// events, No job ID, path or name in a note; the science agent's ruling on #640,
// 16 Nov 2026). Every typed event goes to the durable log in beta, which is kept
// for the grant and may be exported, so a note names no path, user name, account
// or grant, and no job ID. A note that would copy text from outside the
// controller (an agent's check message, an engine's or a filesystem's error, a
// Provider's condition or status text) gives the reason code and a FIXED
// description instead; the detail stays in the object's status, where the
// condition's own message holds it.

import "regexp"

// stagingFailedNote is the note of a restart for a failed staging: the agent's own
// message names the path it could not write, and stays in the agent's report.
const stagingFailedNote = "staging failed"

// verificationNote is the fixed description of a failed verification's cause, for
// the logged note: the record's own message can carry an engine's error or a
// checkpoint file's name, and stays in the record and the Instance's conditions.
func verificationNote(cause string) string {
	switch cause {
	case causeRehash:
		return "the staged checkpoint's files differ from the identity record"
	case causeError:
		return "the agent could not verify"
	case causeContradiction:
		return "the engine reported settings that contradict the declared ones"
	case causeShape:
		return "the workload's shapes do not fit the context limit the engine reported"
	case causeMissingRequired:
		return "the engine did not report a setting that verification requires"
	}
	return "verification failed"
}

// reasonCode matches one bare word: a scheduler's state reason, such as
// NonZeroExitCode, or one of the framework's own reason codes.
var reasonCode = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

// reasonCodeOf returns s when it is a bare reason code, and "" for anything
// longer or with spaces, slashes or punctuation in it: free text that may be
// another component's error.
func reasonCodeOf(s string) string {
	if reasonCode.MatchString(s) {
		return s
	}
	return ""
}
