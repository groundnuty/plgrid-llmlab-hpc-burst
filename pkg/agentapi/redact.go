package agentapi

// RedactedValue replaces a secret in a desired state or report that is logged.
const RedactedValue = "[redacted]"

// Redacted returns a copy of d that is safe to log: DesiredInstance.HubToken,
// the one secret a desired state carries, is RedactedValue when it was not
// empty. The Version is left as it is, so the copy still names the state it
// came from. Redacted changes nothing d points to; the copy shares d's other
// slices, which it does not write.
func (d DesiredState) Redacted() DesiredState {
	if d.Instance != nil {
		inst := *d.Instance
		if inst.HubToken != "" {
			inst.HubToken = RedactedValue
		}
		d.Instance = &inst
	}
	return d
}

// Redacted returns a copy of r that is safe to log: InstanceReport.Key, the one
// secret a report carries, is RedactedValue when it was not empty. Redacted
// changes nothing r points to; the copy shares r's other slices, which it does
// not write.
func (r Report) Redacted() Report {
	if r.Instance != nil {
		inst := *r.Instance
		if inst.Key != "" {
			inst.Key = RedactedValue
		}
		r.Instance = &inst
	}
	return r
}
