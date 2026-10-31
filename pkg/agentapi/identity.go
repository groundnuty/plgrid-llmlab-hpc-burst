package agentapi

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
)

// agentInstanceIDForm is an agent instance ID's form: 128 random bits as 32
// lowercase hex characters. The form is pinned because the controller copies
// the ID into the Instance's status (InstanceStatus.Agent), whose field has a
// length bound, so an ID of another length would make a status write fail.
var agentInstanceIDForm = regexp.MustCompile(`^[0-9a-f]{32}$`)

// CheckAgentInstanceID returns an error unless s is an agent instance ID's
// exact form, 32 lowercase hex characters. The server calls it on
// HeaderAgentInstanceID and answers another form with CodeBadRequest before it
// calls the Backend. The error never repeats s, which is a header a caller
// chose.
func CheckAgentInstanceID(s string) error {
	if !agentInstanceIDForm.MatchString(s) {
		return fmt.Errorf("an agent instance ID is 32 lowercase hex characters; got %d bytes of another form", len(s))
	}
	return nil
}

// NewAgentInstanceID returns a new agent instance ID: 16 bytes from crypto/rand
// as 32 lowercase hex characters. The agent draws one at its start and sends it
// on every call (HeaderAgentInstanceID).
func NewAgentInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("agent instance ID: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
