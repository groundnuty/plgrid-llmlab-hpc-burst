package wire

import (
	"errors"
	"fmt"
	"math"
	"path"
	"regexp"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The forms a message's values must have. Errors name the field, never its
// value: a value can name a host or a user.
var (
	// providerForm is a Provider's name, a Kubernetes object name (DNS-1123
	// subdomain, at most 253). It also names the keeper's -F file.
	providerForm = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)
	// uidForm is a Kubernetes UID or the like, as agentapi.RelayArgs takes it.
	uidForm = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,63}$`)
	// tagForm is v1alpha1.JobTag's output, as agentapi checks it (config.go).
	tagForm = regexp.MustCompile(`^hb-[a-z0-9][-a-z0-9.]{0,39}-[0-9a-f]{12}$`)
	// jobIDForm is a Slurm job ID, as agentapi.RelayLine takes it.
	jobIDForm = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
	// safePath is HBEP-4's safe pattern for an absolute path.
	safePath = regexp.MustCompile(`^/[A-Za-z0-9._:=,/-]*$`)
	// hostForm is a login host's name, as slurm.ParseSSHConfig takes it.
	hostForm = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$`)
	// genForm is a credentials generation (providerctl.credentialsGen: 16
	// hex characters), with room for another short name.
	genForm = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// cleanPath reports whether p is an absolute, clean path of HBEP-4's safe
// characters.
func cleanPath(p string) bool { return safePath.MatchString(p) && path.Clean(p) == p }

// Validate returns every problem with s, joined.
func (s DesiredSet) Validate() error {
	var errs []error
	bad := func(field, want string) { errs = append(errs, fmt.Errorf("%s: want %s", field, want)) }
	if !providerForm.MatchString(s.Provider) {
		bad("provider", "a Provider's name")
	}
	if !uidForm.MatchString(s.ProviderUID) {
		bad("providerUID", "a Kubernetes UID")
	}
	if s.ProviderCreated.IsZero() {
		bad("providerCreated", "the Provider's creationTimestamp")
	}
	if s.LeaseEpoch < 0 || s.LeaseEpoch > math.MaxInt32 {
		bad("leaseEpoch", "a Lease's leaseTransitions, 0 to 2^31-1")
	}
	if s.ListingAgeMS < 0 || s.ListingAgeMS > MaxListingAge.Milliseconds() {
		bad("listingAgeMs", fmt.Sprintf("0 to %d", MaxListingAge.Milliseconds()))
	}
	if s.ValidForMS < 1 || s.ValidForMS > MaxValidFor.Milliseconds() {
		bad("validForMs", fmt.Sprintf("1 to %d", MaxValidFor.Milliseconds()))
	}
	errs = append(errs, s.SSH.validate()...)
	if len(s.Allocations) > MaxAllocations {
		bad("allocations", fmt.Sprintf("at most %d", MaxAllocations))
	}
	uids, tags := map[string]bool{}, map[string]bool{}
	for i, e := range s.Allocations {
		for _, err := range e.validate() {
			errs = append(errs, fmt.Errorf("allocations[%d].%w", i, err))
		}
		if uids[e.UID] || tags[e.Tag] {
			bad(fmt.Sprintf("allocations[%d]", i), "a UID and a tag that no other Allocation of the set has")
		}
		uids[e.UID], tags[e.Tag] = true, true
	}
	return errors.Join(errs...)
}

func (h SSH) validate() []error {
	var errs []error
	bad := func(field, want string) { errs = append(errs, fmt.Errorf("ssh.%s: want %s", field, want)) }
	if len(h.HostName) > 253 || !hostForm.MatchString(h.HostName) {
		bad("hostName", "a host name")
	}
	if !agentapi.ValidUserName(h.User) {
		bad("user", "a portable user name")
	}
	if h.Port < 0 || h.Port > 65535 {
		bad("port", "0 (ssh's default) to 65535")
	}
	if !cleanPath(h.IdentityFile) {
		bad("identityFile", "a clean absolute path of letters, digits and ._:=,/-")
	}
	if h.KnownHostsFile != "" && !cleanPath(h.KnownHostsFile) {
		bad("knownHostsFile", "empty, or a clean absolute path of letters, digits and ._:=,/-")
	}
	return errs
}

func (e Entry) validate() []error {
	var errs []error
	bad := func(field, want string) { errs = append(errs, fmt.Errorf("%s: want %s", field, want)) }
	if !uidForm.MatchString(e.UID) {
		bad("uid", "an Allocation's UID")
	}
	if !tagForm.MatchString(e.Tag) {
		bad("tag", "an Allocation's job tag")
	}
	if !jobIDForm.MatchString(e.JobID) {
		bad("job", "a Slurm job ID")
	}
	if !cleanPath(e.AllocDir) {
		bad("allocDir", "a clean absolute path of letters, digits and ._:=,/-")
	}
	if !cleanPath(e.AgentPath) {
		bad("agentPath", "a clean absolute path of letters, digits and ._:=,/-")
	}
	if !genForm.MatchString(e.Gen) {
		bad("gen", "a credentials generation")
	}
	return errs
}

// Validate returns every problem with a, joined.
func (a Ack) Validate() error {
	var errs []error
	bad := func(field, want string) { errs = append(errs, fmt.Errorf("%s: want %s", field, want)) }
	if !providerForm.MatchString(a.Provider) {
		bad("provider", "a Provider's name")
	}
	if a.LeaseEpoch < 0 || a.LeaseEpoch > math.MaxInt32 {
		bad("leaseEpoch", "a Lease's leaseTransitions, 0 to 2^31-1")
	}
	if !uidForm.MatchString(a.UID) {
		bad("uid", "an Allocation's UID")
	}
	if !jobIDForm.MatchString(a.JobID) {
		bad("job", "a Slurm job ID")
	}
	if _, err := agentapi.ParseRelayChannel(string(a.Channel)); err != nil {
		bad("channel", "control or serving")
	}
	if a.Epoch < 0 {
		bad("epoch", "0 or more")
	}
	if a.Seq < 1 {
		bad("seq", "1 or more")
	}
	switch a.Outcome {
	case OutcomeMarked:
		if a.Reason != "" {
			bad("reason", "empty for a mark that was written")
		}
	case OutcomeRefused:
		switch a.Reason {
		case AckAnotherUID, AckAnotherJob, AckNotRunning, AckDeleted:
		default:
			bad("reason", "another_uid, another_job, not_running or deleted")
		}
	default:
		bad("outcome", "marked or refused")
	}
	return errors.Join(errs...)
}
