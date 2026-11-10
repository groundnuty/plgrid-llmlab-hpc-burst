package wire_test

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A value that is invalid in its field carries this text, so a test can see
// whether an error repeats the value.
const secret = "SECRET"

const keyText = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmU\n-----END OPENSSH PRIVATE KEY-----\n"

// wantOneProblem checks that err is a single problem that names field and
// repeats none of the values.
func wantOneProblem(t *testing.T, err error, field string, values ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("accepted; want an error naming %s", field)
		return
	}
	text := err.Error()
	if lines := strings.Split(text, "\n"); len(lines) != 1 || !strings.HasPrefix(lines[0], field+":") {
		t.Errorf("error %q; want one problem, naming %s", text, field)
	}
	for _, v := range values {
		if v != "" && strings.Contains(text, v) {
			t.Errorf("error %q repeats the invalid value %q", text, v)
		}
	}
}

// problem is one invalid field of a DesiredSet.
type problem struct {
	name   string
	field  string // the field the error must name
	value  string // text the error must not contain
	mutate func(*wire.DesiredSet)
}

func str(field, bad string, set func(*wire.DesiredSet, string)) problem {
	short := bad
	if len(short) > 24 {
		short = short[:24]
	}
	return problem{name: fmt.Sprintf("%s=%q", field, short), field: field, value: distinctive(bad),
		mutate: func(s *wire.DesiredSet) { set(s, bad) }}
}

// num makes a problem of a number; the value is checked for in the error only
// when it is long enough that it cannot be a part of the error's own text.
func num(field string, bad int64, set func(*wire.DesiredSet, int64)) problem {
	value := ""
	if bad >= 10000 || bad <= -10000 {
		value = strconv.FormatInt(bad, 10)
	}
	return problem{name: fmt.Sprintf("%s=%d", field, bad), field: field, value: value,
		mutate: func(s *wire.DesiredSet) { set(s, bad) }}
}

func entry(field, bad string, set func(*wire.Entry, string)) problem {
	return str("allocations[0]."+field, bad, func(s *wire.DesiredSet, v string) { set(&s.Allocations[0], v) })
}

// distinctive is the part of a bad value that an error must not repeat: a
// short value can be a part of the error's own words.
func distinctive(bad string) string {
	if strings.Contains(bad, secret) || len(bad) >= 20 {
		return bad
	}
	return ""
}

func many(n int) []wire.Entry {
	var es []wire.Entry
	for i := 1; i <= n; i++ {
		es = append(es, validEntry(i))
	}
	return es
}

func setProblems() []problem {
	provider := func(s *wire.DesiredSet, v string) { s.Provider = v }
	uid := func(s *wire.DesiredSet, v string) { s.ProviderUID = v }
	host := func(s *wire.DesiredSet, v string) { s.SSH.HostName = v }
	user := func(s *wire.DesiredSet, v string) { s.SSH.User = v }
	identity := func(s *wire.DesiredSet, v string) { s.SSH.IdentityFile = v }
	known := func(s *wire.DesiredSet, v string) { s.SSH.KnownHostsFile = v }
	leaseEpoch := func(s *wire.DesiredSet, v int64) { s.LeaseEpoch = v }
	age := func(s *wire.DesiredSet, v int64) { s.ListingAgeMS = v }
	validFor := func(s *wire.DesiredSet, v int64) { s.ValidForMS = v }
	port := func(s *wire.DesiredSet, v int64) { s.SSH.Port = int(v) }
	eUID := func(e *wire.Entry, v string) { e.UID = v }
	eTag := func(e *wire.Entry, v string) { e.Tag = v }
	eJob := func(e *wire.Entry, v string) { e.JobID = v }
	eDir := func(e *wire.Entry, v string) { e.AllocDir = v }
	eAgent := func(e *wire.Entry, v string) { e.AgentPath = v }
	eGen := func(e *wire.Entry, v string) { e.Gen = v }

	ps := []problem{
		str("provider", "", provider),
		str("provider", "Provider-"+secret, provider),
		str("provider", "-provider-"+secret, provider),
		str("provider", "provider-"+secret+"-", provider),
		str("provider", "provider_"+secret, provider),
		str("provider", strings.Repeat("a", 254), provider),

		str("providerUID", "", uid),
		str("providerUID", "uid_"+secret, uid),
		str("providerUID", "-uid-"+secret, uid),
		str("providerUID", strings.Repeat("a", 65), uid),

		{name: "providerCreated is zero", field: "providerCreated", mutate: func(s *wire.DesiredSet) { s.ProviderCreated = time.Time{} }},

		num("leaseEpoch", -1, leaseEpoch),
		num("leaseEpoch", math.MaxInt32+1, leaseEpoch),
		num("leaseEpoch", 9876543210, leaseEpoch),
		num("listingAgeMs", -1, age),
		num("listingAgeMs", -987654, age),
		num("validForMs", 0, validFor),
		num("validForMs", -1, validFor),
		num("validForMs", wire.MaxValidFor.Milliseconds()+1, validFor),
		num("validForMs", 987654321, validFor),

		str("ssh.hostName", "", host),
		str("ssh.hostName", "-oProxyCommand="+secret, host),
		str("ssh.hostName", "login "+secret+".example", host),
		str("ssh.hostName", "login.example-"+secret+"/x", host),
		str("ssh.hostName", "login.example-"+secret+"\n", host),
		str("ssh.hostName", "login.example-"+secret+"-", host),
		str("ssh.hostName", strings.Repeat("a", 254), host),

		str("ssh.user", "", user),
		str("ssh.user", "1"+secret, user),
		str("ssh.user", "-o"+secret, user),
		str("ssh.user", "user "+secret, user),
		str("ssh.user", "user;"+secret, user),
		str("ssh.user", strings.Repeat("a", 33), user),

		num("ssh.port", -1, port),
		num("ssh.port", 65536, port),
		num("ssh.port", 987654, port),

		str("ssh.identityFile", "", identity),
		str("ssh.identityFile", "relative/"+secret, identity),
		str("ssh.identityFile", "~/"+secret, identity),
		str("ssh.identityFile", "/home/"+secret+" key", identity),
		str("ssh.identityFile", "/home/../"+secret, identity),
		str("ssh.identityFile", "/home//"+secret, identity),
		str("ssh.identityFile", "/home/"+secret+"/", identity),
		str("ssh.identityFile", "/home/"+secret+"\n", identity),
		str("ssh.identityFile", "/home/"+secret+"$x", identity),
		str("ssh.identityFile", "/home/"+secret+";x", identity),
		str("ssh.identityFile", "/home/"+secret+"\x00", identity),
		str("ssh.identityFile", "/home/"+secret+"'x", identity),
		str("ssh.identityFile", "/home/"+secret+"%2e", identity),
		str("ssh.identityFile", "/home/"+secret+"|x", identity),
		str("ssh.identityFile", keyText, identity),
		str("ssh.identityFile", "/"+keyText, identity),
		str("ssh.identityFile", strings.ReplaceAll(keyText, "\n", ""), identity),
		str("ssh.identityFile", strings.ReplaceAll(keyText, "\n", " "), identity),

		str("ssh.knownHostsFile", "relative/"+secret, known),
		str("ssh.knownHostsFile", "/home/../"+secret, known),
		str("ssh.knownHostsFile", "/home/"+secret+"\n", known),
		str("ssh.knownHostsFile", keyText, known),
		str("ssh.knownHostsFile", "login.example ssh-ed25519 AAAA"+secret, known),

		{name: "more than MaxAllocations", field: "allocations", mutate: func(s *wire.DesiredSet) { s.Allocations = many(wire.MaxAllocations + 1) }},
		{name: "the same UID twice", field: "allocations[1]", value: validEntry(1).UID,
			mutate: func(s *wire.DesiredSet) { s.Allocations[1].UID = s.Allocations[0].UID }},
		{name: "the same tag twice", field: "allocations[1]", value: validEntry(1).Tag,
			mutate: func(s *wire.DesiredSet) { s.Allocations[1].Tag = s.Allocations[0].Tag }},
	}
	ps = append(ps,
		entry("uid", "", eUID),
		entry("uid", "u_"+secret, eUID),
		entry("uid", strings.Repeat("a", 65), eUID),
		entry("tag", "", eTag),
		entry("tag", "Tag-"+secret, eTag),
		entry("tag", "hb-job-1-0000abcdef0"+secret, eTag),  // 11 hex digits, then text
		entry("tag", "hb-job-1-0000ABCDEF01"+secret, eTag), // upper case, with text
		entry("tag", "job-1-0000abcdef01", eTag),           // no hb- prefix
		entry("job", "", eJob),
		entry("job", "0", eJob),
		entry("job", "012", eJob),
		entry("job", "4001"+secret, eJob),
		entry("job", "123456789012345678901", eJob),
		entry("allocDir", "", eDir),
		entry("allocDir", "relative/"+secret, eDir),
		entry("allocDir", "/a/../"+secret, eDir),
		entry("allocDir", "/a b/"+secret, eDir),
		entry("allocDir", keyText, eDir),
		entry("agentPath", "", eAgent),
		entry("agentPath", "relative/"+secret, eAgent),
		entry("agentPath", "/a/./"+secret, eAgent),
		entry("agentPath", keyText, eAgent),
		entry("gen", "", eGen),
		entry("gen", "gen "+secret, eGen),
		entry("gen", strings.Repeat("a", 65), eGen),
	)
	return ps
}

func TestDesiredSetValidateAcceptsAValidSet(t *testing.T) {
	if err := validSet().Validate(); err != nil {
		t.Fatalf("a valid set: %v", err)
	}
}

func TestDesiredSetValidateAcceptsTheEdgesOfEachField(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*wire.DesiredSet)
	}{
		{"no Allocations, complete", func(s *wire.DesiredSet) { s.Allocations = nil }},
		{"no Allocations, not complete", func(s *wire.DesiredSet) { s.Allocations, s.Complete = nil, false }},
		{"MaxAllocations Allocations", func(s *wire.DesiredSet) { s.Allocations = many(wire.MaxAllocations) }},
		{"one Allocation that ended for good", func(s *wire.DesiredSet) { s.Allocations = many(1); s.Allocations[0].EndedForGood = true }},
		{"leaseEpoch 0", func(s *wire.DesiredSet) { s.LeaseEpoch = 0 }},
		{"leaseEpoch 2^31-1", func(s *wire.DesiredSet) { s.LeaseEpoch = math.MaxInt32 }},
		{"listingAgeMs 0", func(s *wire.DesiredSet) { s.ListingAgeMS = 0 }},
		{"validForMs at its maximum", func(s *wire.DesiredSet) { s.ValidForMS = wire.MaxValidFor.Milliseconds() }},
		{"validForMs 1", func(s *wire.DesiredSet) { s.ValidForMS = 1 }},
		{"port 0, ssh's default", func(s *wire.DesiredSet) { s.SSH.Port = 0 }},
		{"port 65535", func(s *wire.DesiredSet) { s.SSH.Port = 65535 }},
		{"no known-hosts file, ssh's default", func(s *wire.DesiredSet) { s.SSH.KnownHostsFile = "" }},
		{"a Provider name of 253 characters", func(s *wire.DesiredSet) { s.Provider = strings.Repeat("a", 253) }},
		{"a Provider name with dots and hyphens", func(s *wire.DesiredSet) { s.Provider = "team-1.provider-a" }},
		{"a host name of 253 characters", func(s *wire.DesiredSet) { s.SSH.HostName = strings.Repeat("a", 253) }},
		{"a job of 20 digits", func(s *wire.DesiredSet) { s.Allocations[0].JobID = "12345678901234567890" }},
		{"paths with the safe punctuation", func(s *wire.DesiredSet) { s.SSH.IdentityFile = "/home/a-b/.ssh/id_x.y:z=w,v" }},
		{"not complete", func(s *wire.DesiredSet) { s.Complete = false }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := validSet()
			c.mutate(&s)
			if err := s.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

func TestDesiredSetValidateRefusesEachInvalidField(t *testing.T) {
	for _, p := range setProblems() {
		t.Run(p.name, func(t *testing.T) {
			s := validSet()
			p.mutate(&s)
			wantOneProblem(t, s.Validate(), p.field, p.value)
		})
	}
}

func TestDesiredSetValidateNamesEveryProblemAtOnce(t *testing.T) {
	s := validSet()
	s.Provider = "Provider-" + secret
	s.SSH.Port = -1
	s.Allocations[1].JobID = "0"
	s.ValidForMS = 0
	err := s.Validate()
	if err == nil {
		t.Fatal("accepted")
	}
	text := err.Error()
	for _, field := range []string{"provider:", "ssh.port:", "allocations[1].job:", "validForMs:"} {
		if !strings.Contains(text, field) {
			t.Errorf("error %q does not name %s", text, field)
		}
	}
	if n := len(strings.Split(text, "\n")); n != 4 {
		t.Errorf("error has %d lines; want one per problem, 4:\n%s", n, text)
	}
	if strings.Contains(text, secret) {
		t.Errorf("error %q repeats a value", text)
	}
}

func TestAZeroMessageIsRefused(t *testing.T) {
	if err := (wire.DesiredSet{}).Validate(); err == nil {
		t.Error("the zero DesiredSet validates")
	}
	if err := (wire.Ack{}).Validate(); err == nil {
		t.Error("the zero Ack validates")
	}
}

func TestMaxAllocationsIsAtLeastTheHostDialedMaximum(t *testing.T) {
	if wire.MaxAllocations < agentapi.HostDialedMaxAllocations {
		t.Errorf("MaxAllocations %d is below the host-dialed maximum %d", wire.MaxAllocations, agentapi.HostDialedMaxAllocations)
	}
}

// ack problems

type ackProblem struct {
	name   string
	field  string
	values []string // text the error must not contain
	mutate func(*wire.Ack)
}

func ackStr(field, bad string, set func(*wire.Ack, string)) ackProblem {
	return ackProblem{name: fmt.Sprintf("%s=%q", field, bad), field: field, values: []string{distinctive(bad)},
		mutate: func(a *wire.Ack) { set(a, bad) }}
}

func ackNum(field string, bad int64, set func(*wire.Ack, int64)) ackProblem {
	value := ""
	if bad >= 10000 || bad <= -10000 {
		value = strconv.FormatInt(bad, 10)
	}
	return ackProblem{name: fmt.Sprintf("%s=%d", field, bad), field: field, values: []string{value},
		mutate: func(a *wire.Ack) { set(a, bad) }}
}

func ackPair(outcome, reason string) ackProblem {
	field := "reason"
	if outcome != wire.OutcomeMarked && outcome != wire.OutcomeRefused {
		field = "outcome"
	}
	return ackProblem{name: fmt.Sprintf("outcome=%q reason=%q", outcome, reason), field: field, values: []string{distinctive(outcome), distinctive(reason)},
		mutate: func(a *wire.Ack) { a.Outcome, a.Reason = outcome, reason }}
}

func ackProblems() []ackProblem {
	provider := func(a *wire.Ack, v string) { a.Provider = v }
	uid := func(a *wire.Ack, v string) { a.UID = v }
	job := func(a *wire.Ack, v string) { a.JobID = v }
	channel := func(a *wire.Ack, v string) { a.Channel = agentapi.RelayChannel(v) }
	leaseEpoch := func(a *wire.Ack, v int64) { a.LeaseEpoch = v }
	epoch := func(a *wire.Ack, v int64) { a.Epoch = v }
	seq := func(a *wire.Ack, v int64) { a.Seq = v }
	return []ackProblem{
		ackStr("provider", "", provider),
		ackStr("provider", "Provider-"+secret, provider),
		ackStr("provider", strings.Repeat("a", 254), provider),
		ackNum("leaseEpoch", -1, leaseEpoch),
		ackNum("leaseEpoch", math.MaxInt32+1, leaseEpoch),
		ackNum("leaseEpoch", 9876543210, leaseEpoch),
		ackStr("uid", "", uid),
		ackStr("uid", "u_"+secret, uid),
		ackStr("uid", strings.Repeat("a", 65), uid),
		ackStr("job", "", job),
		ackStr("job", "0", job),
		ackStr("job", "012", job),
		ackStr("job", "4001"+secret, job),
		ackStr("channel", "", channel),
		ackStr("channel", "other-"+secret, channel),
		ackStr("channel", "Control", channel),
		ackNum("epoch", -1, epoch),
		ackNum("epoch", -987654, epoch),
		ackNum("seq", 0, seq),
		ackNum("seq", -1, seq),
		ackNum("seq", -987654, seq),

		ackPair("", ""),
		ackPair("MARKED", ""),
		ackPair("other-"+secret, ""),
		ackPair("other-"+secret, wire.AckDeleted),
		// marked, with a reason
		ackPair(wire.OutcomeMarked, wire.AckAnotherUID),
		ackPair(wire.OutcomeMarked, wire.AckAnotherJob),
		ackPair(wire.OutcomeMarked, wire.AckNotRunning),
		ackPair(wire.OutcomeMarked, wire.AckDeleted),
		ackPair(wire.OutcomeMarked, "reason-"+secret),
		// refused, with no reason or one that is not a reason
		ackPair(wire.OutcomeRefused, ""),
		ackPair(wire.OutcomeRefused, "reason-"+secret),
		ackPair(wire.OutcomeRefused, "ANOTHER_UID"),
		ackPair(wire.OutcomeRefused, " "+wire.AckDeleted),
		ackPair(wire.OutcomeRefused, wire.AckDeleted+"\n"),
	}
}

func TestAckValidateAcceptsAValidAck(t *testing.T) {
	if err := validAck().Validate(); err != nil {
		t.Fatalf("a valid Ack: %v", err)
	}
}

func TestAckValidateAcceptsAMarkWithNoReason(t *testing.T) {
	a := validAck()
	a.Outcome, a.Reason = wire.OutcomeMarked, ""
	if err := a.Validate(); err != nil {
		t.Errorf("marked with no reason: %v", err)
	}
}

func TestAckValidateAcceptsARefusalWithEachOfTheFourReasons(t *testing.T) {
	reasons := []string{"another_uid", "another_job", "not_running", "deleted"}
	for _, reason := range reasons {
		a := validAck()
		a.Outcome, a.Reason = wire.OutcomeRefused, reason
		if err := a.Validate(); err != nil {
			t.Errorf("refused with %q: %v", reason, err)
		}
	}
}

func TestAckValidateAcceptsTheEdgesOfEachField(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*wire.Ack)
	}{
		{"leaseEpoch 0", func(a *wire.Ack) { a.LeaseEpoch = 0 }},
		{"leaseEpoch 2^31-1", func(a *wire.Ack) { a.LeaseEpoch = math.MaxInt32 }},
		{"epoch 0", func(a *wire.Ack) { a.Epoch = 0 }},
		{"seq 1", func(a *wire.Ack) { a.Seq = 1 }},
		{"the serving channel", func(a *wire.Ack) { a.Channel = agentapi.RelayServing }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := validAck()
			c.mutate(&a)
			if err := a.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

func TestAckValidateRefusesEachInvalidField(t *testing.T) {
	for _, p := range ackProblems() {
		t.Run(p.name, func(t *testing.T) {
			a := validAck()
			p.mutate(&a)
			wantOneProblem(t, a.Validate(), p.field, p.values...)
		})
	}
}

func TestAckValidateNamesEveryProblemAtOnce(t *testing.T) {
	a := validAck()
	a.Provider = "Provider-" + secret
	a.Seq = 0
	a.Outcome, a.Reason = wire.OutcomeRefused, ""
	err := a.Validate()
	if err == nil {
		t.Fatal("accepted")
	}
	for _, field := range []string{"provider:", "seq:", "reason:"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("error %q does not name %s", err, field)
		}
	}
	if n := len(strings.Split(err.Error(), "\n")); n != 3 {
		t.Errorf("error has %d lines; want one per problem, 3:\n%s", n, err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error %q repeats a value", err)
	}
}
