package audit

// (a), the levels: the embedded policy is loaded and evaluated by k8s.io/apiserver's OWN audit policy evaluator, the
// code the API server runs, not a parser of ours. One synthetic request per rule and per non-match gives a level that
// is written out in the table below from HBEP-10's Audit section, and never read from the policy. k8s.io/apiserver is
// imported by this test file only (Kubernetes apiserver, https://github.com/kubernetes/apiserver, Apache-2.0,
// v0.35.0, the module graph's own version).

import (
	"slices"
	"testing"

	auditapi "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit/policy"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// The user names are written out, as the API server reports them.
const (
	lifecycle = "system:serviceaccount:hpcburst-system:hpcburst-lifecycle"
	provider  = "system:serviceaccount:hpcburst-system:hpcburst-provider"
	front     = "system:serviceaccount:hpcburst-system:hpcburst-front"
	agentA    = "system:serviceaccount:hpcburst-agents:agent-a"
	person    = "admin"
	hbGroup   = "hpcburst.groundnuty.github.io"
)

type request struct {
	user, verb, group, resource, subresource string
	// namespace is the request's namespace; empty means example-ns, which no rule names.
	namespace string
	// path is set for a request that is not about a resource, such as /healthz.
	path string
}

func (r request) attributes() authorizer.Attributes {
	return authorizer.AttributesRecord{
		User: &user.DefaultInfo{Name: r.user}, Verb: r.verb, APIGroup: r.group, APIVersion: "v1",
		Resource: r.resource, Subresource: r.subresource, Namespace: cmpNS(r.namespace), Name: "example",
		ResourceRequest: r.path == "", Path: r.path,
	}
}

func cmpNS(ns string) string {
	if ns == "" {
		return "example-ns"
	}
	return ns
}

func (r request) String() string {
	if r.path != "" {
		return r.verb + " " + r.path + " by " + r.user
	}
	res := r.resource
	if r.subresource != "" {
		res += "/" + r.subresource
	}
	return r.verb + " " + res + " (" + r.group + ", in " + cmpNS(r.namespace) + ") by " + r.user
}

func TestThePolicyGivesEachRequestItsLevel(t *testing.T) {
	p, err := policy.LoadPolicyFromBytes(Policy)
	if err != nil {
		t.Fatalf("the embedded policy does not load, or fails the API server's own validation: %v", err)
	}
	eval := policy.NewPolicyRuleEvaluator(p)

	none, meta, req, reqresp := auditapi.LevelNone, auditapi.LevelMetadata, auditapi.LevelRequest, auditapi.LevelRequestResponse
	var rows []struct {
		r    request
		want auditapi.Level
		why  string
	}
	add := func(r request, want auditapi.Level, why string) {
		rows = append(rows, struct {
			r    request
			want auditapi.Level
			why  string
		}{r, want, why})
	}

	// Rule 1, None: the three controllers' reads and renewals of a Lease in hpcburst-system, where their locks live.
	for _, u := range []string{lifecycle, provider, front} {
		for _, verb := range []string{"get", "update", "list", "watch"} {
			add(request{user: u, verb: verb, group: "coordination.k8s.io", resource: "leases", namespace: "hpcburst-system"}, none, "rule 1: a controller's Lease read or renewal")
		}
	}
	// The same requests in any other namespace are logged at Metadata, refused or not: a level is chosen before the answer, so a
	// controller identity that reads or rewrites kube-system's leader Leases must leave a line.
	for _, u := range []string{lifecycle, provider, front} {
		for _, ns := range []string{"kube-system", "default", "example-ns"} {
			for _, verb := range []string{"get", "update", "list", "watch"} {
				add(request{user: u, verb: verb, group: "coordination.k8s.io", resource: "leases", namespace: ns}, meta, "a controller's Lease request outside hpcburst-system is not rule 1's")
			}
		}
	}
	// Its non-matches: a create (the first Lease is worth a line), a delete, a patch, someone else, another kind.
	add(request{user: lifecycle, verb: "create", group: "coordination.k8s.io", resource: "leases", namespace: "hpcburst-system"}, meta, "a Lease create is not at None")
	add(request{user: lifecycle, verb: "delete", group: "coordination.k8s.io", resource: "leases", namespace: "hpcburst-system"}, meta, "a Lease delete is not at None")
	add(request{user: lifecycle, verb: "patch", group: "coordination.k8s.io", resource: "leases", namespace: "hpcburst-system"}, meta, "a Lease patch is not at None")
	add(request{user: person, verb: "update", group: "coordination.k8s.io", resource: "leases", namespace: "hpcburst-system"}, meta, "the person's Lease write is not at None")
	add(request{user: agentA, verb: "get", group: "coordination.k8s.io", resource: "leases", namespace: "hpcburst-system"}, meta, "an agent's Lease read is not at None")
	add(request{user: "system:serviceaccount:hpcburst-system:hpcburst-other", verb: "update", group: "coordination.k8s.io", resource: "leases", namespace: "hpcburst-system"}, meta, "another account of the namespace")
	add(request{user: lifecycle, verb: "get", group: "", resource: "leases", namespace: "hpcburst-system"}, meta, "a leases resource of another group")
	add(request{user: lifecycle, verb: "update", group: "", resource: "configmaps"}, meta, "a controller's other write")

	// Rule 2, RequestResponse: Operations, their status, Approvals and ApprovalPolicies, by every user and verb.
	for _, res := range []struct{ resource, sub string }{{"operations", ""}, {"operations", "status"}, {"approvals", ""}, {"approvalpolicies", ""}} {
		for _, u := range []string{agentA, person, lifecycle, provider} {
			for _, verb := range []string{"create", "update", "patch", "get"} {
				add(request{user: u, verb: verb, group: hbGroup, resource: res.resource, subresource: res.sub}, reqresp, "rule 2")
			}
		}
	}
	add(request{user: lifecycle, verb: "delete", group: hbGroup, resource: "operations"}, reqresp, "rule 2 comes before rule 3: the removal of an Operation")
	add(request{user: lifecycle, verb: "list", group: hbGroup, resource: "operations"}, reqresp, "rule 2 has no verb filter: a controller's list of Operations")
	add(request{user: provider, verb: "watch", group: hbGroup, resource: "approvals"}, reqresp, "rule 2 has no verb filter: a controller's watch")
	// Its non-matches: the same names in another group, and the neighbouring kinds and subresources.
	add(request{user: agentA, verb: "create", group: "example.org", resource: "operations"}, meta, "operations of another group")
	add(request{user: agentA, verb: "update", group: hbGroup, resource: "operations", subresource: "scale"}, meta, "a subresource the rule does not name")
	add(request{user: lifecycle, verb: "update", group: hbGroup, resource: "instances", subresource: "status"}, meta, "instances/status is not an Operation")
	add(request{user: lifecycle, verb: "update", group: hbGroup, resource: "modeldeployments", subresource: "status"}, meta, "modeldeployments/status")
	add(request{user: agentA, verb: "update", group: hbGroup, resource: "modeldeployments"}, meta, "an agent's edit of a ModelDeployment")
	add(request{user: provider, verb: "update", group: hbGroup, resource: "providers", subresource: "status"}, meta, "providers/status")

	// Rule 3, Request: the lifecycle controller's writes to Allocations.
	for _, verb := range []string{"update", "patch", "delete"} {
		add(request{user: lifecycle, verb: verb, group: hbGroup, resource: "allocations"}, req, "rule 3: the lifecycle controller's Allocation write")
	}
	// Its non-matches: another verb, the status subresource, another user, another group.
	for _, c := range []struct {
		r   request
		why string
	}{
		{request{user: lifecycle, verb: "create", group: hbGroup, resource: "allocations"}, "an Allocation create"},
		{request{user: lifecycle, verb: "get", group: hbGroup, resource: "allocations"}, "an Allocation read"},
		{request{user: lifecycle, verb: "list", group: hbGroup, resource: "allocations"}, "an Allocation list"},
		{request{user: lifecycle, verb: "update", group: hbGroup, resource: "allocations", subresource: "status"}, "allocations/status"},
		{request{user: provider, verb: "update", group: hbGroup, resource: "allocations"}, "the Provider controller's Allocation write"},
		{request{user: provider, verb: "delete", group: hbGroup, resource: "allocations"}, "the Provider controller's Allocation delete"},
		{request{user: person, verb: "delete", group: hbGroup, resource: "allocations"}, "the person's Allocation delete"},
		{request{user: agentA, verb: "update", group: hbGroup, resource: "allocations"}, "an agent's Allocation write"},
		{request{user: lifecycle, verb: "update", group: "example.org", resource: "allocations"}, "allocations of another group"},
	} {
		add(c.r, meta, c.why)
	}

	// Rule 4, Metadata: everything else, a Secret's every verb among it, so that no Secret's content is logged.
	for _, verb := range []string{"get", "list", "watch", "create", "update", "patch", "delete"} {
		for _, u := range []string{person, lifecycle, agentA} {
			add(request{user: u, verb: verb, group: "", resource: "secrets"}, meta, "rule 4: a Secret is at Metadata")
		}
	}
	add(request{user: person, verb: "create", group: "", resource: "configmaps"}, meta, "rule 4")
	add(request{user: lifecycle, verb: "create", group: "", resource: "events"}, meta, "rule 4: a controller's event")
	add(request{user: lifecycle, verb: "create", group: "events.k8s.io", resource: "events"}, meta, "rule 4: a controller's event")
	add(request{user: person, verb: "create", group: "authentication.k8s.io", resource: "selfsubjectreviews"}, meta, "rule 4")
	add(request{user: person, verb: "get", path: "/healthz"}, meta, "rule 4: a request that names no resource")

	if len(rows) < 100 {
		t.Fatalf("only %d rows: the table was cut", len(rows))
	}
	for _, row := range rows {
		got := eval.EvaluatePolicyRule(row.r.attributes())
		if got.Level != row.want {
			t.Errorf("%s: level %s, want %s (%s)", row.r, got.Level, row.want, row.why)
		}
		// Every request is logged once, as it is answered: RequestReceived is omitted at every level.
		if !slices.Contains(got.OmitStages, auditapi.StageRequestReceived) {
			t.Errorf("%s: omitted stages %v do not include %s", row.r, got.OmitStages, auditapi.StageRequestReceived)
		}
	}
}

// TestTheFlagsKeepEveryRotatedFile pins the flags with literals: the files are kept for the grant (no limit on their
// number or age) and gzipped, and the one value a caller chooses is the size.
func TestTheFlagsKeepEveryRotatedFile(t *testing.T) {
	want := []string{
		"audit-policy-file=/state/audit-policy.yaml", "audit-log-path=/state/logs/audit.log", "audit-log-maxsize=100",
		"audit-log-maxbackup=0", "audit-log-maxage=0", "audit-log-compress=true",
	}
	if got := Flags("/state/audit-policy.yaml", "/state/logs/audit.log", DefaultMaxSizeMB); !slices.Equal(got, want) {
		t.Errorf("flags:\n got %v\nwant %v", got, want)
	}
	if got := Flags("/p", "/l", 0); !slices.Contains(got, "audit-log-maxsize=100") {
		t.Errorf("a size of 0 is the default 100 MB, got %v", got)
	}
	if got := Flags("/p", "/l", 1); !slices.Contains(got, "audit-log-maxsize=1") || !slices.Contains(got, "audit-log-maxbackup=0") {
		t.Errorf("a size of 1: %v", got)
	}
}
