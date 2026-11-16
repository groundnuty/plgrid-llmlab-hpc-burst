package front

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The record fetch (HBEP-9, Open questions, how a client fetches the record for an ID; HBEP-5, Contract
// and Errors; owner decision, 14 Nov 2026, issue #9): GET /hpcburst/v1/deployments/{id} answers a
// deployment ID's identity record and its verification records, read from the ControllerRevisions labelled
// with the ID. {id} is the full ID or its 12-character display form, of a served deployment or a managed
// catalog entry. It is authenticated like any request.
const (
	// frameworkPrefix holds the front's own routes, apart from the OpenAI-compatible /v1/, so that no route
	// OpenAI adds there can collide with one of ours (HBEP-9).
	frameworkPrefix = "/hpcburst/v1/"
	deploymentsPath = frameworkPrefix + "deployments/"
	// recordsReadTimeout bounds the API server's answer to one request's record reads.
	recordsReadTimeout = 10 * time.Second
)

// deploymentAnswer is the 200 body of the record fetch. It is a public contract: its field shapes are
// named in PR #585's body, and a change to them is one to HBEP-9's record fetch.
type deploymentAnswer struct {
	// ID is the full deployment ID, 64 lowercase hex: the identity record's stored hash.
	ID string `json:"id"`
	// DisplayID is its display form, the first 12 hex characters (HBEP-3, Display form).
	DisplayID string `json:"displayID"`
	// Scheme is the identity record's scheme.
	Scheme string `json:"scheme"`
	// Canonical is the identity record's canonical object as its RFC 8785 text, in a JSON string,
	// re-canonicalised from the stored object, which its writers' JSON encoder escaped. The front serves a
	// record by its stored hash and does not check it: a client verifies an ID by hashing this text, whose
	// SHA-256 is the full ID for any record the controllers wrote (HBEP-9, the record fetch, as PR #586
	// amends it).
	Canonical string `json:"canonical"`
	// IdentityRecords names every identity record that holds the ID: one per namespace that holds it,
	// hpcburst-system for a managed entry's (HBEP-11, Identity records).
	IdentityRecords []recordRef `json:"identityRecords"`
	// Verifications are the ID's verification records; empty, never null, when it has none, as a managed
	// entry's ID never does.
	Verifications []verificationEntry `json:"verifications"`
}

// recordRef names a record.
type recordRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// verificationEntry is one verification record of the ID (HBEP-7, One record per verification; HBEP-11,
// Verification records).
type verificationEntry struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	// InstanceUID is the record's instance-uid label.
	InstanceUID string `json:"instanceUID"`
	// Worker is set on a worker's record of an Instance of several workers (HBEP-7, Several workers).
	Worker string `json:"worker,omitempty"`
	// Generation is the ControllerRevision's revision, the Instance's (or the worker's) verification count.
	Generation int64 `json:"generation"`
	// Outcome is the record's verdict, pass or fail.
	Outcome string `json:"outcome"`
	// Record is the record's data, as the lifecycle controller wrote it: the resolved settings, the rehash,
	// the verdict and its cause among them.
	Record json.RawMessage `json:"record"`
}

// errNoRecordsClient is the read failure of a front built without a client for records.
var errNoRecordsClient = errors.New("the front has no Kubernetes client to read records with")

// serveFramework serves the client listener's /hpcburst/v1/ routes. A request is authenticated like any
// other (HBEP-5, Request path step 1), attributed to the front and recorded.
func (p *Proxy) serveFramework(w http.ResponseWriter, r *http.Request) {
	x := newExchange(r, p.clk, p.cfg.MaxRequestDuration.D())
	defer p.finish(x)
	cio := bindClientIO(w, x, p.cfg.writeGrace)
	defer cio.stop()
	key, ok := p.authenticate(r)
	if !ok {
		p.fail(w, x, http.StatusUnauthorized, codeInvalidAPIKey, "", "", "no client key, or a wrong one")
		return
	}
	x.rec.ClientKeyID = key
	id, ok := strings.CutPrefix(r.URL.Path, deploymentsPath)
	if !ok {
		x.attr.set(w.Header())
		x.rec.Status, x.rec.Finish = http.StatusNotFound, strp("error")
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		p.fail(w, x, http.StatusBadRequest, codeInvalidRequest, "", "", "GET "+deploymentsPath+"{id} is the only method on this path")
		return
	}
	p.serveDeployment(w, x, id)
}

// serveDeployment answers the records of id.
func (p *Proxy) serveDeployment(w http.ResponseWriter, x *exchange, id string) {
	if _, err := v1alpha1.IdentityRecordName(id); err != nil {
		// No record can hold an ID that is not 12 or 64 lowercase hex characters.
		p.notFound(w, x, id)
		return
	}
	ctx, cancel := context.WithTimeout(x.ctx, recordsReadTimeout)
	defer cancel()
	recs, err := listRecords(ctx, p.records, recordNamespaces(p.cfg.Namespaces), id[:v1alpha1.DisplayIDLength])
	if err != nil {
		if cut := x.cutBy(); cut != "" {
			if cut == cutClient {
				x.rec.Status, x.rec.Finish, x.rec.CutBy = statusClientClosed, strp("cut"), strp(cut)
				return
			}
			p.cutBeforeFirstByte(w, x, cut, "the request was cut while its records were read: ")
			return
		}
		p.log.Warn("records not read", "request", x.id, "error", err.Error())
		p.recordsUnavailable(w, x, "the records could not be read from the control plane; send the request again")
		return
	}
	ans, ids, problems := resolveRecords(id, recs)
	if len(problems) > 0 {
		// The front cannot vouch for what it could not read, and a 404 would say no such deployment
		// exists (HBEP-9, the record fetch, as PR #586 amends it).
		for _, pr := range problems {
			p.log.Warn("record not read", "request", x.id, "problem", pr)
		}
		p.recordsUnavailable(w, x, "a record labelled with the deployment ID "+id+" could not be read: "+problems[0])
		return
	}
	switch {
	case len(ids) > 1:
		p.ambiguous(w, x, id, ids)
		return
	case ans == nil:
		p.notFound(w, x, id)
		return
	}
	x.attr.set(w.Header())
	w.Header().Set("Content-Type", "application/json")
	x.rec.Status = http.StatusOK
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	// The verification records' data passes as stored: no escaping of <, > or & is added.
	enc.SetEscapeHTML(false)
	_ = enc.Encode(ans)
}

// notFound is HBEP-5's 404 deployment_not_found: no identity record holds the ID.
func (p *Proxy) notFound(w http.ResponseWriter, x *exchange, id string) {
	p.fail(w, x, http.StatusNotFound, codeDeploymentNotFound, "", "", "no identity record holds the deployment ID "+echoedName(id))
}

// recordsUnavailable answers a request whose records could not be read, or one of whose records could not
// be read or re-canonicalised: 503 records_unavailable with Retry-After: 5 (HBEP-5's Errors table, as PR
// #586 amends it).
func (p *Proxy) recordsUnavailable(w http.ResponseWriter, x *exchange, message string) {
	w.Header().Set("Retry-After", "5")
	p.fail(w, x, http.StatusServiceUnavailable, codeRecordsUnavailable, "", "", message)
}

// ambiguous answers a display form that two identity records hold with different full IDs, as two
// namespaces may (the collision rule is per namespace, HBEP-3): 409 deployment_ambiguous, naming the full
// IDs (HBEP-5's Errors table, as PR #586 amends it).
func (p *Proxy) ambiguous(w http.ResponseWriter, x *exchange, id string, ids []string) {
	p.fail(w, x, http.StatusConflict, codeDeploymentAmbiguous, "", "",
		fmt.Sprintf("the display form %s is held by %d deployment IDs (%s); ask for one by its full ID", id, len(ids), strings.Join(ids, ", ")))
}

// recordNamespaces are the namespaces whose records the front reads: all when it watches all, or else
// each namespace it watches and hpcburst-system, where managed entries' identity records live.
func recordNamespaces(watched []string) []string {
	if len(watched) == 0 {
		return []string{metav1.NamespaceAll}
	}
	out := []string{}
	for _, ns := range append(slices.Clone(watched), v1alpha1.ControllerNamespace) {
		if !slices.Contains(out, ns) {
			out = append(out, ns)
		}
	}
	return out
}

// recordSelector selects the identity and verification records labelled with a display form.
func recordSelector(display string) string {
	return v1alpha1.LabelDeploymentID + "=" + display + "," +
		v1alpha1.LabelRecord + " in (" + v1alpha1.RecordIdentity + "," + v1alpha1.RecordVerification + ")"
}

// listRecords lists, uncached, the identity and verification records labelled with display in each
// namespace: records are immutable and fetched rarely, so no informer holds them.
func listRecords(ctx context.Context, c kubernetes.Interface, namespaces []string, display string) ([]appsv1.ControllerRevision, error) {
	if c == nil {
		return nil, errNoRecordsClient
	}
	var out []appsv1.ControllerRevision
	for _, ns := range namespaces {
		l, err := c.AppsV1().ControllerRevisions(ns).List(ctx, metav1.ListOptions{LabelSelector: recordSelector(display)})
		if err != nil {
			return nil, fmt.Errorf("list the records of %s in namespace %q: %w", display, ns, err)
		}
		out = append(out, l.Items...)
	}
	return out, nil
}

// storedIdentity is an identity record's data (internal/lifecycle's identityData, managedrecord's data),
// and text, its canonical object's RFC 8785 text.
type storedIdentity struct {
	Scheme    string          `json:"scheme"`
	SHA256    string          `json:"sha256"`
	Canonical json.RawMessage `json:"canonical"`
	text      string
}

// storedVerification is the part of a verification record's data the front reads (internal/lifecycle's
// verificationData); the record passes whole, as stored.
type storedVerification struct {
	DeploymentID string `json:"deploymentID"`
	Verdict      string `json:"verdict"`
}

// foundVerification is a verification record that read, and the full ID its data names.
type foundVerification struct {
	deploymentID string
	entry        verificationEntry
}

// identityGroup is the identity records that hold one full ID.
type identityGroup struct {
	refs []recordRef
	data map[recordRef]storedIdentity
}

// resolveRecords resolves id among the records labelled with its display form. Every record found is read
// first: one that does not read as the kind its labels say, whichever ID it holds, is named in problems,
// and then nothing is resolved, since the front cannot vouch for an answer beside a record it could not
// read. A full ID names the records that hold it. A display form names the one full ID its identity records
// hold; when they hold several, as records in two namespaces may, it returns them all, sorted, and no
// answer. A verification record belongs to the ID its data names, whatever namespace holds it. No answer,
// no IDs and no problems: no identity record holds the ID.
func resolveRecords(id string, recs []appsv1.ControllerRevision) (*deploymentAnswer, []string, []string) {
	display := id[:v1alpha1.DisplayIDLength]
	var problems []string
	groups := map[string]*identityGroup{}
	var verifications []foundVerification
	for i := range recs {
		rec := &recs[i]
		if rec.Labels[v1alpha1.LabelDeploymentID] != display {
			problems = append(problems, fmt.Sprintf("record %s/%s is labelled with deployment ID %q, not %s", rec.Namespace, rec.Name, rec.Labels[v1alpha1.LabelDeploymentID], display))
			continue
		}
		switch rec.Labels[v1alpha1.LabelRecord] {
		case v1alpha1.RecordIdentity:
			d, err := readIdentity(rec, display)
			if err != nil {
				problems = append(problems, err.Error())
				continue
			}
			g := groups[d.SHA256]
			if g == nil {
				g = &identityGroup{data: map[recordRef]storedIdentity{}}
				groups[d.SHA256] = g
			}
			ref := recordRef{Namespace: rec.Namespace, Name: rec.Name}
			g.refs = append(g.refs, ref)
			g.data[ref] = d
		case v1alpha1.RecordVerification:
			v, err := readVerification(rec, display)
			if err != nil {
				problems = append(problems, err.Error())
				continue
			}
			verifications = append(verifications, v)
		default:
			problems = append(problems, fmt.Sprintf("record %s/%s is labelled as a %q record", rec.Namespace, rec.Name, rec.Labels[v1alpha1.LabelRecord]))
		}
	}
	if len(problems) > 0 {
		return nil, nil, problems
	}
	full := id
	if len(id) == v1alpha1.DisplayIDLength {
		ids := make([]string, 0, len(groups))
		for k := range groups {
			ids = append(ids, k)
		}
		slices.Sort(ids)
		switch len(ids) {
		case 0:
			return nil, nil, nil
		case 1:
			full = ids[0]
		default:
			return nil, ids, nil
		}
	}
	g := groups[full]
	if g == nil {
		return nil, nil, nil
	}
	slices.SortFunc(g.refs, func(a, b recordRef) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
	first := g.data[g.refs[0]]
	ans := &deploymentAnswer{ID: full, DisplayID: display, Scheme: first.Scheme, Canonical: first.text,
		IdentityRecords: g.refs, Verifications: []verificationEntry{}}
	for _, v := range verifications {
		if v.deploymentID == full {
			ans.Verifications = append(ans.Verifications, v.entry)
		}
	}
	slices.SortFunc(ans.Verifications, func(a, b verificationEntry) int {
		return cmp.Or(strings.Compare(a.Namespace, b.Namespace), strings.Compare(a.InstanceUID, b.InstanceUID),
			strings.Compare(a.Worker, b.Worker), cmp.Compare(a.Generation, b.Generation))
	})
	return ans, nil, nil
}

// readIdentity reads an identity record: named id-<display>, holding a full ID that starts with display and
// a canonical object, which it writes as RFC 8785 text.
func readIdentity(rec *appsv1.ControllerRevision, display string) (storedIdentity, error) {
	var d storedIdentity
	where := rec.Namespace + "/" + rec.Name
	if rec.Name != v1alpha1.IdentityRecordPrefix+display {
		return d, fmt.Errorf("identity record %s is not named %s%s", where, v1alpha1.IdentityRecordPrefix, display)
	}
	if err := json.Unmarshal(rec.Data.Raw, &d); err != nil {
		return d, fmt.Errorf("identity record %s: data does not read: %v", where, err)
	}
	if _, err := v1alpha1.IdentityRecordName(d.SHA256); err != nil || len(d.SHA256) != v1alpha1.FullIDLength || d.SHA256[:v1alpha1.DisplayIDLength] != display {
		return d, fmt.Errorf("identity record %s holds %q, not a full deployment ID of display form %s", where, d.SHA256, display)
	}
	canon, err := deploymentid.Canonicalize(d.Canonical)
	if err != nil {
		return d, fmt.Errorf("identity record %s: its canonical object has no RFC 8785 text: %v", where, err)
	}
	if d.Scheme == "" || canon[0] != '{' {
		return d, fmt.Errorf("identity record %s holds no scheme or no canonical object", where)
	}
	d.text = string(canon)
	return d, nil
}

// readVerification reads a verification record labelled with display, of whichever full ID its data names,
// which must be one of that display form. Its Instance UID is its label, which its name must agree with, and
// its generation its revision, which its name holds too.
func readVerification(rec *appsv1.ControllerRevision, display string) (foundVerification, error) {
	where := rec.Namespace + "/" + rec.Name
	var d storedVerification
	if err := json.Unmarshal(rec.Data.Raw, &d); err != nil {
		return foundVerification{}, fmt.Errorf("verification record %s: data does not read: %v", where, err)
	}
	if _, err := v1alpha1.IdentityRecordName(d.DeploymentID); err != nil || len(d.DeploymentID) != v1alpha1.FullIDLength || d.DeploymentID[:v1alpha1.DisplayIDLength] != display {
		return foundVerification{}, fmt.Errorf("verification record %s names %q, not a full deployment ID of display form %s", where, d.DeploymentID, display)
	}
	uid, worker, gen, err := parseVerificationName(rec.Name)
	if err != nil {
		return foundVerification{}, err
	}
	switch label := rec.Labels[v1alpha1.LabelInstanceUID]; {
	case label != uid:
		return foundVerification{}, fmt.Errorf("verification record %s is labelled with Instance UID %q, which its name does not hold", where, label)
	case gen != rec.Revision:
		return foundVerification{}, fmt.Errorf("verification record %s has revision %d, and its name generation %d", where, rec.Revision, gen)
	case d.Verdict != string(v1alpha1.VerdictPass) && d.Verdict != string(v1alpha1.VerdictFail):
		return foundVerification{}, fmt.Errorf("verification record %s holds verdict %q, neither pass nor fail", where, d.Verdict)
	}
	return foundVerification{deploymentID: d.DeploymentID, entry: verificationEntry{Namespace: rec.Namespace, Name: rec.Name,
		InstanceUID: uid, Worker: worker, Generation: gen, Outcome: d.Verdict, Record: json.RawMessage(rec.Data.Raw)}}, nil
}

// parseVerificationName reads an Instance's record name, ver-<uid>-<generation>, or a worker's,
// ver-<uid>-<worker>-<generation>.
func parseVerificationName(name string) (uid string, worker string, gen int64, err error) {
	if u, g, err := v1alpha1.ParseVerificationRecordName(name); err == nil {
		return string(u), "", g, nil
	}
	u, w, g, werr := v1alpha1.ParseWorkerVerificationRecordName(name)
	if werr != nil {
		return "", "", 0, fmt.Errorf("verification record %s: %v", name, werr)
	}
	return string(u), w, g, nil
}
