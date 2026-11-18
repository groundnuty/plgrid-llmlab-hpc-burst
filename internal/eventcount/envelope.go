package eventcount

// The CloudEvent the durable log stores for a typed event (HBEP-8, The durable log, Streams, The
// envelope; #625, PR E2). It is built from the arguments of the same Eventf call that hands the event
// to the Kubernetes recorder, so the two copies agree on their fields.
//
// Origin: the envelope's construction follows spike S8's evlayer (spikes/s8/evlayer/envelope.go,
// this repository, PR #28): sdk-go v2's event type, never hand-rolled, rendered as structured-mode
// JSON, with the event id doubling as Nats-Msg-Id. S8's subject was hb.ev.<kind>.<name> and its source
// a test constant; HBEP-8 makes the subject hb.ev.<kind>.<uid> and the source /hpcburst/<controller>.
// S8 is its own module, so what is reused is copied.

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// The envelope's constant attributes (HBEP-8, The durable log, Streams).
const (
	// SubjectPrefix roots every event's subject: hb.ev.<kind lower-case>.<regarding UID>.
	SubjectPrefix = "hb.ev."
	// EnvelopeType is the CloudEvents type of every event of the log.
	EnvelopeType = "io.hpcburst.event.v1"
	// SourcePrefix begins the source: /hpcburst/<controller>, with the raised-events counter's
	// controller value.
	SourcePrefix = "/hpcburst/"
)

// ObjectRef is one object an event names, in the envelope's data: the regarding object and the
// related one, each with its UID, which is how a reader matches the log with the API's Events.
type ObjectRef struct {
	APIVersion string    `json:"apiVersion,omitempty"`
	Kind       string    `json:"kind"`
	Namespace  string    `json:"namespace,omitempty"`
	Name       string    `json:"name"`
	UID        types.UID `json:"uid,omitempty"`
}

// Data is the envelope's data: the Kubernetes Event's fields (HBEP-8, The durable log, Streams, The
// envelope). Type is the Event's type, Normal or Warning, not the CloudEvents type.
type Data struct {
	Action              string     `json:"action"`
	Reason              string     `json:"reason"`
	Type                string     `json:"type"`
	Regarding           ObjectRef  `json:"regarding"`
	Related             *ObjectRef `json:"related,omitempty"`
	Note                string     `json:"note"`
	ReportingController string     `json:"reportingController"`
}

// call is what an Eventf call hands over, before it has a time.
type call struct {
	id                  string
	controller          string
	reportingController string
	eventtype           string
	reason, action      string
	note                string
	regarding, related  runtime.Object
}

// refOf names an object for the envelope, with the kind the scheme gives its Go type.
func refOf(obj runtime.Object, scheme *runtime.Scheme) (ObjectRef, error) {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return ObjectRef{}, fmt.Errorf("the object's kind is not in the scheme: %w", err)
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ObjectRef{}, fmt.Errorf("the object has no metadata: %w", err)
	}
	return ObjectRef{APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind, Namespace: acc.GetNamespace(), Name: acc.GetName(), UID: acc.GetUID()}, nil
}

// validToken reports whether s can be one token of a NATS subject: it is not empty and holds no
// separator, wildcard or white space. A Kubernetes kind and a UID always can; anything else is
// refused up front rather than reaching the broker.
func validToken(s string) bool {
	return s != "" && !strings.ContainsAny(s, ".*> \t\r\n/")
}

// subjectOf is hb.ev.<kind lower-case>.<UID>. A regarding object with no UID has none: its events
// are not logged (HBEP-8, The durable log, Streams).
func subjectOf(r ObjectRef) (string, error) {
	kind := strings.ToLower(r.Kind)
	if r.UID == "" {
		return "", fmt.Errorf("%s %q has no UID, so it has no subject", r.Kind, nameOf(r))
	}
	if !validToken(kind) || !validToken(string(r.UID)) {
		return "", fmt.Errorf("%s %q has a kind or UID that cannot be a subject token", r.Kind, nameOf(r))
	}
	return SubjectPrefix + kind + "." + string(r.UID), nil
}

// nameOf is the object's name as a log line names it: namespace/name, or name for a cluster-scoped
// object. It never holds a job ID: an event's regarding object is a resource, not a job.
func nameOf(r ObjectRef) string {
	if r.Namespace == "" {
		return r.Name
	}
	return r.Namespace + "/" + r.Name
}

// cloudSubject is the CloudEvents subject attribute: <kind>/<namespace>/<name>, or <kind>/<name>
// for a cluster-scoped object.
func cloudSubject(r ObjectRef) string { return r.Kind + "/" + nameOf(r) }

// envelope builds the CloudEvent of a call at time at, and returns its structured-mode JSON and the
// NATS subject it is published on.
func (c call) envelope(scheme *runtime.Scheme, at time.Time) (body []byte, subject string, err error) {
	reg, err := refOf(c.regarding, scheme)
	if err != nil {
		return nil, "", fmt.Errorf("regarding: %w", err)
	}
	if subject, err = subjectOf(reg); err != nil {
		return nil, "", err
	}
	d := Data{Action: c.action, Reason: c.reason, Type: c.eventtype, Regarding: reg, Note: c.note, ReportingController: c.reportingController}
	if c.related != nil {
		rel, err := refOf(c.related, scheme)
		if err != nil {
			return nil, "", fmt.Errorf("related: %w", err)
		}
		d.Related = &rel
	}
	e := cloudevents.NewEvent()
	e.SetID(c.id)
	e.SetSource(SourcePrefix + c.controller)
	e.SetType(EnvelopeType)
	e.SetSubject(cloudSubject(reg))
	e.SetTime(at.UTC())
	if err := e.SetData(cloudevents.ApplicationJSON, d); err != nil {
		return nil, "", err
	}
	if err := e.Validate(); err != nil {
		return nil, "", err
	}
	if body, err = encode(e); err != nil {
		return nil, "", err
	}
	return body, subject, nil
}

// encode renders the structured-mode JSON of an event.
func encode(e event.Event) ([]byte, error) { return json.Marshal(e) }

// Decode parses and validates a structured-mode JSON event and its data: what a reader of the log
// does with a stored message. It is exported for the tests of the programs that read the log.
func Decode(b []byte) (event.Event, Data, error) {
	var e event.Event
	if err := json.Unmarshal(b, &e); err != nil {
		return event.Event{}, Data{}, err
	}
	if err := e.Validate(); err != nil {
		return event.Event{}, Data{}, err
	}
	var d Data
	if err := json.Unmarshal(e.Data(), &d); err != nil {
		return event.Event{}, Data{}, fmt.Errorf("the envelope's data: %w", err)
	}
	return e, d, nil
}
