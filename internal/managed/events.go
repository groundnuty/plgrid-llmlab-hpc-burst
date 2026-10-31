package managed

import (
	"fmt"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The controller's events follow HBEP-8 (Events: Form and Repeating
// conditions; @hpc-burst-science-agent, 31 Oct 2026). Each has the CamelCase
// reason code alone as its reason, HBEP-8's type in its action, the Warning
// event type for degraded and failing and Normal for ready, and a note cut to
// provider.MaxEventNote bytes. The types:
//
//   - failing: FileRefused and NoCredentialsFile (CredentialsInvalid),
//     KeyRejected (CredentialsRejected), Expired (CredentialsExpiring) and
//     ReadsFailing (Ready), each when its condition turns bad or its reason
//     changes;
//   - degraded: ExpiresWithinADay (CredentialsExpiring) the same way, and
//     CatalogEntrySkipped and CatalogTruncated, which are no condition and
//     repeat: they fire when they start, then at most once an hour while they
//     last (provider.RepeatGate), never on every read;
//   - ready: when a condition that had raised failing or degraded clears, with
//     the reason the controller wrote as it cleared (FileRead, KeyAccepted,
//     NotExpiring, CatalogRead). The first read of a healthy Provider raises
//     none. The end of a truncation or of a skipped entry raises none either,
//     since HBEP-8 names no reason for it; the gate forgets the condition, so
//     its return fires at once.

// badAction is the HBEP-8 type of each reason that is a bad state of a
// condition the controller writes.
var badAction = map[string]string{
	v1alpha1.ReasonFileRefused:       provider.EventActionFailing,
	v1alpha1.ReasonNoCredentialsFile: provider.EventActionFailing,
	v1alpha1.ReasonKeyRejected:       provider.EventActionFailing,
	v1alpha1.ReasonExpired:           provider.EventActionFailing,
	v1alpha1.ReasonReadsFailing:      provider.EventActionFailing,
	v1alpha1.ReasonExpiresWithinADay: provider.EventActionDegraded,
}

// watched lists the conditions that raise events, with the status that is bad.
var watched = []struct {
	typ string
	bad metav1.ConditionStatus
}{
	{v1alpha1.ConditionCredentialsInvalid, metav1.ConditionTrue},
	{v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue},
	{v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue},
	{v1alpha1.ConditionProviderReady, metav1.ConditionFalse},
}

// emitTransitions records an event for each watched condition that turned bad
// in this write (or changed its bad reason), and a ready event for each that
// left its bad state. The notes are the condition messages, which hold no key
// and no byte the gateway sent.
func (c *Controller) emitTransitions(old, prov *v1alpha1.Provider) {
	if c.recorder == nil {
		return
	}
	for _, w := range watched {
		now := apimeta.FindStatusCondition(prov.Status.Conditions, w.typ)
		if now == nil {
			continue
		}
		was := apimeta.FindStatusCondition(old.Status.Conditions, w.typ)
		switch {
		case now.Status == w.bad:
			if was != nil && was.Status == now.Status && was.Reason == now.Reason {
				continue
			}
			action, ok := badAction[now.Reason]
			if !ok {
				c.log.Info("no HBEP-8 type for a bad condition reason; no event", "condition", w.typ, "reason", now.Reason)
				continue
			}
			c.record(prov, provider.NewManagedEvent(action, now.Reason, fmt.Sprintf("%s: %s", w.typ, now.Message)))
		case was != nil && was.Status == w.bad:
			note := w.typ + " recovered"
			if now.Message != "" {
				note += ": " + now.Message
			}
			c.record(prov, provider.NewManagedEvent(provider.EventActionReady, now.Reason, note))
		}
	}
}

// emitRead records the events of a successful read, which was at now: the
// CatalogEntrySkipped and CatalogTruncated events that are due
// (provider.RepeatGate). Call it only for a read that succeeded; a pass that
// did not read says nothing about the listing, and leaves the gate alone.
func (c *Controller) emitRead(prov *v1alpha1.Provider, now time.Time, merge provider.CatalogMerge) {
	if c.recorder == nil {
		return
	}
	for _, ev := range c.gate.CatalogEvents(now, merge) {
		c.record(prov, ev)
	}
}

func (c *Controller) record(prov *v1alpha1.Provider, ev provider.ManagedEvent) {
	c.recorder.Eventf(prov, nil, ev.Type, ev.Reason, ev.Action, "%s", ev.Note)
}
