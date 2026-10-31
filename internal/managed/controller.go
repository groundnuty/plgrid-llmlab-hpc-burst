package managed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// CatalogInterval is the least time between two catalog reads of one Provider
// (HBEP-4, Managed provider in alpha: "at most once a minute"; Operational
// readiness 6).
const CatalogInterval = time.Minute

// Reasons of the conditions the controller writes. Those of Ready,
// CredentialsRejected and CredentialsExpiring are the ones the stand-in,
// pkg/provider/fake.ManagedController, writes, so the lifecycle controller's
// tests against it hold for this controller too.
const (
	ReasonCatalogRead       = "CatalogRead"
	ReasonKeyAccepted       = "KeyAccepted"
	ReasonKeyRejected       = "KeyRejected"
	ReasonNotExpiring       = "NotExpiring"
	ReasonExpiresWithinADay = "ExpiresWithinADay"
	ReasonExpired           = "Expired"
	// ReasonFileRead: CredentialsInvalid is False, the credentials file was
	// read and accepted.
	ReasonFileRead = "FileRead"
	// ReasonFileRefused: CredentialsInvalid is True, provider.
	// ReadManagedCredentials refused the file (mode, owner, form, or missing).
	ReasonFileRefused = "FileRefused"
	// ReasonNoCredentialsFile: CredentialsInvalid is True, the Provider names
	// no credentials file (none, or a Secret, which alpha does not read).
	ReasonNoCredentialsFile = "NoCredentialsFile"
)

// Limits of status.catalog (api/v1alpha1, CatalogEntryStatus and
// ProviderStatus.Catalog).
const (
	maxCatalogEntries  = 1024
	maxCatalogNameLen  = 253
	maxCatalogRevision = 256
)

// Clock is the controller's time source. A clock that also has
// After(time.Duration) <-chan time.Time, such as k8s.io/utils/clock's, paces
// Start; otherwise Start waits in real time.
type Clock interface{ Now() time.Time }

type afterClock interface {
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Options configure a Controller.
type Options struct {
	// Client writes the Provider's status. Required.
	Client client.Client
	// Reader reads the Provider; nil reads with Client. A manager passes its
	// API reader, so a read once a minute needs no informer.
	Reader client.Reader
	// Provider is the managed Provider's name. Required.
	Provider string
	// Clock is the time source; nil is the real clock.
	Clock Clock
	// Transport carries the catalog reads; nil is a copy of
	// http.DefaultTransport with ConnectTimeout, shared by every pass.
	Transport http.RoundTripper
	// Recorder, when set, gets an event on the Provider when
	// CredentialsRejected, CredentialsExpiring or CredentialsInvalid turns
	// True.
	Recorder events.EventRecorder
	// Log is the controller's logger; the zero Logger discards.
	Log logr.Logger
}

// Controller is one managed Provider's controller: the read-only catalog loop
// of HBEP-4 (Managed provider in alpha). It takes no lifecycle action: it
// reads the gateway's listing and writes the Provider's status, nothing else.
//
// It is a manager.Runnable that needs leader election, so it runs only in the
// manager that holds the Provider's Lease (docs/development.md, Process
// layout): the per-Provider manager supervisor, or the controller binary's
// main, builds one with New and adds it with SetupWithManager.
//
// Each pass (Sync) behaves as pkg/provider/fake.ManagedController's Sync,
// which the lifecycle controller's tests use, with the catalog read over HTTP
// and the key's expiry from the credentials file:
//
//   - It reads at most once per CatalogInterval. A pass sooner than that after
//     the last read, in this process or (by the stored lastReconcileTime and
//     lastCatalogRead) in an earlier one, reads nothing and says how long to
//     wait.
//   - It reads the Provider's spec.credentialsRef.file with
//     provider.ReadManagedCredentials at every pass, so the key is never kept
//     between passes and reaches no log, event, status or command line. A
//     file it refuses, or a Provider that names none, sets CredentialsInvalid
//     True and reads no catalog; a file it accepts sets it False.
//   - A successful read puts one entry in status.catalog for every name ever
//     listed, sorted by name: listed true and lastListedTime now for the ones
//     the read listed, the others kept with listed false. The managed ID is
//     pkg/deploymentid's over the Provider's platform label, the name and the
//     revision (absent when empty). lastCatalogRead is now, Ready is True with
//     CatalogRead, and CredentialsRejected False with KeyAccepted.
//   - A 401 or 403 sets CredentialsRejected True with KeyRejected and leaves
//     the catalog, lastCatalogRead and Ready as they were.
//   - Any other failure is unknown: the pass writes nothing.
//   - CredentialsExpiring follows the file's expiry on every pass that writes:
//     True with ExpiresWithinADay from a day before it (provider.
//     ManagedCredentials.Expiring), Expired from it on, else False with
//     NotExpiring; with no expiry known it is left as it is.
//   - A Provider without the platform label, or a listing the status cannot
//     hold (an empty name, a name above 253 characters, a revision above 256,
//     more than 1024 names ever), is an error that writes nothing.
//   - Every write is a status Update under v1alpha1.FieldManagerProvider with
//     the read's resourceVersion; on a conflict it re-reads the Provider and
//     applies the same read again, so a condition another writer owns, such
//     as DialHomeUnreachable, is kept.
type Controller struct {
	client    client.Client
	reader    client.Reader
	provider  string
	clock     Clock
	transport http.RoundTripper
	recorder  events.EventRecorder
	log       logr.Logger

	mu          sync.Mutex
	lastAttempt time.Time
}

var (
	_ manager.Runnable               = (*Controller)(nil)
	_ manager.LeaderElectionRunnable = (*Controller)(nil)
)

// New returns the controller of one managed Provider.
func New(opts Options) (*Controller, error) {
	if opts.Client == nil {
		return nil, errors.New("managed controller: no client")
	}
	if opts.Provider == "" {
		return nil, errors.New("managed controller: no Provider name")
	}
	c := &Controller{
		client: opts.Client, reader: opts.Reader, provider: opts.Provider, clock: opts.Clock,
		transport: opts.Transport, recorder: opts.Recorder, log: opts.Log,
	}
	if c.reader == nil {
		c.reader = opts.Client
	}
	if c.clock == nil {
		c.clock = realClock{}
	}
	if c.transport == nil {
		// One transport for every pass, so a pass reuses the last one's
		// connection; each pass still reads the key afresh.
		c.transport = newTransport()
	}
	return c, nil
}

// SetupWithManager adds the controller to mgr, the manager of its Provider.
func (c *Controller) SetupWithManager(mgr manager.Manager) error {
	return mgr.Add(c)
}

// NeedLeaderElection implements manager.LeaderElectionRunnable: only the
// holder of the Provider's Lease reads the catalog.
func (c *Controller) NeedLeaderElection() bool { return true }

// Start implements manager.Runnable: it runs a pass, waits as long as the
// pass says, and repeats until ctx ends. A pass that fails is logged, and the
// next comes after CatalogInterval.
func (c *Controller) Start(ctx context.Context) error {
	log := c.log.WithValues("provider", c.provider)
	for {
		wait, err := c.Sync(ctx)
		if err != nil {
			log.Error(err, "managed catalog pass failed")
		}
		if wait <= 0 {
			wait = CatalogInterval
		}
		select {
		case <-ctx.Done():
			return nil
		case <-c.after(wait):
		}
	}
}

func (c *Controller) after(d time.Duration) <-chan time.Time {
	if a, ok := c.clock.(afterClock); ok {
		return a.After(d)
	}
	return time.After(d)
}

// outcome is what one pass found, applied to the Provider by apply.
type outcome struct {
	now      time.Time // whole seconds, UTC
	credsErr error     // the file was refused, or none is named
	noFile   bool      // credsErr is because no file is named
	creds    provider.ManagedCredentials
	entries  []provider.CatalogEntry // a successful read
	rejected error                   // a read that wraps provider.ErrKeyRejected
}

// Sync runs one pass, unless the last read was less than CatalogInterval
// ago. It returns how long to wait before the next pass.
func (c *Controller) Sync(ctx context.Context) (time.Duration, error) {
	log := c.log.WithValues("provider", c.provider)
	var prov v1alpha1.Provider
	if err := c.reader.Get(ctx, client.ObjectKey{Name: c.provider}, &prov); err != nil {
		return CatalogInterval, fmt.Errorf("get Provider %s: %w", c.provider, err)
	}
	exact := c.clock.Now()
	if next := c.nextRead(&prov); exact.Before(next) {
		return next.Sub(exact), nil
	}
	if prov.Labels[v1alpha1.LabelPlatform] == "" {
		return CatalogInterval, fmt.Errorf("no %s label on Provider %s: the managed IDs need the platform", v1alpha1.LabelPlatform, c.provider)
	}
	out := outcome{now: exact.UTC().Truncate(time.Second)}
	switch ref := prov.Spec.CredentialsRef; {
	case ref == nil || ref.File == "":
		out.credsErr, out.noFile = fmt.Errorf("%w: Provider %s names no credentials file; a managed Provider reads a mode-600 file (spec.credentialsRef.file)", provider.ErrCredentials, c.provider), true
	default:
		out.creds, out.credsErr = provider.ReadManagedCredentials(ref.File)
	}
	if out.credsErr == nil {
		mc, err := NewClient(out.creds, c.transport)
		if err != nil {
			out.credsErr = err
		} else {
			c.mu.Lock()
			c.lastAttempt = exact
			c.mu.Unlock()
			entries, err := mc.ListCatalog(ctx)
			switch {
			case err == nil:
				out.entries = entries
			case errors.Is(err, provider.ErrKeyRejected):
				out.rejected = err
			default:
				log.Info("catalog read failed; the status is left as it was", "error", err.Error())
				return CatalogInterval, nil
			}
		}
	}
	if err := c.write(ctx, &prov, out); err != nil {
		return CatalogInterval, err
	}
	return CatalogInterval, nil
}

// nextRead is the earliest time the next catalog read may start: a
// CatalogInterval after the last read this process started, or, before this
// process has read, after the stored lastCatalogRead and lastReconcileTime,
// which an earlier process may have written. Stored times are whole seconds,
// so a second is added to them.
func (c *Controller) nextRead(prov *v1alpha1.Provider) time.Time {
	c.mu.Lock()
	last := c.lastAttempt
	c.mu.Unlock()
	if !last.IsZero() {
		return last.Add(CatalogInterval)
	}
	for _, t := range []*metav1.Time{prov.Status.LastCatalogRead, prov.Status.LastReconcileTime} {
		if t != nil && t.Add(time.Second).After(last) {
			last = t.Add(time.Second)
		}
	}
	if last.IsZero() {
		return time.Time{}
	}
	return last.Add(CatalogInterval)
}

// write applies out to the Provider's status and writes it when it changed.
// On a conflict it re-reads the Provider and applies out again.
func (c *Controller) write(ctx context.Context, prov *v1alpha1.Provider, out outcome) error {
	first := true
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if !first {
			if err := c.reader.Get(ctx, client.ObjectKey{Name: c.provider}, prov); err != nil {
				return fmt.Errorf("get Provider %s: %w", c.provider, err)
			}
		}
		first = false
		old := prov.DeepCopy()
		if err := apply(prov, out); err != nil {
			return err
		}
		if apiequality.Semantic.DeepEqual(old.Status, prov.Status) {
			return nil
		}
		if err := c.client.Status().Update(ctx, prov, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
			return err
		}
		c.emit(old, prov)
		return nil
	})
}

// apply sets the Provider's status from one pass. It changes nothing when it
// returns an error.
func apply(prov *v1alpha1.Provider, out outcome) error {
	platform := prov.Labels[v1alpha1.LabelPlatform]
	if platform == "" {
		return fmt.Errorf("no %s label on Provider %s: the managed IDs need the platform", v1alpha1.LabelPlatform, prov.Name)
	}
	st := prov.Status.DeepCopy()
	cond := func(typ string, status metav1.ConditionStatus, reason, message string) {
		apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type: typ, Status: status, Reason: reason, Message: message,
			ObservedGeneration: prov.Generation, LastTransitionTime: metav1.NewTime(out.now),
		})
	}
	if out.credsErr != nil {
		reason := ReasonFileRefused
		if out.noFile {
			reason = ReasonNoCredentialsFile
		}
		cond(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionTrue, reason, out.credsErr.Error())
	} else {
		cond(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionFalse, ReasonFileRead, "")
		switch {
		case out.rejected != nil:
			cond(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, ReasonKeyRejected, out.rejected.Error())
		default:
			catalog, err := mergeCatalog(st.Catalog, out.entries, platform, out.now)
			if err != nil {
				return err
			}
			st.Catalog, st.LastCatalogRead = catalog, ptrTime(out.now)
			cond(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, ReasonCatalogRead, "")
			cond(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, ReasonKeyAccepted, "")
		}
		if exp := out.creds.Expiry; exp != nil {
			at := exp.UTC().Format(time.RFC3339)
			switch {
			case !out.now.Before(*exp):
				cond(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, ReasonExpired, "the key expired at "+at)
			case out.creds.Expiring(out.now):
				cond(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionTrue, ReasonExpiresWithinADay, "the key expires at "+at)
			default:
				cond(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, ReasonNotExpiring, "the key expires at "+at)
			}
		}
	}
	st.ObservedGeneration, st.LastReconcileTime = prov.Generation, ptrTime(out.now)
	prov.Status = *st
	return nil
}

func ptrTime(t time.Time) *metav1.Time { m := metav1.NewTime(t); return &m }

// emit records an event for each credentials condition that turned True in
// this write. The notes are the condition messages, which hold no key and no
// response body.
func (c *Controller) emit(old, prov *v1alpha1.Provider) {
	if c.recorder == nil {
		return
	}
	for _, typ := range []string{v1alpha1.ConditionCredentialsInvalid, v1alpha1.ConditionCredentialsRejected, v1alpha1.ConditionCredentialsExpiring} {
		now := apimeta.FindStatusCondition(prov.Status.Conditions, typ)
		if now == nil || now.Status != metav1.ConditionTrue {
			continue
		}
		if was := apimeta.FindStatusCondition(old.Status.Conditions, typ); was != nil && was.Status == metav1.ConditionTrue && was.Reason == now.Reason {
			continue
		}
		c.recorder.Eventf(prov, nil, corev1.EventTypeWarning, now.Reason, "ReadCatalog", "%s: %s", typ, now.Message)
	}
}
