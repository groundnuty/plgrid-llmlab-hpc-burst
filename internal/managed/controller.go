package managed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-logr/logr"
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
	// Transport carries the catalog reads; nil is newTransport's, which
	// never replays a request. A transport passed here must not replay one
	// either, or one pass could read twice.
	Transport http.RoundTripper
	// Recorder, when set, gets the typed events of HBEP-8 on the Provider:
	// failing or degraded when CredentialsRejected, CredentialsExpiring or
	// CredentialsInvalid turns True or Ready turns False, ready when one of
	// them clears, and degraded CatalogEntrySkipped and CatalogTruncated from
	// the successful reads (see events.go).
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
// layout). The per-Provider manager supervisor, or the controller binary's
// main, adds it with Setup.
//
// Each pass (Sync) applies the same rules as pkg/provider/fake.
// ManagedController, which the lifecycle controller's tests use: the bounded
// catalog merge (provider.MergeCatalog), the readiness staleness rule
// (provider.SetManagedReadiness) and CredentialsExpiring
// (provider.ExpiringCondition), all three per @hpc-burst-science-agent's
// ruling of 31 Oct 2026. It reads the catalog over HTTP and the key's expiry
// from the credentials file:
//
//   - The Provider's PlatformProfile must exist and be managed, and the
//     Provider must carry the platform label; otherwise the pass is an error
//     that reads and writes nothing.
//   - It reads the Provider's spec.credentialsRef.file with
//     provider.ReadManagedCredentials at every pass, so the key is never kept
//     between passes and reaches no log, event, status or command line. A
//     file it refuses, or a Provider that names none, sets CredentialsInvalid
//     True and reads no catalog; a file it accepts sets it False.
//   - It reads at most once per CatalogInterval, across restarts too: before
//     each read it stores lastReconcileTime, and a pass sooner than a minute
//     after the last read, by this process's clock or by the stored
//     lastReconcileTime and lastCatalogRead, reads nothing.
//   - A successful read merges the listing into status.catalog
//     (provider.MergeCatalog): listed entries with their managed IDs, unlisted
//     ones kept until room is needed, entries the status cannot hold skipped.
//     lastCatalogRead and lastSuccessfulStatusRead are now, Ready is True with
//     CatalogRead, and CredentialsRejected False with KeyAccepted. Each skipped
//     entry raises a CatalogEntrySkipped event, and a listing above 1024
//     raises CatalogTruncated, each when it starts and then at most once an
//     hour while it lasts (provider.RepeatGate); more than 10 skipped entries
//     in an hour get one summary event for the rest.
//   - A 401 or 403 sets CredentialsRejected True with KeyRejected and leaves
//     the catalog and lastCatalogRead as they were.
//   - Any other failure is unknown: the catalog is left as it was.
//   - Every pass that is not a successful read leaves lastSuccessfulStatusRead
//     alone, and turns Ready False with ReadsFailing once the last successful
//     read is more than v1alpha1.ProviderReadyStaleness old.
//   - CredentialsExpiring follows the file on every pass that read it: True
//     from a day before the expiry, Expired from it on, else False with
//     NotExpiring, also when the file names no expiry.
//   - Every write is a status Update under v1alpha1.FieldManagerProvider with
//     the read's resourceVersion; on a conflict it re-reads the Provider and
//     applies the same read again, so a condition another writer owns, such
//     as DialHomeUnreachable, is kept.
//
// Where it differs from the stand-in, which has no clock gate, no file and no
// HTTP: the reservation write before each read, the profile check, the
// CredentialsInvalid condition, the conflict retry, and the events.
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

	// gate holds when each repeating event last fired (events.go). It lives in
	// memory: a restart, or a new leader, starts every condition afresh.
	gate provider.RepeatGate
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
		c.transport = newTransport()
	}
	return c, nil
}

// SetupWithManager adds the controller to mgr, the manager of its Provider.
func (c *Controller) SetupWithManager(mgr manager.Manager) error {
	return mgr.Add(c)
}

// Setup is the per-Provider manager supervisor's hook for a managed Provider,
// with the signature of internal/providerctl's Supervisor.Setup (#59): it
// adds the Provider's controller to mgr, built from mgr's client, API reader,
// event recorder and logger, with the real clock and transport.
func Setup(mgr manager.Manager, provider string) error {
	c, err := New(Options{
		Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Provider: provider,
		Recorder: mgr.GetEventRecorder(v1alpha1.FieldManagerProvider),
		Log:      mgr.GetLogger().WithName("managed-catalog"),
	})
	if err != nil {
		return err
	}
	return c.SetupWithManager(mgr)
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
	read     bool                    // a GET was made
	entries  []provider.CatalogEntry // a successful read
	rejected error                   // a read that wraps provider.ErrKeyRejected
	failed   bool                    // a read whose outcome is unknown
}

// succeeded says the pass read the catalog: the file was accepted and the
// gateway listed it. Only such a pass says whether a listing is truncated.
func (o outcome) succeeded() bool { return o.credsErr == nil && o.rejected == nil && !o.failed }

// Sync runs one pass, unless the last read was less than CatalogInterval
// ago. It returns how long to wait before the next pass, at most
// CatalogInterval.
func (c *Controller) Sync(ctx context.Context) (time.Duration, error) {
	log := c.log.WithValues("provider", c.provider)
	var prov v1alpha1.Provider
	if err := c.reader.Get(ctx, client.ObjectKey{Name: c.provider}, &prov); err != nil {
		return CatalogInterval, fmt.Errorf("get Provider %s: %w", c.provider, err)
	}
	exact := c.clock.Now()
	if next := c.nextRead(&prov); exact.Before(next) {
		// Capped, so a stored time in the future (a clock stepped back) is
		// looked at again a minute on, not slept out (review of PR #85, nit 10).
		return min(next.Sub(exact), CatalogInterval), nil
	}
	if prov.Labels[v1alpha1.LabelPlatform] == "" {
		return CatalogInterval, fmt.Errorf("no %s label on Provider %s: the managed IDs need the platform", v1alpha1.LabelPlatform, c.provider)
	}
	if err := c.checkProfile(ctx, &prov); err != nil {
		return CatalogInterval, err
	}
	out := outcome{now: exact.UTC().Truncate(time.Second)}
	switch ref := prov.Spec.CredentialsRef; {
	case ref == nil || ref.File == "":
		out.credsErr, out.noFile = fmt.Errorf("%w: Provider %s names no credentials file; a managed Provider reads a mode-600 file (spec.credentialsRef.file)", provider.ErrCredentials, c.provider), true
	default:
		out.creds, out.credsErr = provider.ReadManagedCredentials(ref.File)
	}
	var mc *Client
	if out.credsErr == nil {
		mc, out.credsErr = NewClient(out.creds, c.transport)
	}
	if out.credsErr == nil {
		// The read is reserved before it is made: lastReconcileTime is stored
		// first, so no restart, failed read or lost write lets a later process
		// read again within the minute (review of PR #85, Astra B3, Opus B1).
		// A reservation that cannot be stored makes no read.
		c.mu.Lock()
		c.lastAttempt = exact
		c.mu.Unlock()
		if err := c.reserve(ctx, &prov, out.now); err != nil {
			return CatalogInterval, err
		}
		out.read = true
		entries, err := mc.ListCatalog(ctx)
		switch {
		case err == nil:
			out.entries = entries
		case errors.Is(err, provider.ErrKeyRejected):
			out.rejected = err
		default:
			out.failed = true
			log.Info("catalog read failed; the catalog is left as it was", "error", err.Error())
		}
	}
	if err := c.write(ctx, &prov, out); err != nil {
		return CatalogInterval, err
	}
	return CatalogInterval, nil
}

// checkProfile refuses a Provider whose PlatformProfile is missing or not
// managed, before any read: a batch Provider's credentials file is an ssh
// configuration, never a managed one (review of PR #85, nit 9).
func (c *Controller) checkProfile(ctx context.Context, prov *v1alpha1.Provider) error {
	name := prov.Spec.PlatformProfileRef.Name
	var pp v1alpha1.PlatformProfile
	if err := c.reader.Get(ctx, client.ObjectKey{Name: name}, &pp); err != nil {
		return fmt.Errorf("Provider %s: get PlatformProfile %s: %w", c.provider, name, err)
	}
	if pp.Spec.ProviderKind != v1alpha1.PlatformManaged {
		return fmt.Errorf("Provider %s: PlatformProfile %s has providerKind %q, not %q: not a managed Provider", c.provider, name, pp.Spec.ProviderKind, v1alpha1.PlatformManaged)
	}
	return nil
}

// nextRead is the earliest time the next catalog read may start: a
// CatalogInterval after the last read this process started, or, before this
// process has read, after the stored lastCatalogRead and lastReconcileTime,
// which an earlier process wrote before its last read. Stored times are
// whole seconds, so a second is added to them.
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

// reserve stores lastReconcileTime (and observedGeneration) as now, the
// reservation of the read about to be made. prov is updated to what was
// stored.
func (c *Controller) reserve(ctx context.Context, prov *v1alpha1.Provider, now time.Time) error {
	first := true
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if !first {
			if err := c.reader.Get(ctx, client.ObjectKey{Name: c.provider}, prov); err != nil {
				return fmt.Errorf("get Provider %s: %w", c.provider, err)
			}
		}
		first = false
		prov.Status.ObservedGeneration, prov.Status.LastReconcileTime = prov.Generation, ptrTime(now)
		if err := c.client.Status().Update(ctx, prov, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
			return fmt.Errorf("reserve the catalog read: %w", err)
		}
		return nil
	})
}

// write applies out to the Provider's status and writes it when it changed.
// On a conflict it re-reads the Provider and applies out again. The events
// of a successful read are raised once the write succeeded.
func (c *Controller) write(ctx context.Context, prov *v1alpha1.Provider, out outcome) error {
	first := true
	var merge provider.CatalogMerge
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if !first {
			if err := c.reader.Get(ctx, client.ObjectKey{Name: c.provider}, prov); err != nil {
				return fmt.Errorf("get Provider %s: %w", c.provider, err)
			}
		}
		first = false
		old := prov.DeepCopy()
		var err error
		if merge, err = apply(prov, out); err != nil {
			return err
		}
		if apiequality.Semantic.DeepEqual(old.Status, prov.Status) {
			return nil
		}
		if err := c.client.Status().Update(ctx, prov, client.FieldOwner(v1alpha1.FieldManagerProvider)); err != nil {
			return err
		}
		c.emitTransitions(old, prov)
		return nil
	})
	if err != nil {
		return err
	}
	if out.succeeded() {
		c.emitRead(prov, out.now, merge)
	}
	return nil
}

// apply sets the Provider's status from one pass, and returns the merge of a
// successful read. It changes nothing when it returns an error.
//
//   - A refused credentials file: CredentialsInvalid True; no read was made.
//   - A successful read: provider.MergeCatalog's catalog, lastCatalogRead and
//     lastSuccessfulStatusRead now, Ready True with CatalogRead, and
//     CredentialsRejected False with KeyAccepted.
//   - A rejected key: CredentialsRejected True with KeyRejected.
//   - An unknown outcome: nothing of the catalog changes.
//   - Every pass that is not a successful read applies the batch staleness
//     rule to Ready (provider.SetManagedReadiness); every pass that read the
//     file sets CredentialsExpiring from it (provider.ExpiringCondition).
func apply(prov *v1alpha1.Provider, out outcome) (provider.CatalogMerge, error) {
	platform := prov.Labels[v1alpha1.LabelPlatform]
	if platform == "" {
		return provider.CatalogMerge{}, fmt.Errorf("no %s label on Provider %s: the managed IDs need the platform", v1alpha1.LabelPlatform, prov.Name)
	}
	st := prov.Status.DeepCopy()
	cond := func(typ string, status metav1.ConditionStatus, reason, message string) {
		apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type: typ, Status: status, Reason: reason, Message: message,
			ObservedGeneration: prov.Generation, LastTransitionTime: metav1.NewTime(out.now),
		})
	}
	var merge provider.CatalogMerge
	readOK := false
	if out.credsErr != nil {
		reason := v1alpha1.ReasonFileRefused
		if out.noFile {
			reason = v1alpha1.ReasonNoCredentialsFile
		}
		cond(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionTrue, reason, out.credsErr.Error())
	} else {
		cond(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionFalse, v1alpha1.ReasonFileRead, "")
		switch {
		case out.rejected != nil:
			cond(v1alpha1.ConditionCredentialsRejected, metav1.ConditionTrue, v1alpha1.ReasonKeyRejected, out.rejected.Error())
		case !out.failed:
			var err error
			if merge, err = provider.MergeCatalog(st.Catalog, out.entries, platform, out.now); err != nil {
				return provider.CatalogMerge{}, fmt.Errorf("Provider %s: %w", prov.Name, err)
			}
			st.Catalog, st.LastCatalogRead, readOK = merge.Catalog, ptrTime(out.now), true
			cond(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, v1alpha1.ReasonKeyAccepted, "")
		}
		status, reason, message := provider.ExpiringCondition(out.creds.Expiry, out.now)
		cond(v1alpha1.ConditionCredentialsExpiring, status, reason, message)
	}
	provider.SetManagedReadiness(st, prov.Generation, readOK, out.now)
	st.ObservedGeneration, st.LastReconcileTime = prov.Generation, ptrTime(out.now)
	prov.Status = *st
	return merge, nil
}

func ptrTime(t time.Time) *metav1.Time { m := metav1.NewTime(t); return &m }
