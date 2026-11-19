package front

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
)

// chooseSticky is a request's first choice when it names a Workload with sessionAffinity Route (HBEP-5, Workloads, Sticky routes). The conversation is its
// session key, which the request carries in a session header (the proxy has checked that, in plan).
//
// A conversation that holds a live pin goes only to the pinned target: its requests wait in the Workload's FIFO, retried only against that target, and are
// refused when they wait too long, never sent anywhere else. A conversation's first request is decided once, under the key's lock: it takes a target by the
// Workload's route order, as any request of a Workload does, and the pin of that target is appended to the journal and synced BEFORE the request is
// dispatched. A request whose pin can't be written is not dispatched. From the pin on, every attempt of the request routes by it (Resolution.pin).
//
// It never holds Router.mu or a Workload's mutex across a journal write: the target is chosen (and its reservation taken) first, then the pin is written
// with no lock of the router held, and the reservation is given back if the write fails.
func (r *Router) chooseSticky(ctx context.Context, n naming.Name, res *Resolution, key string, exclude []string, withManaged, noServing bool) (*target, *routeError) {
	st := r.pins
	hash := keyHash(key)
	// The lock is taken by the request's own context, bounded by the queue deadline from its arrival, so a second request of a key whose first is waiting for a
	// target gives up as that one would.
	arrival := r.arrivalNow()
	lctx, cancel := context.WithDeadline(ctx, arrival.Add(r.cfg.QueueTimeout.D()))
	defer cancel()
	unlock, err := st.lockKey(lctx, res.uid, hash)
	if err != nil {
		return nil, waitError(ctx, nil, "the conversation's first request, which is choosing a target", "the conversation's first request is still choosing a target").forWorkload(res.Name)
	}
	pn, err := st.arrive(res.uid, hash)
	if err != nil {
		unlock()
		r.log.Warn("a sticky request is refused: the pin journal can't be read or written", "workload", res.Name, "error", err.Error())
		return nil, errPinsUnavailable(res.Name)
	}
	if pn != nil {
		unlock()
		ref := &pinRef{s: st, w: res.uid, hash: hash, p: pn}
		at := pn.target // the Resolution holds its own copy: the pin never changes, and a wake pass reads it
		res.pin = &at
		tg, rerr := r.chooseNamed(ctx, n, res, key, exclude, withManaged, arrival)
		if rerr != nil {
			ref.done()
			return nil, rerr
		}
		tg.pin = ref
		return tg, nil
	}
	defer unlock()
	if noServing {
		return nil, errNotAttached(n.Model).forWorkload(res.Name)
	}
	if st.full(res.uid, res.maxPins) {
		return nil, errPinsFull(res.Name)
	}
	tg, rerr := r.chooseNamed(ctx, n, res, key, exclude, withManaged, arrival)
	if rerr != nil {
		return nil, rerr
	}
	at := pinTargetOf(tg)
	pn, err = st.create(res.uid, hash, at, res.pinTTL, res.maxPins)
	if err != nil {
		tg.release()
		if errors.Is(err, errMaxPins) {
			return nil, errPinsFull(res.Name)
		}
		r.log.Warn("a sticky request is refused: its pin could not be journalled", "workload", res.Name, "error", err.Error())
		return nil, errPinsUnavailable(res.Name)
	}
	res.pin = &at
	tg.pin = &pinRef{s: st, w: res.uid, hash: hash, p: pn}
	return tg, nil
}

// pinTargetOf is the pin a request just placed on tg takes: the ModelDeployment by UID with the full deployment ID of the Instance that took the request,
// or the managed target by Provider and managed ID.
func pinTargetOf(tg *target) pinTarget {
	if tg.managed != nil {
		return pinTarget{MD: tg.managed.UID, Provider: tg.managed.Provider, ManagedID: tg.managed.ManagedID}
	}
	return pinTarget{MD: tg.dep.UID, ID: tg.inst.DeploymentID}
}

// newPins opens the router's pin store over the pin journal in the usage directory. A journal that does not exist is made at the first request of a sticky
// Workload (pinStore.ensure), so a front with none never makes one.
func newPins(r *Router) *pinStore {
	dir := r.cfg.UsageDir
	path := ""
	if dir != "" {
		path = filepath.Join(dir, pinJournalFile)
	}
	st := newPinStore(path, r.clk.Now, r.log)
	// A journal that exists is read at start: a pin it shows in flight is converted at the time of the restart, and the journal is compacted (HBEP-5, Sticky
	// routes, The journal). One that can't be read now is read again at the first request of a sticky Workload, which is refused until it can be.
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			if err := st.ensure(); err != nil {
				st.log.Warn("the pin journal could not be read at start", "error", err.Error())
			}
		}
	}
	return st
}

// closePins closes the pin journal, at the front's stop.
func (r *Router) closePins() { r.pins.close() }
