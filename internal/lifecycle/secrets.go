package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const (
	// sweepEvery bounds how often the credentials directory is swept.
	sweepEvery = time.Minute
	// sweepMinAge spares a file younger than this: an Allocation's credential
	// is written right after its create returns, before a list may show it.
	sweepMinAge = 2 * time.Minute
)

// sweepSecrets removes the credential file of every Allocation and the key
// file of every Instance that no longer exists, in any namespace (review of PR
// 88). It reads the API afresh, at most once a minute, and spares young files,
// so a just-created object's file is never taken. A sweep, rather than a
// removal when this process releases an object, also catches what a restart
// or another deployment's pass would have missed.
func (r *Reconciler) sweepSecrets(ctx context.Context) error {
	m := r.memory()
	now := r.Clock.Now()
	m.mu.Lock()
	if !m.lastSweep.IsZero() && now.Sub(m.lastSweep) < sweepEvery {
		m.mu.Unlock()
		return nil
	}
	m.lastSweep = now
	m.mu.Unlock()
	var al v1alpha1.AllocationList
	if err := r.Reader.List(ctx, &al); err != nil {
		return fmt.Errorf("sweep: list Allocations: %w", err)
	}
	var il v1alpha1.InstanceList
	if err := r.Reader.List(ctx, &il); err != nil {
		return fmt.Errorf("sweep: list Instances: %w", err)
	}
	allocs, insts := map[string]bool{}, map[string]bool{}
	for _, a := range al.Items {
		allocs[string(a.UID)] = true
	}
	for _, in := range il.Items {
		insts[string(in.UID)] = true
	}
	dir := r.Config.CredentialsDir
	return errors.Join(
		sweepDir(filepath.Dir(agentapi.AllocationCredentialPath(dir, "x")), allocs, now),
		sweepDir(filepath.Dir(agentapi.InstanceKeyPath(dir, "x")), insts, now),
	)
}

// sweepDir removes the files of dir named by a UID that live does not hold and
// older than sweepMinAge. Temporary files (a leading dot) are left to their
// writer.
func sweepDir(dir string, live map[string]bool, now time.Time) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sweep %s: %w", dir, err)
	}
	var errs []error
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") || live[e.Name()] {
			continue
		}
		info, err := e.Info()
		if err != nil || now.Sub(info.ModTime()) < sweepMinAge {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("sweep %s: %w", e.Name(), err))
		}
	}
	return errors.Join(errs...)
}
