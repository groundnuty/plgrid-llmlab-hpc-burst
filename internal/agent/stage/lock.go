package stage

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"
)

// lockPoll is how often a busy lock is tried again.
const lockPoll = 25 * time.Millisecond

// A lock is an exclusive flock on a file beside what it guards: the image
// lock beside a digest's image, the checkpoint lock in the checkpoint
// directory (PR #87 review, Astra B6). Every download, every move aside and
// every pull or build runs under one, and re-checks what it found before the
// lock was taken, since another agent may have replaced the file meanwhile.
// The lock files are never removed.
//
// Exclusion between Allocations on other nodes depends on the filesystem's
// flock support: on Lustre it needs the "flock" mount option ("localflock"
// excludes only within a node). Where flock is not supported at all, staging
// goes on without exclusion and says so in the log; the re-checks under the
// lock then still stop a stale result from moving a good file aside, but two
// agents can download the same file twice.
type lock struct{ f *os.File }

// release unlocks. A nil lock, as where flock is unsupported, does nothing.
func (l *lock) release() {
	if l != nil && l.f != nil {
		syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
		l.f.Close()
	}
}

// hooks are test seams: points at which a test holds a Stager still.
type hooks struct {
	// lockBusy is called once per lock wait, when the lock is first found held.
	lockBusy func()
	// afterRehash is called after Checkpoint's unlocked rehash.
	afterRehash func()
	// flock replaces syscall.Flock.
	flock func(fd, how int) error
}

// tryLock takes the lock if it is free.
func (s *Stager) tryLock(path string) (*lock, bool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, err
	}
	flock := syscall.Flock
	if s.hooks.flock != nil {
		flock = s.hooks.flock
	}
	for {
		err := flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case err == nil:
			return &lock{f}, true, nil
		case err == syscall.EINTR:
			continue
		case err == syscall.EWOULDBLOCK:
			f.Close()
			return nil, false, nil
		case flockUnsupported(err):
			f.Close()
			s.noFlock.Do(func() {
				s.logf("flock is not supported at %s (%v): staging goes on without exclusion between agents", path, err)
			})
			return nil, true, nil
		default:
			f.Close()
			return nil, false, fmt.Errorf("locking %s: %w", path, err)
		}
	}
}

// lock waits for the lock until ctx is done.
func (s *Stager) lock(ctx context.Context, path string) (*lock, error) {
	announced := false
	for {
		l, ok, err := s.tryLock(path)
		if err != nil || ok {
			return l, err
		}
		if !announced {
			announced = true
			if s.hooks.lockBusy != nil {
				s.hooks.lockBusy()
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lockPoll):
		}
	}
}

func flockUnsupported(err error) bool {
	return err == syscall.ENOSYS || err == syscall.EOPNOTSUPP || err == syscall.ENOLCK
}
