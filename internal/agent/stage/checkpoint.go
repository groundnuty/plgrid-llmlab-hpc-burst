package stage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// lockName is the checkpoint directory's lock file (lock.go).
const lockName = ".hpcburst-stage.lock"

// Checkpoint implements agent.Stager.
//
// Every listed file present is rehashed, without a lock. A file that needs
// work, a missing one to download or a changed one to restage, is then handled
// under the checkpoint's lock, which re-checks it first: another agent staging
// the same checkpoint may have replaced the file since the rehash, and moving
// the replacement aside on a stale result, or downloading a second copy, is
// what the lock and the re-check prevent (PR #87 review, Astra B6).
//
// With RehashOnly nothing is downloaded, moved, locked or removed, and the
// checkpoint directory is not created.
func (s *Stager) Checkpoint(ctx context.Context, req agent.CheckpointRequest) (agent.CheckpointResult, error) {
	var res agent.CheckpointResult
	if !repoForm.MatchString(req.Repository) {
		return res, fmt.Errorf("checkpoint repository %q is not <owner>/<name>", req.Repository)
	}
	if !revForm.MatchString(req.Revision) {
		return res, fmt.Errorf("checkpoint revision %q is not a full commit id", req.Revision)
	}
	dir := filepath.Join(s.c.StagingDir, agentapi.StagedCheckpointDir(req.Repository, req.Revision))
	res.Dir = dir
	for _, f := range req.Files {
		if err := checkFile(f); err != nil {
			return res, err
		}
	}
	if !req.RehashOnly {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return res, err
		}
		s.removeStaleParts(dir)
	}
	skip := skippedFiles(req.Files, req.LoadFormat)

	present := make([]bool, len(req.Files))
	for i, f := range req.Files {
		fi, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(f.Path)))
		present[i] = err == nil && fi.Mode().IsRegular()
	}
	sums, err := s.rehash(ctx, dir, req.Files, present)
	if err != nil {
		return res, err
	}
	if s.hooks.afterRehash != nil {
		s.hooks.afterRehash()
	}
	res.Files = make([]agentapi.FileResult, len(req.Files))
	var repair []int
	for i, f := range req.Files {
		r := agentapi.FileResult{Path: f.Path}
		switch {
		case present[i] && sums[i] == f.SHA256:
			r.Outcome = agentapi.FileMatch
		case present[i] && req.Restage && !req.RehashOnly && !skip[i]:
			repair = append(repair, i) // rung 2: move the bad copy aside and download it again
		case present[i]:
			r.Outcome = agentapi.FileMismatch
			r.SHA256 = sums[i]
		case skip[i] || req.RehashOnly:
			r.Outcome = agentapi.FileAbsent
		default:
			repair = append(repair, i)
		}
		res.Files[i] = r
	}

	t := &tally{fn: req.Progress, every: time.Second}
	for _, i := range repair {
		r, err := s.repair(ctx, dir, req, req.Files[i], t)
		if err != nil {
			return res, fmt.Errorf("staging %s: %w", req.Files[i].Path, err)
		}
		res.Files[i] = r
	}
	t.finish()
	return res, nil
}

// repair brings one file to the identity record under the checkpoint's lock.
// It looks at the file again first: a file another agent has put right is a
// match; a changed one is moved aside, but only on a restage, and the copy
// moved aside is removed once the new file is in place, or put back if the
// download fails.
func (s *Stager) repair(ctx context.Context, dir string, req agent.CheckpointRequest, f agentapi.FileDigest, t *tally) (agentapi.FileResult, error) {
	r := agentapi.FileResult{Path: f.Path}
	lk, err := s.lock(ctx, filepath.Join(dir, lockName))
	if err != nil {
		return r, err
	}
	defer lk.release()
	p := filepath.Join(dir, filepath.FromSlash(f.Path))
	aside := ""
	if fi, err := os.Lstat(p); err == nil && fi.Mode().IsRegular() {
		sum, err := hashFile(ctx, p, make([]byte, readBuffer))
		if err != nil {
			return r, err
		}
		if sum == f.SHA256 {
			r.Outcome = agentapi.FileMatch
			return r, nil
		}
		if !req.Restage {
			r.Outcome = agentapi.FileMismatch
			r.SHA256 = sum
			return r, nil
		}
		aside = p + ".mismatch-" + time.Now().UTC().Format("20060102T150405.000000000Z")
		if err := os.Rename(p, aside); err != nil {
			return r, fmt.Errorf("moving %s aside: %w", f.Path, err)
		}
	}
	if err := s.download(ctx, dir, req, f, t); err != nil {
		if aside != "" {
			if rerr := os.Rename(aside, p); rerr != nil {
				s.logf("putting %s back: %v", p, rerr)
			}
		}
		return r, err
	}
	if aside != "" {
		if err := os.Remove(aside); err != nil {
			s.logf("removing %s: %v", aside, err)
		}
	}
	r.Outcome = agentapi.FileMatch
	return r, nil
}

// removeStaleParts removes the checkpoint's temporary download files older than
// staleTempAge, which a download that died left. Downloads run under the
// checkpoint's lock, so it takes the lock without waiting and does nothing if
// another agent holds it.
func (s *Stager) removeStaleParts(dir string) {
	lk, ok, err := s.tryLock(filepath.Join(dir, lockName))
	if err != nil || !ok {
		return
	}
	defer lk.release()
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil
		}
		if n := d.Name(); strings.HasPrefix(n, ".") && strings.Contains(n, ".part-") {
			if fi, err := d.Info(); err == nil && time.Since(fi.ModTime()) > staleTempAge {
				if err := os.Remove(p); err != nil {
					s.logf("removing %s: %v", p, err)
				}
			}
		}
		return nil
	})
}

// checkFile refuses a listed path that could leave the checkpoint directory,
// and a hash that is not SHA-256 hex.
func checkFile(f agentapi.FileDigest) error {
	p := f.Path
	if p == "" || strings.HasPrefix(p, "/") || strings.Contains(p, "\\") || path.Clean(p) != p || p == "." ||
		p == ".." || strings.HasPrefix(p, "../") || strings.Contains(p, "\x00") {
		return fmt.Errorf("listed file %q is not a clean relative path", p)
	}
	if !sumForm.MatchString(f.SHA256) {
		return fmt.Errorf("listed file %q: sha256 %q is not 64 lowercase hex characters", p, f.SHA256)
	}
	return nil
}

// rehash returns the SHA-256 of every present file.
func (s *Stager) rehash(ctx context.Context, dir string, files []agentapi.FileDigest, present []bool) ([]string, error) {
	paths := make([]string, len(files))
	for i, f := range files {
		if present[i] {
			paths[i] = filepath.Join(dir, filepath.FromSlash(f.Path))
		}
	}
	sums, errs := hashPaths(ctx, paths, s.c.Readers)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return sums, errors.Join(errs...)
}
