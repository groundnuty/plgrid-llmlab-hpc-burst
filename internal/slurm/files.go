package slurm

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func isNotStarted(err error) bool { return errors.Is(err, ErrNotStarted) }

// checkRelPath refuses a path that is not inside the staging directory.
func checkRelPath(p string) error {
	switch {
	case p == "" || strings.HasPrefix(p, "/"):
		return fmt.Errorf("%w: a staged path must be relative to stagingDir", provider.ErrInvalidRequest)
	case strings.Contains(p, ".."):
		return fmt.Errorf("%w: a staged path must not contain ..", provider.ErrInvalidRequest)
	case !safeValue.MatchString(p):
		return fmt.Errorf("%w: a staged path holds a character outside letters, digits and ._:=,/-", provider.ErrInvalidRequest)
	}
	return nil
}

// remoteSHA256 returns the SHA-256 of the file at abs on the login node, or
// "" when sha256sum fails, for example because the file does not exist.
func (b *Batch) remoteSHA256(ctx context.Context, abs string) (string, error) {
	res, err := b.run(ctx, sha256Line(abs), nil)
	if err != nil {
		return "", err
	}
	out := string(res.Stdout)
	if res.ExitCode != 0 || len(out) < 64 {
		return "", nil
	}
	return out[:64], nil
}

// EnsureFile implements provider.Batch (HBEP-4, Core operations). It does
// nothing when the file under stagingDir already has the SHA-256. Otherwise it
// writes the content through standard input to a temporary name (umask 077,
// so the file is never readable by others), checks the temporary file's
// SHA-256, sets the mode and renames it into place. Neither the content nor a
// path is ever in an error.
func (b *Batch) EnsureFile(ctx context.Context, f provider.File) error {
	if err := checkRelPath(f.Path); err != nil {
		return err
	}
	sum := sha256.Sum256(f.Content)
	if hex.EncodeToString(sum[:]) != f.SHA256 {
		return fmt.Errorf("%w: file %s: the SHA-256 given is not the content's", provider.ErrInvalidRequest, path.Base(f.Path))
	}
	paths, err := b.Paths(ctx)
	if err != nil {
		return err
	}
	abs := paths.Staging + "/" + f.Path
	name := path.Base(f.Path)
	have, err := b.remoteSHA256(ctx, abs)
	if err != nil {
		return fmt.Errorf("check %s: %w", name, err)
	}
	if have == f.SHA256 {
		return nil
	}
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return fmt.Errorf("temporary name: %w", err)
	}
	tmp := abs + ".hbtmp." + hex.EncodeToString(rnd[:])
	defer b.markStale()
	res, err := b.run(ctx, writeTempLine(path.Dir(abs), tmp), f.Content)
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("write %s: exit %d", name, res.ExitCode)
	}
	got, err := b.remoteSHA256(ctx, tmp)
	if err != nil {
		return fmt.Errorf("check written %s: %w", name, err)
	}
	if got != f.SHA256 {
		_, _ = b.run(ctx, removeLine(tmp), nil)
		return fmt.Errorf("write %s: the written file's SHA-256 differs from the content's; removed it", name)
	}
	mode := strconv.FormatUint(uint64(f.Mode.Perm()), 8)
	res, err = b.run(ctx, installLine(mode, tmp, abs), nil)
	if err != nil {
		return fmt.Errorf("install %s: %w", name, err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("install %s: exit %d", name, res.ExitCode)
	}
	return nil
}

// RemoveAllocationFiles implements provider.Batch: it removes alloc/<tag>/
// under stagingDir. Idempotent.
func (b *Batch) RemoveAllocationFiles(ctx context.Context, tag string) error {
	if err := CheckValue("tag", tag); err != nil || strings.Contains(tag, "/") || strings.Contains(tag, "..") {
		return fmt.Errorf("%w: the tag is empty or unsafe", provider.ErrInvalidRequest)
	}
	paths, err := b.Paths(ctx)
	if err != nil {
		return err
	}
	res, err := b.run(ctx, removeTreeLine(paths.Staging+"/"+agentapi.AllocationDir(tag)), nil)
	if err != nil {
		return fmt.Errorf("remove the Allocation's files: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("remove the Allocation's files: exit %d", res.ExitCode)
	}
	return nil
}

// DialHomeKeysPresent reports whether both dial-home files, the restricted key
// and the pinned host key, exist in <home>/.hpcburst/dial-home on the platform
// (HBEP-4, Submission). Health records the answer.
func (b *Batch) DialHomeKeysPresent(ctx context.Context) (bool, error) {
	paths, err := b.Paths(ctx)
	if err != nil {
		return false, err
	}
	dir := paths.DialHomeDir()
	res, err := b.run(ctx, testFilesLine(dir+"/"+agentapi.DialHomeKeyFile, dir+"/"+agentapi.DialHomeKnownHostsFile), nil)
	if err != nil {
		return false, fmt.Errorf("check the dial-home key: %w", err)
	}
	var present bool
	switch res.ExitCode {
	case 0:
		present = true
	case 1:
		present = false
	default:
		return false, fmt.Errorf("check the dial-home key: exit %d", res.ExitCode)
	}
	missing := !present
	b.mu.Lock()
	b.health.DialHomeKeyMissing = &missing
	b.mu.Unlock()
	return present, nil
}
