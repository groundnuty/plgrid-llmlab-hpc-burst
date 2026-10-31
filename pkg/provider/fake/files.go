package fake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// checkPath refuses a path outside the staging directory.
func checkPath(path string) error {
	switch {
	case strings.HasPrefix(path, "/"):
		return fmt.Errorf("%w: path %q is absolute", provider.ErrInvalidRequest, path)
	case strings.Contains(path, ".."):
		return fmt.Errorf("%w: path %q contains ..", provider.ErrInvalidRequest, path)
	case !safeValue.MatchString(path):
		return fmt.Errorf("%w: path %q is empty or unsafe", provider.ErrInvalidRequest, path)
	}
	return nil
}

// EnsureFile implements provider.Batch. It logs op "ensure-file" with the
// path, the SHA-256 and the mode in octal ("600"), never the content.
func (b *Batch) EnsureFile(_ context.Context, f provider.File) error {
	if err := checkPath(f.Path); err != nil {
		return err
	}
	sum := sha256.Sum256(f.Content)
	if got := hex.EncodeToString(sum[:]); f.SHA256 != got {
		return fmt.Errorf("%w: file %q: sha256 %q is not the content's %q", provider.ErrInvalidRequest, f.Path, f.SHA256, got)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if old, ok := b.files[f.Path]; ok && old.SHA256 == f.SHA256 {
		return nil
	}
	f.Content = append([]byte(nil), f.Content...)
	b.files[f.Path] = f
	b.log("ensure-file", f.Path, f.SHA256, strconv.FormatUint(uint64(f.Mode.Perm()), 8))
	return nil
}

// RemoveAllocationFiles implements provider.Batch: it deletes every stored
// file under alloc/<tag>/.
func (b *Batch) RemoveAllocationFiles(_ context.Context, tag string) error {
	if !safeValue.MatchString(tag) || strings.Contains(tag, "..") || strings.Contains(tag, "/") {
		return fmt.Errorf("%w: tag %q is empty or unsafe", provider.ErrInvalidRequest, tag)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.log("remove-allocation-files", tag)
	prefix := "alloc/" + tag + "/"
	for p := range b.files {
		if strings.HasPrefix(p, prefix) {
			delete(b.files, p)
		}
	}
	return nil
}

// Files returns a copy of the stored files, by path.
func (b *Batch) Files() map[string]provider.File {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]provider.File, len(b.files))
	for p, f := range b.files {
		f.Content = append([]byte(nil), f.Content...)
		out[p] = f
	}
	return out
}
