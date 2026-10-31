package stage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// A sandbox image is a directory tree, so its integrity record is a manifest of
// the tree: every directory, every regular file with its size and SHA-256,
// every symlink with its target (PR #87 review, Astra B1, Opus B5). It is
// written beside the sandbox in the step that builds it, and every staging
// walks the sandbox and compares the walk with it, so a file that is missing,
// changed or added fails the check.
//
// A regular file the staging user cannot read, as a non-root sandbox can hold,
// is recorded as such with its size and is checked by size only: the engine
// runs as the same user and cannot read it either. An unreadable directory
// fails the walk, and with it the staging of that image.

// Entry kinds of a manifest.
const (
	entDir        = 'D'
	entFile       = 'F'
	entLink       = 'L'
	entUnreadable = 'U'
	entOther      = 'O' // a socket, a device, a named pipe: its presence only
)

// treeEntry is one directory, file or link of a sandbox.
type treeEntry struct {
	Kind   byte
	Path   string // slash-separated, relative to the sandbox; "." is the root
	Size   int64  // F and U
	SHA256 string // F
	Target string // L
}

// scanTree walks root, without following symlinks, and hashes every regular
// file with readers files read at once.
func scanTree(ctx context.Context, root string, readers int) ([]treeEntry, error) {
	var entries []treeEntry
	var paths []string // index-aligned with entries; "" except for regular files
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		e := treeEntry{Path: filepath.ToSlash(rel)}
		path := ""
		switch t := d.Type(); {
		case t.IsDir():
			e.Kind = entDir
		case t&fs.ModeSymlink != 0:
			e.Kind = entLink
			if e.Target, err = os.Readlink(p); err != nil {
				return err
			}
		case t.IsRegular():
			e.Kind = entFile
			fi, err := d.Info()
			if err != nil {
				return err
			}
			e.Size = fi.Size()
			path = p
		default:
			e.Kind = entOther
		}
		entries = append(entries, e)
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sums, errs := hashPaths(ctx, paths, readers)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i := range entries {
		switch err := errs[i]; {
		case err == nil:
			entries[i].SHA256 = sums[i]
		case errors.Is(err, fs.ErrPermission):
			entries[i].Kind = entUnreadable
		default:
			return nil, err
		}
	}
	return entries, nil
}

// manifestHeader starts a manifest; it names the image digest the tree was
// built from.
func manifestHeader(digest string) string { return "hpcburst-sandbox-manifest/1 " + digest }

// writeManifest writes the entries to path, ending with a count so a truncated
// manifest is recognised.
func writeManifest(path, digest string, entries []treeEntry) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, manifestHeader(digest))
	for _, e := range entries {
		q := strconv.Quote(e.Path)
		switch e.Kind {
		case entDir, entOther:
			fmt.Fprintf(w, "%c\t%s\n", e.Kind, q)
		case entFile:
			fmt.Fprintf(w, "F\t%d\t%s\t%s\n", e.Size, e.SHA256, q)
		case entUnreadable:
			fmt.Fprintf(w, "U\t%d\t%s\n", e.Size, q)
		case entLink:
			fmt.Fprintf(w, "L\t%s\t%s\n", strconv.Quote(e.Target), q)
		}
	}
	fmt.Fprintf(w, "end\t%d\n", len(entries))
	err = w.Flush()
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path)
	}
	return err
}

// readManifest parses the manifest at path, which must be for digest and
// complete.
func readManifest(path, digest string) (map[string]treeEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	line := func() (string, error) {
		l, err := br.ReadString('\n')
		if err == io.EOF && l != "" {
			return "", io.ErrUnexpectedEOF
		}
		return strings.TrimSuffix(l, "\n"), err
	}
	head, err := line()
	if err != nil || head != manifestHeader(digest) {
		return nil, fmt.Errorf("manifest %s does not start %q", path, manifestHeader(digest))
	}
	m := map[string]treeEntry{}
	for {
		l, err := line()
		if err != nil {
			return nil, fmt.Errorf("manifest %s ends without its count: %w", path, err)
		}
		if n, ok := strings.CutPrefix(l, "end\t"); ok {
			if c, cerr := strconv.Atoi(n); cerr != nil || c != len(m) {
				return nil, fmt.Errorf("manifest %s counts %q entries, holds %d", path, n, len(m))
			}
			return m, nil
		}
		e, err := parseEntry(l)
		if err != nil {
			return nil, fmt.Errorf("manifest %s: %w", path, err)
		}
		m[e.Path] = e
	}
}

func parseEntry(l string) (treeEntry, error) {
	f := strings.Split(l, "\t")
	bad := fmt.Errorf("malformed line %q", l)
	if len(f[0]) != 1 {
		return treeEntry{}, bad
	}
	e := treeEntry{Kind: f[0][0]}
	var err error
	var fields int
	switch e.Kind {
	case entDir, entOther:
		fields = 2
	case entFile:
		fields = 4
	case entUnreadable, entLink:
		fields = 3
	default:
		return treeEntry{}, bad
	}
	if len(f) != fields {
		return treeEntry{}, bad
	}
	if e.Path, err = strconv.Unquote(f[len(f)-1]); err != nil {
		return treeEntry{}, bad
	}
	switch e.Kind {
	case entFile:
		e.SHA256 = f[2]
		if !sumForm.MatchString(e.SHA256) {
			return treeEntry{}, bad
		}
		fallthrough
	case entUnreadable:
		if e.Size, err = strconv.ParseInt(f[1], 10, 64); err != nil {
			return treeEntry{}, bad
		}
	case entLink:
		if e.Target, err = strconv.Unquote(f[1]); err != nil {
			return treeEntry{}, bad
		}
	}
	return e, nil
}

// compareTree returns a description of the first difference between the
// sandbox as it is and as its manifest recorded it, or nil.
func compareTree(want map[string]treeEntry, got []treeEntry) error {
	seen := make(map[string]bool, len(got))
	for _, g := range got {
		w, ok := want[g.Path]
		switch {
		case !ok:
			return fmt.Errorf("%s was added", g.Path)
		case w != g:
			return fmt.Errorf("%s changed", g.Path)
		}
		seen[g.Path] = true
	}
	for p := range want {
		if !seen[p] {
			return fmt.Errorf("%s is missing", p)
		}
	}
	return nil
}

// removeAll removes a file or a tree. A sandbox can hold directories its owner
// cannot write to, which os.RemoveAll cannot empty; their permissions are
// restored and the removal is tried again, so a sandbox moved aside or left by
// a failed build does not stay on the disk.
func removeAll(p string) error {
	err := os.RemoveAll(p)
	if err == nil {
		return nil
	}
	filepath.WalkDir(p, func(q string, d fs.DirEntry, _ error) error {
		if d != nil && d.IsDir() {
			os.Chmod(q, 0o700)
		}
		return nil
	})
	return os.RemoveAll(p)
}
