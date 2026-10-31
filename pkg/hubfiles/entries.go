package hubfiles

import (
	"fmt"
)

// toEntries turns the listing into Entries: directories are dropped, every
// file gets its checks. An LFS entry carries its SHA-256 from lfs.oid; the
// SHA-256 of any other entry is filled in by the downloads.
//
// Every path, directories included, is checked and must be unique before any
// directory is dropped, and no file may sit where another file needs a
// directory (a file "a" and a file "a/b"): git cannot hold such a tree, and
// staging could not write it.
func toEntries(listed []treeEntry) ([]Entry, error) {
	kind := make(map[string]string, len(listed))
	for _, t := range listed {
		switch t.Type {
		case "directory", "file":
		default:
			// Fail closed: an entry we do not understand might be a file the
			// deployment ID has to cover.
			return nil, fmt.Errorf("%w: %q has the unknown type %q", ErrInvalidListing, t.Path, t.Type)
		}
		if err := checkListedPath(t.Path); err != nil {
			return nil, err
		}
		if _, dup := kind[t.Path]; dup {
			return nil, fmt.Errorf("%w: %q is listed twice", ErrInvalidListing, t.Path)
		}
		kind[t.Path] = t.Type
	}

	var entries []Entry
	for _, t := range listed {
		if t.Type != "file" {
			continue
		}
		for i := 0; i < len(t.Path); i++ {
			if t.Path[i] == '/' && kind[t.Path[:i]] == "file" {
				return nil, fmt.Errorf("%w: %q is a file, and %q needs it to be a directory", ErrInvalidListing, t.Path[:i], t.Path)
			}
		}
		if !commitPattern.MatchString(t.OID) {
			return nil, fmt.Errorf("%w: %q has no 40-hex git blob id", ErrInvalidListing, t.Path)
		}
		e := Entry{BlobID: t.OID, Size: t.Size}
		e.Path = t.Path
		if t.LFS != nil {
			if !sha256Pattern.MatchString(t.LFS.OID) {
				return nil, fmt.Errorf("%w: %q has an lfs.oid that is not 64 lowercase hex characters", ErrInvalidListing, t.Path)
			}
			e.LFS = true
			e.SHA256 = t.LFS.OID
			e.Size = t.LFS.Size
		}
		entries = append(entries, e)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: the revision has no files", ErrInvalidListing)
	}
	return entries, nil
}
