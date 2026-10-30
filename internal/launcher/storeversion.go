package launcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// StoreVersion records which k3s release last wrote the store. It lives in the state directory,
// apart from the record of running processes, and down never removes it: every later start is
// checked against it, so a version change goes through upgrade (with its backup and its
// one-step rule) or through restore, never through a plain restart.
type StoreVersion struct {
	Version   string    `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
	By        string    `json:"by"` // up, upgrade or restore
}

// loadStoreVersion reads the record; a missing file is (nil, nil).
func (l layout) loadStoreVersion() (*StoreVersion, error) {
	b, err := os.ReadFile(l.storeVersionFile())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var sv StoreVersion
	if err := json.Unmarshal(b, &sv); err != nil {
		return nil, fmt.Errorf("%s is damaged (%w); it says which k3s release wrote the store, so restore it or start from an empty state directory", l.storeVersionFile(), err)
	}
	if _, err := ParseVersion(sv.Version); err != nil {
		return nil, fmt.Errorf("%s is damaged: %w", l.storeVersionFile(), err)
	}
	return &sv, nil
}

func (l layout) saveStoreVersion(version, by string) error {
	b, err := json.MarshalIndent(StoreVersion{Version: version, UpdatedAt: time.Now().UTC(), By: by}, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(l.storeVersionFile(), append(b, '\n'), 0o600)
}

// storeExists reports whether k3s has created its SQLite store in the data directory.
func (l layout) storeExists() (bool, error) {
	_, err := os.Stat(l.storeDB())
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// sameRelease reports whether a requested version names the stored release. A request without a
// +k3sN suffix matches any revision of that MAJOR.MINOR.PATCH.
func sameRelease(req, stored Version) bool {
	return req.Major == stored.Major && req.Minor == stored.Minor && req.Patch == stored.Patch &&
		(req.Rev == "" || req.Rev == stored.Rev)
}

// startVersion decides which release up starts. Without a store, it is the requested one, or
// DefaultVersion. With a store, it is the release that last wrote it: a request for another one is
// refused, because a plain start would upgrade the store without a backup, or downgrade it
// without a restore.
func startVersion(l layout, requested string) (string, error) {
	exists, err := l.storeExists()
	if err != nil {
		return "", fmt.Errorf("cannot tell whether %s holds a store: %w", l.dataDir(), err)
	}
	if !exists {
		if requested == "" {
			return DefaultVersion, nil
		}
		return requested, nil
	}
	sv, err := l.loadStoreVersion()
	if err != nil {
		return "", err
	}
	if sv == nil {
		return "", fmt.Errorf("the store %s has no record of the k3s release that wrote it (%s is missing), so no release can safely be started on it; restore the record, or move the data directory away and start afresh", l.storeDB(), l.storeVersionFile())
	}
	if requested == "" {
		return sv.Version, nil
	}
	req, err := ParseVersion(requested)
	if err != nil {
		return "", err
	}
	stored, err := ParseVersion(sv.Version)
	if err != nil {
		return "", err
	}
	switch c := compareVersions(req, stored); {
	case sameRelease(req, stored):
		return sv.Version, nil
	case c > 0:
		return "", fmt.Errorf("the store was last written by k3s %s, and %s is newer: start %s (hpcburst up without --version), then run hpcburst upgrade --to %s, which backs the store up first and moves one step at a time", sv.Version, req, sv.Version, req)
	default:
		return "", fmt.Errorf("the store was last written by k3s %s, and %s is older: a downgrade is only possible by restoring a backup taken before the upgrade (hpcburst restore --backup DIR), which brings back the store as that release left it", sv.Version, req)
	}
}
