package front

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"syscall"
)

// ClientKeysFile is the form of the front's client keys file (HBEP-5,
// Request path step 1; HBEP-11, Credentials): one JSON object,
//
//	{"keys": [{"id": "alice", "key": "<the key>"}]}
//
// mode 0600, owned by the user who runs the front, not a symlink. Each entry
// has an ID, which usage records carry; the key itself is never logged.
// HBEP-5 and HBEP-11 fix no format beyond "mode-600 file of client keys with
// their IDs", so this one is the front's choice.
type ClientKeysFile struct {
	Keys []ClientKey `json:"keys"`
}

// ClientKey is one client key and its ID.
type ClientKey struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

// MaxClientKeysBytes bounds the file, so a wrong path cannot load a large file.
const MaxClientKeysBytes = 1 << 20

// ClientKeys checks a bearer key against the configured keys.
type ClientKeys struct {
	keys []ClientKey
}

// ReadClientKeys reads and checks the client keys file. Errors name the file
// and the problem, never a key.
func ReadClientKeys(path string) (*ClientKeys, error) {
	b, err := readOwnerOnly(path, MaxClientKeysBytes)
	if err != nil {
		return nil, fmt.Errorf("client keys file: %w", err)
	}
	var f ClientKeysFile
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("client keys file %s is not {\"keys\": [{\"id\", \"key\"}]}", path)
	}
	if len(f.Keys) == 0 {
		return nil, fmt.Errorf("client keys file %s holds no key", path)
	}
	ids, keys := map[string]bool{}, map[string]bool{}
	for i, k := range f.Keys {
		switch {
		case strings.TrimSpace(k.ID) == "":
			return nil, fmt.Errorf("client keys file %s: entry %d has no id", path, i)
		case k.Key == "":
			return nil, fmt.Errorf("client keys file %s: entry %q has no key", path, k.ID)
		case ids[k.ID]:
			return nil, fmt.Errorf("client keys file %s: id %q appears twice", path, k.ID)
		case keys[k.Key]:
			return nil, fmt.Errorf("client keys file %s: entry %q repeats another entry's key", path, k.ID)
		}
		ids[k.ID], keys[k.Key] = true, true
	}
	return &ClientKeys{keys: f.Keys}, nil
}

// Match returns the ID of the key equal to bearer. Every key is compared in
// constant time, so the time taken does not tell which one is near.
func (c *ClientKeys) Match(bearer string) (string, bool) {
	id, found := "", false
	for _, k := range c.keys {
		if subtle.ConstantTimeCompare([]byte(k.Key), []byte(bearer)) == 1 {
			id, found = k.ID, true
		}
	}
	return id, found
}

// readOwnerOnly reads a regular file that only its owner, the current user,
// can read or write, without following a symlink at its last component.
func readOwnerOnly(path string, limit int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	if fi.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("%s has mode %v, want 0600", path, fi.Mode().Perm())
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Getuid() {
		return nil, fmt.Errorf("%s is owned by uid %d, not by the front's user", path, st.Uid)
	}
	if fi.Size() > limit {
		return nil, fmt.Errorf("%s is larger than %d bytes", path, limit)
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New(path + " grew while it was read")
	}
	return b, nil
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}
