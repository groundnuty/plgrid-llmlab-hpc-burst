package usage

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
)

// maxLastLine bounds how much of a file's last line FileLastCounter reads. A usage record is under a
// few kilobytes, but one written before the front bounded the record's client text (#649) can be as
// long as a request body, and a line past this is still read for its counter, from its first bytes.
const maxLastLine = 1 << 20

// counterPrefix is how every line the sink writes begins: Record's first member is its counter, and
// encoding/json writes it with no space.
var counterPrefix = []byte(`{"counter":`)

// LineCounter returns the counter of one usage line, with or without its newline. It reads the
// leading member the sink writes, and falls back to decoding the line as JSON for a line that does
// not begin that way, so a file written by another encoder still counts. ok is false for a line
// that is not a JSON object with a counter.
func LineCounter(line []byte) (n uint64, ok bool) {
	if rest, found := bytes.CutPrefix(line, counterPrefix); found {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i > 0 && i <= 20 && i < len(rest) && (rest[i] == ',' || rest[i] == '}') {
			if n, err := strconv.ParseUint(string(rest[:i]), 10, 64); err == nil {
				return n, true
			}
		}
	}
	var r struct {
		Counter *uint64 `json:"counter"`
	}
	if err := json.Unmarshal(bytes.TrimRight(line, "\r\n"), &r); err != nil || r.Counter == nil {
		return 0, false
	}
	return *r.Counter, true
}

// FileLastCounter returns the counter of the last whole line of the file at path, the line before
// its last newline. ok is false when the file holds no whole line, or its last one carries no
// counter. It reads the end of the file, and for a line past maxLastLine only its first bytes, so
// no length of line is an error: a client can make the front write a record of any size up to its
// request limit (its model name is recorded before it is checked), and such a line can be a file's
// last. The counter is the record's first member, so the head of the line holds it.
func FileLastCounter(path string) (n uint64, ok bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, false, err
	}
	end, err := lineEnd(f, st.Size())
	if err != nil || end == 0 {
		return 0, false, err
	}
	start, err := lineEnd(f, end-1)
	if err != nil {
		return 0, false, err
	}
	// The whole line when it is a record's size, so a line the sink did not write still counts by
	// the JSON fallback; only the head of a longer one, which only the sink's own form can parse.
	line := make([]byte, min(end-1-start, maxLastLine))
	if _, err := f.ReadAt(line, start); err != nil {
		return 0, false, err
	}
	n, ok = LineCounter(line)
	return n, ok, nil
}

// LastCounter returns the counter of the last whole line the usage files in dir hold: it looks at
// the newest file first and passes over a file with no whole line, as a day's file is when the
// first record after midnight is still queued. It returns 0 when no file holds one. A file that
// vanishes between the listing and the read is passed over too.
func LastCounter(dir string) (uint64, error) {
	files, err := Files(dir)
	if err != nil {
		return 0, err
	}
	for i := len(files) - 1; i >= 0; i-- {
		n, ok, err := FileLastCounter(filepath.Join(dir, files[i]))
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case err != nil:
			return 0, err
		case ok:
			return n, nil
		}
	}
	return 0, nil
}
