package usage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// maxLastLine bounds how much of a file's last line LastCounter reads: a usage record is under a
// few kilobytes, so a line past this is not one.
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
// counter. It reads the end of the file only, so a file of any size costs one read.
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
	if end-1-start > maxLastLine {
		return 0, false, fmt.Errorf("usage: the last line of %s is %d bytes, which no record is", path, end-1-start)
	}
	line := make([]byte, end-1-start)
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
