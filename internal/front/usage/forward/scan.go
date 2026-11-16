package forward

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// errStop ends a scan early: a line beyond the bound was reached, and so was the end of what the
// sink has synced.
var errStop = errors.New("scan: past the bound")

// scanFiles gives emit every line of the usage files in dir whose counter is after `after` and not
// above upTo, in file order (the legacy file, then the daily ones oldest first), each with its
// newline, exactly as the file holds it. It is how the forwarder resumes from the files (HBEP-8, The
// durable log, Delivery: "publishes from the next counter present in the files"):
//
//   - a file whose last whole line has a counter at or below `after` is skipped after one read of
//     its end, so a host with a year of files reads only the files that hold something new;
//   - counters can have gaps (a crash's reserved block), so a line is taken for what it carries,
//     never for the counter that was expected: nothing waits for a missing one;
//   - a counter that does not rise above the last one given is skipped, so the batches' ids rise
//     strictly, whatever a file holds;
//   - a line longer than maxLine is never read whole (a client's model name can make a record as
//     long as a request body): its counter is read from its first bytes and long is told it and its
//     size, and the scan goes on after it, so such a line neither stops a catch-up nor costs its
//     length in memory;
//   - a final line with no newline is not whole, and is left: the sink may still be writing it, or
//     may cut it back;
//   - a line past upTo ends the scan, since upTo is the last counter the sink has synced and a
//     later line is written, perhaps not synced (HBEP-8, The durable log, Who writes).
//
// warn is called once for each line it passes over because it carries no counter. It returns the
// last counter it gave to emit or long, or `after` when it gave none.
func scanFiles(dir string, after, upTo uint64, maxLine int, warn func(file string, line int), long func(counter uint64, size int), emit func(counter uint64, line []byte) error) (uint64, error) {
	files, err := usage.Files(dir)
	if err != nil {
		return after, err
	}
	prev := after
	for _, name := range files {
		path := filepath.Join(dir, name)
		if last, ok, err := usage.FileLastCounter(path); err == nil && ok && last <= prev {
			continue
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return prev, err
		}
		var err error
		prev, err = scanFile(path, prev, upTo, maxLine, warn, long, emit)
		switch {
		case errors.Is(err, errStop):
			return prev, nil
		case errors.Is(err, os.ErrNotExist):
			continue // vanished between the listing and the open
		case err != nil:
			return prev, err
		}
	}
	return prev, nil
}

// scanBuffer is the read buffer of scanFile, and so the head it keeps of a line too long to read.
const scanBuffer = 256 << 10

// scanFile is scanFiles for one file. It returns the last counter it gave out, or prev.
func scanFile(path string, prev, upTo uint64, maxLine int, warn func(file string, line int), long func(counter uint64, size int), emit func(counter uint64, line []byte) error) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return prev, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, scanBuffer)
	for n := 1; ; n++ {
		line, size, whole, err := readLine(br, maxLine)
		if err != nil {
			return prev, err
		}
		if !whole {
			return prev, nil // a last line with no newline is not whole: left for the next look
		}
		counter, ok := usage.LineCounter(line)
		switch {
		case !ok:
			warn(filepath.Base(path), n)
		case counter <= prev:
			// at or below what was read already: a line the resume does not want, or one that
			// does not rise
		case counter > upTo:
			return prev, errStop
		case size > maxLine:
			long(counter, size)
			prev = counter
		default:
			if err := emit(counter, line); err != nil {
				return prev, err
			}
			prev = counter
		}
	}
}

// readLine reads one line, with its newline. A line of at most max bytes comes back whole; a longer
// one comes back as its first bytes only (what the buffer held), with its true size, and the rest
// is read past and dropped, so its length costs no memory. whole is false at the end of the file,
// where a last line with no newline is not whole.
func readLine(br *bufio.Reader, max int) (line []byte, size int, whole bool, err error) {
	chunk, err := br.ReadSlice('\n')
	if err == nil {
		return chunk, len(chunk), true, nil // the usual case: a line inside the buffer
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		if errors.Is(err, io.EOF) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	// A line longer than the buffer: keep a copy of what has been read while it is within max, else
	// only the head, and read on until the newline.
	size = len(chunk)
	head := append([]byte(nil), chunk...)
	for {
		chunk, err = br.ReadSlice('\n')
		size += len(chunk)
		if size <= max {
			head = append(head, chunk...)
		}
		switch {
		case err == nil:
			return head, size, true, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return nil, 0, false, nil
		default:
			return nil, 0, false, err
		}
	}
}

// startAfter is where the forwarder starts on an EMPTY stream (the science agent, 16 Nov 2026,
// R4 of #625; HBEP-8, The durable log, Delivery): the oldest daily file of the newest set of files
// whose sizes fit within the usage cap, so that a fresh log on a host with months of files does not
// churn the grant's whole history through its rolling copy. It returns the counter before that
// file's first one, which is what scanFiles takes as `after`.
//
// The set is the newest file and the files before it, as many as their sizes add up to the cap or
// less. The newest file belongs to it even when it alone is larger than the cap: the stream then
// keeps the latest of its records, which its own limit decides. A cap of 0 or less is no limit, and
// the set is every file. A set whose files hold no whole line starts at 0: the first record the
// sink writes is the first the stream holds.
//
// The sizes are the files' bytes, which the stream's per-message overhead (a subject and a header,
// about a hundred bytes for a message of up to 256 records) adds to a little.
func startAfter(dir string, capBytes int64) (uint64, error) {
	files, err := usage.Files(dir)
	if err != nil {
		return 0, err
	}
	first := len(files) // the index of the oldest file in the set
	var sum int64
	for i := len(files) - 1; i >= 0; i-- {
		fi, err := os.Stat(filepath.Join(dir, files[i]))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if capBytes > 0 && i != len(files)-1 && sum+fi.Size() > capBytes {
			break
		}
		sum += fi.Size()
		first = i
	}
	for _, name := range files[min(first, len(files)):] {
		c, ok, err := firstCounter(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if ok {
			return c - 1, nil // counters start at 1, so c is at least 1
		}
	}
	return 0, nil
}

// firstCounter returns the counter of the first whole line of the file at path; ok is false when
// the file holds none, or its first line has no counter.
func firstCounter(path string) (uint64, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	line, err := bufio.NewReaderSize(f, 64<<10).ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, false, nil
		}
		return 0, false, err
	}
	c, ok := usage.LineCounter(line)
	if ok && c == 0 {
		return 0, false, nil // never a record the sink wrote
	}
	return c, ok, nil
}
