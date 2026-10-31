package usage

import (
	"os"
)

// LogHandle is the sink's log file type, so that a test can put a file that
// fails in its place.
type LogHandle = logFile

// OpenWith is OpenOptions with every log file the sink opens passed through
// wrap, and syncDir, when set, in place of the directory sync after a new
// file is created.
func OpenWith(dir string, o Options, wrap func(*os.File) LogHandle, syncDir func(dir string) error) (*Sink, error) {
	h := hooks{syncDir: syncDir}
	if wrap != nil {
		h.wrap = func(f *os.File) logFile { return wrap(f) }
	}
	return open(dir, o, h)
}

// Queued returns how many records wait for the writer goroutine.
func (s *Sink) Queued() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}
