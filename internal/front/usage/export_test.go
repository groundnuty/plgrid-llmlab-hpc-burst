package usage

import "os"

// LogHandle is the sink's log file type, so that a test can put a file that
// fails in its place.
type LogHandle = logFile

// OpenWith is Open with the log file passed through wrap.
func OpenWith(dir string, wrap func(*os.File) LogHandle) (*Sink, error) { return open(dir, wrap) }

// Queued returns how many records wait for the writer goroutine.
func (s *Sink) Queued() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}
