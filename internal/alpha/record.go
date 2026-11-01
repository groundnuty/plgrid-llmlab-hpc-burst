package alpha

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
)

// File names in a run's record directory (HPCBURST_ALPHA_OUT).
const (
	// TagsFile holds the tag of every job the run may submit, one per line.
	// hack/live-checks.sh reads it on every exit and cancels the jobs of those
	// names, and only those.
	TagsFile = "job-tags.txt"
	// RecordFile holds the run's observation and verdict.
	RecordFile = "record.json"
	// PostsFile holds the text to post on the issues the row names.
	PostsFile = "posts.md"
)

// tagRE is the shape of a tag the script accepts (hack/live-checks.sh, own_tags).
var tagRE = regexp.MustCompile(`^hb-[A-Za-z0-9._-]{1,60}$`)

// Recorder writes a run's private records. Everything is mode 600 in a
// directory of mode 700.
type Recorder struct {
	dir  string
	mu   sync.Mutex
	tags map[string]bool
}

// NewRecorder makes the record directory if needed. A directory that exists
// must be private already: a run never loosens a mode it did not set.
func NewRecorder(dir string) (*Recorder, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("the record directory must be an absolute path")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("the record directory could not be made: %w", err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("the record directory is not private (mode 700)")
	}
	return &Recorder{dir: dir, tags: map[string]bool{}}, nil
}

// Tag writes a job tag down, and syncs it, before it returns. A scenario calls
// it with an Allocation's tag before anything can submit that Allocation: a job
// whose tag is not here is never the script's to cancel.
func (r *Recorder) Tag(tag string) error {
	if !tagRE.MatchString(tag) {
		return fmt.Errorf("a job tag must look like hb-<name>-<hash>")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(r.dir, TagsFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("the job tag could not be recorded: %w", err)
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, tag); err != nil {
		return fmt.Errorf("the job tag could not be recorded: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("the job tag could not be synced: %w", err)
	}
	r.tags[tag] = true
	return nil
}

// Tagged reports whether the tag was written down.
func (r *Recorder) Tagged(tag string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tags[tag]
}

// Tags returns the tags written down, sorted.
func (r *Recorder) Tags() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.tags))
	for t := range r.tags {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// SaveJSON writes v to name, mode 600.
func (r *Recorder) SaveJSON(name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.dir, name), append(b, '\n'), 0o600)
}

// SaveText writes text to name, mode 600.
func (r *Recorder) SaveText(name, text string) error {
	return os.WriteFile(filepath.Join(r.dir, name), []byte(text), 0o600)
}
