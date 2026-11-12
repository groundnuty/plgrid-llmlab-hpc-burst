package launcher

// The open files of the tunnel processes (HBEP-11, Single-user host; issue #431). frps and the
// front's process need one or two descriptors per tunnel and per stream in flight (HBEP-6, open
// question 11), so a hard limit that is too low can stop them accepting. Both are Go binaries,
// whose runtime raises its own soft limit to the hard limit at start (Go 1.19 and later): the hard
// limit is what binds, it is the host's to set, and the launcher raises nothing. What it does is
// read and report: status shows each process's use against its limits, and up warns when a hard
// limit is below the figure HBEP-6 gives.
//
// Every number here is read from /proc/<pid>/limits and /proc/<pid>/fd through the launcher's own
// verified handle for that process (handle.go), never by looking a process up by name. A value
// that cannot be read is unknown, never 0 and never healthy by default.

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

const (
	// OpenFilesHardFloor is HBEP-6's figure (open question 11): about 5,400 descriptors for the
	// front's process at 300 tunnels x 8 streams. A tunnel process whose hard limit is below it
	// gets a warning at up.
	OpenFilesHardFloor = 5400
	// openFilesDegradedPercent is the share of the SOFT limit past which status reports a tunnel
	// process as degraded: it is the soft limit that refuses an open, and that is what the Go
	// runtime has already raised to the hard one.
	openFilesDegradedPercent = 80
)

// DegradedOpenFiles is EntryHealth.Degraded for a tunnel process whose descriptors in use pass 80%
// of its soft limit. Status prints it as "degraded: open files" and, as for the usage sink, it is
// never reported as unhealthy and status still exits 0.
const DegradedOpenFiles = "open files"

// tunnelEntry says whether a supervised entry is a tunnel process, one whose open files are
// reported: frps, and the front, which holds both the client listener and the dial-home listener.
func tunnelEntry(name string) bool { return name == frpsEntryName || name == frontEntryName }

// Limit is one resource limit as /proc/<pid>/limits states it: a number, or the word "unlimited".
// The zero Value is a real limit (0), so "unlimited" is its own field and never a number.
type Limit struct {
	Value     uint64
	Unlimited bool
}

func (l Limit) String() string {
	if l.Unlimited {
		return "unlimited"
	}
	return strconv.FormatUint(l.Value, 10)
}

// MarshalJSON writes the number, or the string "unlimited".
func (l Limit) MarshalJSON() ([]byte, error) {
	if l.Unlimited {
		return []byte(`"unlimited"`), nil
	}
	return []byte(strconv.FormatUint(l.Value, 10)), nil
}

// UnmarshalJSON reads what MarshalJSON writes.
func (l *Limit) UnmarshalJSON(b []byte) error {
	if string(b) == `"unlimited"` {
		*l = Limit{Unlimited: true}
		return nil
	}
	v, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return fmt.Errorf("a limit is a number or %q, not %s", "unlimited", b)
	}
	*l = Limit{Value: v}
	return nil
}

// OpenFiles is one tunnel process's descriptors in use and its limits on them (soft and hard "Max
// open files"), read together through the same verified handle.
type OpenFiles struct {
	InUse int   `json:"in_use"` // entries of /proc/<pid>/fd
	Soft  Limit `json:"soft"`
	Hard  Limit `json:"hard"`
}

// degraded reports whether use is past 80% of the soft limit (strictly: exactly 80% is not past
// it). An unlimited soft limit is never passed. The comparison is the exact integer one,
// InUse*100 > Soft*80, written as a quotient and a remainder of Soft so that no product can
// overflow whatever limit /proc states.
func (o OpenFiles) degraded() bool {
	if o.Soft.Unlimited || o.InUse < 0 {
		return false
	}
	s := o.Soft.Value
	return uint64(o.InUse) > s/100*openFilesDegradedPercent+s%100*openFilesDegradedPercent/100
}

// parseOpenFilesLimits returns the soft and hard "Max open files" limits from the content of
// /proc/<pid>/limits. The row is found by its name, never by its position, and is read as the
// kernel writes it: the name, then the soft limit, the hard limit and the unit "files", each a
// number or the word "unlimited". Any other shape is an error that names it.
func parseOpenFilesLimits(content string) (soft, hard Limit, err error) {
	const name = "Max open files"
	for _, line := range strings.Split(content, "\n") {
		rest, ok := strings.CutPrefix(line, name)
		if !ok || !strings.HasPrefix(rest, " ") {
			continue
		}
		f := strings.Fields(rest)
		if len(f) != 3 || f[2] != "files" {
			return Limit{}, Limit{}, fmt.Errorf("the %q row is %q, want a soft limit, a hard limit and the unit files", name, strings.TrimSpace(line))
		}
		if soft, err = parseLimit(f[0]); err != nil {
			return Limit{}, Limit{}, fmt.Errorf("the soft limit of the %q row: %w", name, err)
		}
		if hard, err = parseLimit(f[1]); err != nil {
			return Limit{}, Limit{}, fmt.Errorf("the hard limit of the %q row: %w", name, err)
		}
		return soft, hard, nil
	}
	return Limit{}, Limit{}, fmt.Errorf("no %q row", name)
}

func parseLimit(s string) (Limit, error) {
	if s == "unlimited" {
		return Limit{Unlimited: true}, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return Limit{}, fmt.Errorf("%q is neither a number nor unlimited", s)
	}
	return Limit{Value: v}, nil
}

// readLimits reads the process's open-files limits through the held handle. The read counts only if
// the handle is still the recorded process afterwards (the rule of splitOwned): otherwise the file
// could belong to whatever took over the PID, or be empty because the process was exiting.
func (h *procHandle) readLimits() (soft, hard Limit, err error) {
	b, err := os.ReadFile(procPath(h.rec.PID, "limits"))
	if err == nil {
		soft, hard, err = parseOpenFilesLimits(string(b))
	}
	if err != nil {
		err = fmt.Errorf("pid %d: /proc/%d/limits: %w", h.rec.PID, h.rec.PID, err)
	}
	return soft, hard, h.stillOurs(err)
}

// readOpenFiles reads the process's limits and its descriptor count through the held handle, with
// the same rule: a count of the fd directory is taken only for a process that is still the
// recorded one afterwards. A directory that cannot be listed is an error, never a count of 0.
func (h *procHandle) readOpenFiles() (OpenFiles, error) {
	soft, hard, err := h.readLimits()
	if err != nil {
		return OpenFiles{}, err
	}
	ents, err := os.ReadDir(procPath(h.rec.PID, "fd"))
	if err != nil {
		err = fmt.Errorf("pid %d: /proc/%d/fd: %w", h.rec.PID, h.rec.PID, err)
	}
	if err := h.stillOurs(err); err != nil {
		return OpenFiles{}, err
	}
	return OpenFiles{InUse: len(ents), Soft: soft, Hard: hard}, nil
}

// stillOurs returns err when the read failed, and also when the handle is no longer the recorded
// process, which is then the better explanation.
func (h *procHandle) stillOurs(readErr error) error {
	if v, d := h.verify(); v != Ours {
		if readErr != nil {
			return fmt.Errorf("%w (the process is no longer the recorded one: %s)", readErr, d)
		}
		return errors.New("pid " + strconv.Itoa(h.rec.PID) + " changed while its open files were read: " + d)
	}
	return readErr
}

// checkOpenFiles sets what checkEntry reports of a tunnel process's open files: the reading and,
// past 80% of the soft limit, DegradedOpenFiles. A reading that failed is stated as unknown with
// its cause; it is not a reading of 0, and it does not touch the host's health.
func checkOpenFiles(h *procHandle, eh *EntryHealth) {
	of, err := h.readOpenFiles()
	if err != nil {
		eh.OpenFilesUnknown = err.Error()
		return
	}
	eh.OpenFiles = &of
	if of.degraded() {
		eh.addDegraded(DegradedOpenFiles)
	}
}

// degradedOrder is the order in which causes are named in EntryHealth.Degraded, whichever check
// finds its cause first.
var degradedOrder = []string{DegradedUsage, DegradedOpenFiles}

// addDegraded names one more thing that is degraded while the process keeps serving. Causes are
// joined with "; " in degradedOrder, so a front that has both lost usage records and run low on
// descriptors says both, and always in the same words and order.
func (e *EntryHealth) addDegraded(cause string) {
	var causes []string
	if e.Degraded != "" {
		causes = strings.Split(e.Degraded, "; ")
	}
	causes = append(causes, cause)
	slices.SortStableFunc(causes, func(a, b string) int { return slices.Index(degradedOrder, a) - slices.Index(degradedOrder, b) })
	e.Degraded = strings.Join(causes, "; ")
}

// openFilesWarnings is what up says about the tunnel processes it has just started: one warning
// for each whose HARD limit is below OpenFilesHardFloor, naming the limit and the figure, and one
// for each whose limit could not be read (a check that could not look is not a check that
// passed). It never refuses: the limit is the host's to set. It reads the processes the supervisor
// recorded in supervisor.json, each through a handle verified against its record. read replaces
// the read of the limits (tests only; up's test seam, UpOptions.limitReader); nil reads them from
// /proc through the handle, as status does.
func openFilesWarnings(l layout, read func(h *procHandle) (soft, hard Limit, err error)) []string {
	if read == nil {
		read = (*procHandle).readLimits
	}
	f, err := readSupervisorFile(l)
	if err != nil {
		return []string{fmt.Sprintf("warning: the open-files limits of frps and the front were not checked: cannot read the supervisor's record: %v", err)}
	}
	var out []string
	for _, e := range f.Entries {
		if !tunnelEntry(e.Name) {
			continue
		}
		if e.Proc == nil {
			out = append(out, fmt.Sprintf("warning: the open-files limit of %s was not checked: it has no recorded process", e.Name))
			continue
		}
		h, v, d := openRec(*e.Proc)
		if v != Ours {
			out = append(out, fmt.Sprintf("warning: the open-files limit of %s (pid %d) was not checked: it is not running as recorded: %s", e.Name, e.Proc.PID, d))
			continue
		}
		_, hard, err := read(h)
		h.Close()
		switch {
		case err != nil:
			out = append(out, fmt.Sprintf("warning: the open-files limit of %s (pid %d) was not checked: %v", e.Name, e.Proc.PID, err))
		case !hard.Unlimited && hard.Value < OpenFilesHardFloor:
			out = append(out, fmt.Sprintf("warning: %s (pid %d) has a hard limit of %d open files, below the %d that HBEP-6 gives for the front at 300 tunnels x 8 streams: it can stop accepting connections under that load. The Go runtime already raises the soft limit to the hard one, so the hard limit is what binds; the session or service that runs hpcburst up sets it, so raise it there and restart the host",
				e.Name, e.Proc.PID, hard.Value, OpenFilesHardFloor))
		}
	}
	return out
}
