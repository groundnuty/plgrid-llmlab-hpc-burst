package fakeslurm

import (
	"encoding/json"
	"time"
)

// NV is Slurm's "number with set and infinite flags".
type NV struct {
	Set      bool  `json:"set"`
	Infinite bool  `json:"infinite"`
	Number   int64 `json:"number"`
}

func nv(n int64) NV { return NV{Set: true, Number: n} }

// nvTime is an NV of an epoch second; a zero time is unknown (set false).
func nvTime(t time.Time) NV {
	if t.IsZero() {
		return NV{}
	}
	return nv(t.Unix())
}

func epoch(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

type exitCode struct {
	Status     []string `json:"status"`
	ReturnCode NV       `json:"return_code"`
	Signal     signal   `json:"signal"`
}

type signal struct {
	ID   NV     `json:"id"`
	Name string `json:"name"`
}

// exitCodeFor derives the exit code block from a job state: running or
// pending jobs have none yet, COMPLETED is SUCCESS, any other final state is
// an ERROR with return code 1.
func exitCodeFor(state string) exitCode {
	switch {
	case state == Completed:
		return exitCode{Status: []string{"SUCCESS"}, ReturnCode: nv(0)}
	case IsTerminal(state):
		return exitCode{Status: []string{"ERROR"}, ReturnCode: nv(1)}
	}
	return exitCode{Status: []string{"PENDING"}}
}

type meta struct {
	Plugin metaPlugin `json:"plugin"`
}

type metaPlugin struct {
	DataParser        string `json:"data_parser"`
	AccountingStorage string `json:"accounting_storage"`
}

func newMeta(version string) meta {
	return meta{Plugin: metaPlugin{
		DataParser:        "data_parser/" + version,
		AccountingStorage: "accounting_storage/slurmdbd",
	}}
}

// marshalOut renders v as indented JSON with a trailing newline.
func marshalOut(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b) + "\n", nil
}
