package fakeslurm

// Ported from spikes/s4/fakeslurm/output.go. Added: the fuller meta block of
// S7's real captures (plugin, client, command, slurm), the data_parser reply
// override, and the TRES entries.

import (
	"encoding/json"
	"fmt"
	"strings"
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

// limitNV is a time limit in minutes; 0 means none was given.
func limitNV(minutes int) NV {
	if minutes <= 0 {
		return NV{}
	}
	return nv(int64(minutes))
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
	Plugin  metaPlugin `json:"plugin"`
	Client  metaClient `json:"client"`
	Command []string   `json:"command"`
	Slurm   metaSlurm  `json:"slurm"`
}

type metaPlugin struct {
	Type              string `json:"type"`
	Name              string `json:"name"`
	DataParser        string `json:"data_parser"`
	AccountingStorage string `json:"accounting_storage"`
}

type metaClient struct {
	Source string `json:"source"`
	User   string `json:"user"`
	Group  string `json:"group"`
}

type metaSlurm struct {
	Version metaVersion `json:"version"`
	Release string      `json:"release"`
	Cluster string      `json:"cluster"`
}

type metaVersion struct {
	Major string `json:"major"`
	Micro string `json:"micro"`
	Minor string `json:"minor"`
}

// newMeta is the meta block of a reply to user's command. The data_parser
// value is the platform's version, or the ReplyDataParser override. The Slurm
// release follows the version: v0.0.40 is 23.11 (Athena, Sano), v0.0.42 is
// 24.11 (Helios), as in S7.
func (p *Platform) newMeta(user, command string) meta {
	version := p.cfg.DataParser
	if p.cfg.ReplyDataParser != "" {
		version = strings.TrimPrefix(p.cfg.ReplyDataParser, "data_parser/")
	}
	v := metaVersion{Major: "24", Minor: "11", Micro: "5"}
	if p.cfg.DataParser == "v0.0.40" {
		v = metaVersion{Major: "23", Minor: "11", Micro: "7"}
	}
	return meta{
		Plugin:  metaPlugin{DataParser: "data_parser/" + version, AccountingStorage: "accounting_storage/slurmdbd"},
		Client:  metaClient{User: user},
		Command: []string{command, "--json"},
		Slurm: metaSlurm{
			Version: v,
			Release: fmt.Sprintf("%s.%s.%s", v.Major, v.Minor, v.Micro),
			Cluster: "example-cluster",
		},
	}
}

// tres is one entry of sacct's tres lists.
type tres struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	ID    int    `json:"id"`
	Count int64  `json:"count"`
}

// tresList is the requested (or allocated) TRES of a job, in the order of S7's
// capture: cpu, mem, node, billing, then the GPUs as a gres entry. The ids are
// the base TRES ids of S7; gres/gpu gets the first custom id.
func tresList(j *job) []tres {
	l := []tres{
		{Type: "cpu", ID: 1, Count: int64(j.cpus)},
		{Type: "mem", ID: 2, Count: j.memMiB},
		{Type: "node", ID: 4, Count: int64(j.nodes)},
		{Type: "billing", ID: 5, Count: int64(j.cpus)},
	}
	if j.gpus > 0 {
		l = append(l, tres{Type: "gres", Name: "gpu", ID: 1001, Count: int64(j.gpus)})
	}
	return l
}

// tresStr is squeue's tres_req_str, "cpu=8,mem=16384M,node=1,billing=8,
// gres/gpu=1"; the gres part is left out for a CPU job.
func tresStr(j *job) string {
	s := fmt.Sprintf("cpu=%d,mem=%dM,node=%d,billing=%d", j.cpus, j.memMiB, j.nodes, j.cpus)
	if j.gpus > 0 {
		s += fmt.Sprintf(",gres/gpu=%d", j.gpus)
	}
	return s
}

// marshalOut renders v as indented JSON with a trailing newline.
func marshalOut(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b) + "\n", nil
}
