package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Spec is one configuration's tunnels, written by the driver (s14.py) and read
// by the owner, the node and the load client.
type Spec struct {
	Mode    string   `json:"mode"` // "direct", "host-dialed" or "baseline"
	Tunnels []Tunnel `json:"tunnels"`
}

// Tunnel is one Instance: its own frpc, engine double and, in host-dialed
// mode, its own Allocation with two relays.
type Tunnel struct {
	Index  int    `json:"index"`
	UID    string `json:"uid"`    // the Instance's (and Allocation's) UID: frps's proxy name and domain
	Tag    string `json:"tag"`    // the Allocation's job tag (host-dialed)
	Job    string `json:"job"`    // its Slurm-shaped job ID (host-dialed)
	Dir    string `json:"dir"`    // the Allocation's directory, where the job side publishes relay-socket
	Socket string `json:"socket"` // the engine double's Unix socket
}

// Modes of a configuration.
const (
	ModeDirect     = "direct"
	ModeHostDialed = "host-dialed"
	ModeBaseline   = "baseline"
)

func readSpec(path string) (Spec, error) {
	var s Spec
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("%s: %w", path, err)
	}
	switch s.Mode {
	case ModeDirect, ModeHostDialed, ModeBaseline:
	default:
		return s, fmt.Errorf("%s: mode %q", path, s.Mode)
	}
	if len(s.Tunnels) == 0 {
		return s, fmt.Errorf("%s: no tunnels", path)
	}
	for i, t := range s.Tunnels {
		if t.Index != i || t.UID == "" || t.Socket == "" {
			return s, fmt.Errorf("%s: tunnel %d is incomplete", path, i)
		}
	}
	return s, nil
}

// writeJSONFile writes v to path through a temporary file and a rename, so a
// reader never sees half of it.
func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s14-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}
