package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

// Spec is one configuration: its host layout, its tunnels and the keys they
// use. "s16 setup" writes it (mode 600: it holds throwaway credentials) and
// every other command reads it.
type Spec struct {
	Mode     string `json:"mode"` // "direct" or "host-dialed"
	Dir      string `json:"dir"`  // the configuration's directory
	StateDir string `json:"state_dir"`
	PortBase int    `json:"port_base"`
	// Server is where frpc dials home: the link in front of the front's
	// dial-home listener (direct mode), or "" when the job side's serving
	// listener is frpc's server (host-dialed mode, per tunnel).
	Server        string   `json:"server"`
	FrpsConfig    string   `json:"frps_config"`
	FrpsWebPass   string   `json:"frps_web_password"`
	FrontConfig   string   `json:"front_config"`
	ClientKey     string   `json:"client_key"`
	ClientListen  string   `json:"client_listen"`
	FrontHealth   string   `json:"front_health"`
	CertFile      string   `json:"cert_file"`
	Tunnels       []Tunnel `json:"tunnels"`
	DeploymentsNS string   `json:"namespace"`
	ProviderName  string   `json:"provider"`
	ProfileName   string   `json:"profile"`
	TokenDelayMs  int      `json:"token_delay_ms"`
}

// Tunnel is one Instance: its own frpc, engine double, Allocation (whose
// credential its frpc logs in with) and ModelDeployment (whose model name the
// load client asks for, so each request names its tunnel).
type Tunnel struct {
	Index       int    `json:"index"`
	UID         string `json:"uid"`        // the Instance's UID: frps's proxy name and domain
	Allocation  string `json:"allocation"` // the Allocation's UID
	Token       string `json:"token"`      // the Allocation's credential: frpc's metadatas.token
	InstanceKey string `json:"instance_key"`
	Model       string `json:"model"` // the ModelDeployment's spec.model, the engine's served name
	Socket      string `json:"socket"`
	Frpc        string `json:"frpc"` // frpc's configuration file
	FrpcLog     string `json:"frpc_log"`
	Dir         string `json:"dir"` // the Allocation's directory (host-dialed)
	Tag         string `json:"tag"` // the Allocation's job tag (host-dialed)
	Job         string `json:"job"` // its job ID (host-dialed)
}

// Modes of a configuration.
const (
	ModeDirect     = "direct"
	ModeHostDialed = "host-dialed"
)

var uidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$`)

func readSpec(path string) (Spec, error) {
	var s Spec
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("%s: %w", path, err)
	}
	return s, s.check()
}

// check refuses a spec any command would misread: a mode other than the two,
// no tunnels, a tunnel out of order or missing its UID, Allocation, token,
// model or socket, a UID that is not a Kubernetes UID's form, or a repeat.
func (s Spec) check() error {
	switch s.Mode {
	case ModeDirect, ModeHostDialed:
	default:
		return fmt.Errorf("spec: mode %q", s.Mode)
	}
	if len(s.Tunnels) == 0 {
		return fmt.Errorf("spec: no tunnels")
	}
	seen := map[string]bool{}
	for i, t := range s.Tunnels {
		if t.Index != i || t.UID == "" || t.Allocation == "" || t.Token == "" || t.Model == "" || t.Socket == "" {
			return fmt.Errorf("spec: tunnel %d is incomplete", i)
		}
		if !uidRE.MatchString(t.UID) || !uidRE.MatchString(t.Allocation) {
			return fmt.Errorf("spec: tunnel %d's UIDs are not of a Kubernetes UID's form", i)
		}
		for _, k := range []string{t.UID, t.Allocation, t.Model} {
			if seen[k] {
				return fmt.Errorf("spec: tunnel %d repeats %q", i, k)
			}
			seen[k] = true
		}
	}
	return nil
}

// writeJSONFile writes v to path through a temporary file and a rename, so a
// reader never sees half of it. The file is mode 600.
func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".s16-*")
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
