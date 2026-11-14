package stack

// The keeper rehearsal's part of the host (issue #343, PR 5; HBEP-6, The relay
// keeper): up's options for the relay keeper and for an older k3s, and the
// commands a keeper test runs on a Host after its first up: up again, upgrade
// and restore. New for #343. Without the options, StartHost's up runs exactly
// the arguments it ran before them.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"time"
)

// RelayKeeper makes StartHost's up run the relay keeper: hpcburst up
// --relay-keeper, valid in host-dialed mode only.
func RelayKeeper(h *Host) { h.upExtra = append(h.upExtra, "--relay-keeper") }

// K3sVersion makes StartHost's up start k3s release v (up --version), an older
// release that an upgrade then replaces.
func K3sVersion(v string) HostOption {
	return func(h *Host) { h.upExtra = append(h.upExtra, "--version", v) }
}

// Bounds of the keeper rehearsal's commands: an upgrade stops the host, backs
// its store up and starts the new release (a down and an up); a restore copies
// the store back.
const (
	UpgradeTimeout = DownTimeout + UpTimeout
	RestoreTimeout = 2 * time.Minute
)

// UpAgain runs hpcburst up on this stopped host once more, with StartHost's
// own arguments and extra, never the options' (an up after a restore takes the
// release the store records). It returns up's output. After it, down runs again
// at the clean-up, whatever up did: a down of a stopped host stops nothing. On
// success it waits for the controller's /readyz, as StartHost does.
func (h *Host) UpAgain(extra ...string) (string, error) {
	h.T.Helper()
	args := append(append([]string(nil), h.upArgs...), extra...)
	out, err := Run(h.T, Within(h.T, UpTimeout), h.Env, h.B.HPCBurst, args...)
	h.downed = false
	if err != nil {
		return out, err
	}
	WaitFor(h.T, WaitDeadline, "the controller's /readyz after up", func() bool {
		return HTTPStatus(h.Ports.URL(6, "/readyz")) == http.StatusOK
	})
	return out, nil
}

// UpgradeTo runs hpcburst upgrade --to v on the running host and returns the
// backup it made, read from its JSON report, and its log. On success it waits
// for the controller's /readyz.
func (h *Host) UpgradeTo(v string) (backup, log string, err error) {
	h.T.Helper()
	return h.UpgradeWith(h.B.HPCBurst, v)
}

// UpgradeWith is UpgradeTo run by the hpcburst at path: an operator's upgrade with a new build of the
// launcher, whose supervisor it starts (#552).
func (h *Host) UpgradeWith(path, v string) (backup, log string, err error) {
	h.T.Helper()
	stdout, stderr, over, err := runApart(Within(h.T, UpgradeTimeout), h.Env, path,
		"upgrade", "--state-dir", h.State, "--bin-dir", h.B.K3sDir, "--to", v, "--json")
	if over {
		err = errors.Join(err, pastBound(path, UpgradeTimeout))
	}
	if err != nil {
		return "", stdout + stderr, err
	}
	var rep struct {
		Backup string `json:"backup"`
	}
	if err := json.Unmarshal([]byte(stdout), &rep); err != nil || rep.Backup == "" {
		return "", stdout + stderr, fmt.Errorf("upgrade's report names no backup (%v): %.300s", err, stdout)
	}
	WaitFor(h.T, WaitDeadline, "the controller's /readyz after the upgrade", func() bool {
		return HTTPStatus(h.Ports.URL(6, "/readyz")) == http.StatusOK
	})
	return rep.Backup, stderr, nil
}

// RestoreFrom runs hpcburst restore --backup on this stopped host, and returns
// its output.
func (h *Host) RestoreFrom(backup string) (string, error) {
	h.T.Helper()
	return Run(h.T, Within(h.T, RestoreTimeout), h.Env, h.B.HPCBurst, "restore", "--state-dir", h.State, "--backup", backup)
}

// runApart is runBounded with the command's standard output and error kept
// apart, for a command whose report is JSON on its output.
func runApart(timeout time.Duration, env []string, name string, args ...string) (stdout, stderr string, over bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	cmd.WaitDelay = 10 * time.Second
	var o, e bytes.Buffer
	cmd.Stdout, cmd.Stderr = &o, &e
	err = cmd.Run()
	return o.String(), e.String(), ctx.Err() != nil, err
}
