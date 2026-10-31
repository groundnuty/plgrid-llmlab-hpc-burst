package sshrun

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// checkConfigValues refuses a value that would break the configuration file's
// one-keyword-per-line form: whitespace, control characters, quotes, a
// backslash, a comment sign or a byte outside ASCII. slurm.ParseSSHConfig
// already admits far less; this keeps a hand-built SSHConfig from injecting a
// directive such as ProxyCommand. Errors name the field, never its value.
func checkConfigValues(c slurm.SSHConfig) error {
	for _, f := range []struct{ name, val string }{
		{"HostName", c.HostName}, {"User", c.User}, {"IdentityFile", c.IdentityFile}, {"UserKnownHostsFile", c.UserKnownHostsFile},
	} {
		for i := 0; i < len(f.val); i++ {
			if b := f.val[i]; b <= ' ' || b >= 0x7f || b == '"' || b == '\\' || b == '#' {
				return fmt.Errorf("sshrun: the %s holds a character that is not allowed in an ssh configuration value", f.name)
			}
		}
	}
	if c.HostName == "" || c.User == "" || c.IdentityFile == "" {
		return errors.New("sshrun: HostName, User and IdentityFile are required")
	}
	return nil
}

// writeConfig writes the private ssh configuration file: the Provider's
// credentials in one Host block. The file is created with mode 600 and put in
// place by rename, so ssh never reads a partial one. It is rewritten at every
// master start, which also restores a file that something deleted. Errors say
// nothing about the values or paths.
func (s *SSH) writeConfig() error {
	c := s.o.Config
	var b strings.Builder
	b.WriteString("# Written by the hpc-burst controller; removed when the transport closes.\n")
	b.WriteString("Host " + alias + "\n")
	b.WriteString("  HostName " + c.HostName + "\n")
	b.WriteString("  User " + c.User + "\n")
	b.WriteString("  IdentityFile " + c.IdentityFile + "\n")
	if c.Port != 0 {
		b.WriteString("  Port " + strconv.Itoa(c.Port) + "\n")
	}
	if c.UserKnownHostsFile != "" {
		b.WriteString("  UserKnownHostsFile " + c.UserKnownHostsFile + "\n")
	}
	f, err := os.CreateTemp(s.o.ControlDir, ".conf-*")
	if err != nil {
		return errors.New("the ssh configuration file could not be created")
	}
	_, werr := f.WriteString(b.String())
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(f.Name(), s.config)
	}
	if werr != nil {
		_ = os.Remove(f.Name())
		return errors.New("the ssh configuration file could not be written")
	}
	return nil
}
