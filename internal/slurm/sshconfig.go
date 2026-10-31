package slurm

import (
	"bufio"
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// SSHConfig is a Slurm Provider's credentials: the ssh client configuration
// that its credentials reference names (HBEP-4, Provider settings), parsed and
// validated. The transport runs ssh with -F /dev/null and passes these values
// as options, so nothing in the user's ~/.ssh/config, the system's ssh_config
// or another Include can change what it connects to or open a forward.
type SSHConfig struct {
	// HostName is the login node.
	HostName string
	// User is the remote user.
	User string
	// Port is the ssh port; 0 means ssh's default.
	Port int
	// IdentityFile is the absolute path of the private key, mode 600.
	IdentityFile string
	// UserKnownHostsFile is the absolute path of the known-hosts file; ""
	// means ssh's default.
	UserKnownHostsFile string
}

// The keywords a credentials file may hold. Everything else, Include, Match,
// ProxyCommand, ProxyJump, the forwards and the Control* options among them,
// is refused: the transport owns the connection.
var sshKeywords = map[string]bool{
	"host": true, "hostname": true, "user": true, "port": true, "identityfile": true,
	"userknownhostsfile": true, "stricthostkeychecking": true, "identitiesonly": true,
}

var (
	hostNameForm = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$`)
	userNameForm = regexp.MustCompile(`^[a-z_][a-z0-9_.-]*$`)
)

// ParseSSHConfig parses a credentials file in ssh_config form: one keyword and
// value per line ("Keyword value" or "Keyword=value"), "#" comments, at most
// one Host line, whose pattern is ignored. home expands a leading "~/" in
// paths. Errors name the keyword and the line, never a value.
func ParseSSHConfig(b []byte, home string) (SSHConfig, error) {
	var c SSHConfig
	var errs []string
	seen := map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(b))
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := splitKeyword(line)
		if !ok {
			errs = append(errs, fmt.Sprintf("line %d: not a keyword and a value", n))
			continue
		}
		k := strings.ToLower(key)
		if !sshKeywords[k] {
			errs = append(errs, fmt.Sprintf("line %d: keyword %s is not allowed in a credentials file (allowed: HostName, User, Port, IdentityFile, UserKnownHostsFile, StrictHostKeyChecking yes, IdentitiesOnly yes, one Host line)", n, key))
			continue
		}
		if seen[k] {
			errs = append(errs, fmt.Sprintf("line %d: keyword %s appears twice", n, key))
			continue
		}
		seen[k] = true
		switch k {
		case "host":
		case "hostname":
			if !hostNameForm.MatchString(val) {
				errs = append(errs, fmt.Sprintf("line %d: HostName is not a host name or address", n))
			}
			c.HostName = val
		case "user":
			if !userNameForm.MatchString(val) {
				errs = append(errs, fmt.Sprintf("line %d: User is not a user name", n))
			}
			c.User = val
		case "port":
			p, err := strconv.Atoi(val)
			if err != nil || p < 1 || p > 65535 {
				errs = append(errs, fmt.Sprintf("line %d: Port is not a port number", n))
			}
			c.Port = p
		case "identityfile", "userknownhostsfile":
			p, err := expandPath(val, home)
			if err != nil {
				errs = append(errs, fmt.Sprintf("line %d: %s %v", n, key, err))
			}
			if k == "identityfile" {
				c.IdentityFile = p
			} else {
				c.UserKnownHostsFile = p
			}
		case "stricthostkeychecking", "identitiesonly":
			if !strings.EqualFold(val, "yes") {
				errs = append(errs, fmt.Sprintf("line %d: %s must be yes; the transport always sets it", n, key))
			}
		}
	}
	if err := sc.Err(); err != nil {
		errs = append(errs, err.Error())
	}
	if c.HostName == "" {
		errs = append(errs, "HostName is missing")
	}
	if c.User == "" {
		errs = append(errs, "User is missing")
	}
	if c.IdentityFile == "" {
		errs = append(errs, "IdentityFile is missing")
	}
	if len(errs) > 0 {
		return SSHConfig{}, fmt.Errorf("credentials file: %s", strings.Join(errs, "; "))
	}
	return c, nil
}

func splitKeyword(line string) (string, string, bool) {
	i := strings.IndexAny(line, " \t=")
	if i <= 0 {
		return "", "", false
	}
	key := line[:i]
	val := strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line[i:]), "="))
	val = strings.Trim(val, `"`)
	if val == "" || strings.ContainsAny(val, " \t") {
		return "", "", false
	}
	return key, val, true
}

// expandPath returns p absolute: "~/" is home, anything else must already be
// absolute. A path with a character outside letters, digits and ._:=,/-+@ is
// refused, so it is safe as an ssh option.
func expandPath(p, home string) (string, error) {
	if strings.HasPrefix(p, "~/") {
		if home == "" {
			return "", fmt.Errorf("starts with ~/ and the controller's home is unknown")
		}
		p = filepath.Join(home, p[2:])
	}
	if !filepath.IsAbs(p) {
		return "", fmt.Errorf("must be an absolute path or start with ~/")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9._:=,/+@-]+$`).MatchString(p) {
		return "", fmt.Errorf("holds a character outside letters, digits and ._:=,/+@-")
	}
	return filepath.Clean(p), nil
}
