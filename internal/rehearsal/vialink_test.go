package rehearsal

import (
	"strings"
	"testing"
)

// TestViaLinkRewritesThePortAndTheKnownHosts: the config's Port and UserKnownHostsFile lines, and
// the known-hosts entry, name the link's port; everything else is unchanged. A config or known-hosts
// text that doesn't name what it should is refused, so a client never silently bypasses the link.
func TestViaLinkRewritesThePortAndTheKnownHosts(t *testing.T) {
	conf := "Host login\n  HostName 127.0.0.1\n  Port 41001\n  User example-user\n  IdentityFile /d/key\n  UserKnownHostsFile /d/known_hosts\n  StrictHostKeyChecking yes\n"
	known := "[127.0.0.1]:41001 ssh-ed25519 AAAAexample\n"
	gotConf, gotKnown, err := viaLink(conf, known, 41001, "42002", "/c/creds.known_hosts")
	if err != nil {
		t.Fatal(err)
	}
	wantConf := "Host login\n  HostName 127.0.0.1\n  Port 42002\n  User example-user\n  IdentityFile /d/key\n  UserKnownHostsFile /c/creds.known_hosts\n  StrictHostKeyChecking yes\n"
	if gotConf != wantConf {
		t.Errorf("config:\n%s\nwant:\n%s", gotConf, wantConf)
	}
	if gotKnown != "[127.0.0.1]:42002 ssh-ed25519 AAAAexample\n" {
		t.Errorf("known hosts: %q", gotKnown)
	}
	for name, c := range map[string]struct {
		conf, known string
	}{
		"no Port line":            {strings.Replace(conf, "  Port 41001\n", "", 1), known},
		"two Port lines":          {conf + "  Port 41003\n", known},
		"no known-hosts line":     {strings.Replace(conf, "  UserKnownHostsFile /d/known_hosts\n", "", 1), known},
		"known hosts for another": {conf, "[127.0.0.1]:41009 ssh-ed25519 AAAAexample\n"},
	} {
		if _, _, err := viaLink(c.conf, c.known, 41001, "42002", "/c/creds.known_hosts"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
