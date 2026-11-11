package main

import (
	"os/exec"
	"reflect"
	"testing"
)

// base stands for sshrun.Relay.Command: "ssh" with the relay's own arguments, the log level among them.
func base(line string) (*exec.Cmd, error) {
	return exec.Command("ssh", "-F", "/cfg", "-o", "LogLevel=ERROR", "-T", "alias", "--", line), nil
}

// The extra arguments go right after the program's name: ssh takes the first value of an option, so -v sets the
// debug level before the relay's -o LogLevel=ERROR can, and RekeyLimit is not overridden by anything after it.
// The relay's own arguments follow unchanged, and the debug log's name is a new one at each start.
func TestSSHCommandAddsTheDebugFlagsAndOptionsBeforeTheRelaysOwnArguments(t *testing.T) {
	mk := sshCommand(base, []string{"RekeyLimit=256M", "Ciphers=aes256-ctr"}, "/logs")
	cmd, err := mk("srun relay serving")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ssh", "-v", "-E", "/logs/ssh-001.log", "-o", "RekeyLimit=256M", "-o", "Ciphers=aes256-ctr",
		"-F", "/cfg", "-o", "LogLevel=ERROR", "-T", "alias", "--", "srun relay serving"}
	if !reflect.DeepEqual(cmd.Args, want) {
		t.Errorf("args\n got %q\nwant %q", cmd.Args, want)
	}
	again, _ := mk("srun relay control")
	if got := again.Args[3]; got != "/logs/ssh-002.log" {
		t.Errorf("the second start's log is %q, want /logs/ssh-002.log", got)
	}
}

// With no option and no directory the command is the relay's own, untouched.
func TestSSHCommandChangesNothingWhenAskedForNothing(t *testing.T) {
	cmd, _ := sshCommand(base, nil, "")("x")
	want := []string{"ssh", "-F", "/cfg", "-o", "LogLevel=ERROR", "-T", "alias", "--", "x"}
	if !reflect.DeepEqual(cmd.Args, want) {
		t.Errorf("args %q, want %q", cmd.Args, want)
	}
}

// Only options: no debug flags.
func TestSSHCommandWithOnlyOptionsAddsNoDebugFlags(t *testing.T) {
	cmd, _ := sshCommand(base, []string{"RekeyLimit=1G"}, "")("x")
	want := []string{"ssh", "-o", "RekeyLimit=1G", "-F", "/cfg", "-o", "LogLevel=ERROR", "-T", "alias", "--", "x"}
	if !reflect.DeepEqual(cmd.Args, want) {
		t.Errorf("args %q, want %q", cmd.Args, want)
	}
}
