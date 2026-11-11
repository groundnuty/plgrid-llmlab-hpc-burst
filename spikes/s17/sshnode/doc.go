// Package sshnode is spike S17's node side for real ssh (issue #413, criterion 2), built as a test binary
// (go test -c -o s17ssh.test ./sshnode) because it links internal/rehearsal and the engine double, which only
// _test.go files may (their import guards walk the whole module tree, spikes included). Only this binary
// links internal/rehearsal: the driver's other programs do not.
package sshnode
