#!/usr/bin/env bash
# Captures what the go command prints for the errors CI should annotate, and for the test log lines it
# should not, into hack/gotoolchain/testdata/go-tool-output.txt (issue #340).
#
# usage: hack/gotoolchain/testdata/capture-go-output.sh WORKDIR > hack/gotoolchain/testdata/go-tool-output.txt
#
# It builds a throwaway module of deliberately broken packages in WORKDIR/repo (a new or empty
# directory, which the script leaves in place), runs the go on PATH over each, and prints each command
# ("### $ ..."), its output exactly as the tool printed it, and its exit status ("### exit N"). Only the machine's own paths are rewritten, to the shape of a
# hosted runner's, so that no real directory name reaches the repo. The committed fixture was made
# with the Go that CI installs (hack/go-pins.txt), and its first line says which one.
#
# The fixture is a record of real tool output, not something to regenerate casually: the test in
# problem_matcher_test.go lists by hand which of its lines the matcher must annotate. Re-run this when
# a new Go release changes the format, and the hand-written list will show what moved.
set -euo pipefail

export GOTOOLCHAIN=local GOFLAGS= GOWORK=off GO111MODULE=on
work=${1:?usage: capture-go-output.sh WORKDIR > go-tool-output.txt}
mod="$(mkdir -p "$work" && cd "$work" && pwd)/repo"
mkdir "$mod" # fails when WORKDIR already holds a run, so one run never overwrites another
mkdir "$mod"/{compile,syntax,vetp,tbuild,dupbuild,badbuild,mixbuild,logs,panics}
cd "$mod"

printf 'module example.com/fx\n\ngo 1.26.0\n' > go.mod

# Type errors: an unused import, an unused variable and an undefined name.
cat > compile/a.go <<'EOF'
package compile

import "os"

func F() int {
	unused := 1
	return Foo
}
EOF

# A syntax error.
cat > syntax/b.go <<'EOF'
package syntax

func G( {
}
EOF

# Compiles, but go vet finds three problems: a copied lock twice and a wrong printf argument count.
cat > vetp/c.go <<'EOF'
package vetp

import (
	"fmt"
	"sync"
)

func H(n int) string {
	var mu sync.Mutex
	cp := mu
	_ = cp
	return fmt.Sprintf("%d %s", n)
}
EOF

# A package that builds, with a test file that does not.
cat > tbuild/d.go <<'EOF'
package tbuild

func D() int { return 1 }
EOF
cat > tbuild/d_test.go <<'EOF'
package tbuild

import "testing"

func TestD(t *testing.T) {
	if D() != Missing {
		t.Fatal("x")
	}
}
EOF

# Build constraints. Two //go:build lines, and one that does not parse, are package-loading errors
# that name the file and no line. +build lines that disagree with the //go:build line are a vet error
# with a line and column.
cat > dupbuild/x.go <<'EOF'
//go:build linux
//go:build amd64

package dupbuild
EOF
cat > badbuild/y.go <<'EOF'
//go:build linux &&

package badbuild
EOF
cat > mixbuild/z.go <<'EOF'
//go:build linux
// +build darwin

package mixbuild
EOF

# Test log lines: one that cites a source file later in its text, as the rehearsal's ABSENCE lines do,
# and one that fails.
cat > logs/l_test.go <<'EOF'
package logs

import "testing"

func TestAbsenceLogPasses(t *testing.T) {
	t.Logf("ABSENCE not raised (first read after the controller restart): hpcburst_events_raised_total{controller=%q}: not raised: 0 at both reads; quiet: TunnelOrphaned has one source, the tunnel-state reader (cmd/hpcburst-controller, tunnel_state.go: readOnce, then reportOrphans)", "lifecycle")
	t.Log("a plain line")
}

func TestFails(t *testing.T) {
	t.Errorf("want 1, got %d", 2)
}
EOF

# A panic, which prints a goroutine trace with file:line and an offset.
cat > panics/p_test.go <<'EOF'
package panics

import "testing"

func TestPanics(t *testing.T) {
	panic("boom")
}
EOF

run() {
	echo "### \$ $*"
	local rc=0
	"$@" 2>&1 || rc=$?
	echo "### exit $rc"
}
in_dir() {
	local dir=$1
	shift
	echo "### \$ (cd $dir && $*)"
	local rc=0
	(cd "$dir" && "$@" 2>&1) || rc=$?
	echo "### exit $rc"
}

{
	echo "### $(go version)"
	run go build ./compile/
	in_dir compile go build .
	run go build ./syntax/
	run go vet ./vetp/
	run go vet ./tbuild/
	run go vet ./compile/
	run go test -count=1 ./tbuild/
	run go test -count=1 ./vetp/
	run go build ./dupbuild/
	run go vet ./dupbuild/
	run go test -count=1 ./dupbuild/
	run go build ./badbuild/
	run go vet ./mixbuild/
	run go test -count=1 -v ./logs/
	run go test -count=1 ./panics/
} | sed -e "s#$mod#/home/runner/work/repo/repo#g" -e "s#$(go env GOROOT)#/home/runner/work/_temp/go-toolchain/go#g"
