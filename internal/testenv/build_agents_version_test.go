package testenv

// hack/build-agents.sh --version VERSION (issue #469): what it gives the go command, and what it refuses,
// with the go shim of build_agents_env_test.go. The real builds' bytes and -version are in
// build_agents_version_build_test.go.

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// buildArgs is the shim's ARGS of one build: the script's flags, then -o, the stage file and the package.
// The version, when there is one, is the one -ldflags, and its value is one argument (ARGC counts it).
var (
	argsWithoutVersion = regexp.MustCompile(`^build -trimpath -buildvcs=false -o \S+/\.build\.[^/ ]+/hb-agent-(x86_64|aarch64) \./cmd/hb-agent$`)
	argsWithVersion    = func(v string) *regexp.Regexp {
		return regexp.MustCompile(`^build -trimpath -buildvcs=false -ldflags -X main\.version=` + regexp.QuoteMeta(v) + ` -o \S+/\.build\.[^/ ]+/hb-agent-(x86_64|aarch64) \./cmd/hb-agent$`)
	}
)

// With --version, every build gets -ldflags "-X main.version=VERSION" as its one linker flag, the value
// one argument, and the caller's own -ldflags in GOFLAGS is still dropped; the output names the version.
// Without it, no build gets -ldflags at all, as before the option. A tag and both shapes of a
// pseudo-version are versions.
func TestBuildAgentsGivesTheLinkerTheVersionAndNothingElse(t *testing.T) {
	requireGNU(t)
	for _, v := range []string{"v0.1.0-alpha.1", "v0.0.0-20261112114635-dfa10bb29622", "v0.1.0-alpha.1.0.20261112114635-dfa10bb29622", "v1.2.3"} {
		t.Run(v, func(t *testing.T) {
			root, env, log := shimRoot(t, goModOK)
			out, code := runBuildAgents(t, root, []string{"--version", v, t.TempDir()},
				append(env, "GOFLAGS=-p=2 -ldflags=-X=main.version=from-the-caller")...)
			if code != 0 {
				t.Fatalf("exit %d:\n%s", code, out)
			}
			builds := shimBuilds(t, log)
			if len(builds) != 2 {
				t.Fatalf("%d go build calls, want one for each cpuArch", len(builds))
			}
			for _, b := range builds {
				// build -trimpath -buildvcs=false -ldflags "-X main.version=V" -o OUT ./cmd/hb-agent
				if !argsWithVersion(v).MatchString(b["ARGS"]) || b["ARGC"] != "8" {
					t.Errorf("go build ran with %s arguments %q, want -ldflags with -X main.version=%s as one argument", b["ARGC"], b["ARGS"], v)
				}
				if b["GOFLAGS"] != "-mod=readonly -p=2" {
					t.Errorf("go build got GOFLAGS=%q, want the caller's -ldflags dropped", b["GOFLAGS"])
				}
			}
			for _, arch := range []string{"x86_64", "aarch64"} {
				if !regexp.MustCompile(`(?m)^built    hb-agent-` + arch + ` \(.*\) sha256 [0-9a-f]{64} \(go version .*\) version ` + regexp.QuoteMeta(v) + `$`).MatchString(out) {
					t.Errorf("the output does not name the version of hb-agent-%s:\n%s", arch, out)
				}
			}
			if !strings.Contains(out, "ignoring GOFLAGS entries that can change the bytes (only -p=N is kept): -ldflags=-X=main.version=from-the-caller") {
				t.Errorf("the script does not say it dropped the caller's -ldflags:\n%s", out)
			}
		})
	}

	t.Run("no version", func(t *testing.T) {
		root, env, log := shimRoot(t, goModOK)
		out, code := runBuildAgents(t, root, []string{t.TempDir()}, env...)
		if code != 0 {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		for _, b := range shimBuilds(t, log) {
			if !argsWithoutVersion.MatchString(b["ARGS"]) || b["ARGC"] != "6" {
				t.Errorf("go build ran with %s arguments %q, want no -ldflags", b["ARGC"], b["ARGS"])
			}
		}
		// A build line ends at the go version, with no version after it.
		if regexp.MustCompile(`(?m)^(built|ok) .*\) version \S+$`).MatchString(out) {
			t.Errorf("the output names a version without --version:\n%s", out)
		}
	})
}

// A version that is not a Go version is refused before the go command is called at all, and nothing
// is written to DIR: no tag shape, build metadata, anything a shell or the linker would split or read.
func TestBuildAgentsRefusesAVersionThatIsNotAGoVersion(t *testing.T) {
	requireGNU(t)
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"no value", []string{"--version"}, "--version needs a value"},
		{"an empty value", []string{"--version", "", "DIR"}, "--version needs a value"},
		{"no v", []string{"--version", "0.1.0", "DIR"}, "is not a Go version"},
		{"two numbers", []string{"--version", "v0.1", "DIR"}, "is not a Go version"},
		{"a leading zero", []string{"--version", "v01.0.0", "DIR"}, "is not a Go version"},
		{"build metadata", []string{"--version", "v0.1.0+build.1", "DIR"}, "is not a Go version"},
		{"an empty pre-release", []string{"--version", "v0.1.0-", "DIR"}, "is not a Go version"},
		{"an empty pre-release field", []string{"--version", "v0.1.0-alpha..1", "DIR"}, "is not a Go version"},
		{"a space and another flag", []string{"--version", "v0.1.0 -X main.other=x", "DIR"}, "is not a Go version"},
		{"a semicolon", []string{"--version", "v0.1.0;id", "DIR"}, "is not a Go version"},
		{"a quote", []string{"--version", "v0.1.0'", "DIR"}, "is not a Go version"},
		{"a command substitution", []string{"--version", "v0.1.0$(id)", "DIR"}, "is not a Go version"},
		{"a newline", []string{"--version", "v0.1.0\nv0.2.0", "DIR"}, "is not a Go version"},
		{"the default", []string{"--version", "dev", "DIR"}, "is not a Go version"},
	} {
		t.Run(c.name, func(t *testing.T) {
			root, env, log := shimRoot(t, goModOK)
			dir := t.TempDir()
			args := append([]string(nil), c.args...)
			for i, a := range args {
				if a == "DIR" {
					args[i] = dir
				}
			}
			out, code := runBuildAgents(t, root, args, env...)
			if code != 1 || !strings.Contains(out, c.want) || !strings.Contains(out, "nothing was built") {
				t.Errorf("exit %d, want 1 with %q and \"nothing was built\":\n%s", code, c.want, out)
			}
			if _, err := os.Stat(log + ".calls"); err == nil {
				b, _ := os.ReadFile(log + ".calls")
				t.Errorf("the go command was called: %q", b)
			}
			if got := dirNames(t, dir); len(got) != 0 {
				t.Errorf("DIR holds %v after a refusal", got)
			}
		})
	}
}
