package quickstart

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The quickstart sets two directories and passes them to every program that needs one (PR #170
// review, B2). The launcher resolves its own defaults from XDG_STATE_HOME, XDG_CACHE_HOME and
// HPCBURST_BIN_DIR, and the scripts take a directory argument, so a guide that hard-codes a path
// or leaves one to a default can send the build step, the launcher and kubectl to different
// places. These tests compare the guide with the launcher, asked for its answer.

// dirsBlock is the shell block that defines STATE and BIN.
func dirsBlock(t *testing.T) block {
	t.Helper()
	return only(t, "block that defines STATE and BIN", func(b block) bool {
		return regexp.MustCompile(`(?m)^STATE=`).MatchString(b.text) && regexp.MustCompile(`(?m)^BIN=`).MatchString(b.text)
	})
}

// docDirs runs the guide's definition lines under an environment and returns the values they give.
func docDirs(t *testing.T, env []string) dirs {
	t.Helper()
	cmd := exec.Command("bash", "-c", "set -eu\n"+dirsBlock(t).text+"\nprintf '%s\\n%s\\n' \"$STATE\" \"$BIN\"")
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("the guide's directory lines fail under %v: %v", env, err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("the guide's directory lines printed %q", out)
	}
	return dirs{state: lines[0], bin: lines[1]}
}

// launcherDirs asks the real hpcburst binary where it looks when it is given no directory flag:
// `status` names the state directory it reads, and `up` names the k3s file it cannot open, which
// sits at <bin dir>/k3s-<version>/k3s.
func launcherDirs(t *testing.T, env []string) dirs {
	t.Helper()
	out, _ := run(t, env, hpcburst(t), "status", "--json", "--no-api")
	var st struct {
		StateDir string `json:"state_dir"`
	}
	if err := json.Unmarshal([]byte(out), &st); err != nil || st.StateDir == "" {
		t.Fatalf("hpcburst status --json printed no state_dir under %v: %v\n%s", env, err, out)
	}
	out, code := run(t, env, hpcburst(t), "up")
	m := regexp.MustCompile(`open (\S+)/k3s-[0-9.]+/k3s: no such file`).FindStringSubmatch(out)
	if code == 0 || m == nil {
		t.Fatalf("hpcburst up under %v with an empty binary directory: exit %d and no missing-k3s path in\n%s", env, code, out)
	}
	return dirs{state: st.StateDir, bin: m[1]}
}

// With each combination of the variables the launcher reads, set to a path or empty or unset, the
// guide's two lines give the directories the launcher uses when it gets no flag.
func TestDirectoryLinesResolveLikeTheLauncher(t *testing.T) {
	root, err := os.MkdirTemp("", "qsd")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	home := filepath.Join(root, "h")
	// Each variable is unset, empty, or a path of its own.
	values := func(name string) []string {
		return []string{"", name + "=", name + "=" + filepath.Join(root, strings.ToLower(name))}
	}
	n := 0
	for _, xs := range values("XDG_STATE_HOME") {
		for _, xc := range values("XDG_CACHE_HOME") {
			for _, bd := range values("HPCBURST_BIN_DIR") {
				env := baseEnv(home)
				for _, kv := range []string{xs, xc, bd} {
					if kv != "" {
						env = append(env, kv)
					}
				}
				n++
				t.Run(strings.Join(env[2:], " "), func(t *testing.T) {
					got, want := docDirs(t, env), launcherDirs(t, env)
					if got != want {
						t.Errorf("the guide's lines give %+v; the launcher uses %+v", got, want)
					}
				})
			}
		}
	}
	if n != 27 {
		t.Fatalf("ran %d combinations, want 27", n)
	}
}

// pathOf returns the path an os error names.
func pathOf(t *testing.T, err error) string {
	t.Helper()
	var pe *fs.PathError
	if !errors.As(err, &pe) {
		t.Fatalf("want an error that names a file, got %v", err)
	}
	return pe.Path
}

// Every command of the quickstart that takes a state or binary directory gets the guide's own
// STATE or BIN, and the file layout it implies is the launcher's: where it looks for k3s and frps,
// and where the controller stages the agent builds from.
func TestCommandsPassTheDirectoriesTheLauncherUses(t *testing.T) {
	d, home := testDirs(t)
	_, errK3s := launcher.ResolveBinary(d.bin, launcher.DefaultVersion)
	k3s := pathOf(t, errK3s)
	_, errFrps := launcher.ResolveFrps(d.bin, launcher.DefaultFrpVersion)
	frpsDir := filepath.Dir(pathOf(t, errFrps))
	agentBuilds := agentapi.Host{StateDir: d.state}.AgentBuildsDir()

	seen := map[string]int{}
	flagValue := func(c command, name string) (string, bool) {
		for i, w := range c.words {
			if w == "--"+name && i+1 < len(c.words) {
				return c.words[i+1], true
			}
		}
		return "", false
	}
	for _, c := range repoCommands(t, d, home) {
		switch c.words[0] {
		case "bin/hpcburst":
			sub := c.words[1]
			seen["hpcburst "+sub]++
			// A subcommand that takes a directory flag gets it from the guide.
			for _, f := range []struct{ flag, want string }{{"state-dir", d.state}, {"bin-dir", d.bin}} {
				if !helpFlags(t, home, sub)[f.flag] {
					continue
				}
				if got, ok := flagValue(c, f.flag); !ok || got != f.want {
					t.Errorf("%q: hpcburst %s takes --%s, and the guide passes %q; want %q", c.line, sub, f.flag, got, f.want)
				}
			}
		case "hack/fetch-k3s.sh":
			seen["fetch-k3s"]++
			if len(c.words) != 2 || c.words[1] != d.bin {
				t.Errorf("%q: the binary directory is %q; the script takes it as its only argument", c.line, d.bin)
			}
			if filepath.Dir(filepath.Dir(k3s)) != d.bin {
				t.Fatalf("the launcher looks for k3s at %s, outside the binary directory %s", k3s, d.bin)
			}
		case "hack/fetch-frp.sh":
			if len(c.words) == 3 && c.words[1] == "--agent-builds" {
				seen["fetch-frp agent-builds"]++
				if c.words[2] != agentBuilds {
					t.Errorf("%q: the controller stages frpc from %q", c.line, agentBuilds)
				}
			} else {
				seen["fetch-frp frps"]++
				if len(c.words) != 2 || c.words[1] != frpsDir {
					t.Errorf("%q: the launcher looks for frps in %q", c.line, frpsDir)
				}
			}
		case "hack/build-agents.sh":
			seen["build-agents"]++
			if len(c.words) < 2 || c.words[1] != agentBuilds {
				t.Errorf("%q: the controller stages the agent builds from %q", c.line, agentBuilds)
			}
		}
	}
	for _, want := range []string{"hpcburst up", "hpcburst status", "hpcburst down", "fetch-k3s", "fetch-frp agent-builds", "fetch-frp frps", "build-agents"} {
		if seen[want] == 0 {
			t.Errorf("the quickstart has no %s command", want)
		}
	}

	// The kubectl alias reads the kubeconfig `up` writes into the state directory and runs the
	// k3s the launcher runs.
	alias := regexp.MustCompile(`(?m)^alias hbk='(.*)'$`).FindStringSubmatch(readRepoFile(t, "docs/quickstart.md"))
	if alias == nil {
		t.Fatal("the quickstart defines no alias hbk")
	}
	w := shellWords(t, alias[1], d, home)
	if len(w) != 3 || w[0] != "KUBECONFIG="+filepath.Join(d.state, "kubeconfig") || w[1] != k3s || w[2] != "kubectl" {
		t.Errorf("the alias expands to %q; want the kubeconfig in the state directory (internal/launcher/state.go, layout.kubeconfig) and %s", w, k3s)
	}
}
