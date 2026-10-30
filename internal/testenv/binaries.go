package testenv

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	envAssets = "HPCBURST_ENVTEST_ASSETS"
	envKCM    = "HPCBURST_KUBE_CONTROLLER_MANAGER"

	// envUnpinned, set to 1, uses the binaries named by envAssets and envKCM
	// as they are, without the sha256 check. It is a development override for a
	// binary you built yourself; a run that sets it says so on stderr.
	envUnpinned = "HPCBURST_TESTENV_UNPINNED"
)

// Binaries are the paths of the executables the control plane runs.
type Binaries struct {
	Etcd, KubeAPIServer, KubeControllerManager string
}

// FindBinaries returns the control-plane binaries after hack/test-binaries.sh
// has checked their sha256. The script always runs, also when envAssets and
// envKCM name the locations: they say where to look, not what to trust. A
// binary whose hash differs from the pin is an error. Every path is also
// checked to be an executable file.
//
// With envUnpinned set to 1 the script is skipped and the two variables must
// both be set; the binaries are then used unverified.
func FindBinaries() (Binaries, error) {
	assets, kcm := os.Getenv(envAssets), os.Getenv(envKCM)
	if os.Getenv(envUnpinned) == "1" {
		if assets == "" || kcm == "" {
			return Binaries{}, fmt.Errorf("%s=1 needs both %s and %s", envUnpinned, envAssets, envKCM)
		}
		fmt.Fprintf(os.Stderr, "testenv: %s=1: the control-plane binaries are used WITHOUT the sha256 check\n", envUnpinned)
	} else {
		out, err := runScript()
		if err != nil {
			return Binaries{}, err
		}
		assets, kcm = out[envAssets], out[envKCM]
		if assets == "" || kcm == "" {
			return Binaries{}, fmt.Errorf("hack/test-binaries.sh printed no %s or %s", envAssets, envKCM)
		}
	}
	b := Binaries{
		Etcd:                  filepath.Join(assets, "etcd"),
		KubeAPIServer:         filepath.Join(assets, "kube-apiserver"),
		KubeControllerManager: kcm,
	}
	for _, p := range []string{b.Etcd, b.KubeAPIServer, b.KubeControllerManager} {
		fi, err := os.Stat(p)
		if err != nil {
			return Binaries{}, fmt.Errorf("control-plane binary: %w", err)
		}
		if fi.IsDir() || fi.Mode()&0o111 == 0 {
			return Binaries{}, fmt.Errorf("control-plane binary %s is not an executable file", p)
		}
	}
	return b, nil
}

// runScript runs hack/test-binaries.sh and parses its KEY=VALUE lines.
func runScript() (map[string]string, error) {
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("bash", filepath.Join(root, "hack", "test-binaries.sh"))
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("hack/test-binaries.sh failed (a binary that does not match its pinned sha256 is never used; %s=1 with %s and %s is the explicit unpinned override): %w\n%s",
			envUnpinned, envAssets, envKCM, err, strings.TrimSpace(stderr.String()))
	}
	out := map[string]string{}
	sc := bufio.NewScanner(&stdout)
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), "="); ok {
			out[k] = v
		}
	}
	return out, sc.Err()
}

// repoRoot walks up from the working directory to the directory with go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above the working directory")
		}
		dir = parent
	}
}
