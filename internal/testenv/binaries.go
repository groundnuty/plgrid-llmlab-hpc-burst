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
)

// Binaries are the paths of the executables the control plane runs.
type Binaries struct {
	Etcd, KubeAPIServer, KubeControllerManager string
}

// FindBinaries reads the binary locations from the environment and, when
// they are not both set, from hack/test-binaries.sh. Every path is checked to
// be an executable file.
func FindBinaries() (Binaries, error) {
	assets, kcm := os.Getenv(envAssets), os.Getenv(envKCM)
	if assets == "" || kcm == "" {
		out, err := runScript()
		if err != nil {
			return Binaries{}, err
		}
		for k, v := range out {
			switch k {
			case envAssets:
				if assets == "" {
					assets = v
				}
			case envKCM:
				if kcm == "" {
					kcm = v
				}
			}
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
		return nil, fmt.Errorf("hack/test-binaries.sh failed (set %s and %s to use your own binaries): %w\n%s",
			envAssets, envKCM, err, strings.TrimSpace(stderr.String()))
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
