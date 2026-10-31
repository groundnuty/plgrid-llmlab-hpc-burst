package agent

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// CPUArch returns the node's machine name as the kernel gives it (uname -m):
// aarch64 or x86_64, the strings the platform profile uses
// (agentapi.RegisterRequest.CPUArch). It is the node's, not the binary's
// GOARCH: an x86_64 build under emulation would still report the node.
func CPUArch() (string, error) {
	var u syscall.Utsname
	if err := syscall.Uname(&u); err != nil {
		return "", err
	}
	return utsString(u.Machine[:]), nil
}

// utsString reads a NUL-terminated utsname field, whose element type is int8
// on amd64 and uint8 on arm64.
func utsString[T int8 | uint8](f []T) string {
	b := make([]byte, 0, len(f))
	for _, c := range f {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	return string(b)
}

// ListGPUs asks nvidia-smi for the GPUs the job can use. It is best effort: a
// node without nvidia-smi, or one where it fails, gives an empty list and the
// error, and the agent registers without GPUs (the controller judges).
func ListGPUs(ctx context.Context, nvidiaSMI string) ([]agentapi.GPU, error) {
	if nvidiaSMI == "" {
		nvidiaSMI = "nvidia-smi"
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, nvidiaSMI, "--query-gpu=index,name,memory.total", "--format=csv,noheader,nounits").Output()
	if err != nil {
		return nil, err
	}
	return parseGPUs(out)
}

// parseGPUs reads nvidia-smi's CSV: index, name, memory in MiB.
func parseGPUs(out []byte) ([]agentapi.GPU, error) {
	r := csv.NewReader(bytes.NewReader(out))
	r.TrimLeadingSpace = true
	recs, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	var gpus []agentapi.GPU
	for _, rec := range recs {
		if len(rec) != 3 {
			continue
		}
		idx, err := strconv.Atoi(strings.TrimSpace(rec[0]))
		if err != nil {
			continue
		}
		g := agentapi.GPU{Index: idx, Name: strings.TrimSpace(rec[1])}
		if mib, err := strconv.ParseInt(strings.TrimSpace(rec[2]), 10, 64); err == nil {
			g.MemoryMiB = mib
		}
		gpus = append(gpus, g)
	}
	return gpus, nil
}

// Hostname is the node's host name, as Registration.Node.
func Hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}
