package launcher

import (
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The agent builds a Provider's controller stages for an Allocation are files of the host's
// agent-builds directory, one pair for each cpuArch a partition of some PlatformProfile has: the
// agent build hb-agent-<cpuArch> (hack/build-agents.sh) and the frpc build frpc-<cpuArch>, whose
// sha256 must be the pin of frp-pins.txt (hack/fetch-frp.sh --agent-builds). Without them the
// first submission is refused. up and status check them from the PlatformProfiles in the API
// server (#158), so that the refusal comes at the start and not at the first submission.

// BuildFile is one build file of a cpuArch.
type BuildFile struct {
	Name    string `json:"name"`
	Present bool   `json:"present"`
	// SHA256 is the digest found. For the agent build it is the agentDigest the lifecycle
	// controller will set; nothing pins it, so the file is judged by being there.
	SHA256 string `json:"sha256,omitempty"`
	// Expected is the pinned digest (frpc only; the agent build has none).
	Expected string `json:"expected_sha256,omitempty"`
	// Machine is the ELF machine type of the agent build (EM_X86_64, EM_AARCH64), which must be the
	// one its cpuArch names: a build for another architecture under the right name is the failure
	// that the digest cannot show. Detail says what is wrong with a file that is there but not OK
	// for a reason other than its digest.
	Machine string `json:"machine,omitempty"`
	Detail  string `json:"detail,omitempty"`
	OK      bool   `json:"ok"`
}

// ArchBuilds is the check of one cpuArch some partition has.
type ArchBuilds struct {
	CPUArch    string   `json:"cpu_arch"`
	Partitions []string `json:"partitions"` // profile/partition, sorted
	Agent      BuildFile
	Frpc       BuildFile
}

// BuildProblem is one thing wrong with the builds, and what fixes it.
type BuildProblem struct {
	CPUArch    string   `json:"cpu_arch"`
	Partitions []string `json:"partitions"`
	What       string   `json:"what"`
	Fix        string   `json:"fix"`
}

func (p BuildProblem) String() string {
	return fmt.Sprintf("cpuArch %s (%s): %s; fix: %s", p.CPUArch, strings.Join(p.Partitions, ", "), p.What, p.Fix)
}

// AgentBuilds is the report of the builds check. Checked is false when the PlatformProfiles were
// not read, and Detail says why; with Checked true, Problems is empty exactly when every build
// is in place.
type AgentBuilds struct {
	Dir      string         `json:"dir"`
	Checked  bool           `json:"checked"`
	Detail   string         `json:"detail,omitempty"`
	Profiles int            `json:"profiles"`
	Archs    []ArchBuilds   `json:"archs,omitempty"`
	Problems []BuildProblem `json:"problems,omitempty"`
}

// AgentBuildsError is the error of an up that left the host running while its agent builds are
// missing or do not match (the host is up; the operator fixes the builds and runs status).
type AgentBuildsError struct{ Builds *AgentBuilds }

func (e *AgentBuildsError) Error() string {
	if !e.Builds.Checked {
		return "the agent builds could not be checked: " + e.Builds.Detail
	}
	var b strings.Builder
	fmt.Fprintf(&b, "the agent builds in %s are missing or do not match, so the first submission would be refused:", e.Builds.Dir)
	for _, p := range e.Builds.Problems {
		b.WriteString("\n  " + p.String())
	}
	return b.String()
}

// elfMachineOf is the ELF machine type of a build for each cpuArch. It is the one fact the agent
// build's file is judged by beyond being there; TestElfMachineOfCoversEveryCPUArch keeps its keys
// the cpuArchs of cpuArchOf, so a cpuArch added there has to be given its machine here.
var elfMachineOf = map[string]elf.Machine{"x86_64": elf.EM_X86_64, "aarch64": elf.EM_AARCH64}

// CPUArchs lists the cpuArch values builds exist for, sorted: the values of cpuArchOf, the one map
// of the architecture names (hack/cpu-arch.sh is the bash copy).
func CPUArchs() []string {
	out := make([]string, 0, len(cpuArchOf))
	for _, v := range cpuArchOf {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// partitionArch is a partition and the cpuArch of its accelerator class.
type partitionArch struct{ Profile, Partition, CPUArch string }

var platformProfiles = v1alpha1.GroupVersion.WithResource("platformprofiles")

// listPartitionArchs reads every PlatformProfile of the API server and returns how many there are
// and the cpuArch of each partition. A server without the PlatformProfile CRD has no profiles.
func listPartitionArchs(ctx context.Context, cfg *rest.Config) (int, []partitionArch, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return 0, nil, err
	}
	list, err := dyn.Resource(platformProfiles).List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		return 0, nil, nil // the CRD is not installed: no profile has been applied
	}
	if err != nil {
		return 0, nil, err
	}
	var refs []partitionArch
	for _, item := range list.Items {
		var p v1alpha1.PlatformProfile
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, &p); err != nil {
			return 0, nil, fmt.Errorf("PlatformProfile %s: %w", item.GetName(), err)
		}
		for _, part := range p.Spec.Partitions {
			refs = append(refs, partitionArch{Profile: p.Name, Partition: part.Name, CPUArch: part.CPUArch})
		}
	}
	return len(list.Items), refs, nil
}

// agentBuildPins gives the pinned frpc digest of each cpuArch: frp-pins.txt's, for the frp release
// the host runs. A test replaces it, since no test can make an frpc with the pinned digest.
var agentBuildPins = func() (map[string]string, error) { return FrpcDigests(DefaultFrpVersion) }

// CheckAgentBuilds reads the PlatformProfiles of the API server cfg reaches and checks the builds
// in dir for every cpuArch their partitions have, against the frpc digests frp-pins.txt pins for
// DefaultFrpVersion (agentBuildPins). An error means the check could not run (the profiles could not be read);
// builds that are missing or do not match are the report's Problems, not an error.
func CheckAgentBuilds(ctx context.Context, cfg *rest.Config, dir string) (*AgentBuilds, error) {
	pins, err := agentBuildPins()
	if err != nil {
		return nil, err
	}
	n, refs, err := listPartitionArchs(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("read the PlatformProfiles: %w", err)
	}
	return checkAgentBuilds(n, refs, dir, pins), nil
}

// checkAgentBuilds judges the builds in dir for the cpuArchs of refs. pins gives the pinned frpc
// digest of each cpuArch; one without a pin is a problem of its own.
func checkAgentBuilds(profiles int, refs []partitionArch, dir string, pins map[string]string) *AgentBuilds {
	rep := &AgentBuilds{Dir: dir, Checked: true, Profiles: profiles}
	byArch := map[string][]string{}
	for _, r := range refs {
		byArch[r.CPUArch] = append(byArch[r.CPUArch], r.Profile+"/"+r.Partition)
	}
	if len(byArch) == 0 {
		rep.Detail = "no partition in any PlatformProfile: nothing to build for"
		return rep
	}
	known := CPUArchs()
	var unknown []string
	for arch := range byArch {
		sort.Strings(byArch[arch])
		if !slices.Contains(known, arch) {
			unknown = append(unknown, arch)
		}
	}
	for _, arch := range known {
		parts, ok := byArch[arch]
		if !ok {
			continue
		}
		a := ArchBuilds{CPUArch: arch, Partitions: parts}
		a.Agent = checkAgentFile(agentapi.AgentBuildName(arch), dir, arch)
		a.Frpc = checkBuildFile(agentapi.FrpcBuildName(arch), dir, pins[arch])
		rep.Archs = append(rep.Archs, a)
		problem := func(what, fix string) {
			rep.Problems = append(rep.Problems, BuildProblem{CPUArch: arch, Partitions: parts, What: what, Fix: fix})
		}
		if !a.Agent.OK {
			problem(a.Agent.problem(dir), "hack/build-agents.sh "+dir+" "+arch)
		}
		switch {
		case pins[arch] == "":
			problem("frp-pins.txt pins no frpc for it", "pin frp for the architecture in internal/launcher/frp-pins.txt")
		case !a.Frpc.OK:
			problem(a.Frpc.problem(dir), "hack/fetch-frp.sh --agent-builds "+dir)
		}
	}
	sort.Strings(unknown)
	for _, arch := range unknown {
		what := fmt.Sprintf("%q is not a cpuArch with builds (%s); no hb-agent or frpc build exists for it", arch, strings.Join(known, ", "))
		fix := "give the partition the kernel's machine name for its cpuArch, one of " + strings.Join(known, ", ")
		if kernel, ok := cpuArchOf[arch]; ok {
			fix = fmt.Sprintf("give the partition cpuArch %s: %s is Go's and frp's name for it, and the profiles use the kernel's", kernel, arch)
		}
		rep.Problems = append(rep.Problems, BuildProblem{CPUArch: arch, Partitions: byArch[arch], What: what, Fix: fix})
	}
	return rep
}

// problem says what is wrong with a build file that is not OK.
func (f BuildFile) problem(dir string) string {
	switch {
	case !f.Present:
		return fmt.Sprintf("%s is missing from %s", f.Name, dir)
	case f.SHA256 == "":
		return fmt.Sprintf("%s in %s cannot be read as a file", f.Name, dir)
	case f.Detail != "":
		return fmt.Sprintf("%s in %s %s", f.Name, dir, f.Detail)
	default:
		return fmt.Sprintf("%s in %s has sha256 %s, but the pin (frp-pins.txt) says %s", f.Name, dir, f.SHA256, f.Expected)
	}
}

// checkBuildFile reads dir/name. With a non-empty expected digest the file is OK only when its
// sha256 is that digest; without one, when it is a readable regular file.
func checkBuildFile(name, dir, expected string) BuildFile {
	f := BuildFile{Name: name, Expected: expected}
	sum, err := sha256Of(filepath.Join(dir, name))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return f
	case err != nil:
		f.Present = true
		return f
	}
	f.Present, f.SHA256 = true, sum
	f.OK = expected == "" || sum == expected
	return f
}

// checkAgentFile is checkBuildFile for the agent build of a cpuArch, which has no pinned digest and
// is judged by its ELF machine type instead: the file must be an ELF file for the machine the
// cpuArch names.
func checkAgentFile(name, dir, cpuArch string) BuildFile {
	f := checkBuildFile(name, dir, "")
	if !f.Present || f.SHA256 == "" {
		return f // missing, or not a readable file: already said
	}
	want, ok := elfMachineOf[cpuArch]
	if !ok {
		f.OK, f.Detail = false, "has no known ELF machine type for cpuArch "+cpuArch
		return f
	}
	ef, err := elf.Open(filepath.Join(dir, name))
	if err != nil {
		f.OK, f.Detail = false, "is not an ELF file ("+err.Error()+")"
		return f
	}
	defer ef.Close()
	f.Machine = ef.Machine.String()
	if ef.Machine != want {
		f.OK, f.Detail = false, fmt.Sprintf("is an ELF file for %s, not for %s as %s is", ef.Machine, want, cpuArch)
	}
	return f
}

// sha256Of is the sha256 of a regular file, read as a stream; a directory or an unreadable file is
// an error.
func sha256Of(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", path)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
