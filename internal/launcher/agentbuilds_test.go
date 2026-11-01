package launcher

// Tests of the agent-builds check (#158). The expected digests are computed here with crypto/sha256
// from bytes the test chose, and the profiles are written as the JSON an API server returns, so the
// code under test is judged against neither its own hashing nor its own types. A profile that lists
// only the host's own cpuArch would prove nothing, so every case has an aarch64 partition beside an
// x86_64 one, and the control case shows the check follows the profiles and not the machine.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
)

// platformProfilesPath is where the API server lists PlatformProfiles (group, version, plural).
const platformProfilesPath = "/apis/hpcburst.groundnuty.github.io/v1alpha1/platformprofiles"

func hexSum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

var (
	frpcX86 = []byte("an example frpc for x86_64\n")
	frpcArm = []byte("an example frpc for aarch64\n")
	testPin = map[string]string{"x86_64": hexSum(frpcX86), "aarch64": hexSum(frpcArm)}
)

// useTestPins makes the check judge frpc against testPin for the rest of the test.
func useTestPins(t *testing.T) {
	t.Helper()
	old := agentBuildPins
	agentBuildPins = func() (map[string]string, error) { return testPin, nil }
	t.Cleanup(func() { agentBuildPins = old })
}

// buildsIn makes a directory holding the given files.
func buildsIn(t *testing.T, files map[string][]byte) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "agent-builds")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// both is a profile with a GH200 partition (aarch64) and an A100 one (x86_64), as on Helios and Athena.
var both = []partitionArch{
	{"example-batch", "example-gpu", "aarch64"},
	{"example-batch", "example-cpu", "x86_64"},
}

// The machine numbers of ELF's e_machine, written out here (EM_X86_64 and EM_AARCH64), not taken
// from debug/elf, which the code under test reads them with.
const (
	machX86_64  = 62
	machAarch64 = 183
)

// fakeELF is the 64-byte ELF64 header of a little-endian executable for the machine, and then tag: a
// file debug/elf opens and a loader would not run, whose bytes differ with the tag.
func fakeELF(machine uint16, tag string) []byte {
	h := make([]byte, 64)
	copy(h, "\x7fELF")
	h[4], h[5], h[6] = 2, 1, 1                     // 64-bit, little-endian, version 1
	binary.LittleEndian.PutUint16(h[16:], 2)       // ET_EXEC
	binary.LittleEndian.PutUint16(h[18:], machine) // e_machine
	binary.LittleEndian.PutUint32(h[20:], 1)       // e_version
	binary.LittleEndian.PutUint16(h[52:], 64)      // e_ehsize
	binary.LittleEndian.PutUint16(h[54:], 56)      // e_phentsize
	binary.LittleEndian.PutUint16(h[58:], 64)      // e_shentsize
	return append(h, tag...)
}

var allBuilds = map[string][]byte{
	"hb-agent-x86_64": fakeELF(machX86_64, " an example agent for x86_64\n"), "frpc-x86_64": frpcX86,
	"hb-agent-aarch64": fakeELF(machAarch64, " an example agent for aarch64\n"), "frpc-aarch64": frpcArm,
}

func problemOf(t *testing.T, rep *AgentBuilds, cpuArch string) []BuildProblem {
	t.Helper()
	var out []BuildProblem
	for _, p := range rep.Problems {
		if p.CPUArch == cpuArch {
			out = append(out, p)
		}
	}
	return out
}

// archOf finds the report's entry for a cpuArch.
func archOf(t *testing.T, rep *AgentBuilds, cpuArch string) ArchBuilds {
	t.Helper()
	for _, a := range rep.Archs {
		if a.CPUArch == cpuArch {
			return a
		}
	}
	t.Fatalf("no entry for %s in %+v", cpuArch, rep.Archs)
	return ArchBuilds{}
}

func TestAgentBuildsAllPresent(t *testing.T) {
	dir := buildsIn(t, allBuilds)
	rep := checkAgentBuilds(1, both, dir, testPin)
	if !rep.Checked || len(rep.Problems) != 0 {
		t.Fatalf("problems with every build in place: %+v", rep.Problems)
	}
	if len(rep.Archs) != 2 || rep.Archs[0].CPUArch != "aarch64" || rep.Archs[1].CPUArch != "x86_64" {
		t.Fatalf("archs %+v, want aarch64 then x86_64 (sorted)", rep.Archs)
	}
	for _, a := range rep.Archs {
		if !a.Agent.OK || !a.Frpc.OK || !a.Agent.Present || a.Frpc.Expected != testPin[a.CPUArch] || a.Frpc.SHA256 != testPin[a.CPUArch] {
			t.Errorf("%s: agent %+v, frpc %+v", a.CPUArch, a.Agent, a.Frpc)
		}
		// The agent build has no pin; the digest found is the agentDigest, and its ELF machine is the
		// cpuArch's (the numbers are the test's own).
		if want := map[string]string{"x86_64": "EM_X86_64", "aarch64": "EM_AARCH64"}[a.CPUArch]; a.Agent.Machine != want {
			t.Errorf("%s: agent machine %q, want %q", a.CPUArch, a.Agent.Machine, want)
		}
		if want := hexSum(allBuilds["hb-agent-"+a.CPUArch]); a.Agent.SHA256 != want || a.Agent.Expected != "" {
			t.Errorf("%s: agent sha256 %q (expected %q), want found %s and none expected", a.CPUArch, a.Agent.SHA256, a.Agent.Expected, want)
		}
	}
	if got := strings.Join(archOf(t, rep, "aarch64").Partitions, ","); got != "example-batch/example-gpu" {
		t.Errorf("aarch64 partitions %q", got)
	}
}

// The control of the cases below: the same directory, which holds the x86_64 builds only, passes for a
// profile that has x86_64 partitions only and fails for one that adds a GH200 partition. The check
// follows the profiles, not the machine it runs on.
func TestAgentBuildsFollowTheProfiles(t *testing.T) {
	dir := buildsIn(t, map[string][]byte{"hb-agent-x86_64": allBuilds["hb-agent-x86_64"], "frpc-x86_64": frpcX86})
	if rep := checkAgentBuilds(1, both[1:], dir, testPin); len(rep.Problems) != 0 {
		t.Fatalf("x86_64 partitions only: %+v", rep.Problems)
	}
	rep := checkAgentBuilds(1, both, dir, testPin)
	if len(rep.Problems) != 2 || len(problemOf(t, rep, "aarch64")) != 2 || len(problemOf(t, rep, "x86_64")) != 0 {
		t.Fatalf("with a GH200 partition: %+v, want the two aarch64 builds and nothing for x86_64", rep.Problems)
	}
}

func TestAgentBuildsMissingAgent(t *testing.T) {
	files := map[string][]byte{}
	for k, v := range allBuilds {
		if k != "hb-agent-aarch64" {
			files[k] = v
		}
	}
	dir := buildsIn(t, files)
	rep := checkAgentBuilds(1, both, dir, testPin)
	ps := problemOf(t, rep, "aarch64")
	if len(rep.Problems) != 1 || len(ps) != 1 {
		t.Fatalf("problems %+v, want only the aarch64 agent build", rep.Problems)
	}
	p := ps[0]
	if want := "hb-agent-aarch64 is missing from " + dir; p.What != want {
		t.Errorf("what %q, want %q", p.What, want)
	}
	if want := "hack/build-agents.sh " + dir + " aarch64"; p.Fix != want {
		t.Errorf("fix %q, want %q", p.Fix, want)
	}
	if strings.Join(p.Partitions, ",") != "example-batch/example-gpu" {
		t.Errorf("partitions %v", p.Partitions)
	}
	if a := archOf(t, rep, "aarch64"); a.Agent.Present || a.Agent.OK || !a.Frpc.OK {
		t.Errorf("aarch64 agent %+v, frpc %+v", a.Agent, a.Frpc)
	}
}

func TestAgentBuildsMismatchedFrpcNamesBothDigests(t *testing.T) {
	other := []byte("a frpc nobody pinned\n")
	files := map[string][]byte{}
	for k, v := range allBuilds {
		files[k] = v
	}
	files["frpc-aarch64"] = other
	dir := buildsIn(t, files)
	rep := checkAgentBuilds(1, both, dir, testPin)
	ps := problemOf(t, rep, "aarch64")
	if len(rep.Problems) != 1 || len(ps) != 1 {
		t.Fatalf("problems %+v, want only the aarch64 frpc", rep.Problems)
	}
	for _, want := range []string{"frpc-aarch64", "sha256 " + hexSum(other), "says " + testPin["aarch64"]} {
		if !strings.Contains(ps[0].What, want) {
			t.Errorf("what %q lacks %q", ps[0].What, want)
		}
	}
	if want := "hack/fetch-frp.sh --agent-builds " + dir; ps[0].Fix != want {
		t.Errorf("fix %q, want %q", ps[0].Fix, want)
	}
	if f := archOf(t, rep, "aarch64").Frpc; f.SHA256 != hexSum(other) || f.Expected != testPin["aarch64"] || f.OK {
		t.Errorf("frpc %+v", f)
	}
	// frpc-x86_64 is the pinned one and stays fine: the mismatch is per cpuArch.
	if f := archOf(t, rep, "x86_64").Frpc; !f.OK {
		t.Errorf("x86_64 frpc %+v", f)
	}
}

// A binary for the other architecture under the right name has the right name and a digest of its own,
// which is all a file check sees: it is the ELF machine type that shows it. The control is the same
// directory with each binary under its own name.
func TestAgentBuildsAnAgentForAnotherArchitecture(t *testing.T) {
	if rep := checkAgentBuilds(1, both, buildsIn(t, allBuilds), testPin); len(rep.Problems) != 0 {
		t.Fatalf("control: %+v", rep.Problems)
	}
	files := map[string][]byte{}
	for k, v := range allBuilds {
		files[k] = v
	}
	files["hb-agent-aarch64"] = allBuilds["hb-agent-x86_64"] // an x86_64 binary under the aarch64 name
	dir := buildsIn(t, files)
	rep := checkAgentBuilds(1, both, dir, testPin)
	ps := problemOf(t, rep, "aarch64")
	if len(rep.Problems) != 1 || len(ps) != 1 {
		t.Fatalf("problems %+v, want only the aarch64 agent build", rep.Problems)
	}
	for _, want := range []string{"hb-agent-aarch64 in " + dir, "ELF file for EM_X86_64", "not for EM_AARCH64"} {
		if !strings.Contains(ps[0].What, want) {
			t.Errorf("what %q lacks %q", ps[0].What, want)
		}
	}
	if want := "hack/build-agents.sh " + dir + " aarch64"; ps[0].Fix != want {
		t.Errorf("fix %q, want %q", ps[0].Fix, want)
	}
	if a := archOf(t, rep, "aarch64"); a.Agent.OK || a.Agent.Machine != "EM_X86_64" || a.Agent.SHA256 != hexSum(allBuilds["hb-agent-x86_64"]) {
		t.Errorf("aarch64 agent %+v", a.Agent)
	}
	// And the other way round, and a file that is not an ELF file at all.
	files["hb-agent-aarch64"] = allBuilds["hb-agent-aarch64"]
	files["hb-agent-x86_64"] = []byte("#!/bin/sh\necho not a binary\n")
	rep = checkAgentBuilds(1, both, buildsIn(t, files), testPin)
	if ps := problemOf(t, rep, "x86_64"); len(rep.Problems) != 1 || len(ps) != 1 || !strings.Contains(ps[0].What, "is not an ELF file") {
		t.Errorf("a script as the x86_64 agent build: %+v", rep.Problems)
	}
}

// The ELF machine numbers of the table are ELF's (62 and 183, written out here), and the table is the one
// place a cpuArch is added: with a third architecture put into it for the length of a test, the
// enumeration, the name map, the ELF check of an agent build and the known-cpuArch test all follow, and
// the hand-written list of today's two does not. A binary for the new machine passes under its name,
// and one for another machine does not.
func TestArchTableIsTheOnePlaceAnArchitectureIsAdded(t *testing.T) {
	var machines []uint16
	for _, a := range archTable {
		machines = append(machines, uint16(a.ELFMachine))
	}
	if len(machines) != 2 || machines[0] != machX86_64 || machines[1] != machAarch64 {
		t.Errorf("the table holds the machines %v, want %d and %d", machines, machX86_64, machAarch64)
	}

	const machRISCV = 243 // EM_RISCV
	old := archTable
	archTable = append(slices.Clone(old), archInfo{GOARCH: "riscv64", CPUArch: "riscv64", ELFMachine: machRISCV})
	t.Cleanup(func() { archTable = old })

	if got := strings.Join(CPUArchs(), ","); got != "aarch64,riscv64,x86_64" {
		t.Errorf("CPUArchs %q, want aarch64,riscv64,x86_64", got)
	}
	if got := cpuArchOf()["riscv64"]; got != "riscv64" {
		t.Errorf("cpuArchOf maps riscv64 to %q", got)
	}
	refs := []partitionArch{{"example-batch", "example-risc", "riscv64"}}
	files := map[string][]byte{"hb-agent-riscv64": fakeELF(machRISCV, " an example agent for riscv64\n"), "frpc-riscv64": frpcX86}
	rep := checkAgentBuilds(1, refs, buildsIn(t, files), map[string]string{"riscv64": hexSum(frpcX86)})
	if len(rep.Problems) != 0 || len(rep.Archs) != 1 || rep.Archs[0].Agent.Machine != "EM_RISCV" {
		t.Errorf("a riscv64 partition with its builds: %+v, %+v", rep.Problems, rep.Archs)
	}
	files["hb-agent-riscv64"] = allBuilds["hb-agent-x86_64"]
	rep = checkAgentBuilds(1, refs, buildsIn(t, files), map[string]string{"riscv64": hexSum(frpcX86)})
	if ps := problemOf(t, rep, "riscv64"); len(rep.Problems) != 1 || len(ps) != 1 || !strings.Contains(ps[0].What, "ELF file for EM_X86_64") {
		t.Errorf("an x86_64 binary under the riscv64 name: %+v", rep.Problems)
	}
	// A machine changed in the table is the machine the check expects, for an architecture that was there.
	archTable = slices.Clone(old)
	archTable[0].ELFMachine = machRISCV
	rep = checkAgentBuilds(1, both[1:], buildsIn(t, map[string][]byte{"hb-agent-x86_64": allBuilds["hb-agent-x86_64"], "frpc-x86_64": frpcX86}), testPin)
	if ps := problemOf(t, rep, "x86_64"); len(rep.Problems) != 1 || len(ps) != 1 || !strings.Contains(ps[0].What, "ELF file for EM_X86_64, not for EM_RISCV") {
		t.Errorf("with x86_64 expecting EM_RISCV, an x86_64 binary: %+v", rep.Problems)
	}
	archTable = old
	if got := strings.Join(CPUArchs(), ","); got != "aarch64,x86_64" {
		t.Errorf("CPUArchs %q after the table is restored, want aarch64,x86_64", got)
	}
}

func TestAgentBuildsMissingFrpcAndMissingEverything(t *testing.T) {
	rep := checkAgentBuilds(1, both, buildsIn(t, nil), testPin)
	if len(rep.Problems) != 4 {
		t.Fatalf("problems %d (%+v), want the agent and the frpc of both cpuArchs", len(rep.Problems), rep.Problems)
	}
	var fixes []string
	for _, p := range rep.Problems {
		fixes = append(fixes, p.Fix)
	}
	got := strings.Join(fixes, "\n")
	for _, want := range []string{"build-agents.sh", "fetch-frp.sh --agent-builds"} {
		if strings.Count(got, want) != 2 {
			t.Errorf("fixes name %q %d times, want 2:\n%s", want, strings.Count(got, want), got)
		}
	}
}

func TestAgentBuildsUnknownCPUArch(t *testing.T) {
	dir := buildsIn(t, allBuilds)
	refs := append([]partitionArch{{"example-batch", "example-go-named", "amd64"}, {"example-other", "example-risc", "riscv64"}}, both...)
	rep := checkAgentBuilds(2, refs, dir, testPin)
	// The known cpuArchs are still judged, and pass.
	if len(rep.Archs) != 2 || !archOf(t, rep, "x86_64").Agent.OK || !archOf(t, rep, "aarch64").Agent.OK {
		t.Errorf("archs %+v", rep.Archs)
	}
	amd, risc := problemOf(t, rep, "amd64"), problemOf(t, rep, "riscv64")
	if len(rep.Problems) != 2 || len(amd) != 1 || len(risc) != 1 {
		t.Fatalf("problems %+v, want one for amd64 and one for riscv64", rep.Problems)
	}
	if !strings.Contains(amd[0].What, `"amd64"`) || !strings.Contains(amd[0].What, "aarch64, x86_64") {
		t.Errorf("what %q does not name the cpuArch and the known ones", amd[0].What)
	}
	// Go's and frp's name for x86_64 gets the kernel's name as the fix; riscv64 gets the list.
	if !strings.Contains(amd[0].Fix, "cpuArch x86_64") || strings.Contains(risc[0].Fix, "cpuArch x86_64:") || !strings.Contains(risc[0].Fix, "aarch64, x86_64") {
		t.Errorf("fixes %q, %q", amd[0].Fix, risc[0].Fix)
	}
	if strings.Join(amd[0].Partitions, ",") != "example-batch/example-go-named" {
		t.Errorf("partitions %v", amd[0].Partitions)
	}
}

func TestAgentBuildsNoProfilesPass(t *testing.T) {
	for name, refs := range map[string][]partitionArch{"no profiles": nil, "profiles without partitions": {}} {
		rep := checkAgentBuilds(len(refs), refs, buildsIn(t, nil), testPin)
		if !rep.Checked || len(rep.Problems) != 0 || len(rep.Archs) != 0 || rep.Detail == "" {
			t.Errorf("%s: %+v", name, rep)
		}
	}
}

func TestAgentBuildsAFileThatIsNotAFile(t *testing.T) {
	files := map[string][]byte{}
	for k, v := range allBuilds {
		files[k] = v
	}
	dir := buildsIn(t, files)
	if err := os.Remove(filepath.Join(dir, "hb-agent-x86_64")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "hb-agent-x86_64"), 0o755); err != nil {
		t.Fatal(err)
	}
	rep := checkAgentBuilds(1, both, dir, testPin)
	ps := problemOf(t, rep, "x86_64")
	if len(rep.Problems) != 1 || len(ps) != 1 || !strings.Contains(ps[0].What, "cannot be read as a file") {
		t.Fatalf("problems %+v", rep.Problems)
	}
}

func TestAgentBuildsACPUArchWithNoFrpPin(t *testing.T) {
	rep := checkAgentBuilds(1, both, buildsIn(t, allBuilds), map[string]string{"x86_64": testPin["x86_64"]})
	ps := problemOf(t, rep, "aarch64")
	if len(rep.Problems) != 1 || len(ps) != 1 || !strings.Contains(ps[0].What, "frp-pins.txt pins no frpc") {
		t.Fatalf("problems %+v", rep.Problems)
	}
}

// CPUArchs is the cpuArch column of the table, sorted.
func TestCPUArchsAreTheTablesCPUArchs(t *testing.T) {
	if got := strings.Join(CPUArchs(), ","); got != "aarch64,x86_64" {
		t.Errorf("CPUArchs %q, want aarch64,x86_64", got)
	}
}

// ---- the profiles come from the API server ----

// profilesJSON is a list of two profiles as an API server returns it: a batch profile with the two
// partitions and a managed one with none, so only the partitions' cpuArchs count.
const profilesJSON = `{"apiVersion":"hpcburst.groundnuty.github.io/v1alpha1","kind":"PlatformProfileList","metadata":{"resourceVersion":"7"},"items":[
 {"apiVersion":"hpcburst.groundnuty.github.io/v1alpha1","kind":"PlatformProfile","metadata":{"name":"example-batch"},
  "spec":{"providerKind":"batch","holdRelease":false,"partitions":[
   {"name":"example-gpu","gpuModel":"GH200","cpuArch":"aarch64"},{"name":"example-cpu","gpuModel":"none","cpuArch":"x86_64"}]}},
 {"apiVersion":"hpcburst.groundnuty.github.io/v1alpha1","kind":"PlatformProfile","metadata":{"name":"example-managed"},"spec":{"providerKind":"managed"}}]}`

// apiConfig serves the fixed answer to a list of PlatformProfiles (and 404 to anything else) and
// returns a client config for it.
func apiConfig(t *testing.T, status int, body string) *rest.Config {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != platformProfilesPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}
}

func TestListPartitionArchsReadsEveryProfile(t *testing.T) {
	n, refs, err := listPartitionArchs(context.Background(), apiConfig(t, 200, profilesJSON))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"example-batch/example-gpu": "aarch64", "example-batch/example-cpu": "x86_64"}
	if n != 2 || len(refs) != 2 {
		t.Fatalf("%d profiles, partitions %+v", n, refs)
	}
	for _, r := range refs {
		if want[r.Profile+"/"+r.Partition] != r.CPUArch {
			t.Errorf("partition %s/%s has cpuArch %s", r.Profile, r.Partition, r.CPUArch)
		}
	}
}

// A server without the CRD has no profile: that is "no profiles applied yet", not an error. Any
// other failure is an error, so the check never passes for lack of an answer.
func TestListPartitionArchsAnAbsentCRDIsNoProfiles(t *testing.T) {
	notFound := `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"the server could not find the requested resource","reason":"NotFound","code":404}`
	n, refs, err := listPartitionArchs(context.Background(), apiConfig(t, 404, notFound))
	if err != nil || n != 0 || len(refs) != 0 {
		t.Fatalf("an absent CRD: %d, %v, %v", n, refs, err)
	}
	for name, c := range map[string]struct {
		status int
		body   string
	}{
		"server error": {500, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500}`},
		"forbidden":    {403, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`},
		"a bad object": {200, `{"apiVersion":"hpcburst.groundnuty.github.io/v1alpha1","kind":"PlatformProfileList","items":[{"metadata":{"name":"example-bad"},"spec":{"partitions":"x"}}]}`},
	} {
		if _, _, err := listPartitionArchs(context.Background(), apiConfig(t, c.status, c.body)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// An answer that is not a list of PlatformProfiles, or whose items lack the structure the check reads, is an
// error and never "no profiles": client-go decodes an HTTP 200 with a Status body, or a list of {}, without
// complaint, and each would otherwise pass as nothing to build.
func TestListPartitionArchsRefusesWhatIsNotAListOfProfiles(t *testing.T) {
	const gv = "hpcburst.groundnuty.github.io/v1alpha1"
	list := func(items string) string {
		return `{"apiVersion":"` + gv + `","kind":"PlatformProfileList","metadata":{"resourceVersion":"7"},"items":[` + items + `]}`
	}
	item := func(name, spec string) string {
		return `{"apiVersion":"` + gv + `","kind":"PlatformProfile","metadata":{"name":"` + name + `"},"spec":` + spec + `}`
	}
	part := func(name, arch string) string {
		return `{"name":"` + name + `","gpuModel":"GH200","cpuArch":"` + arch + `"}`
	}
	for _, c := range []struct{ name, body, want string }{
		{"a Status body with HTTP 200", `{"apiVersion":"v1","kind":"Status","status":"Failure","code":500}`, "not a PlatformProfileList"},
		{"an empty object", `{}`, "'Kind' is missing"},
		{"a List of the right group", `{"apiVersion":"` + gv + `","kind":"List","items":[]}`, "not a PlatformProfileList"},
		{"another version", `{"apiVersion":"hpcburst.groundnuty.github.io/v1beta1","kind":"PlatformProfileList","items":[]}`, "not a PlatformProfileList"},
		{"a list holding {}", list(`{}`), "no metadata.name"},
		{"an item of another kind", list(`{"apiVersion":"` + gv + `","kind":"Provider","metadata":{"name":"x"}}`), "not a PlatformProfile of"},
		{"an item with no name", list(`{"apiVersion":"` + gv + `","kind":"PlatformProfile","spec":{"providerKind":"managed"}}`), "no metadata.name"},
		{"a profile with no providerKind", list(item("example-p", `{}`)), `providerKind ""`},
		{"a providerKind that is none", list(item("example-p", `{"providerKind":"cloud"}`)), `providerKind "cloud"`},
		{"a batch profile with no partitions", list(item("example-p", `{"providerKind":"batch"}`)), "lists no partitions"},
		{"a partition with no cpuArch", list(item("example-p", `{"providerKind":"batch","partitions":[`+part("example-gpu", "")+`]}`)), "both are required"},
		{"a partition with no name", list(item("example-p", `{"providerKind":"batch","partitions":[`+part("", "x86_64")+`]}`)), "both are required"},
	} {
		t.Run(c.name, func(t *testing.T) {
			n, refs, err := listPartitionArchs(context.Background(), apiConfig(t, 200, c.body))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("%d profiles, partitions %v, error %v; want an error that says %q", n, refs, err, c.want)
			}
		})
	}
	// The controls: an empty list is no profiles, and so are managed and static profiles with no partitions.
	for name, body := range map[string]string{
		"an empty list":      list(``),
		"a managed profile":  list(item("example-m", `{"providerKind":"managed"}`)),
		"a static profile":   list(item("example-s", `{"providerKind":"static"}`)),
		"batch with its cpu": list(item("example-b", `{"providerKind":"batch","partitions":[`+part("example-gpu", "aarch64")+`]}`)),
	} {
		if _, _, err := listPartitionArchs(context.Background(), apiConfig(t, 200, body)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The fix commands are printed for a person to paste, so they must run with the state directory they name,
// whatever it holds: spaces, quotes, and every shell metacharacter. Each is run through sh -c against
// stub scripts that record the arguments they got, one per line, and the test compares them with the
// directory and checks that nothing the path spelled as a command ran.
func TestFixCommandsRunWithAnyStateDirectory(t *testing.T) {
	stubs := t.TempDir()
	hackDir := filepath.Join(stubs, "hack")
	if err := os.Mkdir(hackDir, 0o755); err != nil {
		t.Fatal(err)
	}
	recorder := "#!/bin/sh\nprintf '%s\\0' \"$@\" > \"$STUB_OUT\"\n"
	for _, name := range []string{"build-agents.sh", "fetch-frp.sh"} {
		if err := os.WriteFile(filepath.Join(hackDir, name), []byte(recorder), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{
		"/state/plain-dir_1.2", "/state with spaces/agent-builds", "/it's/agent-builds", `/say "hi"/agent-builds`,
		"/semi;colon/agent-builds", "/amp&pipe|redirect>x/agent-builds", "/$(touch PWNED)/agent-builds", "/back`touch PWNED`tick/agent-builds",
		"/glob*?[x]/agent-builds", "/~tilde/agent-builds", "/#hash/agent-builds", "/new\nline/agent-builds", "/tab\tbed/agent-builds", `/back\slash/agent-builds`,
	} {
		rep := checkAgentBuilds(1, both, dir, testPin)
		for _, p := range rep.Problems {
			out := filepath.Join(t.TempDir(), "args")
			cmd := exec.Command("sh", "-c", p.Fix)
			cmd.Dir = stubs
			cmd.Env = append(os.Environ(), "STUB_OUT="+out)
			if b, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("%q: the fix %q failed: %v\n%s", dir, p.Fix, err, b)
				continue
			}
			raw, err := os.ReadFile(out)
			if err != nil {
				t.Errorf("%q: the stub was not run by %q: %v", dir, p.Fix, err)
				continue
			}
			got := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
			var want []string
			switch {
			case strings.HasPrefix(p.Fix, "hack/build-agents.sh "):
				want = []string{dir, p.CPUArch}
			case strings.HasPrefix(p.Fix, "hack/fetch-frp.sh "):
				want = []string{"--agent-builds", dir}
			default:
				t.Fatalf("a fix that names no script: %q", p.Fix)
			}
			if !slices.Equal(got, want) {
				t.Errorf("%q: the fix %q gave the script %q, want %q", dir, p.Fix, got, want)
			}
		}
		if _, err := os.Stat(filepath.Join(stubs, "PWNED")); err == nil {
			t.Fatalf("%q: the path was run as a command", dir)
		}
	}
	// And the quoting alone: what the shell reads back is the string.
	for _, s := range []string{"", "plain", "a b", "it's", "'", "''", "a'b'c", "$HOME", "`x`", "a\nb", "-n"} {
		out, err := exec.Command("sh", "-c", "printf %s "+ShellQuote(s)).Output()
		if err != nil || string(out) != s {
			t.Errorf("ShellQuote(%q) = %s reads back as %q, %v", s, ShellQuote(s), out, err)
		}
	}
}

// CheckAgentBuilds reads the profiles from the API and the digests from the pins: with the real pins
// of frp-pins.txt and an empty directory, the expected digest of each frpc is the pinned one.
func TestCheckAgentBuildsReadsTheProfilesAndThePins(t *testing.T) {
	pins, err := FrpcDigests(DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	dir := buildsIn(t, nil)
	rep, err := CheckAgentBuilds(context.Background(), apiConfig(t, 200, profilesJSON), dir)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Profiles != 2 || len(rep.Archs) != 2 || len(rep.Problems) != 4 || rep.Dir != dir {
		t.Fatalf("%+v", rep)
	}
	for _, a := range rep.Archs {
		if a.Frpc.Expected != pins[a.CPUArch] || len(a.Frpc.Expected) != 64 {
			t.Errorf("%s: expected frpc %q, pinned %q", a.CPUArch, a.Frpc.Expected, pins[a.CPUArch])
		}
	}
	if _, err := CheckAgentBuilds(context.Background(), apiConfig(t, 500, "{}"), dir); err == nil || !strings.Contains(err.Error(), "PlatformProfiles") {
		t.Errorf("a server error: %v", err)
	}
}
