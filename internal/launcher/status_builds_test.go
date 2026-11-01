package launcher

// status and up check the agent builds against the PlatformProfiles of the API server (#158). These
// tests run Status on the fake host, whose apiserver answers the list of PlatformProfiles with what
// the test sets (profilesStatus, profilesBody), and checkBuildsAtUp on the same fake kubeconfig. The
// frpc digests are the test's (useTestPins), since no test can make an frpc with the pinned digest;
// the real-k3s test of cmd/hpcburst runs up and status against a real API server.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// setProfiles makes the fake apiserver serve profilesJSON.
func (h *fakeHost) setProfiles() { h.profilesStatus, h.profilesBody = 200, profilesJSON }

// buildsDirOf is the agent-builds directory the host reads.
func buildsDirOf(t *testing.T, h *fakeHost) string {
	t.Helper()
	dir := h.l.host(h.st.Ports).AgentBuildsDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func place(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStatusReportsTheAgentBuilds(t *testing.T) {
	useTestPins(t)
	reasons := func(r *StatusReport) string { return strings.Join(r.Reasons, "; ") }

	t.Run("control: no PlatformProfile CRD, so no profiles: healthy, and the check ran", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		rep := s.status(false)
		if rep.Health != Healthy || rep.AgentBuilds == nil || !rep.AgentBuilds.Checked || len(rep.AgentBuilds.Problems) != 0 {
			t.Fatalf("%s %v %+v", rep.Health, rep.Reasons, rep.AgentBuilds)
		}
	})
	t.Run("profiles with both cpuArchs and no builds: unhealthy, each build named with its fix", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.setProfiles()
		dir := buildsDirOf(t, s.fakeHost)
		rep := s.status(false)
		if rep.Health != Unhealthy || rep.AgentBuilds == nil || len(rep.AgentBuilds.Problems) != 4 {
			t.Fatalf("%s %v %+v", rep.Health, rep.Reasons, rep.AgentBuilds)
		}
		for _, want := range []string{
			"cpuArch aarch64", "cpuArch x86_64", "hb-agent-aarch64 is missing from " + dir,
			"fix: hack/build-agents.sh " + dir + " aarch64", "fix: hack/fetch-frp.sh --agent-builds " + dir,
		} {
			if !strings.Contains(reasons(rep), want) {
				t.Errorf("reasons lack %q:\n%s", want, strings.Join(rep.Reasons, "\n"))
			}
		}
	})
	t.Run("one mismatched frpc: only it is reported, with both digests", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.setProfiles()
		files := map[string][]byte{}
		for k, v := range allBuilds {
			files[k] = v
		}
		files["frpc-aarch64"] = []byte("not the pinned one\n")
		place(t, buildsDirOf(t, s.fakeHost), files)
		rep := s.status(false)
		if rep.Health != Unhealthy || len(rep.Reasons) != 1 || !strings.Contains(rep.Reasons[0], hexSum([]byte("not the pinned one\n"))) || !strings.Contains(rep.Reasons[0], testPin["aarch64"]) {
			t.Fatalf("%s %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("every build in place: healthy, and reported ok", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.setProfiles()
		place(t, buildsDirOf(t, s.fakeHost), allBuilds)
		rep := s.status(false)
		if rep.Health != Healthy || rep.AgentBuilds == nil || len(rep.AgentBuilds.Archs) != 2 || len(rep.AgentBuilds.Problems) != 0 {
			t.Fatalf("%s %v %+v", rep.Health, rep.Reasons, rep.AgentBuilds)
		}
	})
	t.Run("--no-api: not checked, and it says why; the missing builds are not guessed at", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.setProfiles()
		rep := s.status(true)
		if rep.Health != Healthy || rep.AgentBuilds == nil || rep.AgentBuilds.Checked || !strings.Contains(rep.AgentBuilds.Detail, "--no-api") {
			t.Fatalf("%s %v %+v", rep.Health, rep.Reasons, rep.AgentBuilds)
		}
	})
	t.Run("the profiles cannot be read: unhealthy, not passed", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.profilesStatus, s.profilesBody = 500, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500}`
		rep := s.status(false)
		if rep.Health != Unhealthy || rep.AgentBuilds == nil || rep.AgentBuilds.Checked || !strings.Contains(reasons(rep), "agent builds could not be checked") {
			t.Fatalf("%s %v %+v", rep.Health, rep.Reasons, rep.AgentBuilds)
		}
	})
	t.Run("a host that runs no controllers stages no builds: nothing to check", func(t *testing.T) {
		h := newFakeHost(t)
		h.setProfiles()
		rep, err := Status(context.Background(), StatusOptions{StateDir: h.l.dir, NoGCProbe: true})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Health != Healthy || rep.AgentBuilds != nil {
			t.Fatalf("%s %v %+v", rep.Health, rep.Reasons, rep.AgentBuilds)
		}
	})
}

// checkBuildsAtUp is what up runs once its controllers are up: it returns an *AgentBuildsError that
// carries the report when a build is missing, and nil when all are in place.
func TestCheckBuildsAtUp(t *testing.T) {
	useTestPins(t)
	s := newSupervisedFakeHost(t)
	s.setProfiles()
	dir := buildsDirOf(t, s.fakeHost)

	rep := &UpReport{}
	err := checkBuildsAtUp(context.Background(), s.l, s.st.Ports, rep)
	abe, ok := err.(*AgentBuildsError)
	if !ok || rep.AgentBuilds == nil || abe.Builds != rep.AgentBuilds || len(rep.AgentBuilds.Problems) != 4 {
		t.Fatalf("%T %v, report %+v", err, err, rep.AgentBuilds)
	}
	for _, want := range []string{"cpuArch aarch64", "hb-agent-aarch64 is missing from " + dir, "hack/build-agents.sh " + dir + " aarch64"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error lacks %q:\n%v", want, err)
		}
	}

	place(t, dir, allBuilds)
	rep = &UpReport{}
	if err := checkBuildsAtUp(context.Background(), s.l, s.st.Ports, rep); err != nil || rep.AgentBuilds == nil || len(rep.AgentBuilds.Archs) != 2 {
		t.Fatalf("with every build in place: %v, %+v", err, rep.AgentBuilds)
	}

	// A check that cannot read the profiles is an error of its own, not a pass.
	s.profilesStatus, s.profilesBody = 500, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500}`
	rep = &UpReport{}
	err = checkBuildsAtUp(context.Background(), s.l, s.st.Ports, rep)
	if abe, ok := err.(*AgentBuildsError); !ok || abe.Builds.Checked || !strings.Contains(err.Error(), "could not be checked") {
		t.Fatalf("%T %v", err, err)
	}
}
