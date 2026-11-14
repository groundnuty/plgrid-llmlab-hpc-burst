package launcher

// up --instance-router (beta, opt-in; #429, #454): recorded, passed to the controller, kept across an
// upgrade, and, with it, Envoy's builds are checked with the rest. Without it, nothing changes.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The controller's command line gains --instance-router envoy with the router, and is alpha's, byte for
// byte, without it (TestSupervisedSetCommandLine's "direct front" case is the same line, without it).
func TestTheRouterReachesTheControllersCommandLine(t *testing.T) {
	alpha := []string{"--state-dir", "/run/st", "--port-base", "29220", "--health-addr", "127.0.0.1:29226",
		"--front-mode", "direct", "--front-addr", "front.example.test:8443"}
	for _, c := range []struct {
		router string
		want   []string
	}{
		{"", alpha},
		{"envoy", append(slices.Clone(alpha), "--instance-router", "envoy")},
	} {
		st := &State{
			Ports:          Ports{API: 29220, HTTPS: 29221, KCM: 29222, Agent: 29223, LB: 29225, Controller: 29226},
			Front:          Front{Mode: "direct", Addr: "front.example.test:8443"},
			Controllers:    &ControllerConfig{Binary: "/opt/hpcburst-controller"},
			InstanceRouter: c.router,
		}
		set, err := st.supervisedSet(layout{"/run/st"})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(set[0].Args, c.want) {
			t.Errorf("router %q: controller argv\n got %q\nwant %q", c.router, set[0].Args, c.want)
		}
	}
}

// up refuses a router other than envoy, and a router with no controller binary, before anything
// starts: no record.
func TestUpRefusesABadRouterBeforeAnythingStarts(t *testing.T) {
	for _, c := range []struct {
		name string
		o    UpOptions
		want string
	}{
		{"another router", UpOptions{InstanceRouter: "vllm-router", ControllerBin: fakeControllerBin(t)}, `--instance-router "vllm-router"`},
		{"no controller binary", UpOptions{InstanceRouter: "envoy", ControllerBin: ""}, "no controller binary"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.o.ControllerBin == "" {
				exe, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(filepath.Dir(exe), ControllerBinName)); err == nil {
					t.Fatal("a controller binary sits beside the test binary, so up would find one")
				}
			}
			l, err := newLayout(filepath.Join(shortDir(t), "st"))
			if err != nil {
				t.Fatal(err)
			}
			c.o.StateDir, c.o.PortBase = l.dir, 28360
			_, err = Up(context.Background(), c.o)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("up: %v; want refused, naming %q", err, c.want)
			}
			if _, err := os.Stat(l.stateFile()); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("a record after the refusal: %v", err)
			}
		})
	}
}

// With a router, every checked cpuArch also needs envoy-<cpuArch> with its pinned digest: in place, no
// problem; missing or another digest, a problem with hack/fetch-envoy.sh --agent-builds as its fix.
// Without a router Envoy is not looked at, even when its build is missing.
func TestEnvoyBuildsAreCheckedOnlyWithARouter(t *testing.T) {
	envoyX86, envoyArm := []byte("an envoy for x86_64\n"), []byte("an envoy for aarch64\n")
	pins := map[string]string{"x86_64": hexSum(envoyX86), "aarch64": hexSum(envoyArm)}
	withEnvoy := func(extra map[string][]byte) map[string][]byte {
		m := map[string][]byte{}
		for k, v := range allBuilds {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	dir := buildsIn(t, withEnvoy(map[string][]byte{"envoy-x86_64": envoyX86, "envoy-aarch64": envoyArm}))
	rep := checkAgentBuilds(1, both, dir, testPin)
	checkEnvoyBuilds(rep, dir, pins)
	if len(rep.Problems) != 0 {
		t.Errorf("with both Envoy builds in place: problems %+v", rep.Problems)
	}
	if a := archOf(t, rep, "aarch64"); a.Envoy == nil || !a.Envoy.OK || a.Envoy.SHA256 != pins["aarch64"] {
		t.Errorf("aarch64's Envoy check: %+v", a.Envoy)
	}

	dir = buildsIn(t, withEnvoy(map[string][]byte{"envoy-x86_64": envoyArm}))
	rep = checkAgentBuilds(1, both, dir, testPin)
	checkEnvoyBuilds(rep, dir, pins)
	x86, arm := problemOf(t, rep, "x86_64"), problemOf(t, rep, "aarch64")
	if len(x86) != 1 || !strings.Contains(x86[0].What, "envoy-x86_64") || !strings.Contains(x86[0].What, "the pin (envoy-pins.txt) says "+pins["x86_64"]) ||
		!strings.HasPrefix(x86[0].Fix, "hack/fetch-envoy.sh --agent-builds ") {
		t.Errorf("an x86_64 Envoy with another digest: %+v", x86)
	}
	if len(arm) != 1 || !strings.Contains(arm[0].What, "envoy-aarch64 is missing") || !strings.HasPrefix(arm[0].Fix, "hack/fetch-envoy.sh --agent-builds ") {
		t.Errorf("no aarch64 Envoy: %+v", arm)
	}

	// The control: without a router the same directory has no problem, and no Envoy check.
	rep = checkAgentBuilds(1, both, dir, testPin)
	if len(rep.Problems) != 0 || archOf(t, rep, "x86_64").Envoy != nil {
		t.Errorf("without a router: problems %+v, Envoy %+v; want none", rep.Problems, archOf(t, rep, "x86_64").Envoy)
	}
}

// An upgrade brings a host with a router back with it, as it does a keeper host: the launch spec
// replaceHost passes the new host's start carries the router; an alpha host is the control.
func TestAnUpgradeKeepsTheRouter(t *testing.T) {
	for _, router := range []string{"envoy", ""} {
		t.Run("router "+router, func(t *testing.T) {
			l := storeLayout(t)
			putStore(t, l, "v1", "tok", "")
			if err := l.saveStoreVersion("v1.35.9+k3s1", "up"); err != nil {
				t.Fatal(err)
			}
			ports, err := PortsFromBase(28360)
			if err != nil {
				t.Fatal(err)
			}
			st := &State{Version: "v1.35.9+k3s1", Ports: ports, Front: Front{Mode: "direct", Addr: "front.example.test:8443"},
				Controllers: &ControllerConfig{Binary: "/opt/example/hpcburst-controller"}, InstanceRouter: router}
			ops := &fakeHostOps{t: t, l: l}
			if _, _, _, _, err := replaceHost(context.Background(), l, st, Binary{Version: mustVersion(t, "v1.36.5+k3s1")}, ops, time.Second, time.Second, nil); err != nil {
				t.Fatal(err)
			}
			if ops.spec.instanceRouter != router {
				t.Fatalf("the new host's launch spec carries router %q, want %q", ops.spec.instanceRouter, router)
			}
		})
	}
}

// up's and status's builds checks read the router the launcher recorded: with up --instance-router
// envoy recorded, the Envoy builds are required, and once placed with their pinned digests, nothing is
// missing. The same host with no router recorded is the control: its builds pass without Envoy's.
func TestTheRecordedRouterMakesUpAndStatusCheckEnvoysBuilds(t *testing.T) {
	useTestPins(t)
	envoyX86, envoyArm := []byte("an envoy for x86_64\n"), []byte("an envoy for aarch64\n")
	pins := map[string]string{"x86_64": hexSum(envoyX86), "aarch64": hexSum(envoyArm)}
	old := envoyBuildPins
	envoyBuildPins = func() (map[string]string, error) { return pins, nil }
	t.Cleanup(func() { envoyBuildPins = old })
	s := newSupervisedFakeHost(t)
	s.setProfiles()
	dir := buildsDirOf(t, s.fakeHost)
	place(t, dir, allBuilds)

	rep := &UpReport{}
	if err := checkBuildsAtUp(context.Background(), s.l, s.st.Ports, rep); err != nil {
		t.Fatalf("no router recorded, every alpha build in place: %v", err)
	}

	s.st.InstanceRouter = "envoy"
	if err := s.l.saveState(s.st); err != nil {
		t.Fatal(err)
	}
	rep = &UpReport{}
	err := checkBuildsAtUp(context.Background(), s.l, s.st.Ports, rep)
	if err == nil || len(rep.AgentBuilds.Problems) != 2 || !strings.Contains(err.Error(), "envoy-x86_64 is missing") || !strings.Contains(err.Error(), "hack/fetch-envoy.sh --agent-builds") {
		t.Fatalf("with the router recorded and no Envoy build: %v, problems %+v", err, rep.AgentBuilds.Problems)
	}
	if st := s.status(false); st.Health != Unhealthy || !strings.Contains(strings.Join(st.Reasons, "; "), "envoy-aarch64") {
		t.Errorf("status with the router recorded and no Envoy build: %s %v", st.Health, st.Reasons)
	}

	place(t, dir, map[string][]byte{"envoy-x86_64": envoyX86, "envoy-aarch64": envoyArm})
	rep = &UpReport{}
	if err := checkBuildsAtUp(context.Background(), s.l, s.st.Ports, rep); err != nil {
		t.Fatalf("with the router recorded and both Envoy builds in place: %v", err)
	}
	if st := s.status(false); st.Health != Healthy {
		t.Errorf("status with both Envoy builds in place: %s %v", st.Health, st.Reasons)
	}
}
