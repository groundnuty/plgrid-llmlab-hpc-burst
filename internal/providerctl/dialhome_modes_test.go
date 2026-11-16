package providerctl

// ReadProfile's dial-home check under the controller, over the real
// slurm.ProfileReader and the in-process fake Slurm (HBEP-4, Core operations,
// ReadProfile; Test plan, The remaining groups): a partition whose dialHomeModes
// lack the launcher's mode is refused with the field named and no call is
// made, so the fake's call log holds no sbatch. The expected counts and phases
// are written by hand from the HBEP, and every refusing case pairs the
// launcher's mode with a list that does NOT hold it: a fixture whose list always
// holds the launcher's mode proves nothing.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// withRealProfile replaces the env's static profile source with the real
// ProfileReader, over a Provider and a PlatformProfile in the env's client: the
// profile is the env's own, with the partition listing the given dialHomeModes,
// and the launcher is in the given mode, with the front settings that mode takes.
func withRealProfile(mode agentapi.DialHomeMode, list []v1alpha1.ProfileDialHomeMode) realOption {
	return func(e *realEnv) {
		dir := e.t.TempDir()
		key := filepath.Join(dir, "key")
		creds := filepath.Join(dir, "creds")
		for path, content := range map[string]string{
			key:   "example private key\n",
			creds: "Host example-login\n  HostName login.example.org\n  User example-user\n  IdentityFile " + key + "\n",
		} {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				e.t.Fatal(err)
			}
		}
		v := e.profiles.v
		spec := v.Profile.Spec
		part := v.Profile.Partition
		part.DialHomeModes = list
		spec.Partitions = []v1alpha1.PartitionProfile{part}
		if err := e.c.Create(e.ctx, &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: v.Profile.Name}, Spec: spec}); err != nil {
			e.t.Fatal(err)
		}
		var prov v1alpha1.Provider
		if err := e.c.Get(e.ctx, client.ObjectKey{Name: e.provider}, &prov); err != nil {
			e.t.Fatal(err)
		}
		prov.Spec = v1alpha1.ProviderSpec{
			PlatformProfileRef: v1alpha1.ObjectReference{Name: v.Profile.Name}, Account: v.Settings.Account,
			AllowedPartitions: []string{part.Name}, CredentialsRef: &v1alpha1.CredentialsReference{File: creds},
		}
		if err := e.c.Update(e.ctx, &prov); err != nil {
			e.t.Fatal(err)
		}
		e.opts.Profiles = &slurm.ProfileReader{Reader: e.c, Home: dir, DialHomeMode: mode}
		front := e.opts.Front
		switch mode {
		case agentapi.DialHomeSSH:
			// As TestDialHomeKeyMissing's ssh mode; its two key files are placed by the test.
			front.Mode, front.Endpoint, front.SSHUser, front.AgentPort, front.DialHomePort = mode, "front.example.org:22", "example-user", 29270, 29274
		case agentapi.DialHomeHostDialed:
			// As hostDialedWith: the mode needs relay settings, which these cases never run.
			front.Mode, front.Endpoint = mode, ""
			e.opts.Relays = &RelaySettings{
				Ports: map[agentapi.RelayChannel]int{agentapi.RelayControl: 1, agentapi.RelayServing: 2}, Slots: hostdial.NewSlots(0),
				Hosts: hostdial.NewHosts(), ControlDir: e.t.TempDir(), SSHBinary: "/bin/false",
				MinBackoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, RestartLimit: 1000,
				Epoch: func(context.Context) (int64, error) { return 3, nil },
			}
		}
	}
}

func TestAPartitionWithoutTheLaunchersDialHomeModeIsRefusedBeforeAnySlurmCall(t *testing.T) {
	const field = "spec.partitions[" + gpuPart + "].dialHomeModes"
	ssh, hostDialed := v1alpha1.ProfileDialHomeSSH, v1alpha1.ProfileDialHomeHostDialed
	for _, c := range []struct {
		name    string
		mode    agentapi.DialHomeMode
		list    []v1alpha1.ProfileDialHomeMode
		refused bool
		sbatch  int // the sbatch calls expected when the profile reads; -1 where the mode's own setup would gate it
	}{
		{"host-dialed, list only ssh", agentapi.DialHomeHostDialed, []v1alpha1.ProfileDialHomeMode{ssh}, true, 0},
		{"ssh, list only host-dialed", agentapi.DialHomeSSH, []v1alpha1.ProfileDialHomeMode{hostDialed}, true, 0},
		{"ssh, list holds ssh", agentapi.DialHomeSSH, []v1alpha1.ProfileDialHomeMode{ssh}, false, 1},
		{"ssh, list absent", agentapi.DialHomeSSH, nil, false, 1},
		{"direct, list only ssh", agentapi.DialHomeDirect, []v1alpha1.ProfileDialHomeMode{ssh}, false, 1},
		{"direct, list only host-dialed", agentapi.DialHomeDirect, []v1alpha1.ProfileDialHomeMode{hostDialed}, false, 1},
		// A host-dialed launcher submits only through its relays, which these cases do not run: the read is judged by the phase.
		{"host-dialed, list holds host-dialed", agentapi.DialHomeHostDialed, []v1alpha1.ProfileDialHomeMode{hostDialed}, false, -1},
		{"host-dialed, list absent", agentapi.DialHomeHostDialed, nil, false, -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withRealProfile(c.mode, c.list))
			if err := sl.MkdirAll("/home/user-a/.hpcburst/dial-home", 0o700); err != nil {
				t.Fatal(err)
			}
			for _, f := range []string{"key", "known_hosts"} {
				if err := sl.WriteFile("/home/user-a/.hpcburst/dial-home/"+f, []byte("example"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 3)
			st := e.get("alloc-a").Status.AtProvider
			if c.refused {
				if st.Phase != v1alpha1.AllocationRefused || !strings.Contains(st.Reason, field) {
					t.Errorf("phase %s, reason %q; want Refused, naming %s", st.Phase, st.Reason, field)
				}
				if n := sl.Count("sbatch"); n != 0 {
					t.Errorf("%d sbatch calls under a refused profile, want 0", n)
				}
				if calls := sl.Calls(); len(calls) != 0 {
					t.Errorf("%d Slurm calls under a refused profile, want none; the first is %v", len(calls), calls[0].Words)
				}
				return
			}
			if st.Phase == v1alpha1.AllocationRefused || strings.Contains(st.Reason, "dialHomeModes") {
				t.Errorf("phase %s, reason %q; want a profile that reads", st.Phase, st.Reason)
			}
			if c.sbatch >= 0 {
				if n := sl.Count("sbatch"); n != c.sbatch {
					t.Errorf("%d sbatch calls, want %d", n, c.sbatch)
				}
			}
		})
	}
}
