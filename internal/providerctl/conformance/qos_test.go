package conformance

// The conformance run's qos (HPCBURST_CONF_QOS) and the shape of each cluster
// row of hack/live-checks.sh. A row whose qos is printed but never reaches the
// Provider would pass every check of the script and submit under the wrong qos,
// so the qos is followed from the row to the request the plugin checks.
//
// Expected values are written out here from the measurements the shipped
// profile cites (helios.yaml: QOS now caps a user at 1 GPU, 72 CPUs and
// 122400Mi; issue #94, comment 6557416899) and from HBEP-4, Submission (the
// plugin refuses a request whose qos is not the Provider's); none is read from
// the code under test.

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// gh200 is a complete set of settings for Helios plgrid-gpu-gh200 under qos now.
func gh200() map[string]string {
	m := full()
	m["HPCBURST_CONF_PARTITION"], m["HPCBURST_CONF_GPUS"], m["HPCBURST_CONF_QOS"] = "plgrid-gpu-gh200", "1", "now"
	return m
}

// An unset HPCBURST_CONF_QOS leaves the qos empty everywhere: in the settings,
// in the Provider's spec, and in every direct submit. A set one reaches all
// three. Each assertion kills a mutant that drops one link.
func TestQOSReachesTheProviderAndEveryDirectSubmit(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"unset", full(), ""},
		{"now", gh200(), "now"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := readSettings(env(tc.env))
			if err != nil {
				t.Fatal(err)
			}
			if s.qos != tc.want {
				t.Errorf("settings qos = %q, want %q", s.qos, tc.want)
			}
			if got := conformanceProvider(s).Spec.QOS; got != tc.want {
				t.Errorf("the Provider's spec.qos = %q, want %q", got, tc.want)
			}
			if got := heldRequest(s, "hb-example-0123456789ab", v1alpha1.BatchParameters{}).QOS; got != tc.want {
				t.Errorf("a direct held submit carries qos %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReadSettingsRefusesAQOSTheProfileDoesNotList(t *testing.T) {
	m := gh200()
	m["HPCBURST_CONF_QOS"] = "nowhere"
	_, err := readSettings(env(m))
	if err == nil || !strings.Contains(err.Error(), `HPCBURST_CONF_QOS "nowhere" is not a qos profile "helios" lists`) ||
		!strings.Contains(err.Error(), "now, plgrid, normal") {
		t.Errorf("error = %v, want a refusal naming the qos and the three the profile lists", err)
	}
	// It is optional: leaving it out is no problem, and the missing-settings
	// message never asks for it.
	_, err = readSettings(env(map[string]string{}))
	if err == nil || strings.Contains(err.Error(), "HPCBURST_CONF_QOS") {
		t.Errorf("empty settings: error = %v, want one that does not name HPCBURST_CONF_QOS", err)
	}
}

// shapeRow is one row of the script's cluster table: name | host | profile |
// partition | qos | slot | account key | GPUs | CPUs | memory | time limit |
// timeout (min) | extra.
type shapeRow struct {
	name, host, profile, partition, qos, slot, acctKey, gpus, cpus, memory, timeLimit, timeout, extra string
}

func readShapeRows(t *testing.T) []shapeRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "hack", "live-checks.sh"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []shapeRow
	in := false
	for _, line := range strings.Split(string(raw), "\n") {
		switch {
		case line == "# BEGIN CLUSTER SHAPES":
			in = true
		case line == "# END CLUSTER SHAPES":
			in = false
		case in && line != "" && !strings.HasPrefix(line, "#"):
			f := strings.Split(line, "|")
			if len(f) != 13 {
				t.Fatalf("a cluster row has %d fields, want 13: %q", len(f), line)
			}
			rows = append(rows, shapeRow{f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9], f[10], f[11], f[12]})
		}
	}
	if len(rows) == 0 {
		t.Fatal("no cluster rows found in hack/live-checks.sh")
	}
	return rows
}

// env is the environment hack/live-checks.sh gives the test for the row
// (env_conformance), with an example account and the credentials of creds.
func (r shapeRow) env(creds string) map[string]string {
	m := map[string]string{
		"HPCBURST_CONF_PROFILE": r.profile, "HPCBURST_CONF_PARTITION": r.partition, "HPCBURST_CONF_CREDENTIALS": creds,
		"HPCBURST_CONF_ACCOUNT": "example-account", "HPCBURST_CONF_GPUS": r.gpus, "HPCBURST_CONF_CPUS": r.cpus, "HPCBURST_CONF_MEMORY": r.memory,
		"HPCBURST_CONF_STAGING": "$SCRATCH/hpc-burst-conformance", "HPCBURST_CONF_HOST_LOCK": "/private/lock",
		"HPCBURST_CONF_CONTROL_DIR": "/private/ctl", "HPCBURST_CONF_OUT": "/private/out",
	}
	if r.qos != "-" {
		m["HPCBURST_CONF_QOS"] = r.qos
	}
	return m
}

// validated reads the row's Provider with the plugin's own ProfileReader, over
// the shipped profile and a mode-600 credentials file written for the purpose.
func (r shapeRow) validated(t *testing.T) (settings, slurm.Validated) {
	t.Helper()
	dir := t.TempDir()
	key, creds := filepath.Join(dir, "key"), filepath.Join(dir, "creds")
	if err := os.WriteFile(key, []byte("example private key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(creds, []byte("Host example-login\n  HostName login.example.org\n  User example-user\n  IdentityFile "+key+"\n  StrictHostKeyChecking yes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := readSettings(env(r.env(creds)))
	if err != nil {
		t.Fatalf("%s: %v", r.name, err)
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	prov := conformanceProvider(s)
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(s.profile, prov).Build()
	v, err := (&slurm.ProfileReader{Reader: c, Home: dir}).Read(context.Background(), prov.Name)
	if err != nil {
		t.Fatalf("%s: the plugin does not accept the row's Provider: %v", r.name, err)
	}
	return s, v
}

func (r shapeRow) request(t *testing.T, s settings) (params v1alpha1.BatchParameters) {
	t.Helper()
	return v1alpha1.BatchParameters{Partition: s.partition, TimeLimit: v1alpha1.Duration(allocTimeLimit), GPUs: s.gpus, CPUsPerTask: s.cpus,
		Memory: s.memory, Nodes: 1, AgentDigest: strings.Repeat("a", 64)}
}

// clock reads a row's "HH:MM:SS" time limit.
func clock(t *testing.T, text string) time.Duration {
	t.Helper()
	f := strings.Split(text, ":")
	if len(f) != 3 {
		t.Fatalf("time limit %q is not HH:MM:SS", text)
	}
	var d time.Duration
	for i, unit := range []time.Duration{time.Hour, time.Minute, time.Second} {
		n, err := strconv.Atoi(f[i])
		if err != nil {
			t.Fatalf("time limit %q: %v", text, err)
		}
		d += time.Duration(n) * unit
	}
	return d
}

// Every conformance row of the script is a request the plugin accepts: its
// Provider passes ProfileReader (the qos is listed in the profile), its
// qos is the one the row names, and the harness's own request for its GPUs,
// CPUs, memory and time limit passes CheckRequest. The time limit in the row is
// the harness's, which the script's table comment says and nothing else checks.
func TestConformanceRowsPassTheRequestCheck(t *testing.T) {
	seen := 0
	for _, r := range readShapeRows(t) {
		if r.profile == "-" {
			continue // real-engine: not a conformance run
		}
		seen++
		t.Run(r.name, func(t *testing.T) {
			s, v := r.validated(t)
			wantQOS := r.qos
			if wantQOS == "-" {
				wantQOS = ""
			}
			if v.Settings.QOS != wantQOS {
				t.Errorf("the plugin's Settings.QOS = %q, the row says %q: the row's qos does not reach the Provider", v.Settings.QOS, wantQOS)
			}
			if got, want := clock(t, r.timeLimit), 5*time.Minute; got != want {
				t.Errorf("time limit %s, the harness's allocTimeLimit %s is %v", r.timeLimit, allocTimeLimit, want)
			}
			req := heldRequest(s, "hb-example-0123456789ab", r.request(t, s))
			if req.QOS != wantQOS {
				t.Errorf("a direct submit carries qos %q, the row says %q", req.QOS, wantQOS)
			}
			if err := slurm.CheckRequest(req, v.Profile, v.Settings); err != nil {
				t.Errorf("the plugin refuses the row's request: %v", err)
			}
		})
	}
	if seen < 3 {
		t.Errorf("%d conformance rows found, want at least conformance-helios, conformance-helios-gh200 and conformance-athena", seen)
	}
}

// The GH200 row is the one under qos now. The request check must be seen to
// apply now's limits to it (measured: 1 GPU, 72 CPUs, 122400Mi per user and a
// wall limit of 12 h), and to refuse a request whose qos is not the
// Provider's. Each refusal below is a control: it shows the check can say no,
// and with a qos other than now (none, say) the over-limit cases would pass
// (the partition's own limits are looser), so they show that now reached it.
func TestGH200RowIsCheckedUnderQOSNow(t *testing.T) {
	var row *shapeRow
	for _, r := range readShapeRows(t) {
		if r.name == "conformance-helios-gh200" {
			r := r
			row = &r
		}
	}
	if row == nil {
		t.Fatal("hack/live-checks.sh has no conformance-helios-gh200 row")
	}
	if row.host != "helios" || row.profile != "helios" || row.partition != "plgrid-gpu-gh200" || row.qos != "now" || row.slot != "now" ||
		row.acctKey != "ACCOUNT_HELIOS_GH200" || row.gpus != "1" {
		t.Errorf("row = %+v, want Helios, profile helios, plgrid-gpu-gh200, qos now, slot now, ACCOUNT_HELIOS_GH200, 1 GPU", *row)
	}
	s, v := row.validated(t)
	good := row.request(t, s)
	for _, tc := range []struct {
		name   string
		qos    string
		change func(*v1alpha1.BatchParameters)
		want   string
	}{
		{"a request without the Provider's qos", "", func(*v1alpha1.BatchParameters) {}, `qos "" is not the Provider's qos "now"`},
		{"two GPUs", "now", func(p *v1alpha1.BatchParameters) { p.GPUs = 2 }, `gpus 2 is above maxGpusPerUser 1 of qos "now"`},
		{"73 CPUs", "now", func(p *v1alpha1.BatchParameters) { p.CPUsPerTask = 73 }, `cpusPerTask 73 is above maxCpusPerUser 72 of qos "now"`},
		{"123000Mi of memory", "now", func(p *v1alpha1.BatchParameters) { p.Memory = resource.MustParse("123000Mi") }, `of qos "now"`},
		{"no CPU", "now", func(p *v1alpha1.BatchParameters) { p.CPUsPerTask = 0 }, "cpusPerTask 0 is below 1"},
		{"a time limit over now's 12 h", "now", func(p *v1alpha1.BatchParameters) { p.TimeLimit = "13h" }, `above maxWall 12h0m0s of qos "now"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := good
			tc.change(&p)
			req := heldRequest(s, "hb-example-0123456789ab", p)
			req.QOS = tc.qos
			err := slurm.CheckRequest(req, v.Profile, v.Settings)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want one containing %q", err, tc.want)
			}
		})
	}
}
