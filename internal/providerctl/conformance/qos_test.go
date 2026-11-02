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
	"go/ast"
	"go/parser"
	"go/token"
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
// timeout (min) | extra | start wait (min). The last column is optional.
type shapeRow struct {
	name, host, profile, partition, qos, slot, acctKey, gpus, cpus, memory, timeLimit, timeout, extra, startWait string
}

// allocLimit is the time limit of the row's Allocation: the harness's, or the reach rows' own, shorter one.
func (r shapeRow) allocLimit() string {
	if strings.HasPrefix(r.name, "reach-") {
		return reachTimeLimit
	}
	return allocTimeLimit
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
			if len(f) != 13 && len(f) != 14 {
				t.Fatalf("a cluster row has %d fields, want 13 or 14: %q", len(f), line)
			}
			if len(f) == 13 {
				f = append(f, "-")
			}
			rows = append(rows, shapeRow{f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9], f[10], f[11], f[12], f[13]})
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
	if r.startWait != "-" {
		m["HPCBURST_CONF_START_WAIT"] = r.startWait + "m"
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
	return v1alpha1.BatchParameters{Partition: s.partition, TimeLimit: v1alpha1.Duration(r.allocLimit()), GPUs: s.gpus, CPUsPerTask: s.cpus,
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

// notAConformanceRun is a row of the script that TestConformance does not run: the alpha rows of issue #194, whose
// leases are the deployments' (the accounting rows run through this harness and keep its 5-minute Allocations).
func notAConformanceRun(name string) bool {
	return strings.HasPrefix(name, "alpha-") || strings.HasPrefix(name, "tunnel-hour-")
}

// Every conformance row of the script is a request the plugin accepts: its
// Provider passes ProfileReader (the qos is listed in the profile), its
// qos is the one the row names, and the harness's own request for its GPUs,
// CPUs, memory and time limit passes CheckRequest. The time limit in the row is
// the harness's, which the script's table comment says and nothing else checks.
func TestConformanceRowsPassTheRequestCheck(t *testing.T) {
	seen := 0
	for _, r := range readShapeRows(t) {
		if r.profile == "-" || notAConformanceRun(r.name) {
			continue // real-engine, and the alpha rows (issue #194): not conformance runs, with leases of their own
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
			want, err := v1alpha1.Duration(r.allocLimit()).AsDuration()
			if err != nil {
				t.Fatal(err)
			}
			if got := clock(t, r.timeLimit); got != want {
				t.Errorf("the row's time limit is %s (%v), the harness's %s is %v", r.timeLimit, got, r.allocLimit(), want)
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
		row.acctKey != "ACCOUNT_HELIOS_GH200" || row.gpus != "1" || row.startWait != "10" || row.timeout != "45" {
		t.Errorf("row = %+v, want Helios, profile helios, plgrid-gpu-gh200, qos now, slot now, ACCOUNT_HELIOS_GH200, 1 GPU, a start wait of 10 min and a timeout of 45 min (the test reaches the timeout command: TestTheRowsTimeoutReachesTheTestBinary)", *row)
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

// The plugin refuses a request whose qos is not the Provider's, so a direct
// submit of the live run that is written out as a literal, without the qos,
// would be refused only when a run reaches it, with a slot in hand. Every
// provider.SubmitRequest this package's tests build is therefore made by
// heldRequest, except in the one test that runs against a fake platform with a
// Provider that sets no qos. The scan covers every declaration of the package's
// test files, package-level ones included: a composite literal of the type, of
// a type declared as it or as an alias of it, a var of the type, and new(...)
// of it. Its limit: a request that arrives from elsewhere (a function's result,
// a struct field) is not seen, and the code has none today.
func TestNoDirectSubmitIsBuiltWithoutTheQOS(t *testing.T) {
	allowed := map[string]bool{"heldRequest": true, "TestRoundTripJobNameAgainstTheFakePlatform": true}
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test files: %v", err)
	}
	builds := 0
	for _, file := range files {
		f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range submitRequestBuilds(f) {
			builds++
			if !allowed[b.where] {
				t.Errorf("%s: %s builds a provider.SubmitRequest as %s; use heldRequest, which carries the Provider's qos", file, b.where, b.what)
			}
		}
	}
	if builds < 2 {
		t.Errorf("found %d provider.SubmitRequest builds, want at least heldRequest's and the fake test's: the scan is not looking at the code", builds)
	}
}

// build is one place a provider.SubmitRequest is made: where (a function's name,
// or "package level") and what (a literal, a var, new).
type build struct{ where, what string }

// submitRequestBuilds finds them in one file. Types declared as provider.SubmitRequest, by
// alias or by definition, count as it. (The aliases of other files of the package are not
// followed; the package declares none.)
func submitRequestBuilds(f *ast.File) []build {
	names := map[string]bool{}
	for _, d := range f.Decls {
		if g, ok := d.(*ast.GenDecl); ok {
			for _, sp := range g.Specs {
				if ts, ok := sp.(*ast.TypeSpec); ok {
					if sel, ok := ts.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "SubmitRequest" {
						names[ts.Name.Name] = true
					}
				}
			}
		}
	}
	isType := func(e ast.Expr) bool {
		switch x := e.(type) {
		case *ast.SelectorExpr:
			return x.Sel.Name == "SubmitRequest"
		case *ast.Ident:
			return names[x.Name]
		}
		return false
	}
	var out []build
	for _, d := range f.Decls {
		where := "package level"
		if fn, ok := d.(*ast.FuncDecl); ok {
			where = fn.Name.Name
		}
		ast.Inspect(d, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CompositeLit:
				if x.Type != nil && isType(x.Type) {
					out = append(out, build{where, "a literal"})
				}
			case *ast.ValueSpec:
				if x.Type != nil && isType(x.Type) {
					out = append(out, build{where, "a var"})
				}
			case *ast.CallExpr:
				if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "new" && len(x.Args) == 1 && isType(x.Args[0]) {
					out = append(out, build{where, "new(...)"})
				}
			}
			return true
		})
	}
	return out
}

// The scan, shown to see what it claims to: package-level literals, aliases, defined types, vars and new(...).
func TestTheSubmitRequestScanSeesTheShapesItClaims(t *testing.T) {
	for name, src := range map[string]string{
		"a package-level literal": `package x
var r = provider.SubmitRequest{}`,
		"an alias": `package x
type req = provider.SubmitRequest
func f() { _ = req{} }`,
		"a defined type": `package x
type req provider.SubmitRequest
func f() { _ = &req{} }`,
		"a var": `package x
func f() { var r provider.SubmitRequest; _ = r }`,
		"new": `package x
func f() { _ = new(provider.SubmitRequest) }`,
	} {
		t.Run(name, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "x.go", src, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(submitRequestBuilds(f)) == 0 {
				t.Errorf("the scan does not see %s", name)
			}
		})
	}
}

// hack/live-checks.sh waits longer for a submission the run could not settle only if the run says so: both
// places that see an ambiguous outcome (the reconcile that finds the Allocation in SubmissionUnknown, and step
// 3's direct submit) write the tag down with unresolve.
func TestBothAmbiguousSubmissionsAreWrittenDown(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "conformance_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	in := map[string]int{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "unresolve" {
					in[fn.Name.Name]++
				}
			}
			return true
		})
	}
	if in["reconcile"] != 1 || in["TestConformance"] != 1 {
		t.Errorf("unresolve is called %v; want once in reconcile and once in TestConformance", in)
	}
}

// The row's start wait (the science agent's ruling, 2 Nov 2026: each step's job
// must start within 10 minutes of its acceptance) reaches the run as
// HPCBURST_CONF_START_WAIT, and the row's timeout is not below what the test
// can take with it: two start waits, and the test's other deadlines on jobs that
// already run (5 min to delete, 3 for accounting, 8 for the second job, 3 for
// accounting, 3 for the name round trip), and the end-of-run wait for a late
// duplicate: 44 min for the GH200 row, under its 45.
func TestTheGH200RowsStartWaitReachesTheRunAndItsTimeoutCoversTheTest(t *testing.T) {
	otherDeadlines := 5 + 3 + 8 + 3 + 3 + int(duplicateWait/time.Minute)
	seen := 0
	for _, r := range readShapeRows(t) {
		if r.profile == "-" || r.startWait == "-" || notAConformanceRun(r.name) {
			continue
		}
		seen++
		t.Run(r.name, func(t *testing.T) {
			n, err := strconv.Atoi(r.startWait)
			if err != nil || n < 1 || n > 30 {
				t.Fatalf("start wait %q is not whole minutes from 1 to 30", r.startWait)
			}
			s, _ := r.validated(t)
			if s.startWait != time.Duration(n)*time.Minute {
				t.Errorf("the run's startWait = %v, the row says %d min", s.startWait, n)
			}
			timeout, err := strconv.Atoi(r.timeout)
			if err != nil {
				t.Fatal(err)
			}
			want := 2*n + otherDeadlines
			if strings.HasPrefix(r.name, "gpu-accounting-") {
				// The accounting row has one job: its start wait, then its end wait and the deletion (gpuacct_test.go).
				want = n + int((accountingEndWait+accountingDeleteWait)/time.Minute)
			}
			if strings.HasPrefix(r.name, "reach-") {
				// The reach row has one job: its start wait, then its end wait, and the read-back and the cleanup after it
				// (reach_run_test.go).
				want = n + int((reachEndWait+reachCleanupWait)/time.Minute)
			}
			if timeout < want {
				t.Errorf("timeout %d min is below the test's own deadlines with this start wait, %d min", timeout, want)
			}
		})
	}
	if seen == 0 {
		t.Error("no row has a start wait: the check looked at nothing")
	}
}
