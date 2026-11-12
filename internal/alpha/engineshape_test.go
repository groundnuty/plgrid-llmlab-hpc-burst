package alpha

// The engine's pick in each candidate shape of issue #445, and the redacted shape of the job's process tree. The
// trees are written out by hand, one literal per shape, and so are the expected shapes: neither comes from the code
// under test. The kinds, the forms and the three conditions are the science agent's ruling on #445 (depth, a kind
// for argv[0], in the job, the cgroup's form, PickEngine's conditions; no path, login, host or number beyond small
// counts and depths). The engine to kill is the process the agent launched through Apptainer: its exit is what the
// agent reports and the worker rung restarts.

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// shapeCmd is the launch's command in these tests, and shapeEngine the engine's command line as a console script
// run by its shebang.
var (
	shapeCmd    = []string{"vllm", "serve", "/hpcburst/model", "--port=8000"}
	shapeEngine = []string{"/usr/bin/python3", "/usr/local/bin/vllm", "serve", "/hpcburst/model", "--port=8000"}
)

// shapeHeader is the shape's first line for these counts.
func shapeHeader(listed, inTree, engines int) string {
	return fmt.Sprintf("the job's process tree on its node, redacted (kinds, depths and counts only): %d processes listed, %d in the job's tree, %d meeting all three conditions", listed, inTree, engines)
}

func TestPickEngineInEachShape(t *testing.T) {
	agent := NodeProc{PID: 5, PPID: 1, InJob: true, Env: EnvJob, Argv: []string{"/scratch/stage/hb-agent", "run"}}
	// Of the job by its cgroup: the job's processes on a node whose cgroup names the job.
	cg := func(p NodeProc) NodeProc {
		p.InJob, p.CgroupJob, p.CgroupJobForm = true, true, true
		return p
	}
	rp := cg(NodeProc{PID: 10, PPID: 5, Env: EnvUnreadable, Argv: []string{"Apptainer runtime parent"}})
	agentLine := "d0 other in-job:env env:job cgroup:other argv:program meets:job parent:unlisted"
	rpLine := "d1 runtime-parent in-job:cgroup env:unreadable cgroup:job_<n> argv:program meets:job"
	for _, c := range []struct {
		name  string
		procs []NodeProc
		want  int // the engine's PID
		shape []string
	}{
		{
			// (a) The engine's parent is Apptainer's shim, which renames itself appinit, its argv[0] overwritten in
			// place and the rest of the old one NULs (Apptainer 1.4.5, process_linux.go, StartProcess).
			name: "(a) the parent is the starter's shim, appinit",
			procs: []NodeProc{agent, rp,
				cg(NodeProc{PID: 11, PPID: 10, Env: EnvOther, Argv: []string{"appinit", "", "", ""}}),
				cg(NodeProc{PID: 12, PPID: 11, Env: EnvOther, Argv: shapeEngine})},
			want: 12,
			shape: []string{shapeHeader(4, 4, 1), agentLine, rpLine,
				"d2 starter in-job:cgroup env:no cgroup:job_<n> argv:program meets:job+apptainer",
				"d3 python in-job:cgroup env:no cgroup:job_<n> argv:python-script meets:job+apptainer+command"},
		},
		{
			// (b) A shell between Apptainer and the engine, and vLLM v1's own children under the engine: a forked
			// copy with the engine's very command line, a spawned EngineCore and one that set its title. The engine
			// is the API server, the process the agent launched; its children are never picked.
			name: "(b) a shell between, and the engine's own children",
			procs: []NodeProc{agent, rp,
				cg(NodeProc{PID: 11, PPID: 10, Env: EnvOther, Argv: []string{"/bin/sh", "/.singularity.d/actions/exec"}}),
				cg(NodeProc{PID: 12, PPID: 11, Env: EnvOther, Argv: shapeEngine}),
				cg(NodeProc{PID: 13, PPID: 12, Env: EnvOther, Argv: shapeEngine}),
				cg(NodeProc{PID: 14, PPID: 12, Env: EnvOther, Argv: []string{"/usr/bin/python3", "-c", "from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=5, pipe_handle=7)", "--multiprocessing-fork"}}),
				cg(NodeProc{PID: 15, PPID: 12, Env: EnvOther, Argv: []string{"VLLM::EngineCore"}})},
			want: 12,
			shape: []string{shapeHeader(7, 7, 1), agentLine, rpLine,
				"d2 shell in-job:cgroup env:no cgroup:job_<n> argv:program meets:job+apptainer",
				"d3 python in-job:cgroup env:no cgroup:job_<n> argv:python-script meets:job+apptainer+command",
				"d4 python in-job:cgroup env:no cgroup:job_<n> argv:python-code meets:job",
				"d4 python in-job:cgroup env:no cgroup:job_<n> argv:python-script meets:job+command",
				"d4 vllm in-job:cgroup env:no cgroup:job_<n> argv:program meets:job"},
		},
		{
			// (c) The interpreter with an option that takes a value before the program's path.
			name: "(c) the interpreter with options",
			procs: []NodeProc{agent, rp,
				cg(NodeProc{PID: 12, PPID: 10, Env: EnvOther, Argv: append([]string{"/usr/bin/python3", "-X", "faulthandler"}, shapeEngine[1:]...)})},
			want: 12,
			shape: []string{shapeHeader(3, 3, 1), agentLine, rpLine,
				"d2 python in-job:cgroup env:no cgroup:job_<n> argv:python-options-script meets:job+apptainer+command"},
		},
		{
			// (d) No cgroup segment names the job, Apptainer's setuid master is not dumpable (its environment cannot
			// be read) and the engine runs --cleanenv: both are of the job by descent from the agent alone.
			name: "(d) of the job by descent alone",
			procs: []NodeProc{agent,
				{PID: 10, PPID: 5, Env: EnvUnreadable, Argv: []string{"Apptainer runtime parent"}},
				{PID: 12, PPID: 10, Env: EnvOther, Argv: shapeEngine}},
			want: 12,
			shape: []string{shapeHeader(3, 3, 1), agentLine,
				"d1 runtime-parent in-job:descent env:unreadable cgroup:other argv:program meets:job",
				"d2 python in-job:descent env:no cgroup:other argv:python-script meets:job+apptainer+command"},
		},
		{
			// (e) The engine's parent is not in the listing (a process another user owns): diagnosed, not picked.
			name:  "(e) the parent unlisted",
			procs: []NodeProc{agent, cg(NodeProc{PID: 12, PPID: 10, Env: EnvOther, Argv: shapeEngine})},
			shape: []string{shapeHeader(2, 2, 0), agentLine,
				"d0 python in-job:cgroup env:no cgroup:job_<n> argv:python-script meets:job+command parent:unlisted"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			p, err := PickEngine(c.procs, shapeCmd, MatchesCommand)
			if c.want == 0 {
				var ec ErrEngineCount
				if !errors.As(err, &ec) || ec.N != 0 || ec.Shape != strings.Join(c.shape, "\n  ") {
					t.Errorf("PickEngine = %+v, %v; want no engine and the shape\n%s", p, err, strings.Join(c.shape, "\n  "))
				}
				return
			}
			if err != nil || p.PID != c.want {
				t.Errorf("PickEngine = %+v, %v; want PID %d", p, err, c.want)
			}
			if got, want := Shape(c.procs, shapeCmd, MatchesCommand), strings.Join(c.shape, "\n  "); got != want {
				t.Errorf("the shape is\n%s\nwant\n%s", got, want)
			}
		})
	}
}

// Near misses: the launch's very command, never picked unless it is the job's engine. Each is shown beside the
// job's engine (the pick is that engine alone) and alone (the pick is none).
func TestPickEngineNeverPicksANearMiss(t *testing.T) {
	agent := NodeProc{PID: 5, PPID: 1, InJob: true, Env: EnvJob, Argv: []string{"/scratch/stage/hb-agent", "run"}}
	// The job's tree of shape (d), whose membership by descent is the widest of the rules.
	job := []NodeProc{agent,
		{PID: 10, PPID: 5, Env: EnvUnreadable, Argv: []string{"Apptainer runtime parent"}},
		{PID: 12, PPID: 10, Env: EnvOther, Argv: shapeEngine}}
	rpOut := NodeProc{PID: 30, PPID: 1, Env: EnvOther, Argv: []string{"Apptainer runtime parent"}}
	for name, miss := range map[string][]NodeProc{
		"outside the job, under Apptainer":           {rpOut, {PID: 31, PPID: 30, Env: EnvOther, Argv: shapeEngine}},
		"outside the job, under a shell":             {rpOut, {PID: 32, PPID: 30, Env: EnvOther, Argv: []string{"bash"}}, {PID: 33, PPID: 32, Argv: shapeEngine}},
		"an environment that cannot be read":         {{PID: 30, PPID: 1, Env: EnvUnreadable, Argv: rpOut.Argv}, {PID: 31, PPID: 30, Env: EnvUnreadable, Argv: shapeEngine}},
		"in the job, run by the agent itself":        {{PID: 40, PPID: 5, InJob: true, Env: EnvJob, Argv: shapeEngine}},
		"in the job, under a shell of the agent":     {{PID: 41, PPID: 5, InJob: true, Argv: []string{"/bin/bash", "run.sh"}}, {PID: 42, PPID: 41, InJob: true, Argv: shapeEngine}},
		"in the job, under Apptainer outside it":     {rpOut, {PID: 51, PPID: 30, InJob: true, CgroupJob: true, CgroupJobForm: true, Argv: shapeEngine}},
		"in the job, under a program not a shell":    {{PID: 60, PPID: 10, Env: EnvOther, Argv: []string{"/usr/bin/python3", "/opt/launcher.py"}}, {PID: 61, PPID: 60, Env: EnvOther, Argv: shapeEngine}},
		"in the job, a forked copy under the engine": {{PID: 70, PPID: 12, Env: EnvOther, Argv: shapeEngine}},
		// Membership by descent, both sides (team-lead's review of the plan): a process with the launch's command
		// that descends from no process of the job (reparented to init, its environment clean, in another job's
		// cgroup or none) and one that descends from ANOTHER job's processes are never the job's.
		"reparented to init, no job":                 {{PID: 80, PPID: 1, Env: EnvOther, Argv: shapeEngine}},
		"reparented to init, another job's cgroup":   {{PID: 81, PPID: 1, Env: EnvOther, CgroupJobForm: true, Argv: shapeEngine}},
		"reparented to init, environment unreadable": {{PID: 82, PPID: 1, Env: EnvUnreadable, Argv: shapeEngine}},
		"descended from another job": {
			{PID: 90, PPID: 1, Env: EnvOther, CgroupJobForm: true, Argv: agent.Argv},
			{PID: 91, PPID: 90, Env: EnvUnreadable, CgroupJobForm: true, Argv: rpOut.Argv},
			{PID: 92, PPID: 91, Env: EnvOther, CgroupJobForm: true, Argv: shapeEngine}},
	} {
		t.Run(name, func(t *testing.T) {
			if p, err := PickEngine(append(append([]NodeProc(nil), job...), miss...), shapeCmd, MatchesCommand); err != nil || p.PID != 12 {
				t.Errorf("beside the job's engine: PickEngine = %+v, %v; want PID 12", p, err)
			}
			alone := append([]NodeProc{agent}, miss...)
			if name == "in the job, under a program not a shell" || name == "in the job, a forked copy under the engine" {
				alone = append(append([]NodeProc(nil), job[:2]...), miss...) // its parents, without the engine
			}
			var ec ErrEngineCount
			if p, err := PickEngine(alone, shapeCmd, MatchesCommand); !errors.As(err, &ec) || ec.N != 0 {
				t.Errorf("alone: PickEngine = %+v, %v; want none", p, err)
			}
		})
	}
}

// The kinds of argv[0], from the names Apptainer and SingularityCE give their processes and from the forms a shell,
// an interpreter and vLLM's titles take; an Apptainer kind is what PickEngine walks up to.
func TestKindOf(t *testing.T) {
	for _, c := range []struct {
		argv      []string
		kind      string
		apptainer bool
	}{
		{[]string{"Apptainer runtime parent"}, "runtime-parent", true},
		{[]string{"Apptainer runtime parent: /x/vllm.sandbox"}, "runtime-parent", true},
		{[]string{"Singularity runtime parent"}, "runtime-parent", true},
		{[]string{"/usr/libexec/apptainer/bin/starter-suid"}, "starter", true},
		{[]string{"/usr/libexec/apptainer/bin/starter"}, "starter", true},
		{[]string{"appinit", "", ""}, "starter", true},
		{[]string{"sinit"}, "starter", true},
		{[]string{"/usr/bin/apptainer", "exec"}, "apptainer", true},
		{[]string{"/usr/bin/singularity", "exec"}, "apptainer", true},
		{[]string{"/usr/bin/python3", "/usr/local/bin/vllm"}, "python", false},
		{[]string{"python3.12"}, "python", false},
		{[]string{"python"}, "python", false},
		{[]string{"/usr/local/bin/vllm", "serve"}, "vllm", false},
		{[]string{"VLLM::EngineCore"}, "vllm", false},
		{[]string{"/bin/sh", "-c", "x"}, "shell", false},
		{[]string{"-bash"}, "shell", false},
		{[]string{"/usr/bin/dash"}, "shell", false},
		{[]string{"/scratch/stage/hb-agent"}, "other", false},
		{[]string{"squashfuse_ll"}, "other", false},
		{[]string{"/usr/bin/pythonista"}, "other", false},
		{[]string{"my-starter"}, "other", false},
		{nil, "other", false},
	} {
		if k, a := kindOf(c.argv), isApptainer(c.argv); k != c.kind || a != c.apptainer {
			t.Errorf("kindOf(%q) = %s, isApptainer %v; want %s, %v", c.argv, k, a, c.kind, c.apptainer)
		}
	}
}

func TestArgvForm(t *testing.T) {
	for _, c := range []struct {
		argv []string
		form string
	}{
		{[]string{"/usr/local/bin/vllm", "serve"}, "program"},
		{[]string{"Apptainer runtime parent"}, "program"},
		{nil, "program"},
		{[]string{"/usr/bin/python3", "/usr/local/bin/vllm", "serve"}, "python-script"},
		{[]string{"python3", "-s", "/usr/local/bin/vllm"}, "python-options-script"},
		{[]string{"python3", "-X", "faulthandler", "/usr/local/bin/vllm"}, "python-options-script"},
		{[]string{"python3", "-m", "vllm.entrypoints.cli.main", "serve"}, "python-module"},
		{[]string{"python3", "-c", "from multiprocessing.spawn import spawn_main"}, "python-code"},
		{[]string{"python3", "vllm"}, "python-other"},
		{[]string{"python3"}, "python"},
	} {
		if f := argvForm(c.argv); f != c.form {
			t.Errorf("argvForm(%q) = %s, want %s", c.argv, f, c.form)
		}
	}
}

// numberRE finds the numbers in a shape.
var numberRE = regexp.MustCompile(`[0-9]+`)

// shapeLeaks lists what in a shape is not a kind, a depth or a count: a path's or a host's punctuation, a forbidden
// token (matched as a whole token, so that a short one inside a longer word is not a false hit), and a number above
// max, the largest count a shape of the listing can carry.
func shapeLeaks(shape string, forbidden []string, max int) []string {
	var out []string
	for _, ch := range []string{"/", "@", "."} {
		if strings.Contains(shape, ch) {
			out = append(out, fmt.Sprintf("the character %q", ch))
		}
	}
	tokens := map[string]bool{}
	for _, tok := range regexp.MustCompile(`[^A-Za-z0-9_-]+`).Split(shape, -1) {
		tokens[strings.ToLower(tok)] = true
	}
	for _, f := range forbidden {
		if tokens[strings.ToLower(f)] {
			out = append(out, fmt.Sprintf("the token %q", f))
		}
	}
	for _, n := range numberRE.FindAllString(shape, -1) {
		if v, err := strconv.Atoi(n); err != nil || v > max {
			out = append(out, fmt.Sprintf("the number %s", n))
		}
	}
	return out
}

// The leak check finds what it must: a test of the test (IMPL-COMMON, a "never contains" guard).
func TestShapeLeaksFindsALeak(t *testing.T) {
	for shape, want := range map[string]string{
		"d0 other meets:job /net":   `the character "/"`,
		"d0 other user@host":        `the character "@"`,
		"d0 other host1.example":    `the character "."`,
		"d0 plgjdoe meets:job":      `the token "plgjdoe"`,
		"d0 other 7654321 meets:no": "the number 7654321",
		"d7 other":                  "the number 7",
	} {
		if got := shapeLeaks(shape, []string{"plgjdoe"}, 6); len(got) == 0 || !slicesContain(got, want) {
			t.Errorf("shapeLeaks(%q) = %q, want it to report %s", shape, got, want)
		}
	}
	// A forbidden token inside a longer word is no leak: the shape's words are its own.
	if got := shapeLeaks("d0 python meets:job", []string{"py", "job_1"}, 6); len(got) != 0 {
		t.Errorf("shapeLeaks reported %q in a clean shape", got)
	}
}

func slicesContain(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// The shape holds no path, login, host or number beyond small counts and depths (the science agent's ruling on
// #445). The listing below is full of real-looking identifiers in every field a listing has (the PIDs, the start
// times, and each argument of each command line); the listing carries no environment and no cgroup path, only what
// they said of the job. None of them appears in the shape, and its numbers are its counts and depths alone. The
// shape still tells the case apart: the engine under the runtime parent, of the job by its cgroup.
func TestTheShapeLeaksNothing(t *testing.T) {
	const job = "7654321"
	listing := strings.Join([]string{
		"self 4100001", // its own process line left out of the listing: a child of it is still the listing's own
		"proc 3456701 3456700 925087416 1 0 o " + b64("/net/people/plgjdoe/hpc-burst/allocs/hb-x7f3q/hb-agent", "--node=ac0412.athena.cyfronet.example", "--job="+job),
		"proc 3456702 3456701 925087417 u 1 j " + b64("Apptainer runtime parent: /net/people/plgjdoe/images/vllm-0.32.sandbox"),
		"proc 3456703 3456702 925087418 0 1 j " + b64("/usr/bin/python3", "-X", "pycache_prefix=/home/plgjdoe/.cache", "/usr/local/bin/vllm", "serve",
			"/hpcburst/model", "--port=29455", "--served-model-name=org/Model-7B"),
		"proc 3456704 3456703 925087419 0 1 j " + b64("VLLM::EngineCore_DP0"),
		"proc 3456705 3456701 925087420 1 1 j " + b64("/bin/bash", "/net/people/plgjdoe/hpc-burst/allocs/hb-x7f3q/job_"+job+".sh"),
		"proc 3456706 1 925087421 1 0 o " + b64("ssh", "plgjdoe@ac0412.athena.cyfronet.example"),
		"proc 4100002 4100001 925087422 1 1 j " + b64("grep", "-qx", "SLURM_JOB_ID="+job),
		"listed", "",
	}, "\n")
	procs, err := ParseListing([]byte(listing))
	if err != nil {
		t.Fatal(err)
	}
	command := []string{"vllm", "serve", "/hpcburst/model", "--port=29455", "--served-model-name=org/Model-7B"}
	shape := Shape(procs, command, MatchesCommand)
	forbidden := []string{"plgjdoe", "ac0412", "athena", "cyfronet", "example", "people", "net", "home", "images", "allocs",
		"hb-x7f3q", "hb-agent", "hpc-burst", "hpcburst", "model", "Model-7B", "org", "sandbox", "cache", "pycache_prefix",
		"EngineCore_DP0", "DP0", "serve", "served-model-name", "port", "bash", "ssh", "grep", "SLURM_JOB_ID", job,
		"job_" + job, "29455", "4100001", "4100002", "3456700", "3456701", "3456702", "3456703", "3456704", "3456705",
		"3456706", "925087416", "925087417", "925087418", "925087419", "925087420", "925087421", "925087422"}
	if leaks := shapeLeaks(shape, forbidden, len(procs)); len(leaks) != 0 {
		t.Errorf("the shape leaks %q:\n%s", leaks, shape)
	}
	if !strings.Contains(shape, "\n  d1 runtime-parent in-job:cgroup env:unreadable cgroup:job_<n> argv:program meets:job\n") ||
		!strings.Contains(shape, "\n  d2 python in-job:cgroup env:no cgroup:job_<n> argv:python-options-script meets:job+apptainer+command\n") ||
		!strings.Contains(shape, "\n  d1 shell in-job:env+cgroup env:job cgroup:job_<n> argv:program meets:job") {
		t.Errorf("the shape does not show the engine under the runtime parent:\n%s", shape)
	}
	if p, err := PickEngine(procs, command, MatchesCommand); err != nil || p.PID != 3456703 {
		t.Errorf("PickEngine = %+v, %v; want the engine", p, err)
	}
	// A failure's message is the shape's too, and leaks nothing either.
	_, err = PickEngine(procs, shapeCmd, MatchesCommand)
	if err == nil || !strings.Contains(err.Error(), shapeHeader(len(procs), 6, 0)) {
		t.Fatalf("PickEngine with another command: %v, want a count of 0 with the shape", err)
	}
	if leaks := shapeLeaks(err.Error(), forbidden, len(procs)); len(leaks) != 0 {
		t.Errorf("the failure leaks %q:\n%v", leaks, err)
	}
}

// A shape past maxShapeLines processes is cut, with the count of the rest.
func TestTheShapeIsBounded(t *testing.T) {
	procs := []NodeProc{{PID: 5, PPID: 1, InJob: true, Env: EnvJob, Argv: []string{"bash"}}}
	for i := range 60 {
		procs = append(procs, NodeProc{PID: 100 + i, PPID: 5, InJob: true, Env: EnvJob, Argv: []string{"sleep", "300"}})
	}
	s := Shape(procs, shapeCmd, MatchesCommand)
	if n := strings.Count(s, "\n  d"); n != maxShapeLines || !strings.HasSuffix(s, "\n  and 21 more") {
		t.Errorf("the shape of 61 processes has %d process lines and ends %q, want %d and \"and 21 more\"", n, s[max(0, len(s)-20):], maxShapeLines)
	}
}
