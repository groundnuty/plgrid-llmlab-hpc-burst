package enginedouble

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
)

// The fake Apptainer. The real one is called as the agent calls it: pull and
// build for images, exec for the engine. The images are text files: a .sif is
// a file and a .sandbox is a directory, each holding "fake-sif <image>@<digest>".

var imageRef = regexp.MustCompile(`^docker://([^@\s]+)@(sha256:[0-9a-f]{64})$`)

// execBoolFlags and execValueFlags are the exec options the fake accepts; any
// other flag is refused, so a flag the agent starts passing is not ignored
// unseen.
var (
	execBoolFlags  = map[string]bool{"--nv": true, "--cleanenv": true, "-e": true, "--contain": true, "-c": true, "--containall": true, "-C": true, "--no-home": true, "--writable-tmpfs": true}
	execValueFlags = map[string]bool{"--bind": true, "-B": true, "--home": true, "-H": true, "--pwd": true, "--workdir": true, "-W": true}
)

const (
	apptainerEnvPrefix   = "APPTAINERENV_"
	singularityEnvPrefix = "SINGULARITYENV_"
)

// bind is one --bind: a host source, a container destination and the options.
type bind struct{ src, dst, opts string }

// ApptainerMain is the fake apptainer: pull, build --sandbox and exec. args
// are the arguments after the program name. It exits 255 on any refusal, as
// Apptainer does on a fatal error. When HB_FAKE_APPTAINER_LOG is set, each
// invocation is recorded as one JSON line (see ExecRecord).
func ApptainerMain(args []string) int {
	if len(args) == 0 {
		recordApptainer(ExecRecord{Argv: args, PID: os.Getpid()})
		return apptainerFatal("no command: pull, build or exec")
	}
	switch args[0] {
	case "pull", "build":
		return apptainerFetch(args)
	case "exec":
		return apptainerExec(args)
	}
	recordApptainer(ExecRecord{Argv: args, PID: os.Getpid()})
	return apptainerFatal("unknown command %q: the fake knows pull, build and exec", args[0])
}

func apptainerFatal(format string, a ...any) int {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", a...)
	return 255
}

// ExecRecord is one line of HB_FAKE_APPTAINER_LOG. Argv is the invocation's
// arguments after the program name. For exec, Env is the child's environment,
// sorted, with the value of a variable that came from APPTAINERENV_ and whose
// name ends in KEY or TOKEN replaced by its sha256 hex, and PID is the child's;
// for pull and build there is no Env and PID is the fake's own.
type ExecRecord struct {
	Argv []string `json:"argv"`
	Env  []string `json:"env,omitempty"`
	PID  int      `json:"pid"`
}

func recordApptainer(r ExecRecord) {
	if p := os.Getenv(EnvApptainerLog); p != "" {
		if err := (&logFile{path: p}).write(r); err != nil {
			fmt.Fprintln(os.Stderr, "enginedouble: log:", err)
		}
	}
}

// apptainerFetch is "pull [--force] <dest> docker://<image>@sha256:<64 hex>"
// and "build [--force] --sandbox <dest> docker://...". Like Apptainer, it
// refuses to overwrite an existing image without --force.
func apptainerFetch(args []string) int {
	recordApptainer(ExecRecord{Argv: args, PID: os.Getpid()}) // a refused call is recorded too
	sandbox, force := false, false
	var pos []string
	for _, a := range args[1:] {
		switch a {
		case "--sandbox":
			sandbox = true
		case "--force", "-F":
			force = true
		default:
			if strings.HasPrefix(a, "-") {
				return apptainerFatal("%s: unknown flag %q", args[0], a)
			}
			pos = append(pos, a)
		}
	}
	if args[0] == "build" && !sandbox {
		return apptainerFatal("build: the fake builds only with --sandbox")
	}
	if args[0] == "pull" && sandbox {
		return apptainerFatal("pull: --sandbox belongs to build")
	}
	if len(pos) != 2 {
		return apptainerFatal("%s: want <dest> docker://<image>@sha256:<64 hex>, got %d arguments", args[0], len(pos))
	}
	dest := pos[0]
	m := imageRef.FindStringSubmatch(pos[1])
	if m == nil {
		return apptainerFatal("%s: %q is not docker://<image>@sha256:<64 hex>: images are pulled by digest", args[0], pos[1])
	}
	if _, err := os.Lstat(dest); err == nil {
		if !force {
			return apptainerFatal("image file already exists: %q - will not overwrite", dest)
		}
		if err := os.RemoveAll(dest); err != nil {
			return apptainerFatal("%v", err)
		}
	}
	content := []byte("fake-sif " + m[1] + "@" + m[2] + "\n")
	if sandbox {
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return apptainerFatal("%v", err)
		}
		dest = filepath.Join(dest, ".fake-sandbox")
	}
	if err := os.WriteFile(dest, content, 0o644); err != nil {
		return apptainerFatal("%v", err)
	}
	return 0
}

// apptainerExec is "exec [flags] [--bind src:dst[:opts]]... <image> <cmd> [args...]".
func apptainerExec(args []string) int {
	// A refused call is recorded with the fake's own PID and no environment;
	// an accepted one is recorded once the child has started.
	refuse := func(format string, a ...any) int {
		recordApptainer(ExecRecord{Argv: args, PID: os.Getpid()})
		return apptainerFatal(format, a...)
	}
	var (
		binds    []bind
		cleanEnv bool
		i        = 1
	)
	for ; i < len(args) && strings.HasPrefix(args[i], "-"); i++ {
		name, val, hasVal := strings.Cut(args[i], "=")
		switch {
		case execBoolFlags[name] && !hasVal:
			cleanEnv = cleanEnv || name == "--cleanenv" || name == "-e"
		case execValueFlags[name]:
			if !hasVal {
				if i+1 >= len(args) {
					return refuse("exec: %s needs a value", name)
				}
				i++
				val = args[i]
			}
			if name == "--bind" || name == "-B" {
				bs, err := parseBinds(val)
				if err != nil {
					return refuse("exec: %v", err)
				}
				binds = append(binds, bs...)
			}
		default:
			return refuse("exec: unknown flag %q", args[i])
		}
	}
	if i+1 >= len(args) {
		return refuse("exec: want <image> <command> [args...]")
	}
	image, cmdArgs := args[i], args[i+1:]
	if err := checkImage(image); err != nil {
		return refuse("%v", err)
	}
	for _, b := range binds {
		if _, err := os.Stat(b.src); err != nil {
			return refuse("mount source %q does not exist", b.src)
		}
	}
	env, hidden := childEnv(os.Environ(), cleanEnv)
	rewritten := make([]string, len(cmdArgs)-1)
	for k, a := range cmdArgs[1:] {
		rewritten[k] = rewriteArg(a, binds)
	}

	var cmd *exec.Cmd
	if cmdArgs[0] == "vllm" {
		exe, err := os.Executable()
		if err != nil {
			return refuse("%v", err)
		}
		cmd = exec.Command(exe, rewritten...)
		env = setEnv(env, EnvRole, RoleEngine)
	} else {
		path, err := exec.LookPath(cmdArgs[0])
		if err != nil {
			return refuse("exec: %q: executable file not found in $PATH", cmdArgs[0])
		}
		cmd = exec.Command(path, rewritten...)
	}
	cmd.Env = env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr

	// Forward TERM and INT to the child and live on until it has gone; the
	// handler is armed before the child starts, so no signal slips through.
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)
	if err := cmd.Start(); err != nil {
		return refuse("exec: %v", err)
	}
	recordApptainer(ExecRecord{Argv: args, Env: redactedEnv(env, func(n string) bool { return hidden[n] && secretName(n) }), PID: cmd.Process.Pid})
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	for {
		select {
		case s := <-sigs:
			_ = cmd.Process.Signal(s)
		case <-done:
			return exitCodeOf(cmd.ProcessState)
		}
	}
}

// exitCodeOf is the child's exit code, or 128 plus the signal that ended it.
func exitCodeOf(st *os.ProcessState) int {
	if st == nil {
		return 255
	}
	if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return st.ExitCode()
}

// checkImage: a regular file named *.sif, or a directory named *.sandbox.
func checkImage(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("image %q: %v", path, err)
	}
	switch {
	case strings.HasSuffix(path, ".sif") && st.Mode().IsRegular():
		return nil
	case strings.HasSuffix(path, ".sandbox") && st.IsDir():
		return nil
	}
	return fmt.Errorf("image %q is neither a .sif file nor a .sandbox directory", path)
}

// parseBinds reads one --bind value: src:dst[:opts], possibly several
// separated by commas.
func parseBinds(v string) ([]bind, error) {
	var out []bind
	for _, spec := range strings.Split(v, ",") {
		parts := strings.Split(spec, ":")
		if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || !strings.HasPrefix(parts[1], "/") {
			return nil, fmt.Errorf("bind %q is not src:dst[:opts] with an absolute dst", spec)
		}
		b := bind{src: parts[0], dst: strings.TrimRight(parts[1], "/")}
		if len(parts) == 3 {
			b.opts = parts[2]
		}
		out = append(out, b)
	}
	return out, nil
}

// rewriteArg maps a container path to the host path of the bind that covers
// it, on path-component boundaries and the longest destination first, for a
// whole argument and for the value of --flag=value. An argument no bind covers
// stays as it is.
func rewriteArg(arg string, binds []bind) string {
	if strings.HasPrefix(arg, "--") {
		if name, val, ok := strings.Cut(arg, "="); ok {
			return name + "=" + rewritePath(val, binds)
		}
	}
	return rewritePath(arg, binds)
}

func rewritePath(p string, binds []bind) string {
	sorted := append([]bind(nil), binds...)
	sort.SliceStable(sorted, func(i, j int) bool { return len(sorted[i].dst) > len(sorted[j].dst) })
	for _, b := range sorted {
		if p == b.dst || strings.HasPrefix(p, b.dst+"/") {
			return b.src + strings.TrimPrefix(p, b.dst)
		}
	}
	return p
}

// childEnv builds the container's environment: empty with --cleanenv, else the
// caller's own, without the APPTAINERENV_ and SINGULARITYENV_ variables
// themselves; then X=v for each of those, APPTAINERENV_ winning over
// SINGULARITYENV_. hidden names the variables that came from a prefix.
func childEnv(own []string, clean bool) (env []string, hidden map[string]bool) {
	hidden = map[string]bool{}
	var plain []string
	set := map[string]string{}
	for _, prefix := range []string{singularityEnvPrefix, apptainerEnvPrefix} {
		for _, kv := range own {
			if name, val, _ := strings.Cut(kv, "="); strings.HasPrefix(name, prefix) && len(name) > len(prefix) {
				set[strings.TrimPrefix(name, prefix)] = val
			}
		}
	}
	if !clean {
		for _, kv := range own {
			if !strings.HasPrefix(kv, apptainerEnvPrefix) && !strings.HasPrefix(kv, singularityEnvPrefix) {
				plain = append(plain, kv)
			}
		}
	}
	env = plain
	for name, val := range set {
		env = setEnv(env, name, val)
		hidden[name] = true
	}
	sort.Strings(env)
	return env, hidden
}

// setEnv sets name in env, replacing an earlier value.
func setEnv(env []string, name, val string) []string {
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		if !strings.HasPrefix(kv, name+"=") {
			out = append(out, kv)
		}
	}
	return append(out, name+"="+val)
}
