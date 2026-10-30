package fakeslurm

import (
	"fmt"
	"path/filepath"
	"strings"
)

// app is one command invocation.
type app struct {
	env  Env
	cfg  Config
	argv []string // full argv, argv[0] the command word as invoked
}

// result is what a locked command hands back for printing after the lock is
// released.
type result struct {
	out, errOut string
	exit        int
	jobID       int
}

var commands = map[string]func(*app, []string) int{
	"sbatch":       (*app).sbatch,
	"squeue":       (*app).squeue,
	"sacct":        (*app).sacct,
	"scancel":      (*app).scancel,
	"fakeslurmctl": (*app).ctl,
}

// Main dispatches on the base name of argv[0] (sbatch, squeue, sacct,
// scancel, fakeslurmctl). Under any other name, such as the multi-call binary
// itself, the first argument names the command. It returns the exit code.
func Main(env Env, argv []string) int {
	env = env.withDefaults()
	if len(argv) == 0 {
		fmt.Fprintln(env.Stderr, "fakeslurm: empty argv")
		return 2
	}
	if _, ok := commands[filepath.Base(argv[0])]; !ok {
		if len(argv) < 2 {
			fmt.Fprintf(env.Stderr, "usage: fakeslurm <%s> [args...] (or invoke under one of those names)\n", commandNames())
			return 2
		}
		argv = argv[1:]
	}
	cmd := filepath.Base(argv[0])
	fn, ok := commands[cmd]
	if !ok {
		fmt.Fprintf(env.Stderr, "fakeslurm: unknown command %q (want one of %s)\n", argv[0], commandNames())
		return 2
	}
	cfg, errs := loadConfig(env.Getenv)
	if len(errs) > 0 {
		fmt.Fprintf(env.Stderr, "fakeslurm: %s\n", strings.Join(errs, "\nfakeslurm: "))
		return 2
	}
	a := &app{env: env, cfg: cfg, argv: append([]string(nil), argv...)}
	return fn(a, argv[1:])
}

func commandNames() string {
	return "sbatch|squeue|sacct|scancel|fakeslurmctl"
}

// run executes fn in a locked transaction and prints its result after the
// lock is released.
func (a *app) run(fn func(t *tx) result) int {
	var res result
	if err := a.locked(func(t *tx) error {
		res = fn(t)
		return nil
	}); err != nil {
		return a.internalError(err)
	}
	fmt.Fprint(a.env.Stdout, res.out)
	fmt.Fprint(a.env.Stderr, res.errOut)
	return res.exit
}

// internalError reports a failure of the fake itself (not a simulated Slurm
// error): exit 3.
func (a *app) internalError(err error) int {
	fmt.Fprintf(a.env.Stderr, "fakeslurm: internal error: %v\n", err)
	return 3
}
