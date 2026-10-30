// Command fakeslurm is the multi-call fake Slurm binary: install it under the
// names sbatch, squeue, sacct, scancel, scontrol and fakeslurmctl (symlinks),
// or run it as "fakeslurm <command> args...". See package fakeslurm for the
// environment.
package main

import (
	"os"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/fakeslurm"
)

func main() {
	os.Exit(fakeslurm.Main(fakeslurm.Env{}, os.Args))
}
