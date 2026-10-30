package controller

import (
	"encoding/json"
	"os"
	"time"
)

// faultBlock is the kill tests' fault-injection hook. It writes a marker file
// (pid, job ID, time) for the test harness and then blocks this reconcile
// forever, so the harness can SIGKILL the process at a known point: before
// sbatch, or after sbatch returned but before the reconciler persisted
// anything.
func faultBlock(marker string, jobID int64) {
	b, _ := json.Marshal(map[string]any{"pid": os.Getpid(), "job_id": jobID, "ts": time.Now().UTC().Format(time.RFC3339Nano)})
	tmp := marker + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		panic("fault hook: cannot write marker: " + err.Error())
	}
	if err := os.Rename(tmp, marker); err != nil {
		panic("fault hook: cannot rename marker: " + err.Error())
	}
	select {}
}
