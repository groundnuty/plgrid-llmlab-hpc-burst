// Package agent is the allocation agent (HBEP-7, The allocation agent): the
// framework's half inside an Allocation's job. The controller decides and the
// agent carries out.
//
// cmd/hb-agent runs it. It reads agent.json and the Allocation's credential
// from the Allocation's directory (LoadJob), draws a random agent instance ID,
// registers with the scheduler's restart count, long-polls its desired state
// and converges on it, as a kubelet converges on its pods, so a missed desired
// state is corrected by the next poll. It reports full snapshots at least
// every agentapi.ReportInterval and at every change.
//
// For a desired Instance it:
//
//   - stages the image by digest in the partition's image format (a SIF file,
//     or a sandbox directory built from the same digest; spike S1, probe 2)
//     and the checkpoint files of the identity record's list, then rehashes
//     the files present (Stager);
//   - starts one engine worker with Apptainer from the Instance's launch:
//     GPU support, a clean environment plus the launch's variables, the
//     checkpoint bound read-only, the engine on a Unix socket in the job's
//     private run directory, and the Instance's key only in the environment
//     (WorkerExec);
//   - forwards the Instance's serving path to that worker (Forwarder), and
//     serves that path to the front through frpc, one tcpmux proxy named by
//     the Instance's UID, over wss to the dial-home listener (Frpc; HBEP-6,
//     frpc in the job). In ssh mode the agent keeps two forwards, each from
//     a kernel-chosen loopback port: connection 1 for its control traffic,
//     connection 2 for frpc (SSHForward; HBEP-6, Dial-home modes);
//   - runs verification, qualification and warm-up against the engine's
//     local socket when the desired state asks for them, and reports raw
//     results for the controller to judge (Engine);
//   - restarts a part only when its restart generation rises, never by itself:
//     a worker, then the whole Instance (which re-stages what fails its
//     rehash). A new Allocation, the third rung, is the controller's. A part
//     that exits without being told to is reported exited, whatever its code.
//     A stop is TERM to the part's process group, then KILL after the stop
//     grace.
//
// Alpha runs one worker per Instance and no router (HBEP-7, Summary). The
// state keeps a list of workers and the forwarder a replaceable target, so a
// router is one more part and the forwarder's target becomes its socket.
//
// The agent is a static binary for amd64 and arm64: this package imports only
// the standard library and package agentapi, and its subpackages likewise.
//
// # Reuse and provenance
//
//   - The stop discipline: processes are known by PID and start time, never
//     by name, and a stop is TERM, then KILL of what is left. Spike S2 phase
//     B's job script did the same by walking the process tree by parent PID
//     and killing each PID (spikes/s2b/scripts/job.sh, lines 60-66,
//     stop_tree); the agent kills the process group instead (package
//     procgroup), and owns it until every member is gone. A child that calls
//     setsid escapes a group kill, as a reparented one escapes a tree walk. A
//     www-2027 lesson (L-3.3: a router hung in graceful shutdown) sets the
//     forced kill.
//   - The Unix socket per engine in the job's own directory answers L-7.7
//     (loopback ports clashed between jobs on shared nodes).
//   - The rehash (2-4 parallel readers, one reused 8 MiB buffer each, no
//     cap-sized read) follows spike S7's recommendation (spikes/s7/README.md,
//     Recommendation), in package stage.
//   - The image formats follow spike S1, probe 2 (docs/development.md, The
//     image format).
//   - The SSE framing of the engine client is copied from spike S2 phase A
//     (spikes/s2/cmd/s2probe/sse.go), in package engine.
//   - The tunnel runs frp v0.71.1's frpc (https://github.com/fatedier/frp,
//     a pinned release binary staged by digest), with HBEP-6's configuration
//     and spike S1c's heartbeat settings (spikes/s1c). The tunnel tests run
//     a user-level sshd as S1c did (spikes/s1c/s1c_lib.py, make_ssh), in
//     inetd mode, and a Go TLS terminator in the place of S1c's Envoy.
//   - New here: the convergence loop, the check gating, the reporting, frpc's
//     supervision and the forwards' port handling.
package agent
