// Package agentapi is the protocol between the allocation agent and the
// lifecycle controller (HBEP-7, The allocation agent): the wire types, the
// transport framing, agent.json, the file layout both sides share, and the Go
// seam between the agent API's HTTP server and the lifecycle controller.
//
// It imports only the standard library, because the agent is a static binary
// for amd64 and arm64 that runs inside the job. Package agentapi/fake holds a
// fake agent, which drives a Backend as the real agent would, and a fake
// Backend, which stands in for the lifecycle controller; it is
// standard-library only too.
//
// # Transport
//
// Three calls over HTTPS with JSON bodies (ContentType), version in the path:
//
//	POST PathRegister  RegisterRequest  -> RegisterResponse
//	GET  PathDesired?since=<version>    -> DesiredState     (a long poll)
//	POST PathReport    Report           -> 204 No Content
//
// The endpoint listens on loopback on the control-plane host. In ssh dial-home
// mode the agent reaches it through its own forward (ssh -N -L) to
// AgentConfig.SSH.AgentPort; in direct mode the front's dial-home listener
// passes PathPrefix through to it. The launcher creates the endpoint's
// certificate; agent.json carries its pin (AgentConfig.CertSPKISHA256), and the
// agent trusts exactly that key (ClientTLSConfig): no CA and no host name are
// checked. In direct mode the front presents the same certificate, so one pin
// holds on both paths (HBEP-7; HBEP-11, Single-user host).
//
// Every request carries the agent's identity in headers, never in a body, so
// bodies can be logged:
//
//	Authorization:              Bearer <the Allocation's credential>
//	Hpcburst-Tag:               <the Allocation's job tag>
//	Hpcburst-Job-Id:            <the job ID, from the scheduler's environment>
//	Hpcburst-Agent-Instance-Id: <a random ID the agent draws at its start>
//
// The agent instance ID identifies one agent process. It is never the kernel's
// boot ID: a requeued job can land on the same node with no reboot, and must
// still be told apart. The registration also carries the scheduler's restart
// count of the job (RegisterRequest.RestartCount, from SLURM_RESTART_COUNT when
// Slurm sets it), so a requeue shows as a higher count too.
//
// The server authenticates every call, so it holds no session: it finds the
// Allocation whose external name is the tag, reads the credential file named
// by the Allocation's UID (AllocationCredentialPath) and compares it in
// constant time; then it compares the job ID with the Allocation's job-id
// annotation. A wrong or unknown credential is CodeUnauthenticated; the right
// credential with another job ID, or before a job ID is recorded, is
// CodeJobMismatch (HBEP-7: a registration is accepted only when the
// credential is the Allocation's and the job ID is the one the provider
// recorded). Only then does it call the Backend.
//
// Errors are an Error as the JSON body, with Error.HTTPStatus. The agent
// retries CodeUnavailable and CodeJobMismatch with backoff, and answers
// CodeNotRegistered by registering again at once.
//
// Long poll. GET PathDesired returns at once when the desired state's Version
// differs from since (an empty since always differs), and otherwise waits for
// a change for up to LongPollTimeout, then returns the current state, whose
// Version may equal since. A missed answer is corrected by the next poll,
// because the agent converges on the state it is sent, as a kubelet converges
// on its pods.
//
// Versioning. Within v1 a newer endpoint serves older agents. Neither side
// refuses a field it does not know (no DisallowUnknownFields), and a new field
// is optional with a zero value that means what the older protocol meant
// (HBEP-7, Compatibility and upgrades).
//
// # Who decides
//
// The controller decides and the agent carries out. The agent never restarts
// a part by itself: a part restarts when the restart generation it is sent
// rises. The controller raises a generation only after emitting the restart's
// event (HBEP-7, Health and restarts: recorded first).
//
// The agent runs the checks and reports raw results; the controller applies
// the thresholds and sets the verdicts (HBEP-7, Instance start). The split:
//
//   - the agent judges what needs the outputs, which never leave the job:
//     the chat request, the tool-call smoke test, the corruption check (three
//     greedy outputs share their first 32 tokens, none empty or a single
//     repeated token) and the behaviour check (a well-formed answer at the
//     longest context, no loop). It reports each as a CheckOutcome;
//   - the controller judges everything with a threshold or a declared value:
//     the file rehash, the settings comparison, the engine version, the
//     served model names, the maximum length, KV capacity against
//     targetSessions times contextTokens.typical, warm-up settling (no shape
//     more than 10% slower than the round before; at most MaxWarmUpRounds),
//     and every health signal.
//
// # How reports reach the controller
//
// The server passes every authenticated call to the Backend, which the
// lifecycle controller implements in the same process. The controller keeps
// the latest Report per Allocation in memory, with the time it arrived, and
// enqueues that Allocation's ModelDeployment. A Report is a full snapshot, not
// a delta: the agent sends everything it knows about the current Instance,
// check results included, at least every ReportInterval and at every change.
// So a restarted controller has every live agent's state again within one
// interval, and nothing in a Report needs to be stored as it arrives (HBEP-7,
// Operational readiness 2: it "reads the agents' next reports").
//
// What must survive a restart is the controller's judgement, which it writes
// to the API: the Instance's phase, conditions, end reason, start-instruction
// and ready times, throughput and AgentRegistration; the verification records;
// the restart generations (annotations); the ModelDeployment's start-to-ready
// times. Backend.Desired must be computed from those objects alone, so a
// restarted controller serves the same desired state, with the same Version.
//
// Registration is soft state: it is kept in memory, keyed by Allocation UID and
// agent instance ID, and copied to the Instance it starts
// (InstanceStatus.Agent). A controller that does not know a caller's agent
// instance ID answers Desired and Report with CodeNotRegistered, and the agent
// registers again at once, within one poll, with the same agent instance ID.
// That re-registration is the same execution: it fails no Instance and raises
// no generation. So after its own restart, the controller judges no
// registration timeout until LongPollTimeout plus ReportInterval have passed
// since it started.
//
// A registration with the job ID the Instance was started under and another
// agent instance ID, or a higher restart count, is a second execution of the
// job, a requeue: nothing from the first execution is reused, and the Instance
// fails (HBEP-7, Test plan).
//
// # Generations
//
// Three counters, never mixed:
//
//   - DesiredState.Version: an opaque token for the long poll; it changes
//     whenever anything else in the desired state changes;
//   - restart generations, one per part: the whole Instance
//     (DesiredInstance.Generation) and each worker (DesiredWorker.Generation),
//     integers the controller keeps in Instance annotations
//     (v1alpha1.RestartGenerationAnnotation). Reports echo the generation each
//     part last applied;
//   - the verification generation (Checks.VerificationGeneration): the n of
//     the record ver-<instance UID>-<n>. Check results carry it, so a late
//     result of verification n-1 never lands in record n.
//
// # Parts: one worker now, a router later
//
// Alpha runs one engine per Instance and no router (HBEP-7, Summary and open
// question 6): Workers holds exactly one element, worker-0, and the agent
// forwards the Instance's serving path straight to that engine's Unix socket.
// The types extend without a new protocol version: several workers are more
// elements of DesiredInstance.Workers and InstanceReport.Workers, each with its
// own name, socket, generation and health; a router is a new optional field
// beside Workers in both (DesiredInstance.Router, InstanceReport.Router), with
// its own restart generation under the part name "router". An older agent
// ignores both, and a controller that sends neither means alpha's shape.
package agentapi
