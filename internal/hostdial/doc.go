// Package hostdial is HBEP-6's host-dialed mode (Dial-home modes, host-dialed
// mode; science-agent decision, 9 Nov 2026): the control-plane host dials
// each job through the login node, and the job's two connections, the
// agent's control traffic and frpc's wss, run back over what the host
// opened.
//
// Each channel (agentapi.RelayControl, agentapi.RelayServing) has its own
// relay, so control and the token tunnel never share a connection (HBEP-6,
// Risks). A relay is one ssh process on the control-plane host, on a
// connection of its own, running agentapi.RelayLine on the login node: an
// "srun -u --overlap" step whose program is the agent binary's "relay
// <channel> --dir <Allocation directory> --allocation <UID> --epoch <E> --seq
// <N>". After the relay's preamble, one multiplexed session (yamux) runs over
// the step's standard input and output.
//
// The three parts:
//
//   - The job side (Job, in the agent's process). The agent binds one
//     loopback listener per channel at its start, on a port the kernel
//     chooses, and keeps both for the Allocation's life: the control client
//     dials the control listener and frpc's serverPort is the serving
//     listener's. For each connection a listener accepts, the job side opens
//     a stream on its channel's current session and copies bytes. A
//     connection that finds no session waits for one up to Wait (30 s), then
//     is closed; frpc and the control client retry. The relays meet the agent
//     on one Unix socket, in a directory with a random name and mode 700
//     under the job's TMPDIR (/dev/shm only when that path would be too long
//     for a socket), whose path the agent writes to
//     the Allocation directory's relay-socket file (HBEP-6, The rendezvous).
//     It refuses another user's peer (SO_PEERCRED), greets with the
//     Allocation's UID, and keeps the relay with the highest (E, N) on each
//     channel, closing the older session and refusing a lower one as
//     superseded (HBEP-6, Fencing). So an unfenced hand-over of the
//     Provider's controller leaves one session per channel, and the old copy
//     does not take it back.
//   - The relay (Relay, the "relay <channel> ..." step). It reads
//     relay-socket, checks the agent's greeting, says its channel and fence,
//     and on the agent's acceptance writes its preamble and hands the step's
//     standard input and output over; the agent runs the session. It writes
//     nothing else to standard output. Its refusals are fixed lines on
//     standard error (RelayMessage) with their own exit codes.
//   - The control-plane end (Serve, under the batch Provider's controller).
//     It requires the relay's preamble first, within 30 s, then runs the
//     session's other end over the ssh process's pipes, accepts
//     the streams the job opens, and joins each to a new connection to its
//     channel's loopback port, the only address it ever dials: control to the
//     agent endpoint (base+3), serving to the dial-home listener (base+7). A
//     stream carries no address, so a job cannot reach any other port, the
//     plugin endpoint included. It opens no stream of its own.
//
// TLS stays end to end inside the streams: frpc's wss to the dial-home
// listener, and the agent's pinned HTTPS to the agent endpoint.
//
// Provenance: new package. The multiplexer is fatedier/yamux
// (https://github.com/fatedier/yamux, v0.2.0, MPL-2.0), the version frp
// v0.71.1 pins, as HBEP-6's The wire names it, with a 10 s keepalive and a
// 10 s write timeout. Both ends are this package's, so the wire format is
// only this package's concern.
package hostdial
