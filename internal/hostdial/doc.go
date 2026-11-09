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
// <channel>". One multiplexed session (yamux) runs over the step's standard
// input and output.
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
//     is closed; frpc and the control client retry. Each channel also has a
//     Unix socket in the job's relay directory (agentapi.RelayDir, mode 700);
//     a relay that connects there becomes the channel's session, and a newer
//     one replaces the older, which is closed. So an unfenced hand-over of the
//     Provider's controller leaves one session per channel.
//   - The relay (Pipe, the "relay <channel>" step). It connects to its
//     channel's socket and hands the step's standard input and output over;
//     the agent runs the session. It writes nothing else to standard output.
//   - The control-plane end (Serve, under the batch Provider's controller).
//     It runs the session's other end over the ssh process's pipes, accepts
//     the streams the job opens, and joins each to a new connection to its
//     channel's loopback port, the only address it ever dials: control to the
//     agent endpoint (base+3), serving to the dial-home listener (base+7). A
//     stream carries no address, so a job cannot reach any other port, the
//     plugin endpoint included. It opens no stream of its own.
//
// TLS stays end to end inside the streams: frpc's wss to the dial-home
// listener, and the agent's pinned HTTPS to the agent endpoint.
//
// Provenance: new package. The multiplexer is hashicorp/yamux
// (https://github.com/hashicorp/yamux, v0.1.2, MPL-2.0), as HBEP-6's Prior
// art names it; frp v0.71.1 uses its fork fatedier/yamux v0.2.0, but frp does
// not share this session, so the upstream is used. Both ends are this
// package's, so the wire format is only this package's concern.
package hostdial
