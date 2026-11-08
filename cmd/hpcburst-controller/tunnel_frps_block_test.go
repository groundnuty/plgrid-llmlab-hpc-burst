package main

// The rig's move to a fresh port block after a start of frps found a port taken (issue #261,
// review of PR #271), against the model of the kernel's port table (kernelports_test.go): the
// ports the kernel offers, and the strangers on them, are the test's, so the overlaps the review
// describes are made, not waited for.
//
// A tunnel block holds ports at the offsets 3 (agent endpoint), 8 (frps plugin), 9, 10, 11 (frps's
// bind, tcpmux and web) and 12 (metrics), and a health address (HBEP-6, Loopback ports;
// docs/development.md). The expectations below spell those offsets as literals, and do not read
// them from the code under test.

import (
	"fmt"
	"slices"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// blockOffsets are the offsets of a block's ports, as HBEP-6 gives them.
var blockOffsets = []int{3, 8, 9, 10, 11, 12}

// ports are the ports of the block at base.
func ports(base int) []int {
	var out []int
	for _, o := range blockOffsets {
		out = append(out, base+o)
	}
	return out
}

func sharePort(a, b int) bool {
	for _, p := range ports(a) {
		if slices.Contains(ports(b), p) {
			return true
		}
	}
	return false
}

// firstBlock holds a block as a rig does at its start (the block, and a health address) and
// returns it as flags.
func firstBlock(t *testing.T) flags {
	t.Helper()
	base := holdBlock(t, []int{agentapi.FrpsPluginPortOffset}, tunnelOtherOffsets)
	_, health := holdLoopback(t)
	var f flags
	f.host.PortBase, f.healthAddr = base, health
	return f
}

// A replacement block shares no port with any block the rig tried, and what the old block's
// cleanup closes is the old block's alone. The kernel offers, in turn, a block that overlaps the
// block that lost its port (its agent endpoint at an address the first block released for frps),
// and, a retry later, the first block's own base again (its ports are free by then: the stranger
// has gone, as a client socket does when its connection ends), and then a block that overlaps the
// second. A stranger holds the first block's web port at the first retry.
func TestFreshBlocksShareNoPortWithAnAttemptedBlock(t *testing.T) {
	kp := newKernelPorts(t, 41003)
	kp.offers = []int{40003} // the first block: base 40000
	first := firstBlock(t)
	if first.host.PortBase != 40000 {
		t.Fatalf("the first block is at %d, want 40000", first.host.PortBase)
	}
	r := &tunnelRig{t: t, f: first}
	releaseForFrps(first.host) // the rig releases frps's three, then frps starts
	kp.stranger(40011)         // and a stranger takes one of them in that window

	tried := []flags{first}
	for _, step := range []struct {
		name    string
		offers  []int
		want    int
		because string
	}{
		{"retry 1", []int{40009, 40203}, 40200,
			"40009 is the agent endpoint of base 40006, whose block holds 40009: the first block's bind port"},
		{"retry 2", []int{40003, 40209, 40303}, 40300,
			"40003 is the first block's own agent endpoint (base 40000 again), and 40209 is the agent endpoint of base 40206, whose block holds 40209: the second block's bind port"},
	} {
		kp.offers = step.offers
		r.freshBlock(nil)
		live := r.f
		if len(kp.offers) != 0 || !slices.Equal(kp.offered[len(kp.offered)-len(step.offers):], step.offers) {
			t.Errorf("%s: the kernel offered %v, so the script's %v was not all tried: %s", step.name, kp.offered, step.offers, step.because)
		}
		if live.host.PortBase != step.want {
			t.Errorf("%s: the replacement block is at %d, want %d (the first clear one): %s", step.name, live.host.PortBase, step.want, step.because)
		}
		for _, e := range tried {
			if sharePort(e.host.PortBase, live.host.PortBase) {
				t.Errorf("%s: the replacement block at %d shares a port with the attempted block at %d", step.name, live.host.PortBase, e.host.PortBase)
			}
		}
		var liveAddrs []string
		for _, p := range ports(live.host.PortBase) {
			liveAddrs = append(liveAddrs, fmt.Sprintf("127.0.0.1:%d", p))
		}
		liveAddrs = append(liveAddrs, live.healthAddr)
		for _, a := range liveAddrs {
			if !kp.heldAt(a) {
				t.Errorf("%s: %s of the replacement block is not held: the old block's cleanup closed it", step.name, a)
			}
		}
		for _, e := range tried {
			for _, a := range blockAddrs(e) {
				if !slices.Contains(liveAddrs, a) && !kp.releasedAt(a) {
					t.Errorf("%s: %s of the attempted block at %d is still held", step.name, a, e.host.PortBase)
				}
			}
		}
		if step.name == "retry 1" {
			if !kp.strangerHolds(40011) {
				t.Errorf("%s: the stranger no longer holds 40011", step.name)
			}
			kp.strangerLeaves(40011)
		}
		tried = append(tried, live)
		releaseForFrps(live.host) // the next attempt starts frps on this block
	}
}

// What the old block's cleanup closes is the old block's alone, whatever block the test moved to.
// Here the new block overlaps the old one the way a pick that ignored it would: its agent endpoint
// is at the old block's bind port, which was released for frps, and its health address is at the
// old block's tcpmux port, released too. The cleanup closes the old block's own reservations (agent
// endpoint, plugin, metrics, health), and leaves both of the new block's, and all the rest.
func TestDropOldBlockClosesNoListenerOfTheLiveBlock(t *testing.T) {
	kp := newKernelPorts(t, 41003)
	kp.offers = []int{40003} // the old block: base 40000
	old := firstBlock(t)
	releaseForFrps(old.host)

	kp.offers = []int{40009, 40010} // the live block: base 40006, with its health address at 40010
	liveBase := holdBlock(t, []int{agentapi.FrpsPluginPortOffset}, tunnelOtherOffsets)
	_, liveHealth := holdLoopback(t)
	live := flags{healthAddr: liveHealth}
	live.host.PortBase = liveBase
	if liveBase != 40006 || liveHealth != "127.0.0.1:40010" {
		t.Fatalf("the live block is at %d with health %s, want 40006 and 127.0.0.1:40010", liveBase, liveHealth)
	}

	dropOldBlock(old, live)

	for _, a := range blockAddrs(live) {
		if !kp.heldAt(a) {
			t.Errorf("%s of the live block is not held after the old block's cleanup", a)
		}
	}
	for _, a := range []string{"127.0.0.1:40003", "127.0.0.1:40008", "127.0.0.1:40012", old.healthAddr} {
		if !kp.releasedAt(a) {
			t.Errorf("%s, which the old block held, is still held", a)
		}
	}
}

// A retry before the controller starts keeps no health reservation of the old block: it is not the
// binary's to release (releaseAtStart), and a block's cleanup that left it would hold one more
// port for every retry until the test ended.
func TestFreshBlockReleasesTheOldHealthReservation(t *testing.T) {
	kp := newKernelPorts(t, 41003)
	kp.offers = []int{40003}
	first := firstBlock(t)
	if first.healthAddr != "127.0.0.1:41003" {
		t.Fatalf("the first block's health address is %s, want 127.0.0.1:41003", first.healthAddr)
	}
	if !kp.heldAt(first.healthAddr) {
		t.Fatalf("the first block's health address is not held to begin with")
	}
	r := &tunnelRig{t: t, f: first}
	releaseForFrps(first.host)

	kp.offers = []int{40203}
	r.freshBlock(nil)

	if r.f.healthAddr == first.healthAddr {
		t.Fatalf("the fresh block has the old health address %s", first.healthAddr)
	}
	if !kp.releasedAt(first.healthAddr) {
		t.Errorf("the old health address %s is still held after the move to a fresh block", first.healthAddr)
	}
	if !kp.heldAt(r.f.healthAddr) {
		t.Errorf("the fresh block's health address %s is not held", r.f.healthAddr)
	}
}
