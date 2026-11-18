package balancer

// The token budget (HBEP-5, Workloads, The count in tokens; HBEP-764, R4). An Instance whose B is known holds each
// request against two counts besides the request count: N, the tokens charged to its Workload requests in flight, and
// u, its requests in flight that name no Workload. Both live here, beside the group's other counts, under the group's
// one mutex, and a reservation gives its part back once, with the reservation (releaseLocked). The fork's counts are
// unchanged.

import "math"

// Charge is what one request adds to its member's counts. The zero value is a request that names no Workload.
// Named is kept apart from Tokens: a Workload whose declared shape sums to 0 is still a Workload request, and takes
// the Workload condition, never the other one.
type Charge struct {
	// Named says the request names a Workload.
	Named bool
	// Tokens is the request's charge c, its Workload's typical context plus typical output; 0 for a request that names
	// none.
	Tokens int64
}

// tokenFits is HBEP-5's integer inequality for one member, with cap and B their values now, n the tokens charged to its
// Workload requests in flight, u its unnamed requests in flight, and c the request's charge:
//
//	a Workload request:  cap * (n + c) + u * B  <=  cap * B
//	an unnamed request:  cap * n + (u + 1) * B  <=  cap * B
//
// c is read only when named. It is in int64 with every sum and product checked: a value that would overflow refuses
// the request, never wraps into an admit. A negative input is refused too, since none can come from the counts.
func tokenFits(limit, b, n, u, c int64, named bool) bool {
	if limit < 0 || b < 0 || n < 0 || u < 0 || c < 0 {
		return false
	}
	rhs, ok := mulChecked(limit, b)
	if !ok {
		return false
	}
	var lhs int64
	if named {
		lhs, ok = workloadLeft(limit, b, n, u, c)
	} else {
		lhs, ok = unnamedLeft(limit, b, n, u)
	}
	return ok && lhs <= rhs
}

// workloadLeft is limit * (n + c) + u * B, and false when any step overflows.
func workloadLeft(limit, b, n, u, c int64) (int64, bool) {
	nc, ok := addChecked(n, c)
	if !ok {
		return 0, false
	}
	own, ok := mulChecked(limit, nc)
	if !ok {
		return 0, false
	}
	others, ok := mulChecked(u, b)
	if !ok {
		return 0, false
	}
	return addChecked(own, others)
}

// unnamedLeft is limit * n + (u + 1) * B, and false when any step overflows.
func unnamedLeft(limit, b, n, u int64) (int64, bool) {
	charged, ok := mulChecked(limit, n)
	if !ok {
		return 0, false
	}
	u1, ok := addChecked(u, 1)
	if !ok {
		return 0, false
	}
	slots, ok := mulChecked(u1, b)
	if !ok {
		return 0, false
	}
	return addChecked(charged, slots)
}

// mulChecked is a * b for a, b >= 0, and false when it does not fit an int64.
func mulChecked(a, b int64) (int64, bool) {
	if a != 0 && b > math.MaxInt64/a {
		return 0, false
	}
	return a * b, true
}

// addChecked is a + b for a, b >= 0, and false when it does not fit an int64.
func addChecked(a, b int64) (int64, bool) {
	if a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

// tokensFitLocked says whether member m's token condition admits a request with charge ch now (the request count's
// part, the cap, is the caller's). Without a B for m the answer is yes: the count rule alone, as before. The condition
// of a request that names a Workload stays in force at partial membership, with the cap at the value it has when in
// force (State.CapValues); the other one is in force only while the count cap is (State.Caps), and is off with it.
func (g *Group) tokensFitLocked(m string, ch Charge) bool {
	b, known := g.state.Budgets[m]
	if !known {
		return true
	}
	n, u := g.charged[m], g.unnamed[m]
	if ch.Named {
		capNow := int64(g.state.CapValues[m])
		if capNow < 1 {
			return true // no cap value: nothing to compare B with
		}
		return tokenFits(capNow, b, n, u, ch.Tokens, true)
	}
	inForce := int64(g.state.Caps[m])
	if inForce < 1 {
		return true // the count cap is off, and the second condition with it
	}
	return tokenFits(inForce, b, n, u, 0, false)
}

// admitsLocked says whether member m takes a request with charge ch now, given m's count before the request: it holds
// its cap when the count cap is in force and reached, or when the request's token condition fails. The capped pick and
// the spill both ask this, so they cannot differ.
func (g *Group) admitsLocked(m string, load int64, ch Charge) bool {
	if c := g.state.Caps[m]; c > 0 && load >= int64(c) {
		return false
	}
	return g.tokensFitLocked(m, ch)
}

// countLocked adds a reservation's charge to its member's N or u.
func (g *Group) countLocked(m string, ch Charge) {
	if ch.Named {
		g.charged[m] += ch.Tokens
		return
	}
	g.unnamed[m]++
}

// uncountLocked takes a reservation's charge off its member's N or u. It is called once per reservation, from
// releaseLocked, so a count never goes below the sum of the charges still open.
func (g *Group) uncountLocked(m string, ch Charge) {
	if ch.Named {
		g.charged[m] -= ch.Tokens
		return
	}
	g.unnamed[m]--
}

// Charged is member m's N and u: the tokens charged to its Workload requests in flight, and its requests in flight
// that name no Workload. ok is false for a name that is not a member.
func (g *Group) Charged(m string) (tokens, unnamed int64, ok bool) {
	g.lock()
	defer g.mu.Unlock()
	if !g.members[m] {
		return 0, 0, false
	}
	return g.charged[m], g.unnamed[m], true
}

// copyBudgets copies the budgets, dropping any below 1: a budget of 0 or less is no size an engine reports, so it is
// unknown, never a budget that refuses every request.
func copyBudgets(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		if v >= 1 {
			out[k] = v
		}
	}
	return out
}
