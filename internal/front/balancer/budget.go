package balancer

// The token budget (HBEP-5, Workloads, The count in tokens; HBEP-764, R4). An Instance whose B is known holds each
// request against two counts besides the request count: N, the tokens charged to its Workload requests in flight, and
// u, its requests in flight that name no Workload. Both live here, beside the group's other counts, under the group's
// one mutex, and a reservation gives its part back once, with the reservation (releaseLocked). The fork's counts are
// unchanged.

import "math/bits"

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

// Budget is a member's token budget B (HBEP-5, The count in tokens), held as the two factors it is the product of:
// PerWorker, the Instance's kvCacheTokens, and Workers, the workers its cap counts. B = PerWorker * Workers can be larger than any
// int64 (two workers of 2^62 tokens each), and a budget that is known stays known, so the product is never formed: tokenFits takes
// the factors and compares exactly. Both must be at least 1; a member without such a budget has none in State.Budgets.
type Budget struct {
	PerWorker, Workers int64
}

// tokenFits is HBEP-5's integer inequality for one member, EXACTLY, for every int64 input: with limit the member's cap now, B its
// budget (a Budget's two factors), n the tokens charged to its Workload requests in flight, u its unnamed requests in flight, and
// c the request's charge,
//
//	a Workload request:  limit * (n + c) + u * B  <=  limit * B
//	an unnamed request:  limit * n + (u + 1) * B  <=  limit * B
//
// c is read only when named. B is at least 1, so each is the same as
//
//	limit * m  <=  d * B       with  m = n + c and d = limit - u            (a Workload request)
//	                                 m = n     and d = limit - u - 1        (an unnamed one)
//
// and a negative d refuses (its right side is below 0, its left side is not). Both sides are then unsigned and are formed in full,
// the left in 128 bits and the right, d * PerWorker * Workers, in 192, so no overflow can change a verdict: a budget that is large
// is compared, never refused, and a wrapped number never admits. A negative or non-positive input is refused, since none can come
// from the counts or from copyBudgets.
func tokenFits(limit int64, b Budget, n, u, c int64, named bool) bool {
	if limit < 0 || b.PerWorker < 1 || b.Workers < 1 || n < 0 || u < 0 || c < 0 {
		return false
	}
	m, d := uint64(n), limit-u-1
	if named {
		m, d = uint64(n)+uint64(c), limit-u // each part is below 2^63, so the sum fits a uint64
	}
	if d < 0 {
		return false
	}
	xHi, xLo := bits.Mul64(uint64(limit), m)
	pHi, pLo := bits.Mul64(uint64(d), uint64(b.PerWorker))
	y2, y1, y0 := mul192(pHi, pLo, uint64(b.Workers))
	return le192(xHi, xLo, y2, y1, y0)
}

// mul192 is the 192-bit product of the 128-bit hi:lo and m, as three limbs, most significant first.
func mul192(hi, lo, m uint64) (r2, r1, r0 uint64) {
	h0, l0 := bits.Mul64(lo, m)
	h1, l1 := bits.Mul64(hi, m)
	r1, carry := bits.Add64(h0, l1, 0)
	return h1 + carry, r1, l0 // the whole product is below 2^192, so the top limb cannot overflow
}

// le192 reports whether the 128-bit xHi:xLo is at most the 192-bit y2:y1:y0.
func le192(xHi, xLo, y2, y1, y0 uint64) bool {
	if y2 != 0 {
		return true // y is at least 2^128, above every 128-bit x
	}
	if xHi != y1 {
		return xHi < y1
	}
	return xLo <= y0
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

// copyBudgets copies the budgets, dropping any whose factors are below 1: such a budget is no size an engine reports, so it is
// unknown, never a budget that refuses every request.
func copyBudgets(m map[string]Budget) map[string]Budget {
	out := make(map[string]Budget, len(m))
	for k, v := range m {
		if v.PerWorker >= 1 && v.Workers >= 1 {
			out[k] = v
		}
	}
	return out
}
