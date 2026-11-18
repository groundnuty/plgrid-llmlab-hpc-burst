package balancer

// The token budget (HBEP-5, Workloads, The count in tokens). A member with a known B takes
//
//	a Workload request   only when count < cap and cap * (N + c) + u * B <= cap * B
//	an unnamed request   only when count < cap and cap * N + (u + 1) * B <= cap * B
//
// N the tokens charged to its Workload requests in flight, u its unnamed requests in flight, count their number (the
// Workload requests plus u), c the request's charge. Without a B, the count rule alone. Below full membership the count
// cap is off and so is the second condition; the first stays, with the cap at its value when in force.
//
// Every expected admit or refuse below is a literal worked from those two inequalities in the comment beside it. None
// comes from calling tokenFits, and the reduction's oracle is the count rule, u + 1 <= cap, never the inequality. The one
// test that does compare with a second computation (TestTheComparisonAgreesWithBigIntegerArithmetic) checks the 192-bit
// ARITHMETIC against math/big on the same inequalities; the rule itself is pinned only by the typed rows.
//
// No test in this package may call t.Parallel (helpers_test.go).

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"testing"
)

// tokRow is one request offered to one member in a known state. loads are the charges of the Workload requests the
// member holds in flight (N is their sum, and each is one request of the count), u the unnamed ones, and b the member's B
// (0: no B known). partial is membership below the desired count: the count cap is off, and the cap's value is still
// known.
type tokRow struct {
	name    string
	cap     int
	b       int64 // B's per-worker factor, kvCacheTokens (0: no B known)
	w       int64 // B's workers factor; 0 is 1
	loads   []int64
	u       int
	partial bool
	req     Charge
	admit   bool
}

func workload(c int64) Charge { return Charge{Named: true, Tokens: c} }

var noWorkload = Charge{}

// tokRows are the rows of the truth table. Each is worked by hand; a row's cap and B are those of the member.
var tokRows = []tokRow{
	// --- A request that names a Workload: count < cap and cap*(N+c) + u*B <= cap*B.
	// cap 4, B 100, nothing in flight. c = 100: 4*(0+100) + 0 = 400 <= 400: admitted at the edge of the budget.
	{name: "workload fills the budget exactly", cap: 4, b: 100, req: workload(100), admit: true},
	// c = 101: 4*101 = 404 > 400: one token over.
	{name: "workload one token over the budget", cap: 4, b: 100, req: workload(101), admit: false},
	// N = 60 (two requests, 30 each), u = 1, count 3 < 4. c = 15: 4*(60+15) + 1*100 = 300 + 100 = 400 <= 400: admitted.
	{name: "workload meets the budget exactly beside N and u", cap: 4, b: 100, loads: []int64{30, 30}, u: 1, req: workload(15), admit: true},
	// c = 16: 4*76 + 100 = 404 > 400: refused by its budget while the count has room (3 < 4).
	{name: "workload refused by its budget while the count has room", cap: 4, b: 100, loads: []int64{30, 30}, u: 1, req: workload(16), admit: false},
	// N = 0, u = 3, count 3 < 4. c = 25: 4*25 + 3*100 = 400 <= 400: the unnamed requests' shares count in full.
	{name: "workload meets the budget exactly beside the unnamed shares", cap: 4, b: 100, u: 3, req: workload(25), admit: true},
	// c = 26: 4*26 + 300 = 404 > 400.
	{name: "workload one token over beside the unnamed shares", cap: 4, b: 100, u: 3, req: workload(26), admit: false},
	// Count side. cap 2, B 1000, one request of 1 token in flight: count 1 < 2, and 2*(1+1) + 0 = 4 <= 2000: admitted.
	{name: "workload one below the cap", cap: 2, b: 1000, loads: []int64{1}, req: workload(1), admit: true},
	// Two of 1 token: count 2 = cap: refused although 2*(2+1) + 0 = 6 <= 2000 holds.
	{name: "workload at the cap is refused by the count", cap: 2, b: 1000, loads: []int64{1, 1}, req: workload(1), admit: false},
	// A Workload whose shape sums to 0 is still a Workload request. cap 4, B 100, N = 50 (one request), u = 2, count 3 < 4.
	// c = 0: 4*(50+0) + 2*100 = 200 + 200 = 400 <= 400: admitted (an unnamed request would need 4*50 + 3*100 = 500).
	{name: "a Workload of charge 0 takes the Workload condition", cap: 4, b: 100, loads: []int64{50}, u: 2, req: workload(0), admit: true},

	// --- A request that names none: count < cap and cap*N + (u+1)*B <= cap*B.
	// cap 4, B 100, N = 0, u = 3, count 3 < 4: 4*0 + 4*100 = 400 <= 400: admitted, u + 1 = cap.
	{name: "unnamed takes the last count slot", cap: 4, b: 100, u: 3, req: noWorkload, admit: true},
	// u = 4: count 4 = cap: refused (and 5*100 = 500 > 400).
	{name: "unnamed at the cap", cap: 4, b: 100, u: 4, req: noWorkload, admit: false},
	// N = 25, u = 2, count 3 < 4: 4*25 + 3*100 = 400 <= 400: admitted while N leaves room for its share.
	{name: "unnamed beside N at the edge", cap: 4, b: 100, loads: []int64{25}, u: 2, req: noWorkload, admit: true},
	// N = 26: 4*26 + 300 = 404 > 400: refused when N and its share exceed B, with the count at 3 < 4.
	{name: "unnamed refused when N and its share exceed B", cap: 4, b: 100, loads: []int64{26}, u: 2, req: noWorkload, admit: false},
	// cap 8, B 80, N = 40, u = 3, count 4 < 8: 8*40 + 4*80 = 320 + 320 = 640 <= 640: admitted.
	{name: "unnamed at the edge with a larger cap", cap: 8, b: 80, loads: []int64{40}, u: 3, req: noWorkload, admit: true},
	// N = 41: 8*41 + 320 = 648 > 640.
	{name: "unnamed one token of N over", cap: 8, b: 80, loads: []int64{41}, u: 3, req: noWorkload, admit: false},

	// --- B unknown: the count rule alone, for both kinds, whatever N is.
	// cap 2, N = 1,000,000 (one request), count 1 < 2: both admitted.
	{name: "unknown B admits a Workload request by the count", cap: 2, loads: []int64{1_000_000}, req: workload(1_000_000), admit: true},
	{name: "unknown B admits an unnamed request by the count", cap: 2, loads: []int64{1_000_000}, req: noWorkload, admit: true},
	// count 2 = cap: both refused, by the count rule.
	{name: "unknown B still holds the cap for a Workload request", cap: 2, loads: []int64{1, 1}, req: workload(1), admit: false},
	{name: "unknown B still holds the cap for an unnamed request", cap: 2, u: 2, req: noWorkload, admit: false},

	// --- Below full membership: the count cap is off, and the second condition with it; the first stays, with the cap's value.
	// cap 4, B 100, nothing in flight: a long request, c = 101: 4*101 = 404 > 400: refused by its budget.
	{name: "partial: a long Workload request is refused by its budget", cap: 4, b: 100, partial: true, req: workload(101), admit: false},
	// ... and c = 100: 400 <= 400: admitted.
	{name: "partial: a Workload request that fits is admitted", cap: 4, b: 100, partial: true, req: workload(100), admit: true},
	// An unnamed request on the same member: N = 60, u = 3. In force it would need 4*60 + 4*100 = 640 <= 400 (no) and
	// count 4 < 4 (no). With the cap off it is admitted: today's behaviour below full membership.
	{name: "partial: an unnamed request is admitted although its condition would fail", cap: 4, b: 100, loads: []int64{60}, u: 3, partial: true, req: noWorkload, admit: true},
	// A Workload request at count 3 >= cap 2 is not held by the count when the count cap is off: 2*(3+1) + 0 = 8 <= 2000.
	{name: "partial: a Workload request is not held by the count", cap: 2, b: 1000, loads: []int64{1, 1, 1}, partial: true, req: workload(1), admit: true},
	// u = 3, N = 0, c = 26: 4*26 + 3*100 = 404 > 400: the unnamed shares hold a Workload request at partial membership too.
	{name: "partial: the unnamed shares hold a Workload request", cap: 4, b: 100, u: 3, partial: true, req: workload(26), admit: false},
	// ... and c = 25: 400 <= 400.
	{name: "partial: the unnamed shares leave room for a Workload request", cap: 4, b: 100, u: 3, partial: true, req: workload(25), admit: true},
	// No B and the count cap off: nothing holds either kind.
	{name: "partial: unknown B holds nothing", cap: 2, loads: []int64{1, 1, 1}, partial: true, req: workload(1), admit: true},

	// --- A budget beyond what a product in int64 holds: the comparison is exact, so none of these is refused for its size.
	// An idle Instance, cap 2, B = 2^62, nothing in flight: an unnamed request needs 2*0 + 1*B <= 2*B: admitted. (cap * B = 2^63 does
	// not fit an int64; a plain product refused it.)
	{name: "huge B: an idle Instance admits an unnamed request", cap: 2, b: 1 << 62, req: noWorkload, admit: true},
	// u = 1, count 1 < 2: 2*0 + 2*B <= 2*B: admitted, the last count slot.
	{name: "huge B: the second unnamed request fits", cap: 2, b: 1 << 62, u: 1, req: noWorkload, admit: true},
	// u = 2: count 2 = cap: refused by the count (and 3*B > 2*B).
	{name: "huge B: the third unnamed request is refused by the count", cap: 2, b: 1 << 62, u: 2, req: noWorkload, admit: false},
	// B = MaxInt64 = 2^63 - 1, cap 2, N = 2^63 - 2 (one request): count 1 < 2. c = 1: 2*(N+c) + 0 = 2*(2^63 - 1) <= 2*B: admitted,
	// N + c = B exactly.
	{name: "B near MaxInt64: N plus c equals B", cap: 2, b: math.MaxInt64, loads: []int64{math.MaxInt64 - 1}, req: workload(1), admit: true},
	// N = 2^63 - 1: c = 1 makes N + c = 2^63 = B + 1: 2*(B + 1) > 2*B: refused by one token.
	{name: "B near MaxInt64: N plus c is one over B", cap: 2, b: math.MaxInt64, loads: []int64{math.MaxInt64}, req: workload(1), admit: false},
	// u = 1: 2*(0 + c) + 1*B <= 2*B, so 2c <= B = 2^63 - 1, c <= 2^62 - 1 (2^62 - 1 = 4,611,686,018,427,387,903).
	{name: "B near MaxInt64: the largest charge beside one unnamed request", cap: 2, b: math.MaxInt64, u: 1, req: workload(1<<62 - 1), admit: true},
	// c = 2^62: 2c = 2^63 > 2^63 - 1: refused by one token.
	{name: "B near MaxInt64: one token more beside one unnamed request", cap: 2, b: math.MaxInt64, u: 1, req: workload(1 << 62), admit: false},
	// Two workers each reporting 2^62 tokens: B = 2^63, above any int64, and still a budget. At partial membership (the count cap off)
	// with u = cap = 2: 2*(0 + 1) + 2*B <= 2*B is false: a Workload request with a positive charge is refused (it is never taken
	// when the unnamed requests already hold the whole budget). A B turned unknown by the overflow would admit it.
	{name: "partial: two workers of 2^62 each, the unnamed requests hold the budget", cap: 2, b: 1 << 62, w: 2, u: 2, partial: true, req: workload(1), admit: false},
	// ... and with u = 1 it fits: 2*(0 + c) + 1*B <= 2*B, so c <= B / 2 = 2^62: c = 2^62 admitted,
	{name: "partial: two workers of 2^62 each, half the budget", cap: 2, b: 1 << 62, w: 2, u: 1, partial: true, req: workload(1 << 62), admit: true},
	// c = 2^62 + 1 refused by one token.
	{name: "partial: two workers of 2^62 each, one token over half", cap: 2, b: 1 << 62, w: 2, u: 1, partial: true, req: workload(1<<62 + 1), admit: false},
}

// onlyMember is a filter admitting one member.
func onlyMember(m string) Match { return func(name string, _ Attrs) bool { return name == m } }

// pickMode is how a request reaches the member under test.
type pickMode int

const (
	// direct: the key's home is the member, which KubeAI chooses and reserves; the other member is not eligible.
	direct pickMode = iota
	// filtered: the key's home is the other member, which is not eligible; the walk of the eligible ones takes the member.
	filtered
	// spilled: the key's home is the other member, eligible and at its count cap; the request spills to the member.
	spilled
)

func (m pickMode) String() string { return [...]string{"direct", "filtered", "spilled"}[m] }

// tokFix is a group of two members, "m" under test and "o" the other, with the keys homed on each (the fork's own ring is
// the oracle, forkHome).
type tokFix struct {
	t      *testing.T
	g      *Group
	keyOnM string
	keyOnO string
}

func newTokFix(t *testing.T) *tokFix {
	t.Helper()
	members := []string{"m", "o"}
	f := &tokFix{t: t, g: New()}
	f.g.Update(stateOf(members, noBound, 0))
	for i := 0; f.keyOnM == "" || f.keyOnO == ""; i++ {
		if i > 1000 {
			t.Fatal("no keys homed on each member")
		}
		k := fmt.Sprintf("home-%d", i)
		if forkHome(t, forkOf(members), k) == "m" {
			f.keyOnM = k
		} else {
			f.keyOnO = k
		}
	}
	return f
}

// load puts the row's requests in flight on member m, under no cap and no budget, so every one is taken.
func (f *tokFix) load(m string, loads []int64, u int) {
	f.t.Helper()
	for _, c := range loads {
		r, err := f.g.TryPickFor(f.keyOnM, onlyMember(m), workload(c))
		if err != nil || r.Member != m {
			f.t.Fatalf("loading a Workload request of %d on %s: %v, %v", c, m, r, err)
		}
	}
	for range u {
		r, err := f.g.TryPickFor(f.keyOnM, onlyMember(m), noWorkload)
		if err != nil || r.Member != m {
			f.t.Fatalf("loading an unnamed request on %s: %v, %v", m, r, err)
		}
	}
}

// probeState is the state the row's request meets: m's cap in force (or, partial, only its value), its B when known.
func probeState(row tokRow, mode pickMode) State {
	s := State{
		Members: []string{"m", "o"}, Eligible: map[string]bool{"m": true}, Percent: noBound,
		CapValues: map[string]int{"m": row.cap}, Budgets: map[string]Budget{},
	}
	if !row.partial {
		s.Caps = map[string]int{"m": row.cap}
	}
	if row.b > 0 {
		w := row.w
		if w == 0 {
			w = 1
		}
		s.Budgets["m"] = Budget{PerWorker: row.b, Workers: w}
	}
	if mode == spilled {
		// The home is eligible and holds its one request against a cap of 1: every request is refused there by the count.
		s.Eligible["o"] = true
		if s.Caps == nil {
			s.Caps = map[string]int{}
		}
		s.Caps["o"] = 1
	}
	return s
}

func TestTheTruthTableOnEveryPathToAMember(t *testing.T) {
	for _, row := range tokRows {
		for _, mode := range []pickMode{direct, filtered, spilled} {
			t.Run(row.name+"/"+mode.String(), func(t *testing.T) {
				f := newTokFix(t)
				f.load("m", row.loads, row.u)
				if mode == spilled {
					f.load("o", nil, 1)
				}
				f.g.Update(probeState(row, mode))
				key := f.keyOnM
				if mode != direct {
					key = f.keyOnO
				}
				r, err := f.g.TryPickFor(key, nil, row.req)
				if row.admit {
					if err != nil || r == nil || r.Member != "m" {
						t.Fatalf("got %v, %v; want the request admitted on m", r, err)
					}
					if want := mode == spilled; r.Spilled != want {
						t.Fatalf("Spilled = %v, want %v", r.Spilled, want)
					}
					return
				}
				if !errors.Is(err, ErrAllAtCap) || r != nil {
					t.Fatalf("got %v, %v; want ErrAllAtCap and no reservation", r, err)
				}
				// Nothing was reserved or counted by the refusal: m holds what the row loaded.
				n, u, _ := f.g.Charged("m")
				var wantN int64
				for _, c := range row.loads {
					wantN += c
				}
				if n != wantN || u != int64(row.u) {
					t.Fatalf("after a refusal m holds N = %d and u = %d, want %d and %d", n, u, wantN, row.u)
				}
			})
		}
	}
}

// A request admitted adds its own charge, and only that: a Workload request its c to N, an unnamed one 1 to u.
func TestAnAdmittedRequestIsCountedAsWhatItIs(t *testing.T) {
	f := newTokFix(t)
	f.g.Update(State{Members: []string{"m", "o"}, Eligible: map[string]bool{"m": true}, Percent: noBound})
	if _, err := f.g.TryPickFor(f.keyOnM, nil, workload(70)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.TryPickFor(f.keyOnM, nil, workload(5)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.TryPickFor(f.keyOnM, nil, noWorkload); err != nil {
		t.Fatal(err)
	}
	// By hand: N = 70 + 5 = 75, u = 1, count 3.
	if n, u, ok := f.g.Charged("m"); !ok || n != 75 || u != 1 {
		t.Fatalf("m holds N = %d, u = %d (member %v), want 75 and 1", n, u, ok)
	}
	wantInFlight(t, f.g, "m", 3, "three requests")
	if n, u, ok := f.g.Charged("o"); !ok || n != 0 || u != 0 {
		t.Fatalf("o holds N = %d, u = %d (member %v), want 0 and 0", n, u, ok)
	}
	if _, _, ok := f.g.Charged("nobody"); ok {
		t.Fatal("Charged reports a count for a name that is not a member")
	}
}

// The inequalities, EXACTLY, at the edge of the type (a plain int64 product wraps or refuses on each of these rows). Each verdict is
// worked by hand from the inequality itself; B here is one worker's size unless w says otherwise.
func TestTheInequalitiesAreExactAtTheEdgeOfTheType(t *testing.T) {
	const two31, two61 = int64(1) << 31, int64(1) << 61
	rows := []struct {
		name      string
		cap, b, w int64
		n, u      int64
		c         int64
		named     bool
		admit     bool
		why       string
	}{
		// cap = B = 2^31: cap*B = 2^62. c = 2^31: cap*c = 2^62 <= 2^62: admitted, big but exact.
		{"a large exact fit", two31, two31, 1, 0, 0, two31, true, true, "2^62 <= 2^62"},
		// c = 2^31 + 1: 2^62 + 2^31 > 2^62: refused, one token over.
		{"a large one over", two31, two31, 1, 0, 0, two31 + 1, true, false, "2^62 + 2^31 > 2^62"},
		// c = 2^33: cap*c = 2^64 > 2^62; wrapped it is 0 and would be admitted.
		{"cap times charge is above 2^64", two31, two31, 1, 0, 0, 1 << 33, true, false, "cap*c = 2^64 > 2^62"},
		// cap = 1, B = 2^62, u = 4: u*B = 2^64 and cap*B = 2^62: 1*1 + 2^64 > 2^62, refused (a wrapped u*B would admit it).
		{"u times B is above 2^64", 1, 1 << 62, 1, 0, 4, 1, true, false, "1 + 2^64 > 2^62"},
		// cap = 2, B = 2^61, u = 2, c = 2^61: 2*2^61 + 2*2^61 = 2^63 > 2*2^61 = 2^62: refused (the sum does not fit an int64).
		{"the two terms sum to 2^63", 2, two61, 1, 0, 2, two61, true, false, "2^63 > 2^62"},
		// N = MaxInt64, c = 1: N + c = 2^63; cap = 1, B = 2^40: 2^63 > 2^40: refused.
		{"N plus c is 2^63", 1, 1 << 40, 1, math.MaxInt64, 0, 1, true, false, "2^63 > 2^40"},
		// cap = B = 2^40, c = 1: cap*(N+c) = 2^40 <= cap*B = 2^80: ADMITTED, though cap*B does not fit an int64.
		{"cap times B is 2^80, a Workload request", 1 << 40, 1 << 40, 1, 0, 0, 1, true, true, "2^40 <= 2^80"},
		// unnamed, the same: 2^40 * 0 + 1 * 2^40 <= 2^80: admitted.
		{"cap times B is 2^80, an unnamed request", 1 << 40, 1 << 40, 1, 0, 0, 0, false, true, "2^40 <= 2^80"},
		// cap = 2, B = 2^61, N = 2^61: cap*N = 2^62, (u+1)*B = 2^61: 2^62 + 2^61 > cap*B = 2^62: refused.
		{"unnamed sum over without overflow", 2, two61, 1, two61, 0, 0, false, false, "2^62 + 2^61 > 2^62"},
		// u = MaxInt64, cap = 1: u + 1 = 2^63 > cap: refused.
		{"u plus 1 is 2^63", 1, 1, 1, 0, math.MaxInt64, 0, false, false, "2^63 > 1"},
		// cap = 2^30, B = 2^31, N = 2^34: cap*N = 2^64 > cap*B = 2^61: refused (wrapped, cap*N is 0).
		{"cap times N is 2^64", 1 << 30, two31, 1, 1 << 34, 0, 0, false, false, "2^64 > 2^61"},
		// cap = 2, B = 2^62, idle: 2*0 + 1*B <= 2*B: admitted (cap*B = 2^63 does not fit an int64).
		{"an idle Instance at B = 2^62", 2, 1 << 62, 1, 0, 0, 0, false, true, "2^62 <= 2^63"},
		// u = 1: 2*0 + 2*B <= 2*B: admitted. u = 2: 3*B > 2*B: refused.
		{"one unnamed request at B = 2^62", 2, 1 << 62, 1, 0, 1, 0, false, true, "2*2^62 <= 2*2^62"},
		{"two unnamed requests at B = 2^62", 2, 1 << 62, 1, 0, 2, 0, false, false, "3*2^62 > 2*2^62"},
		// Two workers of 2^62: B = 2^63. u = cap = 2: 2*(0+1) + 2*B <= 2*B is false.
		{"B = 2^63, the unnamed requests hold it all", 2, 1 << 62, 2, 0, 2, 1, true, false, "2 + 2*2^63 > 2*2^63"},
		// B = 2^63 - 1, cap = 2^62, N = 0, u = 0, c = 2^63 - 1: cap*c = 2^62 * (2^63 - 1) <= cap*B: equal: admitted.
		{"cap and B both near the top", 1 << 62, math.MaxInt64, 1, 0, 0, math.MaxInt64, true, true, "equal products"},
		// c = 2^63 - 1 with N = 1: N + c = 2^63 > B: refused by one token.
		{"cap and B both near the top, one over", 1 << 62, math.MaxInt64, 1, 1, 0, math.MaxInt64, true, false, "2^62 * 2^63 > 2^62 * (2^63 - 1)"},
		// cap = MaxInt64 = u + 1: an unnamed request at u = cap - 1 with B = 2^63 - 1: (u+1)*B = cap*B: admitted.
		{"cap at MaxInt64, the last slot", math.MaxInt64, math.MaxInt64, 1, 0, math.MaxInt64 - 1, 0, false, true, "cap*B <= cap*B"},
		// A non-positive size or a negative count cannot come from the group and is refused.
		{"a negative charge is refused", 4, 100, 1, 0, 0, -1, true, false, "c < 0"},
		{"a zero workers factor is refused", 4, 100, 0, 0, 0, 1, true, false, "no budget"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			b := Budget{PerWorker: row.b, Workers: row.w}
			if got := tokenFits(row.cap, b, row.n, row.u, row.c, row.named); got != row.admit {
				t.Fatalf("tokenFits(cap %d, B %d x %d, N %d, u %d, c %d, named %v) = %v, want %v (%s)", row.cap, row.b, row.w, row.n, row.u, row.c, row.named, got, row.admit, row.why)
			}
		})
	}
}

// The comparison's arithmetic against math/big, on the same two inequalities written as HBEP-5 writes them and evaluated without any
// rearrangement. This checks the 128- and 192-bit carries, not the rule (the typed rows pin that): random inputs over the whole int64
// range, and for each Workload case the charge at the edge, the largest the member admits and one more, built from the exact
// quotient.
func TestTheComparisonAgreesWithBigIntegerArithmetic(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261118, 783))
	bi := func(v int64) *big.Int { return big.NewInt(v) }
	pick := func() int64 {
		switch rng.IntN(5) {
		case 0:
			return 1 + rng.Int64N(512)
		case 1:
			return 1 + rng.Int64N(1<<32)
		case 2:
			return math.MaxInt64 - rng.Int64N(4)
		default:
			return 1 + rng.Int64N(math.MaxInt64)
		}
	}
	// exact is HBEP-5's two inequalities as written, in unbounded integers.
	exact := func(limit int64, b Budget, n, u, c int64, named bool) bool {
		whole := new(big.Int).Mul(bi(b.PerWorker), bi(b.Workers))
		left := new(big.Int)
		if named {
			left.Mul(bi(limit), new(big.Int).Add(bi(n), bi(c)))
			left.Add(left, new(big.Int).Mul(bi(u), whole))
		} else {
			left.Mul(bi(limit), bi(n))
			left.Add(left, new(big.Int).Mul(new(big.Int).Add(bi(u), bi(1)), whole))
		}
		return left.Cmp(new(big.Int).Mul(bi(limit), whole)) <= 0
	}
	checked := 0
	for i := range 100_000 {
		limit, pw := pick(), pick()
		w := int64(1 + rng.IntN(64))
		if rng.IntN(8) == 0 {
			w = math.MaxInt32
		}
		b := Budget{PerWorker: pw, Workers: w}
		u := rng.Int64N(limit) // 0..cap-1; the last slot and the cap itself are drawn below
		if rng.IntN(8) == 0 {
			u = limit
		}
		n := rng.Int64N(pick())
		c := rng.Int64N(pick()) + 1
		for _, named := range []bool{false, true} {
			if got, want := tokenFits(limit, b, n, u, c, named), exact(limit, b, n, u, c, named); got != want {
				t.Fatalf("case %d: cap %d, B %d x %d, N %d, u %d, c %d, named %v: %v, exact arithmetic says %v", i, limit, pw, w, n, u, c, named, got, want)
			}
			checked++
		}
		// The edge, for one case in five: the largest charge the member admits, by bisection on the exact arithmetic, is admitted and
		// the next one is refused.
		if i%5 != 0 || !exact(limit, b, n, u, 0, true) {
			continue
		}
		lo, hi := int64(0), int64(math.MaxInt64)
		for lo < hi {
			gap := hi - lo
			mid := lo + gap/2 + gap%2 // the upper middle, written so that it cannot overflow
			if exact(limit, b, n, u, mid, true) {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		if !tokenFits(limit, b, n, u, lo, true) {
			t.Fatalf("case %d: cap %d, B %d x %d, N %d, u %d: the largest charge %d is refused", i, limit, pw, w, n, u, lo)
		}
		if lo < math.MaxInt64 && tokenFits(limit, b, n, u, lo+1, true) {
			t.Fatalf("case %d: cap %d, B %d x %d, N %d, u %d: a charge of %d, one above the largest, is admitted", i, limit, pw, w, n, u, lo+1)
		}
		checked++
	}
	if checked < 200_000 {
		t.Fatalf("only %d cases checked", checked)
	}
}

// An overflow-sized B and cap on a live group (cap = B = 2^40, so cap * B = 2^80, which no int64 holds): the comparison is exact, so
// each request meets its real verdict. Four rows, typed by hand from the inequalities:
//
//	full membership, a Workload request, c = 1:     2^40 * 1 + 0 <= 2^40 * 2^40: admitted.
//	full membership, an unnamed request:            2^40 * 0 + 1 * 2^40 <= 2^80: admitted.
//	partial membership, a Workload request, c = 1:  the same, admitted.
//	partial membership, an unnamed request:         admitted (its condition is off).
//
// and a Workload request whose charge is 2^41 beside a member that holds N = 2^80 - 1 cannot be built here (N is an int64), so the
// refusing rows of the huge budgets are in the truth table.
func TestAnOverflowSizedBudgetOnALiveGroupIsComparedNotRefused(t *testing.T) {
	for _, row := range []struct {
		name    string
		partial bool
		req     Charge
	}{
		{"full membership, a Workload request", false, workload(1)},
		{"full membership, an unnamed request", false, noWorkload},
		{"partial membership, a Workload request", true, workload(1)},
		{"partial membership, an unnamed request", true, noWorkload},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newTokFix(t)
			s := State{
				Members: []string{"m", "o"}, Eligible: map[string]bool{"m": true}, Percent: noBound,
				CapValues: map[string]int{"m": 1 << 40}, Budgets: map[string]Budget{"m": {PerWorker: 1 << 40, Workers: 1}},
			}
			if !row.partial {
				s.Caps = map[string]int{"m": 1 << 40}
			}
			f.g.Update(s)
			r, err := f.g.TryPickFor(f.keyOnM, nil, row.req)
			if err != nil || r == nil || r.Member != "m" {
				t.Fatalf("got %v, %v, want the request admitted on m", r, err)
			}
		})
	}
}

// The reduction (HBEP-5: with no Workload request in flight, the second condition is u + 1 <= cap, the count rule), over the FULL
// range: B's per-worker size uniform over 1..MaxInt64 with the edges 1, 2, 2^31, 2^32, 2^62 and MaxInt64 each drawn often, 1 to 64
// workers (and 2^31 - 1), and cap from 1 to 512 in half the cases and over 1..MaxInt64 in the rest, with u from 0 to cap (near cap
// when cap is large: cap - 2, cap - 1, cap, and one uniform). The oracle is the count rule itself, u + 1 <= cap, written as
// u <= cap - 1 so that it cannot overflow, never the inequality. Fixed seed.
func TestWithNoWorkloadRequestInFlightTheUnnamedConditionIsTheCountRule(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261118, 764))
	edges := []int64{1, 2, 1 << 31, 1 << 32, 1 << 62, math.MaxInt64}
	const cases = 400_000
	for i := range cases {
		var limit int64
		if rng.IntN(2) == 0 {
			limit = 1 + rng.Int64N(512)
		} else {
			limit = 1 + rng.Int64N(math.MaxInt64)
		}
		pw := 1 + rng.Int64N(math.MaxInt64)
		if rng.IntN(4) == 0 {
			pw = edges[rng.IntN(len(edges))]
		}
		w := int64(1 + rng.IntN(64))
		if rng.IntN(16) == 0 {
			w = math.MaxInt32
		}
		var u int64
		if limit <= 512 {
			u = rng.Int64N(limit + 1)
		} else {
			u = []int64{0, limit - 2, limit - 1, limit, rng.Int64N(limit)}[rng.IntN(5)]
		}
		want := u <= limit-1 // the count rule: u + 1 <= cap
		if got := tokenFits(limit, Budget{PerWorker: pw, Workers: w}, 0, u, 0, false); got != want {
			t.Fatalf("case %d: cap %d, B %d x %d, u %d: the unnamed condition says %v, the count rule %v", i, limit, pw, w, u, got, want)
		}
	}
}

// The same reduction through a live group, the whole path: with N = 0 a request that names no Workload is admitted exactly
// when the member's count is below its cap, whether B is known or not, and however large B is.
func TestWithNoWorkloadRequestInFlightAnUnnamedRequestMeetsTheCountRuleThroughTheGroup(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 764))
	for i := range 600 {
		cap := 1 + rng.IntN(6)
		u := rng.IntN(cap + 1)
		known := rng.IntN(2) == 0
		f := newTokFix(t)
		f.load("m", nil, u)
		row := tokRow{cap: cap, partial: false}
		if known {
			row.b = 1 + rng.Int64N(math.MaxInt64)
			row.w = int64(1 + rng.IntN(8))
		}
		f.g.Update(probeState(row, direct))
		r, err := f.g.TryPickFor(f.keyOnM, nil, noWorkload)
		if want := u+1 <= cap; (err == nil) != want {
			t.Fatalf("case %d: cap %d, B known %v (%d x %d), u %d: admitted %v (%v), the count rule says %v", i, cap, known, row.b, row.w, u, err == nil, err, want)
		} else if want && r.Member != "m" {
			t.Fatalf("case %d: admitted on %s, want m", i, r.Member)
		}
	}
}

// B is unknown when the state has none, and a state that carries 0 or less is no budget either: it must not turn into a B of 0
// that refuses everything.
func TestABudgetBelowOneIsUnknown(t *testing.T) {
	f := newTokFix(t)
	f.g.Update(State{
		Members: []string{"m", "o"}, Eligible: map[string]bool{"m": true}, Percent: noBound,
		Caps: map[string]int{"m": 4}, CapValues: map[string]int{"m": 4}, Budgets: map[string]Budget{"m": {PerWorker: 0, Workers: 1}},
	})
	// cap 4 and nothing in flight: a Workload request of 1,000,000 tokens would be refused by any B, and is admitted
	// because B = 0 is no budget: the count rule alone.
	if r, err := f.g.TryPickFor(f.keyOnM, nil, workload(1_000_000)); err != nil || r.Member != "m" {
		t.Fatalf("got %v, %v, want the request admitted on m", r, err)
	}
}
