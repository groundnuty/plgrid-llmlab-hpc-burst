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
// comes from calling tokenFits, and the reduction's oracle is the count rule, u + 1 <= cap, never the inequality.
//
// No test in this package may call t.Parallel (helpers_test.go).

import (
	"errors"
	"fmt"
	"math"
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
	b       int64
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
		CapValues: map[string]int{"m": row.cap}, Budgets: map[string]int64{},
	}
	if !row.partial {
		s.Caps = map[string]int{"m": row.cap}
	}
	if row.b > 0 {
		s.Budgets["m"] = row.b
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

// The inequalities in int64 with an overflow guard (refuse, never wrap), at the edge of the type. Each row is worked by
// hand; a wrapping implementation would admit every row marked as such.
func TestTheInequalitiesRefuseOnOverflowAndNeverWrap(t *testing.T) {
	const two31, two61 = int64(1) << 31, int64(1) << 61
	rows := []struct {
		name         string
		cap, b, n, u int64
		c            int64
		named        bool
		admit        bool
		why          string
	}{
		// cap = B = 2^31: cap*B = 2^62 fits. c = 2^31: cap*c = 2^62 <= 2^62: admitted, big but exact.
		{"a large exact fit", two31, two31, 0, 0, two31, true, true, "2^62 <= 2^62"},
		// c = 2^31 + 1: 2^62 + 2^31 > 2^62: refused, no overflow involved.
		{"a large one over", two31, two31, 0, 0, two31 + 1, true, false, "2^62 + 2^31 > 2^62"},
		// c = 2^33: cap*c = 2^64 does not fit; wrapped it is 0 <= 2^62 and would be admitted.
		{"cap times charge overflows", two31, two31, 0, 0, 1 << 33, true, false, "cap*c = 2^64 wraps to 0"},
		// cap = 1, B = 2^62, u = 4: u*B = 2^64 wraps to 0 and the request would be admitted.
		{"u times B overflows", 1, 1 << 62, 0, 4, 1, true, false, "u*B = 2^64 wraps to 0"},
		// cap = 2, B = 2^61, u = 2, c = 2^61: cap*(N+c) = 2^62 and u*B = 2^62 each fit, their sum 2^63 does not.
		{"the two terms overflow when added", 2, two61, 0, 2, two61, true, false, "2^62 + 2^62 = 2^63 > MaxInt64"},
		// N = MaxInt64, c = 1: N + c does not fit.
		{"N plus c overflows", 1, 1 << 40, math.MaxInt64, 0, 1, true, false, "N + c = 2^63"},
		// cap = B = 2^40: cap*B = 2^80 does not fit, whichever kind.
		{"cap times B overflows, Workload", 1 << 40, 1 << 40, 0, 0, 1, true, false, "cap*B = 2^80"},
		{"cap times B overflows, unnamed", 1 << 40, 1 << 40, 0, 0, 0, false, false, "cap*B = 2^80"},
		// Unnamed: cap = 2, B = 2^61, N = 2^61: cap*N = 2^62 fits, (u+1)*B = 2^61 fits, the sum 2^62 + 2^61 > cap*B = 2^62: refused.
		{"unnamed sum over without overflow", 2, two61, two61, 0, 0, false, false, "2^62 + 2^61 > 2^62"},
		// Unnamed: u + 1 overflows when u = MaxInt64.
		{"u plus 1 overflows", 1, 1, 0, math.MaxInt64, 0, false, false, "u + 1 = 2^63"},
		// Unnamed: cap = 2^30 and B = 2^31 give cap*B = 2^61, which fits. N = 2^34 makes cap*N = 2^64, which wraps to 0, and
		// then 0 + (0+1)*B <= 2^61 would admit it.
		{"cap times N overflows", 1 << 30, two31, 1 << 34, 0, 0, false, false, "cap*N = 2^64 wraps to 0"},
		// A negative value cannot come from the counts and is refused.
		{"a negative charge is refused", 4, 100, 0, 0, -1, true, false, "c < 0"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if got := tokenFits(row.cap, row.b, row.n, row.u, row.c, row.named); got != row.admit {
				t.Fatalf("tokenFits(cap %d, B %d, N %d, u %d, c %d, named %v) = %v, want %v (%s)", row.cap, row.b, row.n, row.u, row.c, row.named, got, row.admit, row.why)
			}
		})
	}
}

// An overflow-sized B and cap on a live group: both kinds are refused cleanly, nothing is reserved, and the member's counts do
// not move.
func TestAnOverflowSizedBudgetRefusesCleanlyThroughTheGroup(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, req := range []Charge{workload(1), noWorkload} {
			f := newTokFix(t)
			s := State{
				Members: []string{"m", "o"}, Eligible: map[string]bool{"m": true}, Percent: noBound,
				CapValues: map[string]int{"m": 1 << 40}, Budgets: map[string]int64{"m": 1 << 40},
			}
			if !partial {
				s.Caps = map[string]int{"m": 1 << 40}
			}
			f.g.Update(s)
			r, err := f.g.TryPickFor(f.keyOnM, nil, req)
			// cap * B = 2^80: a Workload request is always refused. An unnamed request is refused when the cap is in force;
			// below full membership its condition is off, and it is admitted as before the budget existed.
			wantRefused := req.Named || !partial
			if wantRefused {
				if !errors.Is(err, ErrAllAtCap) || r != nil {
					t.Fatalf("partial %v, %+v: got %v, %v, want ErrAllAtCap", partial, req, r, err)
				}
				wantCounts(t, f.g, 0, "after the refusal")
			} else if err != nil || r == nil {
				t.Fatalf("partial %v, %+v: got %v, %v, want it admitted", partial, req, r, err)
			}
		}
	}
}

// The reduction (HBEP-5: with no Workload request in flight, the second condition is u + 1 <= cap, the count rule). The
// oracle is the count rule itself, u + 1 <= cap, never the inequality. Fixed seed.
func TestWithNoWorkloadRequestInFlightTheUnnamedConditionIsTheCountRule(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261118, 764))
	const cases = 200_000
	for i := range cases {
		cap := 1 + rng.IntN(512)
		b := 1 + rng.Int64N(1<<40)
		u := rng.IntN(cap + 1)
		want := u+1 <= cap // the count rule
		if got := tokenFits(int64(cap), b, 0, int64(u), 0, false); got != want {
			t.Fatalf("case %d: cap %d, B %d, u %d: the unnamed condition says %v, the count rule %v", i, cap, b, u, got, want)
		}
	}
}

// The same reduction through a live group, the whole path: with N = 0 a request that names no Workload is admitted exactly
// when the member's count is below its cap, whether B is known or not.
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
			row.b = 1 + rng.Int64N(1<<40)
		}
		f.g.Update(probeState(row, direct))
		r, err := f.g.TryPickFor(f.keyOnM, nil, noWorkload)
		if want := u+1 <= cap; (err == nil) != want {
			t.Fatalf("case %d: cap %d, B known %v, u %d: admitted %v (%v), the count rule says %v", i, cap, known, u, err == nil, err, want)
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
		Caps: map[string]int{"m": 4}, CapValues: map[string]int{"m": 4}, Budgets: map[string]int64{"m": 0},
	})
	// cap 4 and nothing in flight: a Workload request of 1,000,000 tokens would be refused by any B, and is admitted
	// because B = 0 is no budget: the count rule alone.
	if r, err := f.g.TryPickFor(f.keyOnM, nil, workload(1_000_000)); err != nil || r.Member != "m" {
		t.Fatalf("got %v, %v, want the request admitted on m", r, err)
	}
}
