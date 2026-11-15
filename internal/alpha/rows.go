package alpha

import (
	"fmt"
	"strings"
	"time"
)

// The deployment timing every stack row applies: the shortest that still shows
// a handover. An Allocation whose time limit is not longer than the lead an
// Instance needs hosts none (PartitionTooShort), and the lead is the Instance's
// drain window plus handoverMargin plus health.startTimeout
// (internal/lifecycle/timelimit.go:22). The drain window is
// routing.maxRequestDuration plus the profile's drainMargin when that is shorter
// than the profile's drainWindow (api/v1alpha1/writers.go, InstanceDrainWindow).
// Time limits are whole minutes and the check is "not longer than"
// (timelimit.go:42), so the shortest lease is the lead plus one minute.
//
// With a 5-minute routing.maxRequestDuration and the shipped profiles' values
// (drainMargin 10 min, handoverMargin 30 min, health.startTimeout 1 h) the lead
// is 1 h 45 min, the figure profiles/athena.yaml:39 gives for plgrid-now, and the
// shortest lease 1 h 46 min. A shorter maxRequestDuration would shorten it, but
// the streams the rows hold across a drain start must fit in it.
const (
	MaxRequestDuration = 5 * time.Minute
	DrainMargin        = 10 * time.Minute
	HandoverMargin     = 30 * time.Minute
	StartTimeout       = time.Hour
	// IdleLimit is the idle limit of the no-overlap row's ReleaseWhenIdle: the
	// predecessor's job is released this long after its Instance ended.
	IdleLimit = 2 * time.Minute
)

// Lead is the lead an Instance needs on a platform with these margins: the
// drain window (maxRequestDuration plus drainMargin; the profile's own
// drainWindow is longer here) plus handoverMargin plus health.startTimeout.
func Lead() time.Duration {
	return MaxRequestDuration + DrainMargin + HandoverMargin + StartTimeout
}

// MinLease is the shortest leaseLimit that is longer than Lead, in whole minutes.
func MinLease() time.Duration { return Lead() + time.Minute }

// The hour of the tunnel rows (HBEP-6:354 and :375, ruled by the science agent on
// 2 Nov 2026): an hour of one tunnel session staying healthy under real use, not
// one stream an hour long. The front cuts a request at the deployment's
// routing.maxRequestDuration (HBEP-5, Errors), so every request is within the
// 5-minute deadline, and the hour is what the tunnel carries across them.
const (
	// TunnelHour is how long the traffic runs, S1's 3,660 s: the hour and a minute.
	TunnelHour = 61 * time.Minute
	// MinChain is the fewest requests in the shortest run of back-to-back
	// requests the hour must contain, and MaxChainGap the longest pause between
	// two requests of such a run.
	MinChain    = 3
	MaxChainGap = 10 * time.Second
	// NearDeadline is the length of the longest request the hour must contain
	// when its long stream is sized at the cap: 4 minutes of the 4 min 30 s cap
	// (LongCap), close to the 5-minute deadline. A stream sized to the context at
	// the measured rate is judged at the same share of what it was sized for
	// (Timing.withLongPlan; issue #451).
	NearDeadline = 4 * time.Minute
	// LongFloor is the shortest fit the hour's long stream is sized at (issue #451; the science agent's ruling (b), 12 Nov 2026, and its
	// ruling (ii) on #452): a fit under it is NOT RUN. It is 135 s so that no stream under 2 minutes is ever a pass: the bar a stream
	// sized at the floor is judged at, its share NearDeadline/LongCap of 135 s, is 120 s (Timing.LongBarFloor).
	LongFloor = 135 * time.Second
	// ConcurrentStreams streams of at least StreamRate tokens a second each must be
	// open at the same moment.
	ConcurrentStreams = 16
	StreamRate        = 20.0
	// MinLargeBody is the smallest request body that counts as large, 0.1 MB (issue
	// #224): the hour must send such requests, among the streams, so that the upload
	// direction carries real load and the tunnel's head-of-line pressure shows there.
	MinLargeBody = 100_000
	// UploadBody, UploadStreams and UploadPause are the live tunnel hour's large
	// requests (issue #224, the upload direction, front to node; the science
	// agent's ruling (4) on PR 255): prompts of up to 0.4 MB, the top of #224's
	// 0.1 to 0.4 MB, and fewer tokens than the deployment's context, from two
	// senders, each pausing half a second after a reply, for the 30 s beside the
	// download's measurement: bodies among the 16 streams, each served by the
	// engine with one token, a prefill of the whole prompt, and counted in its
	// usage (PromptTokensOf).
	UploadBody    = 400_000
	UploadStreams = 2
	UploadPause   = 500 * time.Millisecond
	// MinIdleGap is the shortest stretch with no request open at all, where the
	// session carries heartbeats only.
	MinIdleGap = 2 * time.Minute
	// MaxSampleGap is the longest time between two looks at the tunnel session
	// (frps's proxy list and the Instance's TunnelConnected condition).
	MaxSampleGap = 2 * time.Minute
)

// The tunnel rows' lease. The hour of traffic must end before the drain start,
// a drain window (maxRequestDuration plus drainMargin) before the lease ends, so
// the Instance must be ready within LatestReady(lease) of its job's start. The
// science agent's rule (2 Nov 2026): take GH200's measured start-to-ready with the
// cold SIF build included, and keep the shortest lease (MinLease) if it is under
// 20 minutes, else TunnelLease. The measurement is the second real-engine run on
// Helios GH200 (issue #94, comment 6557821833, cold build, empty image cache, a
// slow Hub): the job started at 11:42:26Z and its first Instance was ready,
// qualification and warm-up done, at 12:06:05Z, 23 min 39 s later. That is over
// 20 minutes, so the default is TunnelLease. It is a parameter of the run
// (LIVE_TUNNEL_LEASE_MINUTES), so the owner can choose the other value when
// giving the go.
const (
	// MeasuredStartToReady is that measurement, 11:42:26Z to 12:06:05Z.
	MeasuredStartToReady = 23*time.Minute + 39*time.Second
	// ShortLeaseRule is the measurement under which the short lease is kept.
	ShortLeaseRule = 20 * time.Minute
	// MaxTunnelLease is the longest lease the owner may choose for a tunnel row:
	// a few hours at most (the science agent), not the 12 h maxWall of QOS now.
	MaxTunnelLease = 4 * time.Hour
)

// TunnelLease is the default lease of the tunnel rows: the Instance may take the
// whole health.startTimeout to become ready, then the hour of traffic, then the
// drain window, and a minute: 2 h 17 min. It is the longest case the profile
// allows, and the measurement above says a start can take half the short lease.
func TunnelLease() time.Duration {
	l := StartTimeout + TunnelHour + MaxRequestDuration + DrainMargin + time.Minute
	if l < MinLease() {
		return MinLease()
	}
	return l
}

// DefaultTunnelLease is the lease the measurement chooses: MinLease when the
// measured start-to-ready is under ShortLeaseRule, else TunnelLease.
func DefaultTunnelLease() time.Duration { return tunnelLeaseFor(MeasuredStartToReady) }

// tunnelLeaseFor is the rule: under ShortLeaseRule, the shortest lease; else TunnelLease.
func tunnelLeaseFor(measured time.Duration) time.Duration {
	if measured < ShortLeaseRule {
		return MinLease()
	}
	return TunnelLease()
}

// LatestReady is how long after its job starts the Instance may become ready for
// the hour of traffic to end before the drain start, under the lease.
func LatestReady(lease time.Duration) time.Duration {
	return lease - (MaxRequestDuration + DrainMargin) - TunnelHour
}

// WithTunnelLease is the row with another lease, for the owner's choice. Only a
// tunnel row takes one, in whole minutes, from MinLease (the lead plus a minute) to
// MaxTunnelLease; the row's slot hold and run budget follow the lease, the budget as
// the script's timeout does (TunnelRunOver).
func WithTunnelLease(r Row, lease time.Duration) (Row, error) {
	if !strings.HasPrefix(r.Name, "tunnel-hour-") {
		return r, fmt.Errorf("only a tunnel row takes a lease of its own")
	}
	if lease%time.Minute != 0 || lease < MinLease() || lease > MaxTunnelLease {
		return r, fmt.Errorf("a tunnel lease is whole minutes from %s to %s", MinLease(), MaxTunnelLease)
	}
	r.Lease = lease
	r.Budget = lease + TunnelRunOver
	if r.SlotHold > 0 {
		r.SlotHold = lease
	}
	return r, nil
}

// Row is one row of hack/live-checks.sh's table as the test sees it. The
// script's cluster shapes are the same facts; hack/livechecks fails when the
// two drift.
type Row struct {
	// Name is the row's name in the script.
	Name string
	// Cluster is the ssh alias and the platform: helios or athena.
	Cluster string
	// Partition is the partition the run's jobs must have run on.
	Partition string
	// QOS is the QOS the run's jobs run under; empty for the partition's default.
	QOS string
	// GPUs is the number of GPUs of each job.
	GPUs int
	// Lease is the leaseLimit of the run's deployment, and so each job's time
	// limit. The handover rows use MinLease; the tunnel rows DefaultTunnelLease,
	// which the owner may change (WithTunnelLease); the accounting rows' job is 5 minutes.
	Lease time.Duration
	// Budget is how long the run may take once its first job has started: the
	// second clock of the run. The first is the wait in the queue.
	Budget time.Duration
	// SlotHold is how long the run holds the one-job slot of QOS now, for the
	// rows that run there: the owner's shared GPU (#145). Zero for a row that
	// holds no slot.
	SlotHold time.Duration
	// StartWait bounds how long a job of the run may wait to start, from its
	// acceptance, for the rows under a one-job QOS that guard the owner's slot with
	// it (the science agent, 2 Nov 2026; HPCBURST_CONF_START_WAIT): past it the
	// run cancels its jobs by their tags and is NOT RUN. Zero: no bound.
	StartWait time.Duration
	// MostJobs is the number of jobs of the run, and so the most of them in the
	// queue at once: the verdicts check the cluster's own record of job identities
	// and occupancy against it.
	MostJobs int
	// PostsTo names where the run's result and its slot use are posted: the issue (or issues) a row's text says it goes to.
	// A row posts to #145, the alpha list, and to the issue of its own work where it has one that is open (#376 for the
	// plgrid-now hour rows, #94 for the Helios real-engine run). It names no closed issue: #93, which the accounting rows and
	// the handover named, was closed and was about the hold lift (`@hpc-burst-science-agent`, 12 Nov 2026, on #145), so those
	// four rows post to #145 alone. TestNoRowPostsToAClosedIssue holds the list.
	PostsTo string
	// Origin names the closed issue a row was built under, where its text still names it: #225 for the reach rows and #305 for the
	// host-dialed rows (`@hpc-burst-science-agent`, 12 Nov 2026, on #145). The text says it, in the verdict line (PostsNote), and
	// never posts to it: it is closed. Empty for a row with no such issue.
	Origin string
	// Timing is the clock the row is judged on; the zero Timing is the live
	// one (Clock).
	Timing Timing
	// Profile names the row's shipped platform profile (profiles/<name>.yaml);
	// empty for the profile named after the row's cluster (ProfileName). Only the
	// plgrid-now rows of issue #376 run under another, athena-now.yaml.
	Profile string
	// OwnerGo names the setting that must hold today's UTC date before the row
	// runs, the owner's go for the slot it holds (the development rule on #145):
	// LIVE_OWNER_GO_HELIOS_NOW for the rows that hold QOS now's GPU for hours,
	// LIVE_OWNER_GO_ATHENA_NOW for the rows that hold plgrid-now's slot under
	// athena-now.yaml (issue #376, Q6). Empty for a row that needs none.
	// hack/livechecks compares it with the script's owner-go table.
	OwnerGo string
	// ReleaseWhenIdle says the row's deployment releases each job once its Instance has ended, after the live idle
	// limit (Timing.IdleLimit): the no-overlap rows, whose successor waits on the predecessor's job, on Helios's GPU cap
	// (alpha-helios-nooverlap) or on plgrid-now's one-job slot (alpha-athena-now-nooverlap, issue #376's row B;
	// hbep/0007-lifecycle-controller/README.md:483). Row.Plan reads it; no row name does.
	ReleaseWhenIdle bool
	// PostsGlibcNotRun says the row writes a text to post when its run is NOT RUN on a compute node whose glibc is under the 2.30 Envoy
	// needs (ErrGlibcTooOld): the version is the reading the row exists for. Only the Helios router row does (issue #566: its glibc is
	// posted on any outcome); the Athena router row's NOT RUN posts nothing, as ruled on #521.
	PostsGlibcNotRun bool
}

// ProfileName is the row's shipped platform profile: Profile, else its cluster's.
func (r Row) ProfileName() string {
	if r.Profile != "" {
		return r.Profile
	}
	return r.Cluster
}

// Timing is the clock a row's judgement runs on: the deployment timing its
// scenario applies and what its hour of traffic must hold. The zero Timing is
// the live one: a zero field means this file's constant (LiveTiming). A
// stand-in run of the live steps, which drives the real stack against a fake
// cluster within a 20-minute hold, scales it (issue #245). Every live row keeps
// the zero Timing but the rows on plgrid-now (issue #376): their clock is the
// margins of the shipped profile they run under, profiles/athena-now.yaml
// (AthenaNowTiming), since athena.yaml's give a lead of 1 h 45 min, which the
// partition's 1 h limit cannot host (the science agent's ruling Q1 on #376:
// the rows test what ships, so the profile is a shipped one and not an overlay
// in the test). TestTheNowRowsClockIsTheShippedProfiles reads the file.
type Timing struct {
	MaxRequestDuration, DrainMargin, HandoverMargin, StartTimeout time.Duration
	// IdleLimit is the no-overlap row's idleLimit, and MaxIdleLimit the longest
	// one its verdict takes as short.
	IdleLimit, MaxIdleLimit time.Duration
	// TunnelHour is how long the hour's traffic runs, and Hour the span its
	// requests and the job must cover.
	TunnelHour, Hour time.Duration
	MinChain         int
	MaxChainGap      time.Duration
	NearDeadline     time.Duration
	// LongFloor is the shortest fit the hour's long stream is sized at (LongFloor; FitLong).
	LongFloor         time.Duration
	ConcurrentStreams int
	StreamRate        float64
	MinIdleGap        time.Duration
	MaxSampleGap      time.Duration
	// UploadBody is the size of the large requests' bodies in bytes,
	// UploadStreams how many senders send them, and UploadPause the pause each
	// sender takes after a reply.
	UploadBody, UploadStreams int
	UploadPause               time.Duration
	// ServeTokens is the exact length, in tokens, of row A's long stream on
	// plgrid-now (ServeStreamTokens live; issue #376, Q4).
	ServeTokens int
}

// LiveTiming is the live rows' clock: this file's constants, and S1's hour.
func LiveTiming() Timing {
	return Timing{MaxRequestDuration: MaxRequestDuration, DrainMargin: DrainMargin, HandoverMargin: HandoverMargin, StartTimeout: StartTimeout,
		IdleLimit: IdleLimit, MaxIdleLimit: 10 * time.Minute, TunnelHour: TunnelHour, Hour: Hour, MinChain: MinChain, MaxChainGap: MaxChainGap,
		NearDeadline: NearDeadline, LongFloor: LongFloor, ConcurrentStreams: ConcurrentStreams, StreamRate: StreamRate, MinIdleGap: MinIdleGap, MaxSampleGap: MaxSampleGap,
		UploadBody: UploadBody, UploadStreams: UploadStreams, UploadPause: UploadPause, ServeTokens: ServeStreamTokens}
}

// Clock is the row's Timing with each zero field taken from LiveTiming.
func (r Row) Clock() Timing {
	t, l := r.Timing, LiveTiming()
	fill := func(v *time.Duration, d time.Duration) {
		if *v == 0 {
			*v = d
		}
	}
	fill(&t.MaxRequestDuration, l.MaxRequestDuration)
	fill(&t.DrainMargin, l.DrainMargin)
	fill(&t.HandoverMargin, l.HandoverMargin)
	fill(&t.StartTimeout, l.StartTimeout)
	fill(&t.IdleLimit, l.IdleLimit)
	fill(&t.MaxIdleLimit, l.MaxIdleLimit)
	fill(&t.TunnelHour, l.TunnelHour)
	fill(&t.Hour, l.Hour)
	fill(&t.MaxChainGap, l.MaxChainGap)
	fill(&t.NearDeadline, l.NearDeadline)
	fill(&t.LongFloor, l.LongFloor)
	fill(&t.MinIdleGap, l.MinIdleGap)
	fill(&t.MaxSampleGap, l.MaxSampleGap)
	if t.MinChain == 0 {
		t.MinChain = l.MinChain
	}
	if t.ConcurrentStreams == 0 {
		t.ConcurrentStreams = l.ConcurrentStreams
	}
	if t.StreamRate == 0 {
		t.StreamRate = l.StreamRate
	}
	if t.UploadBody == 0 {
		t.UploadBody = l.UploadBody
	}
	if t.UploadStreams == 0 {
		t.UploadStreams = l.UploadStreams
	}
	fill(&t.UploadPause, l.UploadPause)
	if t.ServeTokens == 0 {
		t.ServeTokens = l.ServeTokens
	}
	return t
}

// Lead is the lead an Instance needs under this timing (see the package-level Lead).
func (t Timing) Lead() time.Duration {
	return t.MaxRequestDuration + t.DrainMargin + t.HandoverMargin + t.StartTimeout
}

// ProviderRef is a Provider a deployment allows, by name and by the platform it
// is on.
type ProviderRef struct {
	Name    string `json:"name"`
	Cluster string `json:"cluster"`
}

// DeploymentSpec is what a run's ModelDeployment says: the Providers it allows
// and the timing it applies. Plan gives what a row requires; the scenario
// reports what the API held.
type DeploymentSpec struct {
	Providers          []ProviderRef `json:"providers"`
	LeaseLimit         time.Duration `json:"leaseLimit"`
	MaxRequestDuration time.Duration `json:"maxRequestDuration"`
	ReleaseWhenIdle    bool          `json:"releaseWhenIdle"`
	IdleLimit          time.Duration `json:"idleLimit"`
}

// Plan is the deployment the row's scenario must apply: the row's Provider alone,
// the shortest lease, the 5-minute request deadline, and for the no-overlap rows
// (Row.ReleaseWhenIdle) ReleaseWhenIdle with a short idle limit
// (hbep/0007-lifecycle-controller/README.md:483).
func (r Row) Plan() DeploymentSpec {
	tm := r.Clock()
	d := DeploymentSpec{Providers: []ProviderRef{{Name: r.Cluster, Cluster: r.Cluster}}, LeaseLimit: r.Lease, MaxRequestDuration: tm.MaxRequestDuration}
	if r.ReleaseWhenIdle {
		d.ReleaseWhenIdle, d.IdleLimit = true, tm.IdleLimit
	}
	return d
}

// rows are the live rows of the runbook that run the stack.
var rows = []Row{
	// Two Allocations together on plgrid-gpu-a100, which has no one-job slot.
	{Name: "alpha-athena-handover", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: MinLease(), Budget: 150 * time.Minute, MostJobs: 2, PostsTo: "#145"},
	// The predecessor, and the successor pending beside it in QOS now.
	{Name: "alpha-helios-nooverlap", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: MinLease(), Budget: 213 * time.Minute, SlotHold: MinLease() + 45*time.Minute, MostJobs: 2, PostsTo: "#145 and #94", OwnerGo: OwnerGoHeliosNow, ReleaseWhenIdle: true},
	{Name: "tunnel-hour-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: DefaultTunnelLease(), Budget: DefaultTunnelLease() + TunnelRunOver, SlotHold: DefaultTunnelLease(), MostJobs: 1, PostsTo: "#145", OwnerGo: OwnerGoHeliosNow},
	{Name: "tunnel-hour-athena", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: DefaultTunnelLease(), Budget: DefaultTunnelLease() + TunnelRunOver, MostJobs: 1, PostsTo: "#145"},
	// Row A of issue #376 (nowserve.go): one job on plgrid-now, the owner's shared one-job slot, under athena-now.yaml.
	NowServeRow(),
	// Row B of issue #376 (nownooverlap.go): two jobs in turn on the same slot, no-overlap mode on the one-slot path.
	NowNoOverlapRow(),
	// The router row of issue #521 (betarouter.go), beta's: Envoy's first run on a compute node, on row A's hold of the same slot.
	BetaRouterRow(),
	// The keeper row of issue #521 (betakeeper.go), beta's: a controller restart while a stream runs, in host-dialed mode with the relay keeper.
	BetaKeeperRow(),
	// The bulk row of issue #521 (betabulk.go), beta's: the bulk path's real-path buffer, read from a loaded round trip, on the engine double.
	BetaBulkRow(),
	// The router row on Helios's GH200 (issue #566): its compute node's glibc, read when Helios returns.
	BetaHeliosRouterRow(),
}

// The settings of the owner's go (Row.OwnerGo).
const (
	OwnerGoHeliosNow = "LIVE_OWNER_GO_HELIOS_NOW"
	OwnerGoAthenaNow = "LIVE_OWNER_GO_ATHENA_NOW"
)

// accountingRows are the rows of the accounting check, which runs in
// internal/providerctl/conformance (a GPU job through the Provider's controller,
// no stack and no dial-home route). Lease and Budget are the job's.
//
// gpu-accounting-athena-now is the same check on Athena's one-job partition plgrid-now, under
// athena-now.yaml (`@hpc-burst-science-agent`, 12 Nov 2026, #145: the A100 queue was about two days out, and a
// row on plgrid-now counts for plgrid-gpu-a100 when it runs one job of one GPU within an hour, #373's
// equivalence). It has the one-job slot's guards (the slot read before and after the builds, a start wait of 10
// minutes) and the owner's go of plgrid-now. Its slot hold is the start wait, the job's time limit and a minute
// for the end, as the host-dialed rows' is. gpu-accounting-athena stays for when the A100 queue opens.
var accountingRows = []Row{
	{Name: "gpu-accounting-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 5 * time.Minute, Budget: 32 * time.Minute, SlotHold: 15 * time.Minute, StartWait: 10 * time.Minute, MostJobs: 1, PostsTo: "#145"},
	{Name: "gpu-accounting-athena", Cluster: "athena", Partition: "plgrid-gpu-a100", GPUs: 1, Lease: 5 * time.Minute, Budget: 25 * time.Minute, MostJobs: 1, PostsTo: "#145"},
	{Name: "gpu-accounting-athena-now", Cluster: "athena", Profile: "athena-now", Partition: "plgrid-now", GPUs: 1, Lease: 5 * time.Minute, Budget: 32 * time.Minute, SlotHold: 16 * time.Minute, StartWait: 10 * time.Minute, MostJobs: 1, PostsTo: "#145", Timing: AthenaNowTiming(), OwnerGo: OwnerGoAthenaNow},
}

// reachRows are the rows of the reach check (issue #225), which runs in
// internal/providerctl/conformance: one minimal job on a compute node of each alpha
// platform that tries a TCP connect to the gateway's forwarded port. Lease and Budget
// are the job's (a time limit of two minutes: the probe ends in seconds, and a stalled job
// must not hold the owner's slot longer); the Athena row is on the one-job partition plgrid-now (the same A100
// nodes as plgrid-gpu-a100, so the same network path; `@hpc-burst-science-agent`,
// 2 Nov 2026), whose own QOS applies, so it names none.
var reachRows = []Row{
	{Name: "reach-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 2 * time.Minute, Budget: 20 * time.Minute, SlotHold: 13 * time.Minute, StartWait: 10 * time.Minute, MostJobs: 1, PostsTo: "#145", Origin: "#225"},
	{Name: "reach-athena", Cluster: "athena", Partition: "plgrid-now", GPUs: 1, Lease: 2 * time.Minute, Budget: 20 * time.Minute, SlotHold: 13 * time.Minute, StartWait: 10 * time.Minute, MostJobs: 1, PostsTo: "#145", Origin: "#225"},
}

// hostDialedRows are the rows of the host-dialed relay check (issue #305), which runs in
// internal/providerctl/conformance: one short job of the real agent in host-dialed mode with no
// Instance, and the two relays the Provider's controller starts to it through the login node
// (HBEP-6, Dial-home modes). Lease and Budget are the job's: a time limit of five minutes, which a run
// that goes well ends in about one. Each is under its one-job slot with a start wait of 10 minutes, as
// the reach rows are, so the slot hold is the start wait, the lease and a minute for the end.
var hostDialedRows = []Row{
	{Name: "reach-host-dialed-helios", Cluster: "helios", Partition: "plgrid-gpu-gh200", QOS: "now", GPUs: 1, Lease: 5 * time.Minute, Budget: 25 * time.Minute, SlotHold: 16 * time.Minute, StartWait: 10 * time.Minute, MostJobs: 1, PostsTo: "#145", Origin: "#305"},
	{Name: "reach-host-dialed-athena", Cluster: "athena", Partition: "plgrid-now", GPUs: 1, Lease: 5 * time.Minute, Budget: 25 * time.Minute, SlotHold: 16 * time.Minute, StartWait: 10 * time.Minute, MostJobs: 1, PostsTo: "#145", Origin: "#305"},
}

// stageRows are the pre-staging rows (issue #376; nowserve.go), which run in internal/providerctl/conformance: one job
// whose agent build stages the image and the checkpoint where the agent stages, with no stack and no Instance.
var stageRows = []Row{NowStageRow()}

// StageRows returns a copy of the pre-staging rows.
func StageRows() []Row { return append([]Row(nil), stageRows...) }

// Rows returns a copy of the stack rows: the ones this package runs.
func Rows() []Row { return append([]Row(nil), rows...) }

// HostDialedRows returns a copy of the host-dialed relay rows.
func HostDialedRows() []Row { return append([]Row(nil), hostDialedRows...) }

// AccountingRows returns a copy of the accounting rows.
func AccountingRows() []Row { return append([]Row(nil), accountingRows...) }

// ReachRows returns a copy of the reach rows.
func ReachRows() []Row { return append([]Row(nil), reachRows...) }

// RowByName finds a row, of any kind.
func RowByName(name string) (Row, bool) {
	for _, r := range append(append(append(append(append([]Row(nil), rows...), accountingRows...), reachRows...), hostDialedRows...), stageRows...) {
		if r.Name == name {
			return r, true
		}
	}
	return Row{}, false
}
