package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// instance is the Instance the agent runs or last ran.
type instance struct {
	uid string
	// launch is the Instance's launch as first applied. Every start and
	// restart runs exactly it (HBEP-7: the launch is fixed when the Instance
	// is created and every restart reuses it); a later desired state that
	// carries another launch for the same UID is logged and not run.
	launch       agentapi.Launch
	launchWarned bool
	key          string
	keyStored    bool
	hubToken     string
	// generation is the whole-Instance restart generation last applied: every
	// desired worker runs a process started for it.
	generation int64

	// Staging, for the Instance generation stageGen. firstGen is the
	// generation the Instance was created at: a staging for a later one is a
	// whole-Instance restart (rung 2), which repairs what fails its rehash.
	firstGen int64
	stageGen int64
	// files is the identity record's file list, from the first desired state
	// of the Instance generation filesGen whose Checks.Verify carried one.
	// Staging for a generation waits for it (PR #87 review, Opus B3;
	// `@hpc-burst-science-agent`, 31 Oct 2026).
	files         []agentapi.FileDigest
	filesGen      int64
	haveFiles     bool
	awaitWarned   int64 // the generation whose wait for files was logged, +1
	imageVerified bool  // the staged image passed its integrity check
	staging       agentapi.Staging
	stageRunning  bool
	stageCancel   context.CancelFunc
	staged        bool // staging for stageGen succeeded
	imagePath     string
	ckptDir       string
	// fileResults are the rehash results of the last staging, for the file
	// list whose hash is filesKey. They serve only the first verification
	// after that staging (fileResultsFresh); a later one rehashes (PR #87
	// review, Astra B2, Opus B1).
	fileResults      []agentapi.FileResult
	filesKey         [32]byte
	fileResultsFresh bool

	workers []*worker

	// Checks of the current verification generation, and the one check unit
	// running, if any.
	checks *agentapi.CheckResults
	unit   *checkUnit
	// sectionKeys holds, for each section of checks, the processes it was
	// taken against.
	sectionKeys map[string]procKey
}

// worker is one engine process of the Instance.
type worker struct {
	name       string
	phase      agentapi.PartPhase
	proc       *procgroup.Group
	exitCode   *int
	generation int64 // the worker's restart generation the process was started for
	instGen    int64 // the Instance's restart generation it was started for
	socket     string
	health     *agentapi.Health
	failures   int32
	lastHealth agentapi.Health // last metrics read, carried when a read fails
	stopProbe  context.CancelFunc
	// cleaning: the leader exited without being told to, and the rest of its
	// process group is being stopped.
	cleaning bool
}

func (w *worker) alive() bool {
	return w.phase == agentapi.PartStarting || w.phase == agentapi.PartRunning
}

// live reports whether any process of the worker's group may still run: a
// stopping worker, or an exited one whose group is being cleaned up.
func (w *worker) live() bool { return w.alive() || w.phase == agentapi.PartStopping || w.cleaning }

func (in *instance) anyLive() bool {
	for _, w := range in.workers {
		if w.live() {
			return true
		}
	}
	return false
}

func (in *instance) find(name string) *worker {
	for _, w := range in.workers {
		if w.name == name {
			return w
		}
	}
	return nil
}

// knownAction reports whether this agent's protocol version has the Action.
func knownAction(act agentapi.Action) bool {
	switch act {
	case agentapi.ActionIdle, agentapi.ActionRun, agentapi.ActionStop:
		return true
	}
	return false
}

// converge carries out the newest desired state, as far as the processes
// allow now. It is level-triggered: it runs after every event and re-reads
// the whole state, so a missed desired state is corrected by the next one
// (HBEP-7, Desired state, pulled).
func (a *Agent) converge() {
	if !a.haveDesired {
		return
	}
	d := a.desired
	if !knownAction(d.Action) {
		// An Action this agent does not know changes nothing (agentapi's
		// documentation, Versioning): parts keep running, a pending stop stays
		// pending, its settings are not taken and no check advances.
		return
	}
	if err := checkDesired(d); err != nil {
		if a.malformedLogged != d.Version {
			a.malformedLogged = d.Version
			a.log.Warn("malformed desired state ignored", "version", d.Version, "err", err)
		}
		return
	}
	a.takeSettings(d.Settings)
	switch d.Action {
	case agentapi.ActionRun:
		a.applyRun(*d.Instance)
	case agentapi.ActionIdle:
		a.idle()
	case agentapi.ActionStop:
		a.stopping = true
		a.idle()
	}
}

// checkDesired refuses a desired state the agent cannot act on.
func checkDesired(d agentapi.DesiredState) error {
	if d.Action != agentapi.ActionRun {
		return nil
	}
	if d.Instance == nil {
		return fmt.Errorf("it runs an Instance but carries none")
	}
	if len(d.Instance.Workers) == 0 {
		return fmt.Errorf("its Instance has no worker")
	}
	for _, w := range d.Instance.Workers {
		if w.EngineSocket != agentapi.ContainerEngineSocket(w.Name) {
			return fmt.Errorf("worker %q's engine socket %q is not %q", w.Name, w.EngineSocket, agentapi.ContainerEngineSocket(w.Name))
		}
	}
	if c := d.Instance.Checks; c != nil && c.WarmUp != nil {
		if n := c.WarmUp.TargetSessions; n < 1 || n > agentapi.MaxTargetSessions {
			return fmt.Errorf("warm-up targetSessions %d is outside 1..%d", n, agentapi.MaxTargetSessions)
		}
	}
	return nil
}

func (a *Agent) takeSettings(s agentapi.AgentSettings) {
	a.stopGrace = DefaultStopGrace
	if s.StopGrace > 0 {
		a.stopGrace = time.Duration(s.StopGrace)
	}
	a.probeInterval = DefaultProbeInterval
	if s.ProbeInterval > 0 {
		a.probeInterval = time.Duration(s.ProbeInterval)
	}
	a.probeEvery.Store(int64(a.probeInterval))
	a.imageFormat = agentapi.ImageFormatSIF
	if s.ImageFormat != "" {
		a.imageFormat = s.ImageFormat
	}
}

// idle stops every part and every task; the Instance stays as the one last
// run, for the reports (HBEP-7, After the Instance).
func (a *Agent) idle() {
	in := a.inst
	if in == nil {
		return
	}
	a.stopWorkers(in)
	a.cancelStaging(in)
	a.cancelUnit(in)
	a.finishWarmUp(in)
}

// applyRun converges on the desired Instance.
func (a *Agent) applyRun(i agentapi.DesiredInstance) {
	if a.inst != nil && a.inst.uid != i.UID {
		// A new Instance replaces the old one once the old one's processes are
		// gone; until then the reports keep showing the old one.
		a.idle()
		if a.inst.anyLive() {
			return
		}
		a.fwd.SetTarget("")
		a.inst = nil
	}
	if a.inst == nil {
		key, err := agentapi.NewKey()
		if err != nil {
			a.log.Error("cannot draw the Instance's key", "err", err)
			return
		}
		a.inst = &instance{uid: i.UID, launch: i.Launch, key: key, generation: i.Generation, firstGen: i.Generation, stageGen: i.Generation}
		a.log.Info("new Instance", "instance", i.Name, "uid", i.UID, "generation", i.Generation)
	}
	in := a.inst
	if !in.launchWarned && !reflect.DeepEqual(in.launch, i.Launch) {
		in.launchWarned = true
		a.log.Warn("the desired state carries another launch for the same Instance; the original launch keeps running", "uid", in.uid)
	}
	in.keyStored = i.KeyStored
	in.hubToken = i.HubToken
	if c := i.Checks; c != nil && c.Verify != nil && len(c.Verify.Files) > 0 && (!in.haveFiles || in.filesGen != i.Generation) {
		in.files = append([]agentapi.FileDigest(nil), c.Verify.Files...)
		in.filesGen = i.Generation
		in.haveFiles = true
	}

	// A raised Instance generation (rung 2): stop every part, then stage again,
	// which rehashes the staged files and re-stages those that fail, then
	// start.
	if i.Generation > in.stageGen {
		a.cancelUnit(in)
		for _, w := range in.workers {
			if w.instGen < i.Generation {
				a.terminate(w)
			}
		}
		if in.anyLive() {
			return
		}
		a.cancelStaging(in)
		in.stageGen = i.Generation
		in.staged = false
		in.staging = agentapi.Staging{}
	}
	if !in.staged {
		if !in.haveFiles || in.filesGen != in.stageGen {
			// A first start and a rung-2 restage need the identity record's
			// file list for this generation: without it the checkpoint cannot
			// be staged or rehashed, so no worker starts.
			in.staging = agentapi.Staging{Phase: agentapi.StagingInProgress, Message: msgFilesAwaited}
			if in.awaitWarned != in.stageGen+1 {
				in.awaitWarned = in.stageGen + 1
				a.log.Warn("not starting: the desired state for this Instance generation carries no checks.verify.files", "uid", in.uid, "generation", in.stageGen)
			}
			return
		}
		if !in.stageRunning && in.staging.Phase != agentapi.StagingFailed {
			a.startStaging(in)
		}
		a.runChecks(in, i)
		return
	}
	a.convergeWorkers(in, i)
	a.runChecks(in, i)
}

// convergeWorkers starts, restarts and stops workers to match the desired
// Instance. A restart stops the old process first and starts the new one
// once the old one is gone. A worker that exited or was stopped does not start
// again until a generation rises: the agent never restarts a part by itself.
func (a *Agent) convergeWorkers(in *instance, i agentapi.DesiredInstance) {
	for _, dw := range i.Workers {
		w := in.find(dw.Name)
		switch {
		case w == nil:
			in.workers = append(in.workers, a.startWorker(in, dw, i.Generation))
		case i.Generation > w.instGen || dw.Generation > w.generation:
			a.terminate(w)
			if !w.live() {
				*w = *a.startWorker(in, dw, i.Generation)
			}
		}
	}
	for _, w := range in.workers {
		if !desires(i.Workers, w.name) {
			a.terminate(w)
		}
	}
	started := true
	for _, dw := range i.Workers {
		if w := in.find(dw.Name); w == nil || w.instGen < i.Generation {
			started = false
		}
	}
	if started {
		in.generation = i.Generation
	}
}

func desires(ws []agentapi.DesiredWorker, name string) bool {
	for _, w := range ws {
		if w.Name == name {
			return true
		}
	}
	return false
}

// runsFor reports whether every desired worker runs a process started for its
// desired generation and the desired Instance generation: only then may checks
// run, so a result never comes from a process the desired state replaces.
func (in *instance) runsFor(i agentapi.DesiredInstance) bool {
	if len(i.Workers) == 0 {
		return false
	}
	for _, dw := range i.Workers {
		w := in.find(dw.Name)
		if w == nil || w.phase != agentapi.PartRunning || w.instGen < i.Generation || w.generation < dw.Generation {
			return false
		}
	}
	return true
}

// ---- staging ----

// msgFilesAwaited is the staging message while a start waits for the
// identity record's file list.
const msgFilesAwaited = "file list awaited: the start instruction for this Instance generation must carry checks.verify.files"

func (a *Agent) startStaging(in *instance) {
	ctx, cancel := context.WithCancel(a.ctx)
	in.stageCancel = cancel
	in.stageRunning = true
	in.staging = agentapi.Staging{Phase: agentapi.StagingInProgress}
	gen := in.stageGen
	restage := gen > in.firstGen // a whole-Instance restart: rung 2
	files := append([]agentapi.FileDigest(nil), in.files...)
	launch, token, format := in.launch, in.hubToken, a.imageFormat
	loadFormat := LoadFormat(launch.Command)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		imgRes, err := a.o.Stager.Image(ctx, launch.Image, launch.ImageDigest, format)
		var ck CheckpointResult
		if err == nil {
			ck, err = a.o.Stager.Checkpoint(ctx, CheckpointRequest{
				Repository: launch.Checkpoint.Repository,
				Revision:   launch.Checkpoint.Revision,
				Files:      files,
				HubToken:   token,
				Restage:    restage,
				LoadFormat: loadFormat,
				Progress: func(done, total int64) {
					a.post(func() {
						if a.inst == in && in.stageGen == gen && in.stageRunning {
							in.staging.BytesDone, in.staging.BytesTotal = done, total
						}
					})
				},
			})
		}
		a.post(func() { a.stagingDone(in, gen, imgRes, ck, files, err) })
	}()
}

func (a *Agent) stagingDone(in *instance, gen int64, img ImageResult, ck CheckpointResult, files []agentapi.FileDigest, err error) {
	if a.inst != in || in.stageGen != gen || !in.stageRunning {
		return
	}
	in.stageRunning = false
	in.stageCancel = nil
	if err != nil {
		if a.ctx.Err() != nil {
			return
		}
		a.log.Warn("staging failed", "uid", in.uid, "generation", gen, "err", err)
		in.staging = agentapi.Staging{Phase: agentapi.StagingFailed, Message: agentapi.TruncateUTF8(err.Error(), agentapi.MaxMessageBytes)}
		return
	}
	in.staging.Phase = agentapi.StagingDone
	in.staging.Message = ""
	in.staged = true
	in.imagePath = img.Path
	in.imageVerified = img.Verified
	in.ckptDir = ck.Dir
	in.fileResults = ck.Files
	in.filesKey = filesKey(files)
	in.fileResultsFresh = true
}

// LoadFormat returns the --load-format a launch's command passes, "" when it
// passes none (vLLM's default, auto). The controller renders every flag as one
// "--name=value" argument; the underscore spelling is read too.
func LoadFormat(command []string) string {
	for _, a := range command {
		for _, p := range []string{"--load-format=", "--load_format="} {
			if v, ok := strings.CutPrefix(a, p); ok {
				return v
			}
		}
	}
	return ""
}

func (a *Agent) cancelStaging(in *instance) {
	if in.stageCancel != nil {
		in.stageCancel()
		in.stageCancel = nil
	}
	if in.stageRunning {
		in.stageRunning = false
		in.staging = agentapi.Staging{}
	}
}

// filesKey identifies a file list, so a rehash is reused only for the list it
// was made for.
func filesKey(files []agentapi.FileDigest) [32]byte {
	b, _ := json.Marshal(files)
	return sha256.Sum256(b)
}
