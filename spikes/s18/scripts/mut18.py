"""Spike S18's validity mutants (#528): each mutant of the harness's validity rules is applied to a detached worktree
of the spike, its tests are run against a green baseline built from the same tree, and it must fail them. The worktree
is restored after each mutant, and the run stops if it is not.

  S18_WT=<a detached worktree of the spike's branch> S18_OUT=<a scratch directory> python3 -I scripts/mut18.py [V1 ...]

S18_ENV, when set, is a shell prefix run before each command (for example, sourcing a Go environment). Each test binary
runs under `systemd-run --user --scope` with a memory cap when S18_CAP is set (the shared development VM's rule)."""
import os, subprocess, sys, re
WT = os.environ["S18_WT"]
MOD = WT + "/spikes/s18"
OUT = os.environ["S18_OUT"]
LOG = os.path.join(OUT, os.environ.get("MUTLOG", "mut18.log"))
ENV = os.environ.get("S18_ENV", "true") + f" && export TMPDIR={OUT}"
CAP = "nice -n 19 systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=0 " if os.environ.get("S18_CAP") else ""
R = "rig/"
MUT = [
 ("V1 the F1 fault aimed at every Instance, not the one named", "run", "^TestF1UnderAlpha$",
  [(R + "rig.go", 'm += fmt.Sprintf(",die-on=%s%d", MarkF1, i)', 'm += ",die-on=" + MarkF1')]),
 ("V2 a request ID reused", "run", "^TestNoFaultEveryRequestIsBegunOnceUnderAlpha$",
  [(R + "client.go", 'return fmt.Sprintf("s18id-%s-%d", c.Run, c.seq.Add(1))', 'return fmt.Sprintf("s18id-%s-%d", c.Run, c.seq.Add(0))')]),
 ("V3 the layer at budget 0 retrying", "run", "^TestTheLayerAtBudget0EqualsAlphaRequestForRequest$",
  [(R + "layer.go", 'var Budget0 = Policy{Name: "budget0"}', 'var Budget0 = Policy{Name: "budget0", MaxRetries: 1}')]),
 ("V4 deaths counted from the layer's closed attempts", "rig", "^TestAnalyzeCountsFromTheRecords$",
  [(R + "analyze.go", "\ts.Deaths = len(deaths)\n", "\tfor _, a := range attempts {\n\t\tif a.Kind == \"closed\" {\n\t\t\ts.Deaths++\n\t\t}\n\t}\n")]),
 ("V5 an F4 trial not waiting for the front's pause", "run", "^TestEveryPoisonTrialFires$",
  [(R + "rig.go", " || r.Front.Router.Paused(r.Front.Instances[i].Key)", "")]),
 ("V6 the cutter cutting every marked write, not once per ID", "run", "^TestEachArmOnEachFault$",
  [(R + "transports.go", "\tif id == \"\" || c.done[id] >= max(1, c.Times) {\n\t\treturn false\n\t}\n", "\tif id == \"\" {\n\t\treturn false\n\t}\n")]),
 ("V7 a false death judged by a time window", "rig", "^TestAnalyzeCountsFromTheRecords$",
  [(R + "analyze.go", "\t\t\tif !diedUnder(begins[a.ID], a.Target) {\n", "\t\t\tif !diedNear(deaths, a.Target, a.At) {\n"),
   (R + "analyze.go", "// diedUnder reports", "func diedNear(ds []Exit, i int, at time.Time) bool {\n\tfor _, d := range ds {\n\t\tif d.Instance == i && d.At.Sub(at) < 5*time.Second && d.At.Sub(at) > -time.Second {\n\t\t\treturn true\n\t\t}\n\t}\n\treturn false\n}\n\n// diedUnder reports")]),
 ("V8 the layer also reporting home's outcome (the bias run 1 had)", "run", "^TestTheLayerAtBudget0PausesAsAlphaDoes$",
  [(R + "layer.go", "\t\treport := func(failed bool) {\n\t\t\tif target != home {\n", "\t\treport := func(failed bool) {\n\t\t\tif true {\n")]),
]
def sh(c):
    p = subprocess.run(["bash", "-c", f"{ENV} && cd {MOD} && {c}"], capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr
def build(pkg, out):
    return sh(f"go test -c -o {out} ./{pkg}")
def run(binary, pkg, regex):
    rc, out = sh(f"cd {pkg} && {CAP}timeout 15m {binary} -test.count=1 -test.v -test.run '{regex}'")
    lines = [l.strip() for l in out.splitlines() if l.strip().startswith("--- ")]
    fails = [l.strip() for l in out.splitlines() if re.search(r"_test\.go:\d+:", l)][:3]
    return rc, lines, fails
log = open(LOG, "w")
bases = {}
for pkg in ("run", "rig"):
    b = os.path.join(OUT, f"mut18-base-{pkg}.test")
    rc, out = build(pkg, b)
    if rc != 0:
        print("BASELINE BUILD FAILED", pkg, out[-400:], file=log); sys.exit(1)
    bases[pkg] = b
for name, pkg, regex, edits in [m for m in MUT if len(sys.argv) < 2 or any(m[0].startswith(a + ' ') for a in sys.argv[1:])]:
    print(f"== {name}", file=log, flush=True)
    brc, bl, _ = run(bases[pkg], pkg, regex)
    if brc != 0 or any("FAIL" in l for l in bl) or not bl:
        print(f"  BASELINE not green: exit={brc} {bl}", file=log, flush=True); continue
    originals, ok = {}, True
    for path, old, new in edits:
        fp = os.path.join(MOD, path)
        originals.setdefault(path, open(fp).read())
        cur = open(fp).read()
        if cur.count(old) != 1:
            print(f"  ANCHOR in {path} found {cur.count(old)} times", file=log, flush=True); ok = False; break
        open(fp, "w").write(cur.replace(old, new))
    if ok:
        mb = os.path.join(OUT, "mut18-m.test")
        rc, out = build(pkg, mb)
        if rc != 0:
            print("  MUTANT BUILD FAILED", out[-300:], file=log, flush=True)
        else:
            mrc, ml, mf = run(mb, pkg, regex)
            print(f"  mutant exit={mrc} {[l for l in ml if 'FAIL' in l][:3]}", file=log)
            print("  KILLED" if mrc != 0 else "  SURVIVED", file=log)
            for f in mf:
                print(f"  failing line: {f}", file=log)
            log.flush()
    for path, s in originals.items():
        open(os.path.join(MOD, path), "w").write(s)
    rc, out = sh("git status --porcelain")
    if out.strip():
        print(f"  NOT RESTORED: {out}", file=log, flush=True); sys.exit(1)
