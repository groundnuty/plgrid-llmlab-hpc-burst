package lifecycle

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The tests in this file need no rig. The boolean rules are HBEP-3, What
// admission checks (amended 2026-11-18, #788). The expected arguments are
// written out by hand from vLLM v0.32.0's source (commit 5df63a72), never read
// from the data file they check: the file and the renderer read one source, so
// agreement between them proves only that the renderer consults the file. The
// hand pins and the generator's in-image probe are what check the file.

// testPinnedDigest is the v0.32.0 image's digest, typed here from the brief
// and not read from the code under test.
const testPinnedDigest = "sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34"

// onThePinnedImage gives a spec the pinned v0.32.0 image's digest (the rig's
// own digest, rigDigest, has no dump file, so a "false" there is refused).
func onThePinnedImage(md *v1alpha1.ModelDeployment) *v1alpha1.ModelDeployment {
	md.Spec.Source.Served.Engine.ImageDigest = testPinnedDigest
	return md
}

// boolMD is a served deployment on an image, with the given flags.
func boolMD(digest string, flags ...v1alpha1.EngineFlag) *v1alpha1.ModelDeployment {
	md := specMD(flags...)
	md.Spec.Source.Served.Engine.ImageDigest = digest
	return md
}

// boolArgs checks a spec as the controller does and returns the arguments
// rendered for its flags: what lies between --served-model-name <name> and
// the closing --max-num-seqs=256.
func boolArgs(t *testing.T, md *v1alpha1.ModelDeployment) []string {
	t.Helper()
	cmd := specLaunch(t, md).Command
	head := specLaunchArgv()
	if len(cmd) < len(head)+1 || !slices.Equal(cmd[:len(head)], head) || cmd[len(cmd)-1] != "--max-num-seqs=256" {
		t.Fatalf("command %q is not the launch head, the flags, and --max-num-seqs=256", cmd)
	}
	return cmd[len(head) : len(cmd)-1]
}

// pinnedDump is the pinned image's dump as the controller loaded it.
func pinnedDump(t *testing.T) *booleanDump {
	t.Helper()
	d := engineBooleanDumps.byDigest[testPinnedDigest]
	if d == nil {
		t.Fatalf("no dump for the pinned image %s", testPinnedDigest)
	}
	return d
}

func mustBeUnsupported(t *testing.T, err error) *unsupported {
	t.Helper()
	var u *unsupported
	if !errors.As(err, &u) {
		t.Fatalf("error = %v (%T), want *unsupported", err, err)
	}
	return u
}

// TestEveryBooleanOfThePinnedDumpRendersAsItsKindSays: "true" is exactly one
// argument, --name, with no "="; "false" is --no-name for an optional boolean
// and no argument that holds the name for a store_true; an underscore spelling
// renders the same. This reads its population from the dump, so it shows the
// renderer consults the file and not that the file is right (the hand pins
// below do that).
func TestEveryBooleanOfThePinnedDumpRendersAsItsKindSays(t *testing.T) {
	d := pinnedDump(t)
	if len(d.Flags) < 50 {
		t.Fatalf("the pinned dump has %d rows; a v0.32.0 parser has about a hundred", len(d.Flags))
	}
	for _, row := range d.Flags {
		for _, spelling := range []string{row.Name, strings.ReplaceAll(row.Name, "-", "_")} {
			got := boolArgs(t, boolMD(testPinnedDigest, specFlag(spelling, "true")))
			if !slices.Equal(got, []string{"--" + row.Name}) || strings.Contains(got[0], "=") {
				t.Errorf("%s (%s) true renders %q, want exactly [--%s]", spelling, row.Kind, got, row.Name)
			}
			got = boolArgs(t, boolMD(testPinnedDigest, specFlag(spelling, "false")))
			switch row.Kind {
			case kindOptionalBoolean:
				if !slices.Equal(got, []string{"--no-" + row.Name}) {
					t.Errorf("%s (%s) false renders %q, want [--no-%s]", spelling, row.Kind, got, row.Name)
				}
			default:
				for _, a := range got {
					if strings.Contains(a, row.Name) {
						t.Errorf("%s (%s) false renders %q, want no argument that holds the name", spelling, row.Kind, got)
					}
				}
			}
		}
	}
}

// TestHandPinnedBooleansOfVLLM0320 pins the kind and the argv of six flags from
// the vLLM v0.32.0 source (commit 5df63a72), written here by hand; the dump
// must give each the kind the source gives it.
func TestHandPinnedBooleansOfVLLM0320(t *testing.T) {
	pins := []struct {
		name   string
		kind   booleanKind
		source string
		on     []string // for "true"
		off    []string // for "false"
	}{
		// A plain bool field of ParallelConfig (vllm/config/parallel.py:240),
		// registered by the generic path (vllm/engine/arg_utils.py:1334-1336,
		// which takes :378-380): argparse.BooleanOptionalAction.
		{"disable-custom-all-reduce", kindOptionalBoolean, "parallel.py:240, arg_utils.py:1334-1336",
			[]string{"--disable-custom-all-reduce"}, []string{"--no-disable-custom-all-reduce"}},
		// arg_utils.py:1748 and :1273: BooleanOptionalAction, unchanged from the
		// seven the controller listed before the dump.
		{"enable-chunked-prefill", kindOptionalBoolean, "arg_utils.py:1748",
			[]string{"--enable-chunked-prefill"}, []string{"--no-enable-chunked-prefill"}},
		{"enable-expert-parallel", kindOptionalBoolean, "arg_utils.py:1273",
			[]string{"--enable-expert-parallel"}, []string{"--no-enable-expert-parallel"}},
		// arg_utils.py:1880-1884: add_argument("--disable-log-stats",
		// action="store_true"): no --no- form, so "false" is no argument.
		{"disable-log-stats", kindStoreTrue, "arg_utils.py:1880-1884",
			[]string{"--disable-log-stats"}, nil},
		// vllm/entrypoints/launchers/cli_args.py:434-439: store_true.
		{"headless", kindStoreTrue, "launchers/cli_args.py:434-439",
			[]string{"--headless"}, nil},
		// arg_utils.py:3205-3207: an explicit BooleanOptionalAction.
		{"enable-log-requests", kindOptionalBoolean, "arg_utils.py:3205-3207",
			[]string{"--enable-log-requests"}, []string{"--no-enable-log-requests"}},
	}
	d := pinnedDump(t)
	for _, p := range pins {
		t.Run(p.name, func(t *testing.T) {
			if got := d.kinds[p.name]; got != p.kind {
				t.Errorf("the dump gives %s the kind %q, vLLM's source (%s) says %q", p.name, got, p.source, p.kind)
			}
			if got := boolArgs(t, boolMD(testPinnedDigest, specFlag(p.name, "true"))); !slices.Equal(got, p.on) {
				t.Errorf("true renders %q, want %q", got, p.on)
			}
			got := boolArgs(t, boolMD(testPinnedDigest, specFlag(p.name, "false")))
			if !slices.Equal(got, p.off) {
				t.Errorf("false renders %q, want %q", got, p.off)
			}
			for _, bad := range []string{"--" + p.name + "=false", "--" + p.name + "=true"} {
				if slices.Contains(got, bad) {
					t.Errorf("false renders %q: vLLM's argparse refuses a value on a flag that takes none", bad)
				}
			}
			if p.kind == kindStoreTrue {
				for _, a := range got {
					if strings.Contains(a, p.name) {
						t.Errorf("false renders %q, want nothing: a store_true has no --no- form", got)
					}
				}
			}
		})
	}
}

// TestAFlagTheDumpDoesNotClassifyKeepsItsValue: a name that is no boolean of
// the list renders as one --name=value argument, whatever the value, "false"
// and "true" included; so does a JSON value.
func TestAFlagTheDumpDoesNotClassifyKeepsItsValue(t *testing.T) {
	json := `{"method": "ngram", "num_speculative_tokens": 3}`
	for _, digest := range []string{testPinnedDigest, rigDigest} {
		got := boolArgs(t, boolMD(digest,
			specFlag("dtype", "true"),
			specFlag("kv-cache-dtype", "false"),
			specFlag("speculative-config", json),
			specFlag("seed", "-1")))
		want := []string{"--dtype=true", "--kv-cache-dtype=false", "--speculative-config=" + json, "--seed=-1"}
		if !slices.Equal(got, want) {
			t.Errorf("on %s: got %q, want %q", digest, got, want)
		}
	}
}

// TestARepeatedBooleanIsRefusedWhateverItsKind: a boolean named more than once
// in one spec is refused with SpecUnsupported, naming the flag, for a
// store_true and for an optional boolean, in every order and across the _ and
// - spellings (HBEP-3: so an ordered "true" then "false" cannot leave the first
// in force). A repeated non-boolean still renders every occurrence, in order.
func TestARepeatedBooleanIsRefusedWhateverItsKind(t *testing.T) {
	for _, name := range []string{"disable-log-stats", "enable-prefix-caching"} { // store_true, BooleanOptionalAction
		for _, vals := range [][2]string{{"true", "false"}, {"false", "true"}, {"true", "true"}, {"false", "false"}} {
			t.Run(name+" "+vals[0]+" then "+vals[1], func(t *testing.T) {
				specRefused(t, boolMD(testPinnedDigest, specFlag(name, vals[0]), specFlag(name, vals[1])), name, "more than once")
			})
		}
		t.Run(name+" in two spellings", func(t *testing.T) {
			specRefused(t, boolMD(testPinnedDigest, specFlag(strings.ReplaceAll(name, "-", "_"), "true"), specFlag(name, "true")), name)
		})
		t.Run(name+" with a flag between", func(t *testing.T) {
			specRefused(t, boolMD(testPinnedDigest, specFlag(name, "true"), specFlag("seed", "1"), specFlag(name, "false")), name)
		})
	}
	// An image with no dump classifies by the pinned list, so the refusal holds there ("true" twice).
	specRefused(t, boolMD(rigDigest, specFlag("disable-log-stats", "true"), specFlag("disable-log-stats", "true")), "disable-log-stats")

	got := boolArgs(t, boolMD(testPinnedDigest, specFlag("middleware", "a"), specFlag("seed", "1"), specFlag("middleware", "b")))
	if want := []string{"--middleware=a", "--seed=1", "--middleware=b"}; !slices.Equal(got, want) {
		t.Errorf("a repeated non-boolean renders %q, want %q (as before)", got, want)
	}
}

// TestABooleanValueThatIsNotTrueOrFalseIsRefused: "1", "True" and "" on a
// boolean of the image's list are refused, naming the flag. Admission refuses
// them first on the seven of HBEP-3's table; the controller checks every
// boolean of the list, and disable-custom-all-reduce is not of the seven.
func TestABooleanValueThatIsNotTrueOrFalseIsRefused(t *testing.T) {
	for _, name := range []string{"disable-custom-all-reduce", "disable-log-stats", "enable-prefix-caching"} {
		for _, v := range []string{"1", "True", "", "yes", "0", "TRUE", " true"} {
			t.Run(name+" "+v, func(t *testing.T) {
				_, err := checkServed(boolMD(testPinnedDigest, specFlag(name, v)))
				u := mustBeUnsupported(t, err)
				if !strings.Contains(u.Error(), name) || !strings.Contains(u.Error(), `"true"`) {
					t.Errorf("message %q does not name the flag %s and the accepted values", u.Error(), name)
				}
			})
		}
	}
	// "true" and "false" are accepted.
	for _, v := range []string{"true", "false"} {
		specCheck(t, boolMD(testPinnedDigest, specFlag("disable-custom-all-reduce", v)))
	}
}

// TestAnImageWithNoDumpRendersTrueBareAndRefusesFalse: for a digest that has no
// file, the pinned list decides what is a boolean, "true" is the bare flag, and
// "false" on any boolean is refused, naming the flag and the digest (an older
// engine's default may be true, so omitting a "false" could run True).
func TestAnImageWithNoDumpRendersTrueBareAndRefusesFalse(t *testing.T) {
	if engineBooleanDumps.byDigest[rigDigest] != nil {
		t.Fatalf("the rig's digest has a dump file: this test means an image with none")
	}
	for _, name := range []string{"disable-custom-all-reduce", "disable-log-stats", "enable-prefix-caching", "headless"} {
		got := boolArgs(t, boolMD(rigDigest, specFlag(name, "true")))
		if !slices.Equal(got, []string{"--" + name}) {
			t.Errorf("%s true on an image with no dump renders %q, want [--%s]", name, got, name)
		}
		specRefused(t, boolMD(rigDigest, specFlag(name, "false")), name, rigDigest)
	}
	// A name the pinned list does not classify keeps its value, "false" included.
	if got := boolArgs(t, boolMD(rigDigest, specFlag("kv-cache-dtype", "false"))); !slices.Equal(got, []string{"--kv-cache-dtype=false"}) {
		t.Errorf("kv-cache-dtype false renders %q, want [--kv-cache-dtype=false]", got)
	}
}

// TestADumpFileIsSelectedByItsImageDigest loads a testdata FS of two files and
// shows each digest gets its own list: the same name is an optional boolean in
// one and a store_true in the other, so "false" renders as --no-name or as
// nothing, and an image with no file of its own takes the pinned list and its
// refusal of "false".
func TestADumpFileIsSelectedByItsImageDigest(t *testing.T) {
	dumps, err := loadBooleanDumps(os.DirFS("testdata/enginebooleans"))
	if err != nil {
		t.Fatal(err)
	}
	second := "sha256:" + strings.Repeat("2", 64)
	flags := []v1alpha1.EngineFlag{specFlag("enable-prefix-caching", "false"), specFlag("special-flag", "true"), specFlag("headless", "true")}

	own := dumps.forImage(second)
	if !own.dump || own.kinds["enable-prefix-caching"] != kindStoreTrue || own.kinds["special-flag"] != kindOptionalBoolean {
		t.Fatalf("the second file's rules: %+v", own)
	}
	if err := refuseBooleans(flags[:2], own); err != nil {
		t.Errorf("a file's own digest refused a false: %v", err)
	}
	// store_true here: false is nothing; special-flag is a BooleanOptionalAction; headless is no boolean of this file.
	if got, want := renderServedFlags(flags, own), []string{"--special-flag", "--headless=true"}; !slices.Equal(got, want) {
		t.Errorf("rendered on the second image: %q, want %q", got, want)
	}

	pinned := dumps.forImage(testPinnedDigest)
	if !pinned.dump || pinned.kinds["enable-prefix-caching"] != kindOptionalBoolean {
		t.Fatalf("the pinned fixture's rules: %+v", pinned)
	}
	if got, want := renderServedFlags(flags[:1], pinned), []string{"--no-enable-prefix-caching"}; !slices.Equal(got, want) {
		t.Errorf("rendered on the pinned image: %q, want %q", got, want)
	}

	none := dumps.forImage(rigDigest)
	if none.dump || none.kinds["enable-prefix-caching"] != kindOptionalBoolean {
		t.Fatalf("an image with no file: %+v, want the pinned list and no dump", none)
	}
	err = refuseBooleans(flags[:1], none)
	if u := mustBeUnsupported(t, err); !strings.Contains(u.Error(), "enable-prefix-caching") || !strings.Contains(u.Error(), rigDigest) {
		t.Errorf("message %q does not name the flag and the digest", u.Error())
	}
}

// TestThePinnedDumpIsTheV0320ParserDump checks the shipped file's provenance and
// shape. The digest, version and commit are typed here from the brief.
func TestThePinnedDumpIsTheV0320ParserDump(t *testing.T) {
	d := pinnedDump(t)
	for _, c := range []struct{ field, got, want string }{
		{"format", d.Format, "hpcburst-engine-booleans/1"},
		{"engine", d.Engine, "vllm"},
		{"version", d.Version, "0.32.0"},
		{"image", d.Image, "docker.io/vllm/vllm-openai"},
		{"imageDigest", d.ImageDigest, testPinnedDigest},
		{"sourceCommit", d.SourceCommit, "5df63a72"},
		{"generator", d.Generator, "hack/vllm-flags/generate.sh"},
	} {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.field, c.got, c.want)
		}
	}
	if !strings.HasPrefix(d.Python, "3.") || d.Provenance == "" {
		t.Errorf("python %q, provenance %q", d.Python, d.Provenance)
	}
	if want := "cpu, forced on a GPU-less node; the boolean set does not depend on it (checked per image)"; d.ParserPlatform != want {
		t.Errorf("parserPlatform %q, want %q", d.ParserPlatform, want)
	}
	if strings.Contains(d.Provenance+d.Note+d.Generator, "/home") || strings.Contains(d.Provenance, "scratch") {
		t.Errorf("a path in the provenance fields: %q %q", d.Provenance, d.Note)
	}
	var storeTrue, optional int
	for i, r := range d.Flags {
		if i > 0 && d.Flags[i-1].Name >= r.Name {
			t.Errorf("names are not sorted and unique at %q", r.Name)
		}
		switch r.Kind {
		case kindStoreTrue:
			storeTrue++
			if r.Default == nil || *r.Default {
				t.Errorf("store_true %s has default %v, want false", r.Name, r.Default)
			}
		case kindOptionalBoolean:
			optional++
		default:
			// A store_false would make "true" mean something else than "pass the flag"
			// (the spec names the flag, not the dest): none exist at v0.32.0, and a
			// later pin that adds one is seen here and decided on.
			t.Errorf("%s has kind %q, want only store_true and BooleanOptionalAction at v0.32.0", r.Name, r.Kind)
		}
	}
	if storeTrue == 0 || optional == 0 {
		t.Errorf("%d store_true and %d BooleanOptionalAction rows: expected both kinds", storeTrue, optional)
	}
	// The seven booleans the controller listed before the dump, still optional booleans.
	for _, name := range []string{"trust-remote-code", "enforce-eager", "enable-expert-parallel", "enable-auto-tool-choice",
		"enable-prefix-caching", "enable-chunked-prefill", "enable-log-requests"} {
		if d.kinds[name] != kindOptionalBoolean {
			t.Errorf("%s has kind %q, want BooleanOptionalAction (it rendered --name / --no-name before the dump)", name, d.kinds[name])
		}
	}
}

// TestTheGateReadsCapacityBooleansBackExactly: gate.go's launchCapacityFlags
// reads a capacity boolean from the command as --name (true) or --no-name
// (false). A store_true capacity flag would drop its "false" from the command
// and the gate would miss it, so every boolean of the dump that HBEP-3's table
// classes as capacity must be an optional boolean; and a rendered launch reads
// back what the spec said.
func TestTheGateReadsCapacityBooleansBackExactly(t *testing.T) {
	d := pinnedDump(t)
	seen := 0
	for _, r := range d.Flags {
		if deploymentid.IsCapacityFlag(r.Name) {
			seen++
			if r.Kind != kindOptionalBoolean {
				t.Errorf("capacity boolean %s is a %s: the gate's read of a \"false\" would miss it", r.Name, r.Kind)
			}
		}
	}
	for _, name := range []string{"enable-prefix-caching", "enable-chunked-prefill"} { // HBEP-3's capacity booleans
		if !deploymentid.IsCapacityFlag(name) || d.kinds[name] != kindOptionalBoolean {
			t.Errorf("%s: capacity %v, kind %q", name, deploymentid.IsCapacityFlag(name), d.kinds[name])
		}
	}
	if seen < 2 {
		t.Errorf("%d capacity booleans in the dump, want at least the two HBEP-3 names", seen)
	}
	for _, c := range []struct{ prefix, chunked string }{{"true", "false"}, {"false", "true"}} {
		l := specLaunch(t, boolMD(testPinnedDigest, specFlag("enable-prefix-caching", c.prefix), specFlag("enable-chunked-prefill", c.chunked)))
		got := launchCapacityFlags(l)
		if string(got["enable-prefix-caching"]) != c.prefix || string(got["enable-chunked-prefill"]) != c.chunked {
			t.Errorf("the gate reads %v from %q, want prefix %s and chunked %s", got, l.Command, c.prefix, c.chunked)
		}
	}
}

// dumpJSON is a minimal valid dump with the given flag rows (JSON text) and digest.
func dumpJSON(digest, rows string) string {
	return `{"format": "hpcburst-engine-booleans/1", "engine": "vllm", "version": "1.2.3", "image": "docker.io/vllm/vllm-openai",
 "imageDigest": "` + digest + `", "provenance": "test", "parserPlatform": "cpu", "python": "3.12.0", "generator": "hack/vllm-flags/generate.sh", "flags": [` + rows + `]}`
}

// TestTheLoaderRefusesWhatTheGeneratorWouldNotHaveWritten: static data is
// strict, and a build without the pinned image's dump does not start.
func TestTheLoaderRefusesWhatTheGeneratorWouldNotHaveWritten(t *testing.T) {
	dig := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	ok := `{"name": "a-flag", "kind": "store_true", "default": false}`
	for _, tc := range []struct {
		name string
		fsys fstest.MapFS
		want string
	}{
		{"a store_true with default true", fstest.MapFS{"a.json": {Data: []byte(dumpJSON(dig("1"), `{"name": "a-flag", "kind": "store_true", "default": true}`))}}, "default must be false"},
		{"a store_true with no default", fstest.MapFS{"a.json": {Data: []byte(dumpJSON(dig("1"), `{"name": "a-flag", "kind": "store_true", "default": null}`))}}, "default must be false"},
		{"a store_false with default false", fstest.MapFS{"a.json": {Data: []byte(dumpJSON(dig("1"), `{"name": "a-flag", "kind": "store_false", "default": false}`))}}, "default must be true"},
		{"an unknown kind", fstest.MapFS{"a.json": {Data: []byte(dumpJSON(dig("1"), `{"name": "a-flag", "kind": "store_const", "default": null}`))}}, "kind"},
		{"unsorted names", fstest.MapFS{"a.json": {Data: []byte(dumpJSON(dig("1"), `{"name": "b-flag", "kind": "store_true", "default": false}, {"name": "a-flag", "kind": "store_true", "default": false}`))}}, "sorted"},
		{"a repeated name", fstest.MapFS{"a.json": {Data: []byte(dumpJSON(dig("1"), ok+", "+ok))}}, "sorted and unique"},
		{"an underscore name", fstest.MapFS{"a.json": {Data: []byte(dumpJSON(dig("1"), `{"name": "a_flag", "kind": "store_true", "default": false}`))}}, "dashed"},
		{"an unknown field", fstest.MapFS{"a.json": {Data: []byte(strings.Replace(dumpJSON(dig("1"), ok), `"engine"`, `"help": "x", "engine"`, 1))}}, "unknown field"},
		{"a bad digest", fstest.MapFS{"a.json": {Data: []byte(dumpJSON("sha256:abc", ok))}}, "sha256"},
		{"two files for one digest", fstest.MapFS{"a.json": {Data: []byte(dumpJSON(dig("1"), ok))}, "b.json": {Data: []byte(dumpJSON(dig("1"), ok))}}, "also the digest"},
		{"another format", fstest.MapFS{"a.json": {Data: []byte(strings.Replace(dumpJSON(dig("1"), ok), "booleans/1", "booleans/2", 1))}}, "format"},
		{"no files", fstest.MapFS{"a.txt": {Data: []byte("x")}}, "no *.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadBooleanDumps(tc.fsys)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want one that mentions %q", err, tc.want)
			}
		})
	}
	t.Run("a build without the pinned image's dump panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil || !strings.Contains(r.(string), "pinned") {
				t.Fatalf("recovered %v, want a panic about the pinned image", r)
			}
		}()
		mustLoadBooleanDumps(fstest.MapFS{"d/a.json": {Data: []byte(dumpJSON(dig("1"), ok))}}, "d")
	})
	t.Run("malformed static data panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("no panic")
			}
		}()
		mustLoadBooleanDumps(fstest.MapFS{"d/a.json": {Data: []byte("{")}}, "d")
	})
}

// shippedDumpDigests are the engine images whose dump files this build ships,
// by the vLLM version each holds, typed here from the brief (Docker Hub's index
// digests of the version tags). A new file must be listed here too.
var shippedDumpDigests = map[string]string{
	"0.32.0": testPinnedDigest,
	"0.26.0": "sha256:ffb2d59b1c059a5bd8d781320c9f5189de8293693b7d95da54befddaa54abf52",
}

// TestEveryShippedDumpIsItsImageAndSaysHowItWasMade: each file holds the digest
// and version the list above says, a file other than the pinned image's says
// that its digest is Docker Hub's and not confirmed as the site's pin, and none
// is shipped without a line here.
func TestEveryShippedDumpIsItsImageAndSaysHowItWasMade(t *testing.T) {
	if len(engineBooleanDumps.byDigest) != len(shippedDumpDigests) {
		t.Errorf("%d dump files are shipped, %d are listed in this test", len(engineBooleanDumps.byDigest), len(shippedDumpDigests))
	}
	for version, digest := range shippedDumpDigests {
		d := engineBooleanDumps.byDigest[digest]
		if d == nil {
			t.Errorf("no dump file for vLLM %s (%s)", version, digest)
			continue
		}
		if d.Version != version || d.ImageDigest != digest || d.Image != "docker.io/vllm/vllm-openai" {
			t.Errorf("the file for %s holds version %q, digest %q, image %q", version, d.Version, d.ImageDigest, d.Image)
		}
		if want := "cpu, forced on a GPU-less node; the boolean set does not depend on it (checked per image)"; d.ParserPlatform != want {
			t.Errorf("vLLM %s: parserPlatform %q, want %q", version, d.ParserPlatform, want)
		}
		if digest == testPinnedDigest {
			continue
		}
		if want := "Docker Hub's v" + version + " index digest, not confirmed as Cyfronet's pin"; d.Note != want {
			t.Errorf("vLLM %s: note %q, want %q", version, d.Note, want)
		}
		if want := "apptainer pull by digest; file sha256 checked against the puller's record"; d.Provenance != want {
			t.Errorf("vLLM %s: provenance %q, want %q", version, d.Provenance, want)
		}
		if d.SourceCommit != "" {
			t.Errorf("vLLM %s: sourceCommit %q, but no source checkout of this version was used", version, d.SourceCommit)
		}
	}
}

// TestTheV0260DumpIsSelectedByItsDigest: the 0.26.0 image's own file decides
// for a spec on that image. The kinds of the flags a DeepSeek deployment sets
// are those the generator's per-flag probe saw in that image on 18 Nov 2026
// (--x and --no-x parse, --x=true is refused), typed here by hand. A name only
// that version has (calculate-kv-scales, gone from v0.32.0) is a boolean there
// and no boolean on the pinned image's list, and "false" renders by that file.
func TestTheV0260DumpIsSelectedByItsDigest(t *testing.T) {
	const v0260 = "sha256:ffb2d59b1c059a5bd8d781320c9f5189de8293693b7d95da54befddaa54abf52"
	d := engineBooleanDumps.byDigest[v0260]
	if d == nil {
		t.Fatal("no dump for the 0.26.0 image")
	}
	for _, name := range []string{"trust-remote-code", "enable-prefix-caching", "enable-chunked-prefill", "enable-auto-tool-choice", "disable-custom-all-reduce"} {
		if d.kinds[name] != kindOptionalBoolean {
			t.Errorf("0.26.0: %s has kind %q, want BooleanOptionalAction", name, d.kinds[name])
		}
	}
	for _, name := range []string{"disable-log-stats", "headless"} {
		if d.kinds[name] != kindStoreTrue {
			t.Errorf("0.26.0: %s has kind %q, want store_true", name, d.kinds[name])
		}
	}
	got := boolArgs(t, boolMD(v0260, specFlag("disable-custom-all-reduce", "true"), specFlag("enable-chunked-prefill", "false"), specFlag("disable-log-stats", "false"), specFlag("calculate-kv-scales", "true")))
	if want := []string{"--disable-custom-all-reduce", "--no-enable-chunked-prefill", "--calculate-kv-scales"}; !slices.Equal(got, want) {
		t.Errorf("on the 0.26.0 image: %q, want %q", got, want)
	}
	if got := boolArgs(t, boolMD(testPinnedDigest, specFlag("calculate-kv-scales", "true"))); !slices.Equal(got, []string{"--calculate-kv-scales=true"}) {
		t.Errorf("on the pinned image: %q, want the unclassified form --calculate-kv-scales=true", got)
	}
}
