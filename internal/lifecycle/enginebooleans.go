package lifecycle

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Engine boolean flags (HBEP-3, What admission checks; amended 2026-11-18,
// #788).
//
// vLLM's argparse refuses an explicit value on a flag that takes none, so a
// boolean flag must go out as a bare --name (or, for "false", as --no-name
// where the engine has that form), never as --name=value. Which names are
// booleans, and of which kind, is the engine image's own parser: a dump file
// per image digest, written by hack/vllm-flags/generate.sh from the parser
// inside that image (hack/vllm-flags/dump.py). The files are
// enginebooleans/*.json. They cannot be regenerated in CI, since that needs
// the image; the tests check them against hand pins, and the generator
// probes the image's parser itself.

// pinnedEngineImageDigest is the v0.32.0 image the framework is written
// against (docker.io/vllm/vllm-openai, HBEP-15). Its dump decides what is a
// boolean for an image that has no dump of its own.
const pinnedEngineImageDigest = "sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34"

// booleanDumpFormat is the dump files' format string.
const booleanDumpFormat = "hpcburst-engine-booleans/1"

// booleanKind is how an engine's argparse takes a boolean flag.
type booleanKind string

const (
	// kindOptionalBoolean is argparse's BooleanOptionalAction: --name and
	// --no-name.
	kindOptionalBoolean booleanKind = "BooleanOptionalAction"
	// kindStoreTrue is a plain store_true: --name only, default false.
	kindStoreTrue booleanKind = "store_true"
	// kindStoreFalse is a plain store_false: --name only, default true.
	kindStoreFalse booleanKind = "store_false"
)

// booleanRow is one boolean flag of a dump: its positive name, kind and the
// parser's default (nil when the parser has none).
type booleanRow struct {
	Name    string      `json:"name"`
	Kind    booleanKind `json:"kind"`
	Default *bool       `json:"default"`
}

// booleanDump is one engine image's parser dump.
type booleanDump struct {
	Format       string `json:"format"`
	Engine       string `json:"engine"`
	Version      string `json:"version"`
	Image        string `json:"image"`
	ImageDigest  string `json:"imageDigest"`
	SourceCommit string `json:"sourceCommit"`
	Provenance   string `json:"provenance"`
	Note         string `json:"note"`
	// ParserPlatform says which platform the parser was built on: the CPU one,
	// forced, on a node with no GPU, after a per-image check that the platform
	// cannot change the boolean set.
	ParserPlatform string       `json:"parserPlatform"`
	Python         string       `json:"python"`
	Generator      string       `json:"generator"`
	Flags          []booleanRow `json:"flags"`

	kinds map[string]booleanKind
	// sum is the SHA-256 of the file's bytes, in lowercase hex.
	sum string
}

// booleanDumps are the dump files by image digest.
type booleanDumps struct {
	byDigest map[string]*booleanDump
}

//go:embed enginebooleans/*.json
var embeddedBooleanDumps embed.FS

// engineBooleanDumps are the dumps this build ships. Malformed static data
// panics at start, as does a build without the pinned image's dump.
var engineBooleanDumps = mustLoadBooleanDumps(embeddedBooleanDumps, "enginebooleans")

var (
	dumpDigestRE = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	dumpNameRE   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
)

// expectedBooleanDumps pins the dump files this build ships: each image digest
// with the SHA-256, in lowercase hex, of its committed file's bytes (from
// `sha256sum internal/lifecycle/enginebooleans/vllm-<version>.json`). The build
// refuses to start on any drift: a file missing, one more, or one whose bytes
// changed. Without this a missing or edited 0.26.0 dump would leave that image
// on the pinned image's list, so a name only 0.26.0 has (calculate-kv-scales)
// would render as --name=value and vLLM would refuse the launch late.
var expectedBooleanDumps = map[string]string{
	pinnedEngineImageDigest: "b28cfc0bf7377849edceaf8b11f600c57af7318853d9113e2d712a6d9f67dda1", // vLLM 0.32.0
	"sha256:ffb2d59b1c059a5bd8d781320c9f5189de8293693b7d95da54befddaa54abf52": "a8156f8cdab52cc75434673475be1fd5ba7b2a9181e197d011ebc15016917ab2", // vLLM 0.26.0
	"sha256:7764931211e5b408a10d6e289ab9eca8d6ecd105e9239cb2d528c2c4d0ad67b0": "bcdb680f8bc7e8b1471df7519cf69b656d35aa241c971406abd83857bd088f1d", // vLLM 0.15.0
}

// mustLoadBooleanDumps loads the dumps under dir and panics on any problem or
// when they are not the pinned set (expectedBooleanDumps).
func mustLoadBooleanDumps(fsys fs.FS, dir string) *booleanDumps {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(fmt.Sprintf("engine boolean dumps: %v", err))
	}
	d, err := loadBooleanDumps(sub)
	if err != nil {
		panic(fmt.Sprintf("engine boolean dumps: %v", err))
	}
	if err := checkPinnedDumps(d, expectedBooleanDumps); err != nil {
		panic(fmt.Sprintf("engine boolean dumps: %v", err))
	}
	return d
}

// checkPinnedDumps compares the loaded dumps with the pinned set (image digest
// to the SHA-256 of the file's bytes). The error names every digest that is
// missing, every one that is not expected, and every one whose file differs.
func checkPinnedDumps(d *booleanDumps, expected map[string]string) error {
	var missing, extra, differs []string
	for digest, sum := range expected {
		switch f := d.byDigest[digest]; {
		case f == nil:
			missing = append(missing, digest)
		case f.sum != sum:
			differs = append(differs, digest)
		}
	}
	for digest := range d.byDigest {
		if _, ok := expected[digest]; !ok {
			extra = append(extra, digest)
		}
	}
	var parts []string
	for _, p := range []struct {
		what    string
		digests []string
	}{{"missing", missing}, {"not expected", extra}, {"file checksum differs for", differs}} {
		if len(p.digests) > 0 {
			sort.Strings(p.digests)
			parts = append(parts, p.what+" "+strings.Join(p.digests, ", "))
		}
	}
	if len(parts) > 0 {
		return fmt.Errorf("the dump files are not the pinned set: %s", strings.Join(parts, "; "))
	}
	return nil
}

// loadBooleanDumps reads every *.json file at the root of fsys as a dump. It
// refuses an unknown field, a bad format, an unsorted or repeated name, a kind
// or default the generator would not have written, and two files for one image
// digest.
func loadBooleanDumps(fsys fs.FS) (*booleanDumps, error) {
	names, err := fs.Glob(fsys, "*.json")
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no *.json file")
	}
	d := &booleanDumps{byDigest: map[string]*booleanDump{}}
	for _, file := range names {
		raw, err := fs.ReadFile(fsys, file)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		var f booleanDump
		if err := dec.Decode(&f); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		if err := f.validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		sum := sha256.Sum256(raw)
		f.sum = hex.EncodeToString(sum[:])
		if other := d.byDigest[f.ImageDigest]; other != nil {
			return nil, fmt.Errorf("%s: image digest %s is also the digest of vLLM %s's file", file, f.ImageDigest, other.Version)
		}
		d.byDigest[f.ImageDigest] = &f
	}
	return d, nil
}

func (f *booleanDump) validate() error {
	if f.Format != booleanDumpFormat {
		return fmt.Errorf("format %q, want %q", f.Format, booleanDumpFormat)
	}
	if f.Engine != "vllm" {
		return fmt.Errorf("engine %q, want vllm", f.Engine)
	}
	if f.Version == "" || f.Image == "" || f.Provenance == "" || f.ParserPlatform == "" || f.Generator == "" {
		return fmt.Errorf("version, image, provenance, parserPlatform and generator are all required")
	}
	if !dumpDigestRE.MatchString(f.ImageDigest) {
		return fmt.Errorf("imageDigest %q is not sha256:<64 hex>", f.ImageDigest)
	}
	if len(f.Flags) == 0 {
		return fmt.Errorf("no flags")
	}
	f.kinds = make(map[string]booleanKind, len(f.Flags))
	for i, r := range f.Flags {
		if !dumpNameRE.MatchString(r.Name) {
			return fmt.Errorf("flag %q is not a dashed lower-case name", r.Name)
		}
		if i > 0 && f.Flags[i-1].Name >= r.Name {
			return fmt.Errorf("flag %q is not after %q: names must be sorted and unique", r.Name, f.Flags[i-1].Name)
		}
		switch r.Kind {
		case kindOptionalBoolean:
		case kindStoreTrue:
			if r.Default == nil || *r.Default {
				return fmt.Errorf("store_true flag %q: default must be false (omitting it for \"false\" is exact only then)", r.Name)
			}
		case kindStoreFalse:
			if r.Default == nil || !*r.Default {
				return fmt.Errorf("store_false flag %q: default must be true", r.Name)
			}
		default:
			return fmt.Errorf("flag %q has kind %q", r.Name, r.Kind)
		}
		f.kinds[r.Name] = r.Kind
	}
	return nil
}

// booleanRules are the boolean rules in force for one engine image.
type booleanRules struct {
	kinds map[string]booleanKind
	// digest is the image digest the spec names.
	digest string
	// dump is true when a file records this image's own parser. When it is
	// false the pinned image's dump decides what is a boolean, and "false" is
	// refused: an older engine's default may be true.
	dump bool
}

// forImage selects the rules for an image digest: its own file when there is
// one, else the pinned image's.
func (d *booleanDumps) forImage(digest string) booleanRules {
	if f := d.byDigest[digest]; f != nil {
		return booleanRules{kinds: f.kinds, digest: digest, dump: true}
	}
	rules := booleanRules{digest: digest}
	if f := d.byDigest[pinnedEngineImageDigest]; f != nil {
		rules.kinds = f.kinds
	}
	return rules
}

// refuseBooleans checks the boolean flags of a serving spec against an
// engine image's rules (HBEP-3, What admission checks; amended 2026-11-18,
// #788). Admission already refuses a value other than "true" or "false" on
// seven of them (HBEP-3's table), so the controller never sees one there; it
// checks every boolean of the image's list all the same. It refuses:
//
//   - a value that is neither "true" nor "false";
//   - a boolean named more than once, whatever its kind and whatever the
//     spelling of the name (_ or -): an ordered "true" then "false" must not
//     leave the first in force, and a store_true has no way to say "false";
//   - "false" on any boolean when the image has no dump file of its own, since
//     it renders by kind and the pinned image's kind may not be that engine's.
//
// A flag the list does not classify as a boolean is not touched: it renders
// as --name=value, "false" included.
func refuseBooleans(flags []v1alpha1.EngineFlag, r booleanRules) error {
	seen := map[string]bool{}
	for _, f := range flags {
		name := flagName(f.Name)
		if _, ok := r.kinds[name]; !ok {
			continue
		}
		if f.Value != "true" && f.Value != "false" {
			return refuse("engine flag %s is a boolean flag of this engine image, so its value is \"true\" or \"false\", not %q (HBEP-3, What admission checks)", f.Name, f.Value)
		}
		if seen[name] {
			return refuse("engine flag %s is a boolean flag and is listed more than once: give it once, since a boolean cannot take two values (HBEP-3, What admission checks)", name)
		}
		seen[name] = true
		if f.Value == "false" && !r.dump {
			return refuse("engine flag %s is \"false\" on engine image %s, which has no parser dump in this build: how \"false\" renders depends on the engine's own parser, so only \"true\" is accepted there (HBEP-3, What admission checks)", f.Name, r.digest)
		}
	}
	return nil
}

// renderServedFlags renders a serving spec's flags, in order. A boolean of the
// rules is a bare --name, --no-name or nothing (renderBoolean); every other
// flag is one --name=value argument, so a value that starts with "-" is not
// read as a flag. max-num-seqs is left out: it is passed once, after them, as
// --max-num-seqs=<n> (the Launch's CEL rule).
func renderServedFlags(flags []v1alpha1.EngineFlag, r booleanRules) []string {
	var out []string
	for _, f := range flags {
		name := flagName(f.Name)
		if name == "max-num-seqs" {
			continue
		}
		if kind, ok := r.kinds[name]; ok {
			out = append(out, renderBoolean(name, f.Value, kind)...)
			continue
		}
		out = append(out, "--"+name+"="+f.Value)
	}
	return out
}

// renderBoolean renders a boolean flag's argument, or none: "true" is the bare
// --name, "false" is --no-name for an optional boolean and nothing for a
// store_true or a store_false, whose default is already the value "false" says.
// The value is "true" or "false": refuseBooleans has refused any other.
func renderBoolean(name, value string, kind booleanKind) []string {
	switch {
	case value == "true":
		return []string{"--" + name}
	case value == "false" && kind == kindOptionalBoolean:
		return []string{"--no-" + name}
	case value == "false":
		return nil
	}
	// Not reachable after refuseBooleans. The loud form, so an engine refuses
	// it at start, rather than a guess.
	return []string{"--" + name + "=" + value}
}
