package deploymentid

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
)

const (
	// Scheme is the scheme version written into every canonical object.
	Scheme = "v1"
	// NameLength is the number of hex characters in an ID's display name.
	NameLength = 12

	kindServed  = "served"
	kindManaged = "managed"
)

// Checkpoint names the model weights: a repository and its full commit id.
type Checkpoint struct {
	Repository string
	Revision   string
}

// Engine names the serving engine. ImageDigest is "sha256:" and 64 lowercase
// hex characters.
type Engine struct {
	Name        string
	Version     string
	ImageDigest string
}

// Flag is one engine flag as the serving spec gives it: the long name without
// its leading dashes, and its value. A flag given more than once is several
// Flag entries; their order is meaningful. A boolean flag is given under its
// positive name with "true" or "false".
type Flag struct {
	Name  string
	Value string
}

// EnvVar is one environment variable of the engine process.
type EnvVar struct {
	Name  string
	Value string
}

// ServingSpec is the declared serving spec of a served deployment. Flags and
// Env list every setting the user declared, whether or not scheme v1 counts it
// as an input; the classification table (table.go) decides. Nothing else of an
// Instance can reach the ID: the job ID, node, URL, lease end and every key
// have no field here.
type ServingSpec struct {
	Checkpoint Checkpoint
	Engine     Engine
	Flags      []Flag
	Env        []EnvVar
	// ChatTemplateSHA256 is the SHA-256 in lowercase hex of the chat
	// template's UTF-8 text. ResolveChatTemplate and ChatTemplateSHA256 give
	// it; the text is the empty string when there is no template.
	ChatTemplateSHA256 string
}

// Accelerator is the accelerator class as the PlatformProfile names it for the
// Allocation's partition. A partition without GPUs names GPUModel "none".
type Accelerator struct {
	GPUModel string
	CPUArch  string
}

// File is one file of the pinned checkpoint revision: its path relative to the
// checkpoint root, with '/' separators, and its SHA-256 in lowercase hex.
type File struct {
	Path   string
	SHA256 string
}

// CatalogEntry is a managed provider's catalog entry. Revision is empty when
// the platform reports none.
type CatalogEntry struct {
	Platform    string
	CatalogName string
	Revision    string
}

// ID is a computed deployment ID.
type ID struct {
	// Canonical is the RFC 8785 serialisation of the v1 object.
	Canonical []byte
	// SHA256 is the SHA-256 of Canonical, in lowercase hex (64 characters).
	SHA256 string
	// Name is the display form: the first NameLength characters of SHA256.
	Name string
}

// Compute returns the scheme v1 ID of a served deployment. files is the
// checkpoint's per-file list in any order; Compute sorts it bytewise by path.
// The order of spec.Flags matters only among entries of the same name, and the
// order of spec.Env not at all.
func Compute(spec ServingSpec, accel Accelerator, files []File) (ID, error) {
	t, err := validateServing(spec, accel)
	if err != nil {
		return ID{}, err
	}
	flags, err := servedFlags(t, spec.Flags)
	if err != nil {
		return ID{}, err
	}
	env, err := servedEnv(t, spec.Env)
	if err != nil {
		return ID{}, err
	}
	fileList, err := sortedFiles(files)
	if err != nil {
		return ID{}, err
	}
	return finish(jobject{
		{"scheme", jstring(Scheme)},
		{"kind", jstring(kindServed)},
		{"checkpoint", jobject{
			{"repository", jstring(spec.Checkpoint.Repository)},
			{"revision", jstring(spec.Checkpoint.Revision)},
		}},
		{"engine", jobject{
			{"name", jstring(spec.Engine.Name)},
			{"version", jstring(spec.Engine.Version)},
			{"image_digest", jstring(spec.Engine.ImageDigest)},
		}},
		{"flags", flags},
		{"env", env},
		{"chat_template_sha256", jstring(spec.ChatTemplateSHA256)},
		{"accelerator", jobject{
			{"gpu_model", jstring(accel.GPUModel)},
			{"cpu_arch", jstring(accel.CPUArch)},
		}},
		{"files", fileList},
	})
}

// ComputeManaged returns the scheme v1 ID of a managed catalog entry: the
// platform, the catalog name, the revision when the platform reports one, and
// the marker that everything else is opaque.
func ComputeManaged(e CatalogEntry) (ID, error) {
	if err := requireText("platform", e.Platform); err != nil {
		return ID{}, err
	}
	if err := requireText("catalog name", e.CatalogName); err != nil {
		return ID{}, err
	}
	obj := jobject{
		{"scheme", jstring(Scheme)},
		{"kind", jstring(kindManaged)},
		{"platform", jstring(e.Platform)},
		{"catalog_name", jstring(e.CatalogName)},
		{"opaque", jstring("true")},
	}
	if e.Revision != "" {
		obj = append(obj, jmember{"revision", jstring(e.Revision)})
	}
	return finish(obj)
}

// finish serialises the object and hashes it.
func finish(obj jobject) (ID, error) {
	canonical, err := canonicalize(obj)
	if err != nil {
		return ID{}, invalidf("%v", err)
	}
	sum := sha256.Sum256(canonical)
	full := hex.EncodeToString(sum[:])
	return ID{Canonical: canonical, SHA256: full, Name: full[:NameLength]}, nil
}

// validateServing checks the scalar fields and returns the engine's table.
func validateServing(spec ServingSpec, accel Accelerator) (*table, error) {
	t, ok := tables[spec.Engine.Name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedEngine, spec.Engine.Name)
	}
	checks := []error{
		requireText("checkpoint repository", spec.Checkpoint.Repository),
		requireCommit("checkpoint revision", spec.Checkpoint.Revision),
		requireText("engine version", spec.Engine.Version),
		requireSHA256("chat template sha256", spec.ChatTemplateSHA256),
		requireText("accelerator gpu_model", accel.GPUModel),
		requireText("accelerator cpu_arch", accel.CPUArch),
	}
	for _, err := range checks {
		if err != nil {
			return nil, err
		}
	}
	digest, hasPrefix := strings.CutPrefix(spec.Engine.ImageDigest, "sha256:")
	if !hasPrefix || requireSHA256("engine image digest", digest) != nil {
		return nil, invalidf("engine image digest must be sha256: and 64 lowercase hex characters")
	}
	return t, nil
}

// servedFlags builds the "flags" object. Only input flags are written. A flag
// given more than once maps to an array of its values in the order given; a
// flag given once maps to its value.
func servedFlags(t *table, flags []Flag) (jobject, error) {
	var names []string
	values := map[string][]string{}
	for _, f := range flags {
		if err := checkName(f.Name, false); err != nil {
			return nil, err
		}
		c, reason := t.classify(f.Name, false)
		if err := refuse(c, "flag", f.Name, reason); err != nil {
			return nil, err
		}
		if c != classInput {
			continue
		}
		if _, seen := values[f.Name]; !seen {
			names = append(names, f.Name)
		}
		values[f.Name] = append(values[f.Name], f.Value)
	}
	out := make(jobject, 0, len(names))
	for _, n := range names {
		vs := values[n]
		if len(vs) == 1 {
			out = append(out, jmember{n, jstring(vs[0])})
			continue
		}
		arr := make(jarray, len(vs))
		for i, v := range vs {
			arr[i] = jstring(v)
		}
		out = append(out, jmember{n, arr})
	}
	return out, nil
}

// servedEnv builds the "env" object from the input variables. A variable named
// twice is ambiguous and is refused, whatever its class.
func servedEnv(t *table, env []EnvVar) (jobject, error) {
	seen := map[string]bool{}
	out := jobject{}
	for _, e := range env {
		if err := checkName(e.Name, true); err != nil {
			return nil, err
		}
		if seen[e.Name] {
			return nil, invalidf("environment variable %q is listed twice", e.Name)
		}
		seen[e.Name] = true
		c, reason := t.classify(e.Name, true)
		if err := refuse(c, "environment variable", e.Name, reason); err != nil {
			return nil, err
		}
		if c == classInput {
			out = append(out, jmember{e.Name, jstring(e.Value)})
		}
	}
	return out, nil
}

// refuse returns the error for a class the library will not hash. The value
// of a refused setting is never put in the message.
func refuse(c class, what, name, reason string) error {
	switch c {
	case classNever:
		return fmt.Errorf("%w: %s %q is a key; reference it instead of writing it in the spec", ErrSecret, what, name)
	case classFrameworkPassed:
		return fmt.Errorf("%w: %s %q (%s); remove it from the spec", ErrFrameworkPassed, what, name, reason)
	}
	return nil
}

// sortedFiles checks the file list and returns it as an array sorted bytewise
// by path (HBEP-3). That is the array-order rule of the scheme, not JCS's key
// order: JCS never reorders an array.
func sortedFiles(files []File) (jarray, error) {
	if len(files) == 0 {
		return nil, invalidf("the file list is empty; a served deployment needs its checkpoint's per-file list")
	}
	sorted := slices.Clone(files)
	for _, f := range sorted {
		if err := checkPath(f.Path); err != nil {
			return nil, err
		}
		if err := requireSHA256(fmt.Sprintf("sha256 of %q", f.Path), f.SHA256); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(sorted, func(a, b File) int { return strings.Compare(a.Path, b.Path) })
	out := make(jarray, len(sorted))
	for i, f := range sorted {
		if i > 0 && sorted[i-1].Path == f.Path {
			return nil, invalidf("file path %q is listed twice", f.Path)
		}
		out[i] = jobject{{"path", jstring(f.Path)}, {"sha256", jstring(f.SHA256)}}
	}
	return out, nil
}
