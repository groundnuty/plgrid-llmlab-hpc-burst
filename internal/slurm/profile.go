package slurm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"syscall"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Defaults of the profile fields the plugin reads, the schema defaults
// (HBEP-4, Platform profile schema), for a profile read without them.
const (
	DefaultLostGrace    = 2 * time.Minute
	DefaultUnknownBound = 10 * time.Minute
)

// ProfileReader implements provider.ProfileReader for Slurm Providers.
type ProfileReader struct {
	// Reader reads Providers and PlatformProfiles.
	Reader client.Reader
	// UID is the controller's user, which must own the credentials files; nil
	// means os.Geteuid(). It is a pointer because 0 is a user too (root): an
	// integer left unset would silently demand files owned by root.
	UID *int
	// Home expands "~/" in the credentials file; "" means os.UserHomeDir().
	Home string
}

var _ provider.ProfileReader = (*ProfileReader)(nil)

// Validated is a Provider's validated profile, settings and ssh credentials.
type Validated struct {
	Profile  provider.Profile
	Settings provider.Settings
	SSH      SSHConfig
}

// ReadProfile implements provider.ProfileReader.
func (r *ProfileReader) ReadProfile(ctx context.Context, name string) (provider.Profile, provider.Settings, error) {
	v, err := r.Read(ctx, name)
	return v.Profile, v.Settings, err
}

// sampleTags are two job tags of the form v1alpha1.JobTag gives, the longest
// and a short one, that a jobName.pattern must accept.
var sampleTags = []string{
	v1alpha1.JobTag("example-namespace", "a234567890b234567890c234567890d234567890e2345", "00000000-0000-0000-0000-000000000000"),
	v1alpha1.JobTag("ns", "a", "uid"),
}

// Read reads the named Provider, its PlatformProfile and its credentials file,
// and validates them (HBEP-4, Core operations, ReadProfile). It fails with an
// *provider.InvalidProfileError that names every invalid field, or with an
// error that wraps provider.ErrCredentials for a credentials or key file that
// others can read or another user owns. Read-only.
func (r *ProfileReader) Read(ctx context.Context, name string) (Validated, error) {
	var prov v1alpha1.Provider
	if err := r.Reader.Get(ctx, client.ObjectKey{Name: name}, &prov); err != nil {
		return Validated{}, fmt.Errorf("get Provider %s: %w", name, err)
	}
	var fields []provider.FieldError
	bad := func(field, format string, a ...any) {
		fields = append(fields, provider.FieldError{Field: field, Message: fmt.Sprintf(format, a...)})
	}
	var pp v1alpha1.PlatformProfile
	profileFound := false
	if prov.Spec.PlatformProfileRef.Name == "" {
		bad("provider.spec.platformProfileRef.name", "is empty")
	} else if err := r.Reader.Get(ctx, client.ObjectKey{Name: prov.Spec.PlatformProfileRef.Name}, &pp); err != nil {
		if !apierrors.IsNotFound(err) {
			return Validated{}, fmt.Errorf("get PlatformProfile %s: %w", prov.Spec.PlatformProfileRef.Name, err)
		}
		bad("provider.spec.platformProfileRef.name", "PlatformProfile %s does not exist", prov.Spec.PlatformProfileRef.Name)
	} else {
		profileFound = true
	}

	v := Validated{Settings: provider.Settings{
		Provider: prov.Name, Account: prov.Spec.Account, Grant: prov.Spec.Grant, Labels: prov.Labels,
	}}
	if ref := prov.Spec.CredentialsRef; ref != nil {
		v.Settings.CredentialsFile = ref.File
	}
	if len(prov.Spec.AllowedPartitions) != 1 {
		bad("provider.spec.allowedPartitions", "must name exactly one partition in alpha (HBEP-4, open question 12); it names %d", len(prov.Spec.AllowedPartitions))
	} else {
		v.Settings.Partition = prov.Spec.AllowedPartitions[0]
	}
	if prov.Spec.Account != "" {
		if err := CheckValue("account", prov.Spec.Account); err != nil {
			bad("provider.spec.account", "%v", err)
		}
	}

	if profileFound {
		v.Profile = provider.Profile{Name: pp.Name, Spec: pp.Spec}
		s := pp.Spec
		if s.ProviderKind != v1alpha1.PlatformBatch {
			bad("spec.providerKind", "is %q; the Slurm plugin serves batch platforms", s.ProviderKind)
		}
		if v.Settings.Partition != "" {
			found := false
			for i, p := range s.Partitions {
				if p.Name == v.Settings.Partition {
					found = true
					v.Profile.Partition = p
					checkPartition(fmt.Sprintf("spec.partitions[%d]", i), p, bad)
				}
			}
			if !found {
				bad("provider.spec.allowedPartitions", "partition %q is not listed in the profile", v.Settings.Partition)
			}
		}
		checkProfile(s, bad)
	}

	ssh, credErr := r.readCredentials(prov, bad)
	v.SSH = ssh
	if len(fields) > 0 {
		return v, &provider.InvalidProfileError{Provider: name, Fields: fields}
	}
	if credErr != nil {
		return v, credErr
	}
	return v, nil
}

func checkPartition(field string, p v1alpha1.PartitionProfile, bad func(string, string, ...any)) {
	if err := CheckValue("partition name", p.Name); err != nil {
		bad(field+".name", "%v", err)
	}
	if p.GPUModel == "" {
		bad(field+".gpuModel", "is empty; a CPU partition names none")
	}
	if p.MaxTime != nil {
		if _, err := p.MaxTime.AsDuration(); err != nil {
			bad(field+".maxTime", "is not a duration")
		}
	}
	if p.GPUModel != "" && p.GPUModel != "none" && p.GPUsPerNode == nil {
		bad(field+".gpusPerNode", "is required on a GPU partition")
	}
	if err := checkTmpDir(p); err != nil {
		bad(field+".tmpDir", "%v", err)
	}
}

func checkProfile(s v1alpha1.PlatformProfileSpec, bad func(string, string, ...any)) {
	if s.JobName != nil && s.JobName.MaxLength != nil && *s.JobName.MaxLength < v1alpha1.JobTagLength {
		bad("spec.jobName.maxLength", "is %d; the job tag needs %d characters", *s.JobName.MaxLength, v1alpha1.JobTagLength)
	}
	if s.JobName != nil && s.JobName.Pattern != "" {
		re, err := regexp.Compile(s.JobName.Pattern)
		if err != nil {
			bad("spec.jobName.pattern", "is not an RE2 pattern")
		} else {
			for _, tag := range sampleTags {
				if !re.MatchString(tag) {
					bad("spec.jobName.pattern", "does not accept the job tag form hb-<name>-<12 hex>, for example %q", tag)
					break
				}
			}
		}
	}
	if !knownDataParser(s.JSONDataParser) {
		bad("spec.jsonDataParser", "is %q; the plugin reads %v", s.JSONDataParser, KnownDataParsers)
	}
	if s.InflightWindow == nil {
		bad("spec.inflightWindow", "is required for a batch platform")
	} else if d, err := s.InflightWindow.AsDuration(); err != nil {
		bad("spec.inflightWindow", "is not a duration")
	} else if d <= provider.SubmitTimeout {
		bad("spec.inflightWindow", "is %s; it must exceed the sbatch time-out %s", d, provider.SubmitTimeout)
	}
	for field, d := range map[string]*v1alpha1.Duration{"spec.lostGrace": s.LostGrace, "spec.unknownBound": s.UnknownBound} {
		if d == nil {
			continue
		}
		if v, err := d.AsDuration(); err != nil || v <= 0 {
			bad(field, "is not a positive duration")
		}
	}
	if s.StagingDir == "" {
		bad("spec.stagingDir", "is required for a batch platform: the agent and the Allocation's files are staged there")
	}
}

// readCredentials checks and parses the credentials file and the key file it
// names. Field problems go to bad; ownership and mode problems are returned as
// an error that wraps provider.ErrCredentials.
func (r *ProfileReader) readCredentials(prov v1alpha1.Provider, bad func(string, string, ...any)) (SSHConfig, error) {
	ref := prov.Spec.CredentialsRef
	switch {
	case ref == nil:
		bad("provider.spec.credentialsRef", "is required for a Slurm Provider")
		return SSHConfig{}, nil
	case ref.SecretRef != nil:
		bad("provider.spec.credentialsRef.secretRef", "a Secret is the team option; the single-user host reads a mode-600 file")
		return SSHConfig{}, nil
	case ref.File == "":
		bad("provider.spec.credentialsRef.file", "is empty")
		return SSHConfig{}, nil
	}
	uid := os.Geteuid()
	if r.UID != nil {
		uid = *r.UID
	}
	b, err := readPrivate(ref.File, uid, true)
	if err != nil {
		return SSHConfig{}, fmt.Errorf("the credentials file: %w", err)
	}
	home := r.Home
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	c, err := ParseSSHConfig(b, home)
	if err != nil {
		bad("provider.spec.credentialsRef.file", "%v", err)
		return SSHConfig{}, nil
	}
	if _, err := readPrivate(c.IdentityFile, uid, false); err != nil {
		return SSHConfig{}, fmt.Errorf("the key file the credentials file names: %w", err)
	}
	return c, nil
}

// maxCredentialsSize bounds the credentials file that readPrivate reads.
const maxCredentialsSize = 1 << 20

// readPrivate checks that path is a regular file owned by uid with mode
// exactly 600, as HBEP-4 (ReadProfile) says of the credentials file and the key
// file it names, and reads it when read is set. A private key is only checked,
// never read.
func readPrivate(path string, uid int, read bool) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: it does not exist", provider.ErrCredentials)
		}
		return nil, fmt.Errorf("%w: it cannot be opened", provider.ErrCredentials)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: it cannot be read", provider.ErrCredentials)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	switch {
	case !fi.Mode().IsRegular():
		return nil, fmt.Errorf("%w: it is not a regular file", provider.ErrCredentials)
	case fi.Mode().Perm() != 0o600:
		return nil, fmt.Errorf("%w: its mode is %o; want 600", provider.ErrCredentials, fi.Mode().Perm())
	case !ok || int(st.Uid) != uid:
		return nil, fmt.Errorf("%w: it is not owned by the controller's user", provider.ErrCredentials)
	}
	if !read {
		return nil, nil
	}
	return readBounded(f)
}

// readBounded reads r to its end. A read error other than the end is an error
// that wraps provider.ErrCredentials and returns no bytes: partial content is
// never parsed as if it were the whole file. The error does not carry the read
// error's text, which may name a path.
func readBounded(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxCredentialsSize+1))
	if err != nil {
		return nil, fmt.Errorf("%w: it cannot be read", provider.ErrCredentials)
	}
	if len(b) > maxCredentialsSize {
		return nil, fmt.Errorf("%w: it is larger than 1 MiB", provider.ErrCredentials)
	}
	return b, nil
}
