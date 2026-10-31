package lifecycle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The controller applies the PlatformProfile's schema defaults itself, so an
// object built without the API server means the same (settings.go). These tests
// read the defaults from the committed CRD YAML, an independent source: the CRD
// is generated from the Go markers by controller-gen, the controller's copy is
// typed by hand.

// settingsCRDSpec loads a committed CRD (config/crd/*_<plural>.yaml) and
// returns the schema of the v1alpha1 version's spec.
func settingsCRDSpec(t *testing.T, plural string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join("..", "..", "config", "crd", "*_"+plural+".yaml"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("want exactly one committed CRD for %s under config/crd, got %v (%v)", plural, matches, err)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("parse %s: %v", matches[0], err)
	}
	for _, v := range crd.Spec.Versions {
		if v.Name == "v1alpha1" {
			if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
				t.Fatalf("%s: v1alpha1 has no schema", matches[0])
			}
			spec, ok := v.Schema.OpenAPIV3Schema.Properties["spec"]
			if !ok {
				t.Fatalf("%s: no spec in the schema", matches[0])
			}
			return spec
		}
	}
	t.Fatalf("%s has no v1alpha1 version", matches[0])
	return apiextensionsv1.JSONSchemaProps{}
}

// settingsProp walks a dotted path of property names down a schema.
func settingsProp(t *testing.T, root apiextensionsv1.JSONSchemaProps, path string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	cur := root
	for _, name := range strings.Split(path, ".") {
		next, ok := cur.Properties[name]
		if !ok {
			t.Fatalf("the CRD schema has no property %s", path)
		}
		cur = next
	}
	return cur
}

// settingsDefaultDuration reads the default of a duration property at path.
func settingsDefaultDuration(t *testing.T, root apiextensionsv1.JSONSchemaProps, path string) time.Duration {
	t.Helper()
	p := settingsProp(t, root, path)
	if p.Default == nil {
		t.Fatalf("the CRD schema gives %s no default", path)
	}
	var text string
	if err := json.Unmarshal(p.Default.Raw, &text); err != nil {
		t.Fatalf("default of %s is %s, not a string: %v", path, p.Default.Raw, err)
	}
	d, err := time.ParseDuration(text)
	if err != nil {
		t.Fatalf("default of %s: %v", path, err)
	}
	return d
}

// settingsDefaultInt reads the default of an integer property at path.
func settingsDefaultInt(t *testing.T, root apiextensionsv1.JSONSchemaProps, path string) int64 {
	t.Helper()
	p := settingsProp(t, root, path)
	if p.Default == nil {
		t.Fatalf("the CRD schema gives %s no default", path)
	}
	var n int64
	if err := json.Unmarshal(p.Default.Raw, &n); err != nil {
		t.Fatalf("default of %s is %s, not an integer: %v", path, p.Default.Raw, err)
	}
	return n
}

// TestResolveSettingsEmptyProfileEqualsTheCRDDefaults: resolveSettings of an
// empty profile gives the schema defaults of HBEP-4's Platform profile schema,
// as the committed CRD writes them.
func TestResolveSettingsEmptyProfileEqualsTheCRDDefaults(t *testing.T) {
	spec := settingsCRDSpec(t, "platformprofiles")
	got, err := resolveSettings(v1alpha1.PlatformProfileSpec{})
	if err != nil {
		t.Fatal(err)
	}

	durations := []struct {
		path string
		got  time.Duration
	}{
		{"handoverMargin", got.handoverMargin},
		{"health.probeInterval", got.probeInterval},
		{"health.noProgressTimeout", got.noProgressTimeout},
		{"health.errorOnlyWindow", got.errorOnlyWindow},
		{"health.agentTimeout", got.agentTimeout},
		{"health.registrationTimeout", got.registrationTimeout},
		{"health.startTimeout", got.startTimeout},
		{"restart.stopGrace", got.stopGrace},
		{"restart.window", got.restartWindow},
		{"restart.backoff", got.restartBackoff},
	}
	checked := map[string]bool{}
	for _, d := range durations {
		checked[d.path] = true
		if want := settingsDefaultDuration(t, spec, d.path); d.got != want {
			t.Errorf("%s = %s, the CRD default is %s", d.path, d.got, want)
		}
	}
	ints := []struct {
		path string
		got  int32
	}{
		{"health.failureThreshold", got.failureThreshold},
		{"restart.attemptsPerRung", got.attemptsPerRung},
	}
	for _, n := range ints {
		checked[n.path] = true
		if want := settingsDefaultInt(t, spec, n.path); int64(n.got) != want {
			t.Errorf("%s = %d, the CRD default is %d", n.path, n.got, want)
		}
	}

	// maxIdle has no default: unset means no cap (HBEP-7, Retention).
	if settingsProp(t, spec, "maxIdle").Default != nil || got.maxIdle != nil {
		t.Errorf("maxIdle: CRD default %v, resolved %v; want neither", settingsProp(t, spec, "maxIdle").Default, got.maxIdle)
	}

	// Every other default the CRD gives under health and restart must be read
	// by the controller too, or be known to be outside alpha. kvSkewTimeout
	// needs several workers (HBEP-7, Health and restarts), so it is not in alpha.
	outsideAlpha := []string{"health.kvSkewTimeout"}
	for _, group := range []string{"health", "restart"} {
		for name, p := range settingsProp(t, spec, group).Properties {
			path := group + "." + name
			if p.Default != nil && !checked[path] && !slices.Contains(outsideAlpha, path) {
				t.Errorf("the CRD gives %s a default that resolveSettings does not read", path)
			}
		}
	}

	// A profile whose health and restart groups were defaulted to {} by the
	// API server resolves the same as one without them.
	filled, err := resolveSettings(v1alpha1.PlatformProfileSpec{Health: &v1alpha1.HealthThresholds{}, Restart: &v1alpha1.RestartPolicy{}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(filled, got) {
		t.Errorf("empty groups resolve to %+v, absent groups to %+v", filled, got)
	}
}

// TestPlatformProfileDrainDefaults: the drain window's schema defaults are
// 2h10m and 10m (HBEP-4; the owner's decision of 30 Oct 2026), and the Go
// constants InstanceDrainWindow falls back on equal them.
func TestPlatformProfileDrainDefaults(t *testing.T) {
	spec := settingsCRDSpec(t, "platformprofiles")
	if got, want := settingsDefaultDuration(t, spec, "drainWindow"), 2*time.Hour+10*time.Minute; got != want {
		t.Errorf("CRD drainWindow default = %s, want %s", got, want)
	}
	if got, want := settingsDefaultDuration(t, spec, "drainMargin"), 10*time.Minute; got != want {
		t.Errorf("CRD drainMargin default = %s, want %s", got, want)
	}
	if got := settingsDefaultDuration(t, spec, "drainWindow"); v1alpha1.DefaultDrainWindow != got {
		t.Errorf("DefaultDrainWindow = %s, the CRD default is %s", v1alpha1.DefaultDrainWindow, got)
	}
	if got := settingsDefaultDuration(t, spec, "drainMargin"); v1alpha1.DefaultDrainMargin != got {
		t.Errorf("DefaultDrainMargin = %s, the CRD default is %s", v1alpha1.DefaultDrainMargin, got)
	}
}

// TestWorkloadShapeDefaultsEqualTheCRD: contextTokens.short defaults to 512 and
// outputTokens.longest to 4096 (HBEP-7, What the controller reads), as the
// ModelDeployment CRD writes them; checkServed applies the same.
func TestWorkloadShapeDefaultsEqualTheCRD(t *testing.T) {
	spec := settingsCRDSpec(t, "modeldeployments")
	short := settingsDefaultInt(t, spec, "workload.contextTokens.short")
	longest := settingsDefaultInt(t, spec, "workload.outputTokens.longest")
	if short != 512 || longest != 4096 {
		t.Fatalf("CRD defaults short/outputLongest = %d/%d, HBEP-7 says 512/4096", short, longest)
	}
	sh := specCheck(t, specMD()).shapes
	if int64(sh.short) != short || int64(sh.outputLongest) != longest {
		t.Errorf("checkServed applies short/outputLongest = %d/%d, the CRD defaults are %d/%d", sh.short, sh.outputLongest, short, longest)
	}
}

// TestResolveSettingsKeepsExplicitValues: a profile's own values win over the
// defaults. The values are all different from their defaults.
func TestResolveSettingsKeepsExplicitValues(t *testing.T) {
	p := v1alpha1.PlatformProfileSpec{
		HandoverMargin: dur(45 * time.Minute),
		MaxIdle:        dur(20 * time.Minute),
		Health: &v1alpha1.HealthThresholds{
			ProbeInterval: dur(5 * time.Second), FailureThreshold: ptrTo(int32(7)), NoProgressTimeout: dur(45 * time.Minute),
			ErrorOnlyWindow: dur(2 * time.Minute), AgentTimeout: dur(3 * time.Minute), RegistrationTimeout: dur(20 * time.Minute),
			StartTimeout: dur(2 * time.Hour),
		},
		Restart: &v1alpha1.RestartPolicy{
			StopGrace: dur(15 * time.Second), AttemptsPerRung: ptrTo(int32(4)), Window: dur(time.Hour), Backoff: dur(45 * time.Second),
		},
	}
	got, err := resolveSettings(p)
	if err != nil {
		t.Fatal(err)
	}
	if got.maxIdle == nil || *got.maxIdle != 20*time.Minute {
		t.Errorf("maxIdle = %v, want 20m", got.maxIdle)
	}
	want := settings{
		handoverMargin: 45 * time.Minute, maxIdle: got.maxIdle,
		probeInterval: 5 * time.Second, failureThreshold: 7, noProgressTimeout: 45 * time.Minute, errorOnlyWindow: 2 * time.Minute,
		agentTimeout: 3 * time.Minute, registrationTimeout: 20 * time.Minute, startTimeout: 2 * time.Hour,
		stopGrace: 15 * time.Second, attemptsPerRung: 4, restartWindow: time.Hour, restartBackoff: 45 * time.Second,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("settings\n got %+v\nwant %+v", got, want)
	}
}

// TestResolveSettingsNamesTheBadDuration: a duration that does not parse is an
// error naming the field and the value. One field is broken at a time, because
// the first error is the one reported.
func TestResolveSettingsNamesTheBadDuration(t *testing.T) {
	bad := ptrTo(v1alpha1.Duration("soon"))
	tests := []struct {
		field string
		set   func(*v1alpha1.PlatformProfileSpec)
	}{
		{"handoverMargin", func(p *v1alpha1.PlatformProfileSpec) { p.HandoverMargin = bad }},
		{"maxIdle", func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = bad }},
		{"probeInterval", func(p *v1alpha1.PlatformProfileSpec) { p.Health = &v1alpha1.HealthThresholds{ProbeInterval: bad} }},
		{"noProgressTimeout", func(p *v1alpha1.PlatformProfileSpec) { p.Health = &v1alpha1.HealthThresholds{NoProgressTimeout: bad} }},
		{"errorOnlyWindow", func(p *v1alpha1.PlatformProfileSpec) { p.Health = &v1alpha1.HealthThresholds{ErrorOnlyWindow: bad} }},
		{"agentTimeout", func(p *v1alpha1.PlatformProfileSpec) { p.Health = &v1alpha1.HealthThresholds{AgentTimeout: bad} }},
		{"registrationTimeout", func(p *v1alpha1.PlatformProfileSpec) { p.Health = &v1alpha1.HealthThresholds{RegistrationTimeout: bad} }},
		{"startTimeout", func(p *v1alpha1.PlatformProfileSpec) { p.Health = &v1alpha1.HealthThresholds{StartTimeout: bad} }},
		{"stopGrace", func(p *v1alpha1.PlatformProfileSpec) { p.Restart = &v1alpha1.RestartPolicy{StopGrace: bad} }},
		{"window", func(p *v1alpha1.PlatformProfileSpec) { p.Restart = &v1alpha1.RestartPolicy{Window: bad} }},
		{"backoff", func(p *v1alpha1.PlatformProfileSpec) { p.Restart = &v1alpha1.RestartPolicy{Backoff: bad} }},
	}
	for _, tc := range tests {
		t.Run(tc.field, func(t *testing.T) {
			var p v1alpha1.PlatformProfileSpec
			tc.set(&p)
			_, err := resolveSettings(p)
			if err == nil {
				t.Fatal("no error for a duration that does not parse")
			}
			if !strings.Contains(err.Error(), tc.field) || !strings.Contains(err.Error(), "soon") {
				t.Fatalf("error %q does not name the field %s and the value", err, tc.field)
			}
		})
	}
}
