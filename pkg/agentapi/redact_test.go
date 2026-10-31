package agentapi

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	testHubToken = "hf_0123456789SECRETtokenvalue"
	testKey      = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
)

func desiredWithToken() DesiredState {
	return DesiredState{
		Version: "0123456789abcdef", Action: ActionRun,
		Instance: &DesiredInstance{
			Name: "inst-a", UID: "u1", HubToken: testHubToken, KeyStored: true,
			Launch:  Launch{Image: "example.org/engine", KeyEnv: "VLLM_API_KEY", Command: []string{"serve"}},
			Workers: []DesiredWorker{{Name: "worker-0", Generation: 3}},
		},
	}
}

func reportWithKey() Report {
	return Report{
		Time: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC),
		Instance: &InstanceReport{
			UID: "u1", Key: testKey, ServingEndpoint: "unix:///run/x.sock",
			Workers: []WorkerReport{{Name: "worker-0", Phase: PartRunning}},
		},
	}
}

func TestDesiredStateRedacted(t *testing.T) {
	d := desiredWithToken()
	red := d.Redacted()
	b, err := json.Marshal(red)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), testHubToken) || strings.Contains(string(b), "SECRET") {
		t.Errorf("the redacted JSON holds the token: %s", b)
	}
	if !strings.Contains(string(b), `"hubToken":"[redacted]"`) {
		t.Errorf("the redacted JSON does not show the replacement: %s", b)
	}
	// The receiver is untouched, and the copy is another Instance struct.
	if d.Instance.HubToken != testHubToken {
		t.Errorf("the original's token is now %q", d.Instance.HubToken)
	}
	if red.Instance == d.Instance {
		t.Error("the copy shares the original's Instance")
	}
	// Nothing but the token changed.
	back := red
	inst := *red.Instance
	inst.HubToken = testHubToken
	back.Instance = &inst
	if !reflect.DeepEqual(back, d) {
		t.Errorf("Redacted changed more than the token:\n got %+v\nwant %+v", back.Instance, d.Instance)
	}
	// The version still names the state it came from.
	if red.Version != d.Version {
		t.Errorf("version %q became %q", d.Version, red.Version)
	}

	// An empty token stays empty (and is omitted), an idle state has no Instance.
	none := desiredWithToken()
	none.Instance.HubToken = ""
	if got := none.Redacted(); got.Instance.HubToken != "" {
		t.Errorf("an empty token became %q", got.Instance.HubToken)
	}
	if got := (DesiredState{Action: ActionIdle}).Redacted(); got.Instance != nil || got.Action != ActionIdle {
		t.Errorf("an idle state became %+v", got)
	}
}

func TestReportRedacted(t *testing.T) {
	r := reportWithKey()
	red := r.Redacted()
	b, err := json.Marshal(red)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), testKey) {
		t.Errorf("the redacted JSON holds the key: %s", b)
	}
	if !strings.Contains(string(b), `"key":"[redacted]"`) {
		t.Errorf("the redacted JSON does not show the replacement: %s", b)
	}
	if r.Instance.Key != testKey {
		t.Errorf("the original's key is now %q", r.Instance.Key)
	}
	if red.Instance == r.Instance {
		t.Error("the copy shares the original's Instance")
	}
	back := red
	inst := *red.Instance
	inst.Key = testKey
	back.Instance = &inst
	if !reflect.DeepEqual(back, r) {
		t.Errorf("Redacted changed more than the key:\n got %+v\nwant %+v", back.Instance, r.Instance)
	}

	// Once the controller has stored the key the agent stops sending it; an
	// empty key stays empty, and a report with no Instance does not panic.
	none := reportWithKey()
	none.Instance.Key = ""
	if got := none.Redacted(); got.Instance.Key != "" {
		t.Errorf("an empty key became %q", got.Instance.Key)
	}
	if got := (Report{}).Redacted(); got.Instance != nil {
		t.Errorf("a report with no Instance became %+v", got)
	}
}

// secretWords mark a field name as a secret's. KeyEnv holds the name of a
// variable, not a key.
var (
	secretWords     = []string{"Token", "Key", "Secret", "Password", "Credential"}
	notSecretByName = map[string]bool{"KeyEnv": true}
)

func hasSecretWord(name string) bool {
	for _, w := range secretWords {
		if strings.Contains(name, w) {
			return true
		}
	}
	return false
}

// fill sets every exported field reachable from v: each pointer allocated, one
// element in each slice and map, each string a marker that names its path.
func fill(t *testing.T, v reflect.Value, path string) {
	t.Helper()
	switch v.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fill(t, v.Elem(), path)
	case reflect.Struct:
		for i := range v.NumField() {
			f := v.Type().Field(i)
			if f.PkgPath != "" { // unexported, such as time.Time's
				continue
			}
			fill(t, v.Field(i), path+"."+f.Name)
		}
	case reflect.Slice:
		s := reflect.MakeSlice(v.Type(), 1, 1)
		fill(t, s.Index(0), path+"[]")
		v.Set(s)
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		k, e := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
		fill(t, k, path+"{key}")
		fill(t, e, path+"{}")
		m.SetMapIndex(k, e)
		v.Set(m)
	case reflect.String:
		v.SetString("SECRET:" + path)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	default:
		t.Fatalf("%s: kind %s cannot be filled; extend this walker and decide how Redacted treats it", path, v.Kind())
	}
}

// checkRedacted walks v. Every field whose name holds a secret word, except the
// allowlisted, must be a string equal to RedactedValue (or a number or a
// bool, which holds no secret); it records the paths it checked.
func checkRedacted(t *testing.T, v reflect.Value, path string, checked *[]string) {
	t.Helper()
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			checkRedacted(t, v.Elem(), path, checked)
		}
	case reflect.Struct:
		for i := range v.NumField() {
			f := v.Type().Field(i)
			if f.PkgPath != "" {
				continue
			}
			p, fv := path+"."+f.Name, v.Field(i)
			if hasSecretWord(f.Name) && !notSecretByName[f.Name] {
				switch fv.Kind() {
				case reflect.String:
					*checked = append(*checked, p)
					if fv.String() != RedactedValue {
						t.Errorf("%s holds %q after Redacted; a field named for a secret must be %q", p, fv.String(), RedactedValue)
					}
				case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
				default:
					t.Errorf("%s is named for a secret but is a %s; Redacted must blank it and this walker must handle its kind", p, fv.Kind())
				}
				continue
			}
			checkRedacted(t, fv, p, checked)
		}
	case reflect.Slice:
		for i := range v.Len() {
			checkRedacted(t, v.Index(i), path+"[]", checked)
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			checkRedacted(t, v.MapIndex(k), path+"{}", checked)
		}
	}
}

// A secret field added to DesiredState or Report later must be blanked by
// Redacted, or this test fails: it walks every exported field of a fully
// populated value, whatever the field is called, and checks the secret-named
// ones.
func TestRedactedBlanksEverySecretNamedField(t *testing.T) {
	var d DesiredState
	fill(t, reflect.ValueOf(&d).Elem(), "DesiredState")
	var r Report
	fill(t, reflect.ValueOf(&r).Elem(), "Report")

	// The walk reaches the fields it must: the populated values have them all.
	if d.Instance == nil || len(d.Instance.Workers) == 0 || r.Instance == nil || len(r.Instance.Workers) == 0 ||
		r.Instance.Checks == nil || r.Instance.Checks.WarmUp == nil || len(r.Instance.Checks.WarmUp.Rounds) == 0 {
		t.Fatal("the populated values do not reach the nested structs")
	}

	var dChecked, rChecked []string
	checkRedacted(t, reflect.ValueOf(d.Redacted()), "DesiredState", &dChecked)
	checkRedacted(t, reflect.ValueOf(r.Redacted()), "Report", &rChecked)
	// A walk that never reaches the secrets would pass without proving anything.
	for _, want := range []string{"DesiredState.Instance.HubToken"} {
		if !slices.Contains(dChecked, want) {
			t.Errorf("the walk of DesiredState did not check %s; it checked %v", want, dChecked)
		}
	}
	for _, want := range []string{"Report.Instance.Key"} {
		if !slices.Contains(rChecked, want) {
			t.Errorf("the walk of Report did not check %s; it checked %v", want, rChecked)
		}
	}

	// The originals still hold their markers, and no marker of a secret-named
	// field is in the redacted values' JSON.
	if d.Instance.HubToken != "SECRET:DesiredState.Instance.HubToken" || r.Instance.Key != "SECRET:Report.Instance.Key" {
		t.Errorf("Redacted changed an original: %q, %q", d.Instance.HubToken, r.Instance.Key)
	}
	for _, c := range []struct {
		name    string
		v       any
		checked []string
	}{{"desired state", d.Redacted(), dChecked}, {"report", r.Redacted(), rChecked}} {
		b, err := json.Marshal(c.v)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range c.checked {
			if strings.Contains(string(b), `"SECRET:`+p+`"`) {
				t.Errorf("the redacted %s's JSON still holds the marker of %s", c.name, p)
			}
		}
	}

	// The allowlisted name is not a secret: KeyEnv survives, so the walk does
	// not hide a field that logs need.
	if got := d.Redacted().Instance.Launch.KeyEnv; got != "SECRET:DesiredState.Instance.Launch.KeyEnv" {
		t.Errorf("KeyEnv, a variable's name, became %q", got)
	}
}
