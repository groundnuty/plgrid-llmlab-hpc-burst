package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

var hexID = regexp.MustCompile(`^([0-9a-f]{12}|[0-9a-f]{64})$`)

// uuid is the form of an object UID, as the API server generates it.
var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// maxLabelValue is the longest label value Kubernetes accepts.
const maxLabelValue = 63

// LabelValue returns s as a label value. A value of up to 63 characters is
// kept. A longer one becomes its first 50 characters, then "-", then the first
// 12 hex characters of the SHA-256 of the full value (HBEP-11, API, Labels).
// The full value stays where the object names it in full: an owner reference, a
// spec field or an annotation (a record has the annotation
// AnnotationModelDeployment).
func LabelValue(s string) string {
	if len(s) <= maxLabelValue {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return s[:50] + "-" + hex.EncodeToString(sum[:])[:DisplayIDLength]
}

// IdentityRecordName returns the name of the identity record of a deployment
// ID: id-<first 12 hex>. The ID may be given in its display or full form.
func IdentityRecordName(id string) (string, error) {
	if !hexID.MatchString(id) {
		return "", fmt.Errorf("deployment ID %q is not 12 or 64 lowercase hex characters", id)
	}
	return IdentityRecordPrefix + id[:DisplayIDLength], nil
}

// VerificationRecordName returns the name of an Instance's verification record
// for one verification: ver-<instance UID>-<generation>, where the generation
// counts the Instance's verifications from 1 (HBEP-7, Instance start). It
// returns an error for a generation below 1, or a UID that is not a lowercase
// UUID, the form the API server gives every object.
func VerificationRecordName(instance types.UID, generation int64) (string, error) {
	if !uuid.MatchString(string(instance)) {
		return "", fmt.Errorf("a verification record needs the Instance's UID, a lowercase UUID; got %q", instance)
	}
	if generation < 1 {
		return "", fmt.Errorf("verification generation %d: generations count from 1", generation)
	}
	return VerificationRecordPrefix + string(instance) + "-" + strconv.FormatInt(generation, 10), nil
}

// WorkerVerificationRecordName is the name of one verification of one worker of an Instance of several
// workers: ver-<instance UID>-<worker>-<generation>, the generation counting that worker's
// verifications from 1 (HBEP-7, Several workers). An Instance of one worker keeps VerificationRecordName.
func WorkerVerificationRecordName(instance types.UID, worker string, generation int64) (string, error) {
	if _, err := VerificationRecordName(instance, generation); err != nil {
		return "", err
	}
	if !workerName.MatchString(worker) {
		return "", fmt.Errorf("a worker's verification record needs a worker name, worker-<i>; got %q", worker)
	}
	return VerificationRecordPrefix + string(instance) + "-" + worker + "-" + strconv.FormatInt(generation, 10), nil
}

// workerName is a worker's name, worker-<i> with no leading zero (WorkerName).
var workerName = regexp.MustCompile(`^worker-(0|[1-9][0-9]*)$`)

// ParseVerificationRecordName returns the Instance UID and the generation that
// a verification record's name holds. It accepts exactly the names
// VerificationRecordName returns.
func ParseVerificationRecordName(name string) (types.UID, int64, error) {
	rest, ok := strings.CutPrefix(name, VerificationRecordPrefix)
	i := strings.LastIndexByte(rest, '-')
	if !ok || i <= 0 || !uuid.MatchString(rest[:i]) {
		return "", 0, fmt.Errorf("%q is not a verification record name, ver-<instance UID>-<generation>", name)
	}
	gen, err := strconv.ParseInt(rest[i+1:], 10, 64)
	if err != nil || gen < 1 || strconv.FormatInt(gen, 10) != rest[i+1:] {
		return "", 0, fmt.Errorf("%q is not a verification record name: generation %q is not an integer from 1", name, rest[i+1:])
	}
	return types.UID(rest[:i]), gen, nil
}

// NewIdentityRecord returns the identity record of a deployment ID: a
// ControllerRevision named id-<first 12 hex of the ID>, with the labels of
// HBEP-11, the full ModelDeployment name in an annotation and no owner
// reference, so garbage collection never removes it. Data
// holds the canonical v1 object and the full hash; this package does not
// interpret it. The caller creates the record only if it is absent: the same
// inputs give the same bytes, so an existing record is reused, never rewritten.
func NewIdentityRecord(namespace, modelDeployment, deploymentID string, data []byte) (*appsv1.ControllerRevision, error) {
	name, err := IdentityRecordName(deploymentID)
	if err != nil {
		return nil, err
	}
	if len(deploymentID) != FullIDLength {
		return nil, fmt.Errorf("an identity record needs the full deployment ID, got %q", deploymentID)
	}
	return newRecord(name, namespace, RecordIdentity, modelDeployment, deploymentID, "", data)
}

// NewManagedIdentityRecord returns the identity record of a managed catalog entry's deployment ID (HBEP-9,
// the identity record of a managed catalog entry; HBEP-11, Identity records): a ControllerRevision named
// id-<first 12 hex>, written once and never rewritten, with no owner reference, so it outlives the catalog
// entry and every Provider that listed it. Its labels are the record kind, the ID's display form and the
// platform, which is the Provider's platform label and so a valid label value; the full catalog name is in
// AnnotationCatalogName. A managed ID belongs to no ModelDeployment, so the record has no
// model-deployment label. The caller names the namespace.
func NewManagedIdentityRecord(namespace, platform, catalogName, deploymentID string, data []byte) (*appsv1.ControllerRevision, error) {
	name, err := IdentityRecordName(deploymentID)
	if err != nil {
		return nil, err
	}
	switch {
	case len(deploymentID) != FullIDLength:
		return nil, fmt.Errorf("an identity record needs the full deployment ID, got %q", deploymentID)
	case namespace == "":
		return nil, fmt.Errorf("a managed identity record needs a namespace")
	case platform == "" || len(validation.IsValidLabelValue(platform)) > 0:
		return nil, fmt.Errorf("a managed identity record needs its platform as a label value, got %q", platform)
	case catalogName == "":
		return nil, fmt.Errorf("a managed identity record needs its catalog name")
	case !json.Valid(data):
		return nil, fmt.Errorf("record data is not valid JSON")
	}
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			Labels:      map[string]string{LabelRecord: RecordIdentity, LabelDeploymentID: deploymentID[:DisplayIDLength], LabelPlatform: platform},
			Annotations: map[string]string{AnnotationCatalogName: catalogName},
		},
		Data:     runtime.RawExtension{Raw: append([]byte(nil), data...)},
		Revision: 1,
	}, nil
}

// NewVerificationRecord returns the record of one verification of an Instance:
// a ControllerRevision named ver-<instance UID>-<generation>, written once,
// pass or fail, with the labels of HBEP-11, the full ModelDeployment name in an
// annotation and no owner reference to the ModelDeployment or to the Instance.
// Its Revision is the generation. One exists per verification, and the
// Instance's status names the latest (HBEP-7, Instance start). A record that
// already exists under its name, as after a controller restart, is read and
// never rewritten.
func NewVerificationRecord(namespace, modelDeployment, deploymentID string, instance types.UID, generation int64, data []byte) (*appsv1.ControllerRevision, error) {
	if _, err := IdentityRecordName(deploymentID); err != nil {
		return nil, err
	}
	name, err := VerificationRecordName(instance, generation)
	if err != nil {
		return nil, err
	}
	rec, err := newRecord(name, namespace, RecordVerification, modelDeployment, deploymentID, instance, data)
	if err != nil {
		return nil, err
	}
	rec.Revision = generation
	return rec, nil
}

// ParseWorkerVerificationRecordName returns the Instance UID, the worker and the generation that a
// worker's verification record's name holds. It accepts exactly the names WorkerVerificationRecordName
// returns.
func ParseWorkerVerificationRecordName(name string) (types.UID, string, int64, error) {
	bad := fmt.Errorf("%q is not a worker's verification record name, ver-<instance UID>-<worker>-<generation>", name)
	rest, ok := strings.CutPrefix(name, VerificationRecordPrefix)
	if !ok || len(rest) < 38 || !uuid.MatchString(rest[:36]) || rest[36] != '-' {
		return "", "", 0, bad
	}
	tail := rest[37:]
	j := strings.LastIndexByte(tail, '-')
	if j <= 0 || !workerName.MatchString(tail[:j]) {
		return "", "", 0, bad
	}
	gen, err := strconv.ParseInt(tail[j+1:], 10, 64)
	if err != nil || gen < 1 || strconv.FormatInt(gen, 10) != tail[j+1:] {
		return "", "", 0, bad
	}
	return types.UID(rest[:36]), tail[:j], gen, nil
}

// NewWorkerVerificationRecord is NewVerificationRecord for one worker of an Instance of several workers,
// named WorkerVerificationRecordName (HBEP-7, Several workers). The labels and the contents are a
// verification record's; only the name carries the worker.
func NewWorkerVerificationRecord(namespace, modelDeployment, deploymentID string, instance types.UID, worker string, generation int64, data []byte) (*appsv1.ControllerRevision, error) {
	if _, err := IdentityRecordName(deploymentID); err != nil {
		return nil, err
	}
	name, err := WorkerVerificationRecordName(instance, worker, generation)
	if err != nil {
		return nil, err
	}
	rec, err := newRecord(name, namespace, RecordVerification, modelDeployment, deploymentID, instance, data)
	if err != nil {
		return nil, err
	}
	rec.Revision = generation
	return rec, nil
}

// JobTag returns an Allocation's job tag, hb-<n>-<h> (HBEP-3, Allocation and
// job identity): <n> is the Allocation's name cut to its first 40 characters,
// and <h> the first 12 hex characters of the SHA-256 of the UTF-8 string
// <namespace>/<name>/<uid>. It is at most JobTagLength characters. The
// provider sets it as the Allocation's external name before any submission
// and passes it to Slurm as the job name.
func JobTag(namespace, name string, uid types.UID) string {
	sum := sha256.Sum256([]byte(namespace + "/" + name + "/" + string(uid)))
	n := name
	if len(n) > 40 {
		n = n[:40]
	}
	return "hb-" + n + "-" + hex.EncodeToString(sum[:])[:DisplayIDLength]
}

func newRecord(name, namespace, kind, modelDeployment, deploymentID string, instance types.UID, data []byte) (*appsv1.ControllerRevision, error) {
	if modelDeployment == "" {
		return nil, fmt.Errorf("a record needs the name of its ModelDeployment")
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("record data is not valid JSON")
	}
	labels := map[string]string{
		LabelRecord:          kind,
		LabelDeploymentID:    deploymentID[:DisplayIDLength],
		LabelModelDeployment: LabelValue(modelDeployment),
	}
	if instance != "" {
		labels[LabelInstanceUID] = string(instance)
	}
	// The label holds the shortened name; the annotation keeps the full one for
	// a record that outlives its ModelDeployment.
	annotations := map[string]string{AnnotationModelDeployment: modelDeployment}
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels, Annotations: annotations},
		Data:       runtime.RawExtension{Raw: append([]byte(nil), data...)},
		Revision:   1,
	}, nil
}
