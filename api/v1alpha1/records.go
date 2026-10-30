package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

var hexID = regexp.MustCompile(`^([0-9a-f]{12}|[0-9a-f]{64})$`)

// maxLabelValue is the longest label value Kubernetes accepts.
const maxLabelValue = 63

// LabelValue returns s as a label value. A value of up to 63 characters is
// kept. A longer one becomes its first 50 characters, then "-", then the first
// 12 hex characters of the SHA-256 of the full value (HBEP-11, API, Labels).
// The full value stays where the object names it in full: an owner reference, a
// spec field or an annotation.
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

// VerificationRecordName returns the name of an Instance's verification record.
func VerificationRecordName(instance types.UID) string {
	return VerificationRecordPrefix + string(instance)
}

// NewIdentityRecord returns the identity record of a deployment ID: a
// ControllerRevision named id-<first 12 hex of the ID>, with the labels of
// HBEP-11 and no owner reference, so garbage collection never removes it. Data
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

// NewVerificationRecord returns an Instance's verification record: a
// ControllerRevision written once, pass or fail, with the labels of HBEP-11
// and no owner reference to the ModelDeployment or to the Instance.
func NewVerificationRecord(namespace, modelDeployment, deploymentID string, instance types.UID, data []byte) (*appsv1.ControllerRevision, error) {
	if _, err := IdentityRecordName(deploymentID); err != nil {
		return nil, err
	}
	if instance == "" {
		return nil, fmt.Errorf("a verification record needs the Instance's UID")
	}
	return newRecord(VerificationRecordName(instance), namespace, RecordVerification, modelDeployment, deploymentID, instance, data)
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
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Data:       runtime.RawExtension{Raw: append([]byte(nil), data...)},
		Revision:   1,
	}, nil
}
