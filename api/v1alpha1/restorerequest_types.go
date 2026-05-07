// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RestorePhase tracks the lifecycle of a one-shot restore.
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
type RestorePhase string

const (
	RestorePending   RestorePhase = "Pending"
	RestoreRunning   RestorePhase = "Running"
	RestoreSucceeded RestorePhase = "Succeeded"
	RestoreFailed    RestorePhase = "Failed"
)

// RestoreRequestSpec is immutable after creation. To re-run a restore,
// create a new RestoreRequest.
type RestoreRequestSpec struct {
	// ArchiveRuleRef references the ArchiveRule whose archives should be
	// restored. The referenced rule's database and storage refs are reused.
	ArchiveRuleRef corev1.LocalObjectReference `json:"archiveRuleRef"`

	// Date filters which archived files are restored. Format: YYYY-MM-DD.
	// If empty, every available date is restored.
	// +kubebuilder:validation:Pattern=`^\d{4}-\d{2}-\d{2}$`
	// +optional
	Date string `json:"date,omitempty"`

	// RunID filters by archive run ID (substring match). If empty, every
	// run for the matching date(s) is restored.
	// +optional
	RunID string `json:"runID,omitempty"`

	// Table optionally restricts the restore to a single table.
	// +optional
	Table string `json:"table,omitempty"`
}

// RestoreRequestStatus tracks one-shot restore execution.
type RestoreRequestStatus struct {
	// Phase is the lifecycle phase of this restore.
	// +optional
	Phase RestorePhase `json:"phase,omitempty"`

	// JobRef points to the Job spawned by this request.
	// +optional
	JobRef *corev1.LocalObjectReference `json:"jobRef,omitempty"`

	// RowsRestored is the total row count across all restored tables.
	// +optional
	RowsRestored int64 `json:"rowsRestored,omitempty"`

	// StartTime is when the Job started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the Job reached a terminal phase.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rr,categories=apr
// +kubebuilder:printcolumn:name="Rule",type=string,JSONPath=`.spec.archiveRuleRef.name`
// +kubebuilder:printcolumn:name="Date",type=string,JSONPath=`.spec.date`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Rows-Restored",type=integer,JSONPath=`.status.rowsRestored`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RestoreRequest is a one-shot request to restore previously archived rows
// back to the source database.
type RestoreRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RestoreRequestSpec   `json:"spec,omitempty"`
	Status RestoreRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RestoreRequestList contains a list of RestoreRequest.
type RestoreRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RestoreRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RestoreRequest{}, &RestoreRequestList{})
}
