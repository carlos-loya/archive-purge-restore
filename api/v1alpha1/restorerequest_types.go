// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// RestoreRequestStatus tracks one-shot restore execution. Lifecycle is
// expressed via Conditions (the ad-hoc Phase field was removed in favor of
// the standard metav1.Condition pattern).
type RestoreRequestStatus struct {
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

	// Conditions report the latest observed state of the restore. Standard
	// types:
	//
	//   Ready        request is well-formed and references resolve
	//   Progressing  Job is running
	//   Succeeded    Job completed successfully (terminal)
	//   Failed       Job failed terminally
	//
	// At most one of Succeeded / Failed will be True at a time, and only
	// after the Job reaches a terminal state.
	//
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
// +kubebuilder:printcolumn:name="Progressing",type=string,JSONPath=`.status.conditions[?(@.type=="Progressing")].status`
// +kubebuilder:printcolumn:name="Succeeded",type=string,JSONPath=`.status.conditions[?(@.type=="Succeeded")].status`
// +kubebuilder:printcolumn:name="Failed",type=string,JSONPath=`.status.conditions[?(@.type=="Failed")].status`
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
