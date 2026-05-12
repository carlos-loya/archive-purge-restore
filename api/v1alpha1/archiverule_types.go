// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AnnotationTriggerTime, when set on an ArchiveRule, requests that the
// operator fire an immediate archive run regardless of the cron schedule.
// The annotation value must be RFC3339-formatted; the reconciler tracks
// the last-processed value via Status.LastTriggerTime so the same trigger
// is not honored twice.
const AnnotationTriggerTime = "apr.dev/trigger-time"

// ArchiveRuleSpec declares which rows to archive, where to send them, and
// how often to run.
type ArchiveRuleSpec struct {
	// DatabaseRef references a DatabaseConnection in the same namespace.
	DatabaseRef corev1.LocalObjectReference `json:"databaseRef"`

	// StorageRef references a StorageBackend in the same namespace.
	StorageRef corev1.LocalObjectReference `json:"storageRef"`

	// Table is the source table name.
	// +kubebuilder:validation:MinLength=1
	Table string `json:"table"`

	// DateColumn is the column used to determine row age.
	// +kubebuilder:validation:MinLength=1
	DateColumn string `json:"dateColumn"`

	// DaysOnline is the number of days a row stays in the source database
	// before becoming eligible for archive.
	// +kubebuilder:validation:Minimum=1
	DaysOnline int32 `json:"daysOnline"`

	// Schedule is a standard cron expression that controls when archive
	// runs are launched.
	// +kubebuilder:validation:MinLength=1
	Schedule string `json:"schedule"`

	// BatchSize overrides the engine's default extract/delete batch size.
	// +kubebuilder:validation:Minimum=1
	// +optional
	BatchSize int32 `json:"batchSize,omitempty"`

	// Suspend pauses scheduling without deleting state. While suspended,
	// already-running Jobs continue to completion but no new firings occur.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// MaxFailures is the number of consecutive failures after which the
	// operator stops scheduling new runs and marks the rule Ready=False.
	// 0 means unbounded — the rule will keep retrying forever. Default 5.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxFailures int32 `json:"maxFailures,omitempty"`

	// HistoryLimit is the maximum number of finished Jobs the operator
	// retains for this rule. Older Jobs are deleted on each reconcile.
	// +kubebuilder:validation:Minimum=0
	// +optional
	HistoryLimit int32 `json:"historyLimit,omitempty"`
}

// ArchiveRuleStatus reports the observed state of an ArchiveRule including
// the most recent archive run.
type ArchiveRuleStatus struct {
	// ActiveJobRef points to the currently-running archive Job, if any.
	// At most one Job runs at a time per rule (forbidden concurrency).
	// +optional
	ActiveJobRef *corev1.LocalObjectReference `json:"activeJobRef,omitempty"`

	// LastJobRef points to the most recently completed archive Job. It
	// outlives the Job itself if the Job is later GC'd by the history
	// limit.
	// +optional
	LastJobRef *corev1.LocalObjectReference `json:"lastJobRef,omitempty"`

	// LastRunTime is when the most recent archive Job started.
	// +optional
	LastRunTime *metav1.Time `json:"lastRunTime,omitempty"`

	// LastRunRowsArchived is the row count from the most recent run.
	// +optional
	LastRunRowsArchived int64 `json:"lastRunRowsArchived,omitempty"`

	// LastRunID is the engine's run ID for the most recent run.
	// +optional
	LastRunID string `json:"lastRunID,omitempty"`

	// NextScheduledTime is when the next run will fire. The reconciler
	// computes this from spec.schedule and uses it to set its requeue
	// delay. While suspended or while a Job is active, this is left
	// unset.
	// +optional
	NextScheduledTime *metav1.Time `json:"nextScheduledTime,omitempty"`

	// LastTriggerTime is the most recent value of the
	// `apr.dev/trigger-time` annotation that the reconciler honored. Used
	// to dedupe annotation triggers — the same value will not fire twice.
	// +optional
	LastTriggerTime *metav1.Time `json:"lastTriggerTime,omitempty"`

	// ConsecutiveFailures is the number of finished Jobs in a row whose
	// terminal state was Failed. Resets to zero on the first success. The
	// rule auto-suspends scheduling once this exceeds spec.maxFailures.
	// +optional
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`

	// Conditions report the latest observed state of the rule. Standard types:
	//
	//   Ready          rule is configured correctly and not auto-suspended
	//   ScheduleValid  spec.schedule parses as a cron expression
	//   Progressing    an archive Job is currently running
	//   Degraded       most recent run failed (or consecutive failures > 0)
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
// +kubebuilder:resource:shortName=ar,categories=apr
// +kubebuilder:printcolumn:name="Table",type=string,JSONPath=`.spec.table`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Days-Online",type=integer,JSONPath=`.spec.daysOnline`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Progressing",type=string,JSONPath=`.status.conditions[?(@.type=="Progressing")].status`
// +kubebuilder:printcolumn:name="Degraded",type=string,JSONPath=`.status.conditions[?(@.type=="Degraded")].status`
// +kubebuilder:printcolumn:name="Rows-Archived",type=integer,JSONPath=`.status.lastRunRowsArchived`
// +kubebuilder:printcolumn:name="Next-Run",type=date,JSONPath=`.status.nextScheduledTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ArchiveRule declares a recurring archive job.
type ArchiveRule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ArchiveRuleSpec   `json:"spec,omitempty"`
	Status ArchiveRuleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ArchiveRuleList contains a list of ArchiveRule.
type ArchiveRuleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ArchiveRule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ArchiveRule{}, &ArchiveRuleList{})
}
