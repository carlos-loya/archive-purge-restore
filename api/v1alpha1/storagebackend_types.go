// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageType identifies the object storage backend.
// +kubebuilder:validation:Enum=filesystem;s3;r2;gcs
type StorageType string

const (
	StorageFilesystem StorageType = "filesystem"
	StorageS3         StorageType = "s3"
	StorageR2         StorageType = "r2"
	StorageGCS        StorageType = "gcs"
)

// StorageBackendSpec defines a reusable archive destination.
type StorageBackendSpec struct {
	// Type selects the storage driver.
	Type StorageType `json:"type"`

	// Bucket is the bucket name (s3, r2, gcs) or absolute path (filesystem).
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Region is the cloud region. Required for s3 and r2; ignored for gcs and filesystem.
	// +optional
	Region string `json:"region,omitempty"`

	// Prefix is prepended to every object key written to this backend.
	// Useful for sharing a bucket across environments.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// Endpoint overrides the default service endpoint. Useful for s3-compatible
	// services (MinIO) and for r2 (which always requires a custom endpoint).
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// AccountID is the Cloudflare account ID. Required when type=r2.
	// +optional
	AccountID string `json:"accountID,omitempty"`

	// CredentialsSecretRef references a Secret in the same namespace that
	// holds backend-specific credentials. Expected keys vary by type:
	//   s3, r2:    access_key_id, secret_access_key
	//   gcs:       service_account_json
	//   filesystem: (Secret reference is optional)
	// +optional
	CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`
}

// StorageBackendStatus reports the observed state of a StorageBackend.
type StorageBackendStatus struct {
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
// +kubebuilder:resource:shortName=stb,categories=apr
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Bucket",type=string,JSONPath=`.spec.bucket`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// StorageBackend is a reusable archive destination that ArchiveRule and
// RestoreRequest resources reference.
type StorageBackend struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StorageBackendSpec   `json:"spec,omitempty"`
	Status StorageBackendStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StorageBackendList contains a list of StorageBackend.
type StorageBackendList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StorageBackend `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StorageBackend{}, &StorageBackendList{})
}
