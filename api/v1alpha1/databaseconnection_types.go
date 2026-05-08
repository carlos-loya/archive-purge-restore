// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DatabaseEngine identifies the database backend an APR connection talks to.
// +kubebuilder:validation:Enum=postgres;mysql;timescaledb
type DatabaseEngine string

const (
	EnginePostgres    DatabaseEngine = "postgres"
	EngineMySQL       DatabaseEngine = "mysql"
	EngineTimescaleDB DatabaseEngine = "timescaledb"
)

// DatabaseConnectionSpec defines a reusable handle to a database. ArchiveRule
// and RestoreRequest resources reference a DatabaseConnection by name.
type DatabaseConnectionSpec struct {
	// Engine selects the database driver.
	Engine DatabaseEngine `json:"engine"`

	// Host is the database hostname or in-cluster service DNS name.
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`

	// Port is the database port. If 0, the engine's default is used
	// (5432 for postgres/timescaledb, 3306 for mysql).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// Database is the logical database name to operate on.
	// +kubebuilder:validation:MinLength=1
	Database string `json:"database"`

	// SSLMode controls TLS behavior. Postgres/TimescaleDB only.
	// +kubebuilder:validation:Enum=disable;allow;prefer;require;verify-ca;verify-full
	// +optional
	SSLMode string `json:"sslMode,omitempty"`

	// CredentialsSecretRef references a Secret in the same namespace that
	// holds the database credentials. The Secret must contain keys
	// "username" and "password".
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`
}

// DatabaseConnectionStatus reports the observed state of a DatabaseConnection.
type DatabaseConnectionStatus struct {
	// Conditions report the latest available observations of state. The
	// well-known type is "Ready".
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// ObservedGeneration is the most recent .metadata.generation observed
	// by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=dbconn,categories=apr
// +kubebuilder:printcolumn:name="Engine",type=string,JSONPath=`.spec.engine`
// +kubebuilder:printcolumn:name="Host",type=string,JSONPath=`.spec.host`
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.database`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DatabaseConnection is a reusable handle to a database that ArchiveRule and
// RestoreRequest resources reference.
type DatabaseConnection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatabaseConnectionSpec   `json:"spec,omitempty"`
	Status DatabaseConnectionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatabaseConnectionList contains a list of DatabaseConnection.
type DatabaseConnectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabaseConnection `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DatabaseConnection{}, &DatabaseConnectionList{})
}
