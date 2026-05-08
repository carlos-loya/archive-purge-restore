// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package webhook

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("adding corev1: %v", err)
	}
	if err := aprv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding aprv1alpha1: %v", err)
	}
	return s
}

func newArchiveRule(ns, name, dbcName, sbName, schedule string) *aprv1alpha1.ArchiveRule {
	return &aprv1alpha1.ArchiveRule{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: aprv1alpha1.ArchiveRuleSpec{
			DatabaseRef: corev1.LocalObjectReference{Name: dbcName},
			StorageRef:  corev1.LocalObjectReference{Name: sbName},
			Table:       "orders",
			DateColumn:  "created_at",
			DaysOnline:  30,
			Schedule:    schedule,
		},
	}
}

func newDBC(ns, name string) *aprv1alpha1.DatabaseConnection {
	return &aprv1alpha1.DatabaseConnection{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: aprv1alpha1.DatabaseConnectionSpec{
			Engine:               aprv1alpha1.EnginePostgres,
			Host:                 "pg.example.com",
			Database:             "orders",
			CredentialsSecretRef: corev1.LocalObjectReference{Name: name + "-creds"},
		},
	}
}

func newSB(ns, name string) *aprv1alpha1.StorageBackend {
	return &aprv1alpha1.StorageBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: aprv1alpha1.StorageBackendSpec{
			Type:                 aprv1alpha1.StorageS3,
			Bucket:               "test-archive",
			Region:               "us-west-2",
			CredentialsSecretRef: &corev1.LocalObjectReference{Name: name + "-creds"},
		},
	}
}

func TestArchiveRuleValidator_HappyPath(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newDBC("ns1", "dbc1"), newSB("ns1", "sb1")).
		Build()
	v := &ArchiveRuleValidator{Client: c}

	rule := newArchiveRule("ns1", "rule1", "dbc1", "sb1", "0 2 * * *")
	if _, err := v.ValidateCreate(context.Background(), rule); err != nil {
		t.Fatalf("expected admission, got error: %v", err)
	}
}

func TestArchiveRuleValidator_BadCron(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newDBC("ns1", "dbc1"), newSB("ns1", "sb1")).
		Build()
	v := &ArchiveRuleValidator{Client: c}

	rule := newArchiveRule("ns1", "rule1", "dbc1", "sb1", "definitely not cron")
	_, err := v.ValidateCreate(context.Background(), rule)
	if err == nil {
		t.Fatal("expected rejection for bad cron")
	}
	if !strings.Contains(err.Error(), "spec.schedule") {
		t.Errorf("error should reference spec.schedule: %v", err)
	}
}

func TestArchiveRuleValidator_MissingDatabaseRef(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newSB("ns1", "sb1")).
		Build()
	v := &ArchiveRuleValidator{Client: c}

	rule := newArchiveRule("ns1", "rule1", "missing-dbc", "sb1", "0 2 * * *")
	_, err := v.ValidateCreate(context.Background(), rule)
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.databaseRef.name") {
		t.Errorf("error should reference spec.databaseRef.name: %v", err)
	}
}

func TestArchiveRuleValidator_MissingStorageRef(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newDBC("ns1", "dbc1")).
		Build()
	v := &ArchiveRuleValidator{Client: c}

	rule := newArchiveRule("ns1", "rule1", "dbc1", "missing-sb", "0 2 * * *")
	_, err := v.ValidateCreate(context.Background(), rule)
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.storageRef.name") {
		t.Errorf("error should reference spec.storageRef.name: %v", err)
	}
}

func TestArchiveRuleValidator_AccumulatesErrors(t *testing.T) {
	// All three problems at once: bad cron, missing DBC, missing SB.
	// We expect the validator to surface all of them so the user fixes
	// in one round-trip.
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	v := &ArchiveRuleValidator{Client: c}

	rule := newArchiveRule("ns1", "rule1", "missing-dbc", "missing-sb", "definitely not cron")
	_, err := v.ValidateCreate(context.Background(), rule)
	if err == nil {
		t.Fatal("expected rejection")
	}
	for _, want := range []string{"spec.schedule", "spec.databaseRef", "spec.storageRef"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %s: %v", want, err)
		}
	}
}

func TestArchiveRuleValidator_UpdateAlsoValidated(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newDBC("ns1", "dbc1"), newSB("ns1", "sb1")).
		Build()
	v := &ArchiveRuleValidator{Client: c}

	old := newArchiveRule("ns1", "rule1", "dbc1", "sb1", "0 2 * * *")
	newer := old.DeepCopy()
	newer.Spec.Schedule = "@every garbage"

	if _, err := v.ValidateUpdate(context.Background(), old, newer); err == nil {
		t.Fatal("expected rejection on update with bad cron")
	}
}

func TestArchiveRuleValidator_DeleteAlwaysAllowed(t *testing.T) {
	v := &ArchiveRuleValidator{Client: fake.NewClientBuilder().WithScheme(newScheme(t)).Build()}
	if _, err := v.ValidateDelete(context.Background(), newArchiveRule("ns1", "rule1", "x", "y", "")); err != nil {
		t.Errorf("delete should always be admitted, got %v", err)
	}
}
