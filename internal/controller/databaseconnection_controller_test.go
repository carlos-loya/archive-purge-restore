// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

func TestDBC_SecretMissing(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "dbc-no-secret")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))

	r := &DatabaseConnectionReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "dbc1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getDBC(t, ctx, ns, "dbc1")
	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonSecretNotFound {
		t.Fatalf("expected Ready=False/%s, got %+v", ReasonSecretNotFound, cond)
	}
}

func TestDBC_SecretMissingKeys(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "dbc-bad-secret")

	mustCreate(t, ctx, newSecret(ns, "dbc1-creds", map[string][]byte{"username": []byte("admin")}))
	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))

	r := &DatabaseConnectionReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "dbc1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getDBC(t, ctx, ns, "dbc1")
	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Reason != ReasonSecretMissingKeys {
		t.Fatalf("expected %s, got %+v", ReasonSecretMissingKeys, cond)
	}
}

func TestDBC_HappyPath(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "dbc-happy")

	mustCreate(t, ctx, newSecret(ns, "dbc1-creds", map[string][]byte{
		"username": []byte("admin"),
		"password": []byte("hunter2"),
	}))
	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))

	r := &DatabaseConnectionReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "dbc1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getDBC(t, ctx, ns, "dbc1")
	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready=True, got %+v", cond)
	}
	if got.Status.ObservedGeneration != got.Generation {
		t.Errorf("ObservedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
	}
}

func TestDBC_RecoversWhenSecretCreated(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "dbc-recovery")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))

	r := &DatabaseConnectionReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "dbc1")); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	got := getDBC(t, ctx, ns, "dbc1")
	if isReady(got.Status.Conditions) {
		t.Fatal("should be NotReady before Secret exists")
	}

	mustCreate(t, ctx, newSecret(ns, "dbc1-creds", map[string][]byte{
		"username": []byte("u"), "password": []byte("p"),
	}))
	if _, err := r.Reconcile(ctx, reqFor(ns, "dbc1")); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	got = getDBC(t, ctx, ns, "dbc1")
	if !isReady(got.Status.Conditions) {
		cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
		t.Fatalf("should be Ready after Secret created, got %+v", cond)
	}
}

// --- helpers (DBC-specific; shared helpers live in archiverule_controller_test.go) ---

func newSecret(ns, name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       data,
	}
}

func getDBC(t *testing.T, ctx context.Context, ns, name string) *aprv1alpha1.DatabaseConnection {
	t.Helper()
	var dbc aprv1alpha1.DatabaseConnection
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &dbc); err != nil {
		t.Fatalf("get DatabaseConnection: %v", err)
	}
	return &dbc
}
