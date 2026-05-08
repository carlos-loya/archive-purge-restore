// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

func TestSB_FilesystemNeedsNoSecret(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "sb-fs")

	sb := newStorageBackend(ns, "sb1")
	sb.Spec.Type = aprv1alpha1.StorageFilesystem
	sb.Spec.Bucket = "/tmp/apr-archives"
	sb.Spec.CredentialsSecretRef = nil
	mustCreate(t, ctx, sb)

	r := &StorageBackendReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "sb1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getSB(t, ctx, ns, "sb1")
	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("filesystem backend without Secret should be Ready; got %+v", cond)
	}
}

func TestSB_S3RequiresSecret(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "sb-s3-no-secret")

	sb := newStorageBackend(ns, "sb1")
	mustCreate(t, ctx, sb)

	r := &StorageBackendReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "sb1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getSB(t, ctx, ns, "sb1")
	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Reason != ReasonSecretNotFound {
		t.Fatalf("expected %s, got %+v", ReasonSecretNotFound, cond)
	}
}

func TestSB_S3SecretMissingKeys(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "sb-s3-bad-keys")

	mustCreate(t, ctx, newSecret(ns, "sb1-creds", map[string][]byte{"access_key_id": []byte("AKIA...")}))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))

	r := &StorageBackendReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "sb1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getSB(t, ctx, ns, "sb1")
	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Reason != ReasonSecretMissingKeys {
		t.Fatalf("expected %s, got %+v", ReasonSecretMissingKeys, cond)
	}
}

func TestSB_S3HappyPath(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "sb-s3-happy")

	mustCreate(t, ctx, newSecret(ns, "sb1-creds", map[string][]byte{
		"access_key_id":     []byte("AKIA..."),
		"secret_access_key": []byte("secret"),
	}))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))

	r := &StorageBackendReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "sb1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getSB(t, ctx, ns, "sb1")
	if !isReady(got.Status.Conditions) {
		cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
		t.Fatalf("expected Ready=True, got %+v", cond)
	}
}

func TestSB_GCSWantsServiceAccountJSON(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "sb-gcs")

	mustCreate(t, ctx, newSecret(ns, "sb1-creds", map[string][]byte{"wrong_key": []byte("foo")}))
	sb := newStorageBackend(ns, "sb1")
	sb.Spec.Type = aprv1alpha1.StorageGCS
	mustCreate(t, ctx, sb)

	r := &StorageBackendReconciler{Client: testClient, Scheme: testScheme}
	if _, err := r.Reconcile(ctx, reqFor(ns, "sb1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getSB(t, ctx, ns, "sb1")
	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Reason != ReasonSecretMissingKeys {
		t.Fatalf("expected %s, got %+v", ReasonSecretMissingKeys, cond)
	}
}

func getSB(t *testing.T, ctx context.Context, ns, name string) *aprv1alpha1.StorageBackend {
	t.Helper()
	var sb aprv1alpha1.StorageBackend
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &sb); err != nil {
		t.Fatalf("get StorageBackend: %v", err)
	}
	return &sb
}
