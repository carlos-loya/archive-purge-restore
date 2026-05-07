// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package webhook

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

func newSBOfType(ns, name string, typ aprv1alpha1.StorageType) *aprv1alpha1.StorageBackend {
	sb := &aprv1alpha1.StorageBackend{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: aprv1alpha1.StorageBackendSpec{
			Type:   typ,
			Bucket: "test-bucket",
			Region: "us-west-2",
		},
	}
	if typ != aprv1alpha1.StorageFilesystem {
		sb.Spec.CredentialsSecretRef = &corev1.LocalObjectReference{Name: name + "-creds"}
	}
	if typ == aprv1alpha1.StorageR2 {
		sb.Spec.AccountID = "abc123"
	}
	return sb
}

func TestStorageBackendValidator_S3HappyPath(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newSecret("ns1", "sb1-creds", map[string][]byte{
			"access_key_id":     []byte("AKIA..."),
			"secret_access_key": []byte("secret"),
		})).
		Build()
	v := &StorageBackendValidator{Client: c}

	if _, err := v.ValidateCreate(context.Background(), newSBOfType("ns1", "sb1", aprv1alpha1.StorageS3)); err != nil {
		t.Fatalf("expected admission, got %v", err)
	}
}

func TestStorageBackendValidator_FilesystemNeedsNoSecret(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	v := &StorageBackendValidator{Client: c}

	sb := newSBOfType("ns1", "sb1", aprv1alpha1.StorageFilesystem)
	if _, err := v.ValidateCreate(context.Background(), sb); err != nil {
		t.Fatalf("filesystem backend should be admitted with no Secret, got %v", err)
	}
}

func TestStorageBackendValidator_S3MissingSecret(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	v := &StorageBackendValidator{Client: c}

	_, err := v.ValidateCreate(context.Background(), newSBOfType("ns1", "sb1", aprv1alpha1.StorageS3))
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "credentialsSecretRef") {
		t.Errorf("error should reference credentialsSecretRef: %v", err)
	}
}

func TestStorageBackendValidator_S3MissingKeys(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newSecret("ns1", "sb1-creds", map[string][]byte{
			"access_key_id": []byte("AKIA..."),
		})).
		Build()
	v := &StorageBackendValidator{Client: c}

	_, err := v.ValidateCreate(context.Background(), newSBOfType("ns1", "sb1", aprv1alpha1.StorageS3))
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), `"secret_access_key"`) {
		t.Errorf("error should name the missing secret_access_key: %v", err)
	}
}

func TestStorageBackendValidator_GCSWantsServiceAccountJSON(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newSecret("ns1", "sb1-creds", map[string][]byte{
			"wrong-key": []byte("foo"),
		})).
		Build()
	v := &StorageBackendValidator{Client: c}

	_, err := v.ValidateCreate(context.Background(), newSBOfType("ns1", "sb1", aprv1alpha1.StorageGCS))
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), `"service_account_json"`) {
		t.Errorf("error should name service_account_json: %v", err)
	}
}

func TestStorageBackendValidator_R2NeedsAccountID(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newSecret("ns1", "sb1-creds", map[string][]byte{
			"access_key_id":     []byte("k"),
			"secret_access_key": []byte("s"),
		})).
		Build()
	v := &StorageBackendValidator{Client: c}

	sb := newSBOfType("ns1", "sb1", aprv1alpha1.StorageR2)
	sb.Spec.AccountID = ""
	_, err := v.ValidateCreate(context.Background(), sb)
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "accountID") {
		t.Errorf("error should reference spec.accountID: %v", err)
	}
}
