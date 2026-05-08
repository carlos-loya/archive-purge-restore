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
)

func newSecret(ns, name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Data:       data,
	}
}

func TestDatabaseConnectionValidator_HappyPath(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newSecret("ns1", "dbc1-creds", map[string][]byte{
			"username": []byte("admin"),
			"password": []byte("hunter2"),
		})).
		Build()
	v := &DatabaseConnectionValidator{Client: c}

	if _, err := v.ValidateCreate(context.Background(), newDBC("ns1", "dbc1")); err != nil {
		t.Fatalf("expected admission, got %v", err)
	}
}

func TestDatabaseConnectionValidator_SecretMissing(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	v := &DatabaseConnectionValidator{Client: c}

	_, err := v.ValidateCreate(context.Background(), newDBC("ns1", "dbc1"))
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "credentialsSecretRef") {
		t.Errorf("error should reference credentialsSecretRef: %v", err)
	}
}

func TestDatabaseConnectionValidator_SecretMissingPassword(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newSecret("ns1", "dbc1-creds", map[string][]byte{
			"username": []byte("admin"),
		})).
		Build()
	v := &DatabaseConnectionValidator{Client: c}

	_, err := v.ValidateCreate(context.Background(), newDBC("ns1", "dbc1"))
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), `"password"`) {
		t.Errorf("error should name the missing password key: %v", err)
	}
}

func TestDatabaseConnectionValidator_SecretEmptyValueIsMissing(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newSecret("ns1", "dbc1-creds", map[string][]byte{
			"username": []byte("admin"),
			"password": []byte(""),
		})).
		Build()
	v := &DatabaseConnectionValidator{Client: c}

	_, err := v.ValidateCreate(context.Background(), newDBC("ns1", "dbc1"))
	if err == nil {
		t.Fatal("empty password should be rejected as a missing key")
	}
}
