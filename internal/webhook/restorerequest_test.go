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

func newRR(ns, name, ruleName string) *aprv1alpha1.RestoreRequest {
	return &aprv1alpha1.RestoreRequest{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: aprv1alpha1.RestoreRequestSpec{
			ArchiveRuleRef: corev1.LocalObjectReference{Name: ruleName},
			Date:           "2026-04-01",
		},
	}
}

func TestRestoreRequestValidator_HappyPath(t *testing.T) {
	scheme := newScheme(t)
	rule := newArchiveRule("ns1", "rule1", "dbc1", "sb1", "0 2 * * *")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rule).Build()
	v := &RestoreRequestValidator{Client: c}

	rr := newRR("ns1", "rr1", "rule1")
	if _, err := v.ValidateCreate(context.Background(), rr); err != nil {
		t.Fatalf("expected admission, got %v", err)
	}
}

func TestRestoreRequestValidator_MissingArchiveRule(t *testing.T) {
	scheme := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	v := &RestoreRequestValidator{Client: c}

	rr := newRR("ns1", "rr1", "missing-rule")
	_, err := v.ValidateCreate(context.Background(), rr)
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.archiveRuleRef.name") {
		t.Errorf("error should reference spec.archiveRuleRef.name: %v", err)
	}
}

func TestRestoreRequestValidator_SpecImmutableOnUpdate(t *testing.T) {
	scheme := newScheme(t)
	rule := newArchiveRule("ns1", "rule1", "dbc1", "sb1", "0 2 * * *")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rule).Build()
	v := &RestoreRequestValidator{Client: c}

	old := newRR("ns1", "rr1", "rule1")
	newer := old.DeepCopy()
	newer.Spec.Date = "2026-05-01"

	_, err := v.ValidateUpdate(context.Background(), old, newer)
	if err == nil {
		t.Fatal("expected rejection on spec mutation")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("error should mention immutability: %v", err)
	}
}

func TestRestoreRequestValidator_StatusOnlyUpdateAllowed(t *testing.T) {
	// A common Update is the operator/sink writing status. Spec is
	// unchanged, so it must be admitted.
	scheme := newScheme(t)
	rule := newArchiveRule("ns1", "rule1", "dbc1", "sb1", "0 2 * * *")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rule).Build()
	v := &RestoreRequestValidator{Client: c}

	old := newRR("ns1", "rr1", "rule1")
	newer := old.DeepCopy()
	newer.Status.RowsRestored = 42

	if _, err := v.ValidateUpdate(context.Background(), old, newer); err != nil {
		t.Errorf("status-only update should be admitted, got %v", err)
	}
}

func TestRestoreRequestValidator_AnnotationOnlyUpdateAllowed(t *testing.T) {
	// Adding metadata (e.g., annotations from another controller) is not
	// a spec mutation and must be admitted.
	scheme := newScheme(t)
	rule := newArchiveRule("ns1", "rule1", "dbc1", "sb1", "0 2 * * *")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rule).Build()
	v := &RestoreRequestValidator{Client: c}

	old := newRR("ns1", "rr1", "rule1")
	newer := old.DeepCopy()
	newer.Annotations = map[string]string{"custom.io/touched": "yes"}

	if _, err := v.ValidateUpdate(context.Background(), old, newer); err != nil {
		t.Errorf("annotation-only update should be admitted, got %v", err)
	}
}
