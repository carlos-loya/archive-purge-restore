// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

func TestRR_MissingArchiveRule(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "rr-no-rule")

	mustCreate(t, ctx, newRestoreRequest(ns, "rr1", "missing-rule"))

	r := newRestoreRequestReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rr1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getRR(t, ctx, ns, "rr1")
	if got.Status.Phase != aprv1alpha1.RestoreFailed {
		t.Errorf("Phase = %q, want Failed", got.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Reason != ReasonArchiveRuleNotFound {
		t.Errorf("expected Ready=False/%s, got %+v", ReasonArchiveRuleNotFound, cond)
	}
}

func TestRR_HappyPathCreatesJob(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "rr-happy")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))
	mustCreate(t, ctx, newRestoreRequest(ns, "rr1", "rule1"))

	r := newRestoreRequestReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rr1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getRR(t, ctx, ns, "rr1")
	if got.Status.JobRef == nil {
		t.Fatal("expected status.jobRef set after successful reconcile")
	}
	if got.Status.Phase != aprv1alpha1.RestorePending {
		t.Errorf("Phase = %q, want Pending (Job hasn't started)", got.Status.Phase)
	}

	var job batchv1.Job
	jobKey := types.NamespacedName{Namespace: ns, Name: got.Status.JobRef.Name}
	if err := testClient.Get(ctx, jobKey, &job); err != nil {
		t.Fatalf("get Job: %v", err)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].Name != "rr1" || job.OwnerReferences[0].Kind != "RestoreRequest" {
		t.Errorf("unexpected owner refs: %+v", job.OwnerReferences)
	}
	container := job.Spec.Template.Spec.Containers[0]
	wantArgs := []string{"restore", "--from-cr", ns + "/rr1"}
	if !equalSlices(container.Args, wantArgs) {
		t.Errorf("container args = %v, want %v", container.Args, wantArgs)
	}
}

func TestRR_JobImmutableOnSecondReconcile(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "rr-immutable")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))
	mustCreate(t, ctx, newRestoreRequest(ns, "rr1", "rule1"))

	r := newRestoreRequestReconciler()
	req := reqFor(ns, "rr1")
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	// Both reconciles should leave the same Job in place — no churn.
	var jobs batchv1.JobList
	if err := testClient.List(ctx, &jobs, client.InNamespace(ns)); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Errorf("expected exactly one Job, got %d", len(jobs.Items))
	}
}

func TestRR_PhaseReflectsJobSuccess(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "rr-success")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))
	mustCreate(t, ctx, newRestoreRequest(ns, "rr1", "rule1"))

	r := newRestoreRequestReconciler()
	req := reqFor(ns, "rr1")
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// Simulate the Job completing successfully.
	rr := getRR(t, ctx, ns, "rr1")
	var job batchv1.Job
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rr.Status.JobRef.Name}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	job.Status.Succeeded = 1
	if err := testClient.Status().Update(ctx, &job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("reconcile after Job success: %v", err)
	}
	got := getRR(t, ctx, ns, "rr1")
	if got.Status.Phase != aprv1alpha1.RestoreSucceeded {
		t.Errorf("Phase = %q, want Succeeded after Job.status.Succeeded > 0", got.Status.Phase)
	}
}

// --- helpers ---

func newRestoreRequestReconciler() *RestoreRequestReconciler {
	return &RestoreRequestReconciler{
		Client:       testClient,
		Scheme:       testScheme,
		ArchiveImage: "test/apr:dev",
		RunnerSA:     "default",
	}
}

func newRestoreRequest(ns, name, ruleName string) *aprv1alpha1.RestoreRequest {
	return &aprv1alpha1.RestoreRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aprv1alpha1.RestoreRequestSpec{
			ArchiveRuleRef: corev1.LocalObjectReference{Name: ruleName},
			Date:           "2026-04-01",
		},
	}
}

func getRR(t *testing.T, ctx context.Context, ns, name string) *aprv1alpha1.RestoreRequest {
	t.Helper()
	var rr aprv1alpha1.RestoreRequest
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &rr); err != nil {
		t.Fatalf("get RestoreRequest: %v", err)
	}
	return &rr
}
