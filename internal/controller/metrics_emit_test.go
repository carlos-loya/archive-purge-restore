// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	aprmetrics "github.com/carlos-loya/archive-purge-restore/internal/metrics"
)

func TestArchiveRule_EmitsMetricsOnSuccess(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-metrics-success")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule-emit-success", "dbc1", "sb1"))

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule-emit-success")); err != nil {
		t.Fatal(err)
	}
	rule := getArchiveRule(t, ctx, ns, "rule-emit-success")

	// Stage what a successful archive looks like in cluster: a finished
	// Job AND the rule.Status.LastRunRowsArchived populated by the sink.
	job := newOwnedJob(t, "successful-archive-job", ns, rule)
	mustCreate(t, ctx, job)
	markJobSucceeded(t, ctx, job)

	rule.Status.LastJobRef = &corev1.LocalObjectReference{Name: job.Name}
	rule.Status.LastRunRowsArchived = 250
	if err := testClient.Status().Update(ctx, rule); err != nil {
		t.Fatalf("populating rule status as if sink ran: %v", err)
	}

	before := testutil.ToFloat64(aprmetrics.ArchiveRunsTotal.WithLabelValues("rule-emit-success", aprmetrics.ResultSuccess))
	beforeRows := testutil.ToFloat64(aprmetrics.ArchiveRowsTotal.WithLabelValues("rule-emit-success"))

	if _, err := r.Reconcile(ctx, reqFor(ns, "rule-emit-success")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if got := testutil.ToFloat64(aprmetrics.ArchiveRunsTotal.WithLabelValues("rule-emit-success", aprmetrics.ResultSuccess)); got-before != 1.0 {
		t.Errorf("ArchiveRunsTotal{success} delta = %v, want 1.0", got-before)
	}
	if got := testutil.ToFloat64(aprmetrics.ArchiveRowsTotal.WithLabelValues("rule-emit-success")); got-beforeRows != 250.0 {
		t.Errorf("ArchiveRowsTotal delta = %v, want 250", got-beforeRows)
	}

	// The Job must have been annotated so a re-reconcile doesn't re-emit.
	gotJob := getJob(t, ctx, ns, job.Name)
	if gotJob.Annotations[AnnotationMetricsEmitted] != "true" {
		t.Errorf("Job missing %s annotation: %v", AnnotationMetricsEmitted, gotJob.Annotations)
	}

	// Re-reconcile: count must not advance.
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule-emit-success")); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(aprmetrics.ArchiveRunsTotal.WithLabelValues("rule-emit-success", aprmetrics.ResultSuccess)); got-before != 1.0 {
		t.Errorf("metric re-emitted on second reconcile (got delta %v, want still 1.0)", got-before)
	}
}

func TestArchiveRule_EmitsMetricsOnFailure(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-metrics-fail")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule-emit-fail", "dbc1", "sb1"))

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule-emit-fail")); err != nil {
		t.Fatal(err)
	}
	rule := getArchiveRule(t, ctx, ns, "rule-emit-fail")

	job := newOwnedJob(t, "failed-archive-job", ns, rule)
	mustCreate(t, ctx, job)
	markJobFailed(t, ctx, job)

	before := testutil.ToFloat64(aprmetrics.ArchiveRunsTotal.WithLabelValues("rule-emit-fail", aprmetrics.ResultFailure))

	if _, err := r.Reconcile(ctx, reqFor(ns, "rule-emit-fail")); err != nil {
		t.Fatal(err)
	}

	if got := testutil.ToFloat64(aprmetrics.ArchiveRunsTotal.WithLabelValues("rule-emit-fail", aprmetrics.ResultFailure)); got-before != 1.0 {
		t.Errorf("ArchiveRunsTotal{failure} delta = %v, want 1.0", got-before)
	}

	gotJob := getJob(t, ctx, ns, job.Name)
	if gotJob.Annotations[AnnotationMetricsEmitted] != "true" {
		t.Errorf("Job missing %s annotation after failure emit", AnnotationMetricsEmitted)
	}
}

// --- helpers ---

func getJob(t *testing.T, ctx context.Context, ns, name string) *batchv1.Job {
	t.Helper()
	var job batchv1.Job
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &job); err != nil {
		t.Fatalf("get Job %s/%s: %v", ns, name, err)
	}
	return &job
}
