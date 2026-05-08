// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
	"github.com/carlos-loya/archive-purge-restore/internal/metrics"
)

// emitArchiveMetricsForJob records a sample in the archive run metrics
// the FIRST time the reconciler observes a particular Job in a terminal
// state. Idempotency comes from the AnnotationMetricsEmitted marker the
// reconciler patches onto the Job after a successful emit.
//
// For successful Jobs we wait until the Job pod's sink has propagated
// the row count into rule.Status.LastJobRef / LastRunRowsArchived so the
// counter reflects the actual archive size. For failed Jobs we emit
// immediately — the sink may not have run, but we still want to count
// the failure.
//
// Returns nil if the Job isn't ready to be emitted yet (the caller will
// re-attempt on the next reconcile).
func emitArchiveMetricsForJob(
	ctx context.Context,
	c client.Client,
	rule *aprv1alpha1.ArchiveRule,
	job *batchv1.Job,
) error {
	if job == nil || !isJobFinished(job) {
		return nil
	}
	if job.Annotations[AnnotationMetricsEmitted] == "true" {
		return nil
	}

	var rows int64
	succeeded := didJobSucceed(job)
	if succeeded {
		// Only emit once the sink has caught up — otherwise we'd record
		// rows=0 and the dashboards would mislead.
		if rule.Status.LastJobRef == nil || rule.Status.LastJobRef.Name != job.Name {
			return nil
		}
		rows = rule.Status.LastRunRowsArchived
	}

	result := metrics.ResultSuccess
	if !succeeded {
		result = metrics.ResultFailure
	}
	metrics.RecordArchiveRun(rule.Name, result, jobDuration(job), rows)

	return annotateJobEmitted(ctx, c, job)
}

// emitRestoreMetricsForJob is the analogous helper for RestoreRequest
// Jobs. It uses RestoreRequest.Status.RowsRestored as the row count
// (sink-populated) and the same annotation watermark.
func emitRestoreMetricsForJob(
	ctx context.Context,
	c client.Client,
	rr *aprv1alpha1.RestoreRequest,
	job *batchv1.Job,
) error {
	if job == nil || !isJobFinished(job) {
		return nil
	}
	if job.Annotations[AnnotationMetricsEmitted] == "true" {
		return nil
	}

	var rows int64
	succeeded := didJobSucceed(job)
	if succeeded {
		// Wait for sink. RR has only one Job over its lifetime, so we
		// just check the sink has set RowsRestored at all (or is
		// expected to be zero — but a 0-row successful restore is
		// vanishingly rare in practice; we accept the wait).
		if rr.Status.CompletionTime == nil {
			return nil
		}
		rows = rr.Status.RowsRestored
	}

	result := metrics.ResultSuccess
	if !succeeded {
		result = metrics.ResultFailure
	}
	metrics.RecordRestoreRun(rr.Spec.ArchiveRuleRef.Name, result, jobDuration(job), rows)

	return annotateJobEmitted(ctx, c, job)
}

// jobDuration returns the wall-clock duration of a finished Job. For
// successful Jobs we use CompletionTime - StartTime. Failed Jobs don't
// have a CompletionTime in modern K8s, so we approximate using "now":
// it's the time from start until our reconciler first observes failure,
// which is close enough for dashboard use.
func jobDuration(job *batchv1.Job) time.Duration {
	if job.Status.StartTime == nil {
		return 0
	}
	end := time.Now()
	if job.Status.CompletionTime != nil {
		end = job.Status.CompletionTime.Time
	}
	d := end.Sub(job.Status.StartTime.Time)
	if d < 0 {
		return 0
	}
	return d
}

// annotateJobEmitted patches the Job with AnnotationMetricsEmitted so a
// re-reconcile of the same Job won't double-count.
func annotateJobEmitted(ctx context.Context, c client.Client, job *batchv1.Job) error {
	patch := client.MergeFrom(job.DeepCopy())
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[AnnotationMetricsEmitted] = "true"
	if err := c.Patch(ctx, job, patch); err != nil {
		return fmt.Errorf("annotating Job %s with metrics watermark: %w", job.Name, err)
	}
	return nil
}
