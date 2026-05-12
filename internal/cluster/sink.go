// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package cluster

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
	"github.com/carlos-loya/archive-purge-restore/internal/controller"
	"github.com/carlos-loya/archive-purge-restore/internal/engine"
)

// maxStatusRetries bounds how many times we'll re-read the CR and retry on a
// 409 Conflict. Conflicts are rare in practice (the operator and the Job pod
// touch different status fields) but a small bounded retry keeps us
// resilient.
const maxStatusRetries = 5

// RecordArchiveResult patches ArchiveRule.status with the outcome of an
// engine.RunArchive call. It writes the LastRun* fields and the Degraded
// condition. ScheduleValid, Progressing, Ready, ActiveJobRef, LastJobRef,
// and NextScheduledTime are owned by the operator's reconciler.
func RecordArchiveResult(
	ctx context.Context,
	c client.Client,
	namespace, name string,
	result *engine.RunResult,
	runErr error,
) error {
	return retryOnConflict(func() error {
		var rule aprv1alpha1.ArchiveRule
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &rule); err != nil {
			return err
		}
		applyArchiveResult(&rule, result, runErr)
		return c.Status().Update(ctx, &rule)
	})
}

func applyArchiveResult(rule *aprv1alpha1.ArchiveRule, result *engine.RunResult, runErr error) {
	if result == nil {
		result = &engine.RunResult{}
	}

	if !result.StartTime.IsZero() {
		t := metav1.NewTime(result.StartTime)
		rule.Status.LastRunTime = &t
	}
	rule.Status.LastRunID = result.RunID

	var rows int64
	var firstTableErr error
	for _, t := range result.Tables {
		rows += t.RowsArchived
		if t.Error != nil && firstTableErr == nil {
			firstTableErr = t.Error
		}
	}
	rule.Status.LastRunRowsArchived = rows

	switch {
	case runErr != nil:
		setCond(&rule.Status.Conditions, controller.ConditionDegraded, metav1.ConditionTrue,
			controller.ReasonLastRunFailed,
			fmt.Sprintf("archive run failed: %v", runErr),
			rule.Generation)
	case firstTableErr != nil:
		setCond(&rule.Status.Conditions, controller.ConditionDegraded, metav1.ConditionTrue,
			controller.ReasonLastRunFailed,
			fmt.Sprintf("at least one table failed to archive: %v", firstTableErr),
			rule.Generation)
	default:
		setCond(&rule.Status.Conditions, controller.ConditionDegraded, metav1.ConditionFalse,
			controller.ReasonLastRunSucceeded,
			fmt.Sprintf("archive run %q completed successfully (rows=%d)", result.RunID, rows),
			rule.Generation)
	}
}

// setCond is a thin wrapper over apimeta.SetStatusCondition kept local to
// the sink to avoid pulling more controller-internal helpers into this
// pod-side codepath.
func setCond(conds *[]metav1.Condition, condType string, status metav1.ConditionStatus, reason, message string, generation int64) {
	apimeta.SetStatusCondition(conds, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

// RecordRestoreResult patches RestoreRequest.status with the outcome of an
// engine.RunRestore call.
func RecordRestoreResult(
	ctx context.Context,
	c client.Client,
	namespace, name string,
	result *engine.RestoreResult,
	runErr error,
	startTime time.Time,
) error {
	return retryOnConflict(func() error {
		var rr aprv1alpha1.RestoreRequest
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &rr); err != nil {
			return err
		}
		applyRestoreResult(&rr, result, runErr, startTime)
		return c.Status().Update(ctx, &rr)
	})
}

// applyRestoreResult writes the per-run details of a restore. It does NOT
// touch the lifecycle conditions (Progressing / Succeeded / Failed) —
// those are owned by the operator's RestoreRequestReconciler, which
// derives them from observed Job state. This avoids a write race between
// the in-cluster reconciler and the Job pod.
func applyRestoreResult(
	rr *aprv1alpha1.RestoreRequest,
	result *engine.RestoreResult,
	_ error,
	startTime time.Time,
) {
	if result == nil {
		result = &engine.RestoreResult{}
	}

	st := metav1.NewTime(startTime)
	rr.Status.StartTime = &st
	now := metav1.Now()
	rr.Status.CompletionTime = &now

	var rows int64
	for _, t := range result.Tables {
		rows += t.RowsRestored
	}
	rr.Status.RowsRestored = rows
}

func retryOnConflict(fn func() error) error {
	var lastErr error
	for i := 0; i < maxStatusRetries; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("exhausted %d retries on status update: %w", maxStatusRetries, lastErr)
}
