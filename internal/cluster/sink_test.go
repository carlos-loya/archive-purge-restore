// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package cluster

import (
	"errors"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
	"github.com/carlos-loya/archive-purge-restore/internal/controller"
	"github.com/carlos-loya/archive-purge-restore/internal/engine"
)

func TestApplyArchiveResult_Success(t *testing.T) {
	rule := &aprv1alpha1.ArchiveRule{}
	start := time.Date(2026, 5, 6, 14, 0, 0, 0, time.UTC)
	result := &engine.RunResult{
		RunID:     "abc12345",
		StartTime: start,
		Tables: []engine.ArchiveResult{
			{Table: "orders", RowsArchived: 100},
			{Table: "items", RowsArchived: 50},
		},
	}
	applyArchiveResult(rule, result, nil)

	requireDegraded(t, rule.Status.Conditions, metav1.ConditionFalse, controller.ReasonLastRunSucceeded)
	if rule.Status.LastRunRowsArchived != 150 {
		t.Errorf("LastRunRowsArchived = %d, want 150", rule.Status.LastRunRowsArchived)
	}
	if rule.Status.LastRunID != "abc12345" {
		t.Errorf("LastRunID = %q, want abc12345", rule.Status.LastRunID)
	}
	if rule.Status.LastRunTime == nil || !rule.Status.LastRunTime.Time.Equal(start) {
		t.Errorf("LastRunTime not set correctly: %v", rule.Status.LastRunTime)
	}
}

func TestApplyArchiveResult_RunError(t *testing.T) {
	rule := &aprv1alpha1.ArchiveRule{}
	result := &engine.RunResult{
		RunID:  "xyz",
		Tables: []engine.ArchiveResult{{Table: "orders", RowsArchived: 50}},
	}
	applyArchiveResult(rule, result, errors.New("connection refused"))

	requireDegraded(t, rule.Status.Conditions, metav1.ConditionTrue, controller.ReasonLastRunFailed)
	if rule.Status.LastRunRowsArchived != 50 {
		t.Errorf("partial row count not preserved: %d", rule.Status.LastRunRowsArchived)
	}
}

func TestApplyArchiveResult_PerTableError(t *testing.T) {
	rule := &aprv1alpha1.ArchiveRule{}
	result := &engine.RunResult{
		RunID: "xyz",
		Tables: []engine.ArchiveResult{
			{Table: "orders", RowsArchived: 100},
			{Table: "items", Error: errors.New("disk full")},
		},
	}
	applyArchiveResult(rule, result, nil)

	requireDegraded(t, rule.Status.Conditions, metav1.ConditionTrue, controller.ReasonLastRunFailed)
}

func TestApplyArchiveResult_NilResult(t *testing.T) {
	rule := &aprv1alpha1.ArchiveRule{}
	applyArchiveResult(rule, nil, errors.New("setup failed before engine ran"))
	requireDegraded(t, rule.Status.Conditions, metav1.ConditionTrue, controller.ReasonLastRunFailed)
}

func TestApplyRestoreResult_WritesDetailsButNotConditions(t *testing.T) {
	rr := &aprv1alpha1.RestoreRequest{}
	start := time.Date(2026, 5, 6, 14, 0, 0, 0, time.UTC)
	result := &engine.RestoreResult{
		Tables: []engine.RestoreTableResult{
			{Table: "orders", RowsRestored: 200},
		},
	}
	applyRestoreResult(rr, result, nil, start)

	if rr.Status.RowsRestored != 200 {
		t.Errorf("RowsRestored = %d, want 200", rr.Status.RowsRestored)
	}
	if rr.Status.StartTime == nil || !rr.Status.StartTime.Time.Equal(start) {
		t.Errorf("StartTime mismatch: %v", rr.Status.StartTime)
	}
	if rr.Status.CompletionTime == nil {
		t.Error("CompletionTime not set")
	}
	// Sink must NOT touch lifecycle conditions — that's the reconciler's
	// responsibility (avoids a write race with the Job pod).
	for _, ct := range []string{controller.ConditionProgressing, controller.ConditionSucceeded, controller.ConditionFailed} {
		if c := apimeta.FindStatusCondition(rr.Status.Conditions, ct); c != nil {
			t.Errorf("sink should not touch %s, got %+v", ct, c)
		}
	}
}

func TestApplyRestoreResult_ConditionsUntouchedOnError(t *testing.T) {
	rr := &aprv1alpha1.RestoreRequest{
		Status: aprv1alpha1.RestoreRequestStatus{
			Conditions: []metav1.Condition{{
				Type:   controller.ConditionProgressing,
				Status: metav1.ConditionTrue,
				Reason: controller.ReasonRestoreRunning,
			}},
		},
	}
	applyRestoreResult(rr, nil, errors.New("database closed connection"), time.Now())
	c := apimeta.FindStatusCondition(rr.Status.Conditions, controller.ConditionProgressing)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("sink should never touch Progressing, got %+v", c)
	}
}

func requireDegraded(t *testing.T, conds []metav1.Condition, status metav1.ConditionStatus, reason string) {
	t.Helper()
	c := apimeta.FindStatusCondition(conds, controller.ConditionDegraded)
	if c == nil {
		t.Fatalf("Degraded condition not set; conds=%+v", conds)
	}
	if c.Status != status || c.Reason != reason {
		t.Fatalf("Degraded = {Status:%s, Reason:%s}, want {Status:%s, Reason:%s}",
			c.Status, c.Reason, status, reason)
	}
}
