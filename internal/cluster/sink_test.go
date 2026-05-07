// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package cluster

import (
	"errors"
	"testing"
	"time"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
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

	if rule.Status.LastRunResult != aprv1alpha1.ArchiveRunSucceeded {
		t.Errorf("LastRunResult = %q, want Succeeded", rule.Status.LastRunResult)
	}
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

	if rule.Status.LastRunResult != aprv1alpha1.ArchiveRunFailed {
		t.Errorf("LastRunResult = %q, want Failed", rule.Status.LastRunResult)
	}
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

	if rule.Status.LastRunResult != aprv1alpha1.ArchiveRunFailed {
		t.Errorf("any table error should mark the run Failed; got %q", rule.Status.LastRunResult)
	}
}

func TestApplyArchiveResult_NilResult(t *testing.T) {
	rule := &aprv1alpha1.ArchiveRule{}
	applyArchiveResult(rule, nil, errors.New("setup failed before engine ran"))
	if rule.Status.LastRunResult != aprv1alpha1.ArchiveRunFailed {
		t.Errorf("nil result with error should be Failed, got %q", rule.Status.LastRunResult)
	}
}

func TestApplyRestoreResult_WritesDetailsButNotPhase(t *testing.T) {
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
	// Sink must NOT touch Phase — that's the reconciler's responsibility.
	if rr.Status.Phase != "" {
		t.Errorf("Phase should be untouched by sink, got %q", rr.Status.Phase)
	}
}

func TestApplyRestoreResult_PhaseUntouchedOnError(t *testing.T) {
	rr := &aprv1alpha1.RestoreRequest{Status: aprv1alpha1.RestoreRequestStatus{Phase: aprv1alpha1.RestoreRunning}}
	applyRestoreResult(rr, nil, errors.New("database closed connection"), time.Now())
	if rr.Status.Phase != aprv1alpha1.RestoreRunning {
		t.Errorf("sink should never touch Phase, got %q", rr.Status.Phase)
	}
}
