// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Tests use unique label values per test to keep counters isolated —
// the package-level collectors retain state across tests by design.

// TestMustRegister checks that every APR collector lands in the given
// registry. CounterVec/HistogramVec without observed children don't show
// up in Gather() output, so we discover them via Describe() — each
// collector advertises its descriptors regardless of whether any series
// exist yet.
func TestMustRegister(t *testing.T) {
	reg := prometheus.NewRegistry()
	MustRegister(reg)

	collectors := []prometheus.Collector{
		ArchiveRunsTotal, ArchiveRunDuration, ArchiveRowsTotal,
		RestoreRunsTotal, RestoreRunDuration, RestoreRowsTotal,
		StorageOpDuration,
	}
	for _, c := range collectors {
		// Attempting to re-register an already-registered collector
		// returns AlreadyRegisteredError. That's our positive signal:
		// the collector is in the registry.
		if err := reg.Register(c); err == nil {
			t.Errorf("collector %T was not registered by MustRegister", c)
		} else if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			t.Errorf("unexpected error re-registering: %v", err)
		}
	}
}

func TestRecordArchiveRun_Success(t *testing.T) {
	rule := "rule-archive-success"
	RecordArchiveRun(rule, ResultSuccess, 5*time.Second, 100)

	if got := testutil.ToFloat64(ArchiveRunsTotal.WithLabelValues(rule, ResultSuccess)); got != 1.0 {
		t.Errorf("ArchiveRunsTotal{success} = %v, want 1.0", got)
	}
	if got := testutil.ToFloat64(ArchiveRunsTotal.WithLabelValues(rule, ResultFailure)); got != 0.0 {
		t.Errorf("ArchiveRunsTotal{failure} = %v, want 0", got)
	}
	if got := testutil.ToFloat64(ArchiveRowsTotal.WithLabelValues(rule)); got != 100.0 {
		t.Errorf("ArchiveRowsTotal = %v, want 100", got)
	}
}

func TestRecordArchiveRun_FailureWithZeroRows(t *testing.T) {
	rule := "rule-archive-fail"
	RecordArchiveRun(rule, ResultFailure, 2*time.Second, 0)

	if got := testutil.ToFloat64(ArchiveRunsTotal.WithLabelValues(rule, ResultFailure)); got != 1.0 {
		t.Errorf("ArchiveRunsTotal{failure} = %v, want 1.0", got)
	}
	// Zero-row failures must not bump the rows counter; otherwise we'd
	// have a phantom 1-row contribution from every failed run.
	if got := testutil.ToFloat64(ArchiveRowsTotal.WithLabelValues(rule)); got != 0.0 {
		t.Errorf("ArchiveRowsTotal = %v, want 0 on zero-row failure", got)
	}
}

func TestRecordRestoreRun(t *testing.T) {
	rule := "rule-restore"
	RecordRestoreRun(rule, ResultSuccess, time.Second, 50)
	RecordRestoreRun(rule, ResultSuccess, time.Second, 25)

	if got := testutil.ToFloat64(RestoreRunsTotal.WithLabelValues(rule, ResultSuccess)); got != 2.0 {
		t.Errorf("RestoreRunsTotal = %v, want 2.0", got)
	}
	if got := testutil.ToFloat64(RestoreRowsTotal.WithLabelValues(rule)); got != 75.0 {
		t.Errorf("RestoreRowsTotal = %v, want 75", got)
	}
}

func TestObserveStorageOp(t *testing.T) {
	typ, op := "test-storage-success", "put"

	// Success path.
	end := ObserveStorageOp(typ, op)
	time.Sleep(time.Millisecond) // make a non-zero observation
	var nilErr error
	end(&nilErr)

	// We can't directly read histogram counts via testutil.ToFloat64
	// because histograms are not gauges, but CollectAndCount works for
	// the underlying observations counter.
	if got := testutil.CollectAndCount(StorageOpDuration); got == 0 {
		t.Errorf("expected at least one observation in StorageOpDuration")
	}

	// Failure path uses a different label combo so we can read it back.
	failOp := "put-fail"
	endFail := ObserveStorageOp(typ, failOp)
	failErr := errors.New("simulated")
	endFail(&failErr)

	// Histogram_count for the failure label triple should be 1.
	collected := collectMetricFor(StorageOpDuration, map[string]string{
		"type": typ, "operation": failOp, "result": ResultFailure,
	})
	if collected != 1 {
		t.Errorf("expected 1 observation under {type=%s, operation=%s, result=failure}, got %d",
			typ, failOp, collected)
	}
}

func TestResultOf(t *testing.T) {
	if got := ResultOf(nil); got != ResultSuccess {
		t.Errorf("ResultOf(nil) = %q, want success", got)
	}
	if got := ResultOf(errors.New("boom")); got != ResultFailure {
		t.Errorf("ResultOf(err) = %q, want failure", got)
	}
}

// collectMetricFor pulls the count of observations under a specific label
// set out of a HistogramVec. testutil doesn't have a direct helper for
// this, so we walk the dto representation.
func collectMetricFor(h *prometheus.HistogramVec, labels map[string]string) int {
	ch := make(chan prometheus.Metric, 100)
	go func() {
		h.Collect(ch)
		close(ch)
	}()
	for m := range ch {
		var dto prometheus_dto
		if err := metricLabels(m, &dto); err != nil {
			continue
		}
		match := true
		for k, v := range labels {
			if dto.labels[k] != v {
				match = false
				break
			}
		}
		if match {
			return dto.sampleCount
		}
	}
	return 0
}

// minimal slice of dto.Metric we need
type prometheus_dto struct {
	labels      map[string]string
	sampleCount int
}

func metricLabels(m prometheus.Metric, out *prometheus_dto) error {
	pb := &dtoMetricBridge{}
	if err := m.Write(pb.metric()); err != nil {
		return err
	}
	out.labels = pb.labelsAsMap()
	out.sampleCount = pb.histogramSampleCount()
	return nil
}
