// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

// Package metrics defines the Prometheus collectors APR exposes from its
// manager binary's /metrics endpoint. The package is K8s-unaware — it only
// depends on prometheus/client_golang — so the engine and the providers
// can call into it without breaking the engine isolation contract from
// docs/kubernetes-operator-plan.md.
//
// Collectors are global package variables. They accept Inc/Observe calls
// regardless of whether they've been registered with a Prometheus
// registry. Registration just makes them visible to scrapers; the CLI
// doesn't expose /metrics so its data points stay in-process and discard
// at exit, which is the desired behavior.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// ResultSuccess and ResultFailure are the only two values used in the
	// `result` label. Defining them as constants keeps cardinality bounded
	// and ensures dashboards can hard-code the labels.
	ResultSuccess = "success"
	ResultFailure = "failure"
)

var (
	// ArchiveRunsTotal counts archive runs that reached a terminal state.
	ArchiveRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "apr_archive_runs_total",
			Help: "Total number of completed archive runs by rule and result.",
		},
		[]string{"rule", "result"},
	)

	// ArchiveRunDuration tracks how long archive runs take. Buckets cover
	// 1s through ~1024s — large archive batches can run for many minutes.
	ArchiveRunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "apr_archive_run_duration_seconds",
			Help:    "Duration of archive runs from extract through delete-finalize.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 11),
		},
		[]string{"rule"},
	)

	// ArchiveRowsTotal is the cumulative row count successfully archived
	// per rule. Use rate() in dashboards to derive throughput.
	ArchiveRowsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "apr_archive_rows_total",
			Help: "Cumulative count of rows archived to storage, by rule.",
		},
		[]string{"rule"},
	)

	// RestoreRunsTotal counts restore runs that reached a terminal state.
	RestoreRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "apr_restore_runs_total",
			Help: "Total number of completed restore runs by rule and result.",
		},
		[]string{"rule", "result"},
	)

	RestoreRunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "apr_restore_run_duration_seconds",
			Help:    "Duration of restore runs.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 11),
		},
		[]string{"rule"},
	)

	RestoreRowsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "apr_restore_rows_total",
			Help: "Cumulative count of rows restored from storage, by rule.",
		},
		[]string{"rule"},
	)

	// StorageOpDuration tracks individual storage backend calls. Buckets
	// span 5ms through ~10s — most object-store ops are sub-second but
	// large multipart uploads can be slow.
	StorageOpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "apr_storage_operation_duration_seconds",
			Help:    "Duration of storage backend operations (Put/Get/Delete/List/etc).",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		},
		[]string{"type", "operation", "result"},
	)
)

// MustRegister registers all APR collectors with the given registerer.
// Call this from cmd/manager so the metrics appear at the manager's
// /metrics endpoint. The CLI never registers and hence never exposes
// metrics — the package-level Record* calls in the engine simply update
// in-memory counters that are discarded at process exit.
func MustRegister(reg prometheus.Registerer) {
	reg.MustRegister(
		ArchiveRunsTotal,
		ArchiveRunDuration,
		ArchiveRowsTotal,
		RestoreRunsTotal,
		RestoreRunDuration,
		RestoreRowsTotal,
		StorageOpDuration,
	)
}

// RecordArchiveRun is the convenience wrapper engine code calls when a
// run finishes. result must be one of ResultSuccess or ResultFailure.
// rows can be 0 (e.g., when the run failed before any rows were written).
func RecordArchiveRun(rule, result string, duration time.Duration, rows int64) {
	ArchiveRunsTotal.WithLabelValues(rule, result).Inc()
	ArchiveRunDuration.WithLabelValues(rule).Observe(duration.Seconds())
	if rows > 0 {
		ArchiveRowsTotal.WithLabelValues(rule).Add(float64(rows))
	}
}

// RecordRestoreRun is the convenience wrapper for restore completions.
func RecordRestoreRun(rule, result string, duration time.Duration, rows int64) {
	RestoreRunsTotal.WithLabelValues(rule, result).Inc()
	RestoreRunDuration.WithLabelValues(rule).Observe(duration.Seconds())
	if rows > 0 {
		RestoreRowsTotal.WithLabelValues(rule).Add(float64(rows))
	}
}

// ObserveStorageOp returns a function that, when called, records the
// elapsed time since this call. Pattern:
//
//	defer metrics.ObserveStorageOp("s3", "put")(&err)
//
// The returned closure inspects the *err pointer to derive the result
// label, so callers don't need to thread a separate result variable.
func ObserveStorageOp(typ, operation string) func(*error) {
	start := time.Now()
	return func(errp *error) {
		result := ResultSuccess
		if errp != nil && *errp != nil {
			result = ResultFailure
		}
		StorageOpDuration.WithLabelValues(typ, operation, result).
			Observe(time.Since(start).Seconds())
	}
}

// ResultOf returns "success" if err is nil, "failure" otherwise. Used by
// engine code to derive the result label from a returned error.
func ResultOf(err error) string {
	if err != nil {
		return ResultFailure
	}
	return ResultSuccess
}
