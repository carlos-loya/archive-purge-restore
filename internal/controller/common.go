// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

// Env-var ABI between the operator and the `apr` binary running inside
// CronJob/Job pods. The reconciler projects Secret keys into these env vars
// when constructing pod specs; the `apr archive --from-cr` (and equivalent
// restore) subcommand reads them when building its engine config.
//
// Keep these in sync with the implementation in cmd/apr.
const (
	// EnvCRNamespace and EnvCRName identify the ArchiveRule (or
	// RestoreRequest) the pod was launched for. The pod uses these to
	// fetch the source-of-truth CR from the API server at startup.
	EnvCRNamespace = "APR_NAMESPACE"
	EnvCRName      = "APR_NAME"

	EnvDatabaseUsername = "APR_DATABASE_USERNAME"
	EnvDatabasePassword = "APR_DATABASE_PASSWORD"

	// S3/R2 credentials use AWS-standard names so the AWS SDK picks them
	// up without any rewriting inside the binary.
	EnvStorageAccessKeyID     = "AWS_ACCESS_KEY_ID"
	EnvStorageSecretAccessKey = "AWS_SECRET_ACCESS_KEY"

	// GCS service-account JSON is projected as a string env var; the
	// binary writes it to a temp file at startup and points
	// GOOGLE_APPLICATION_CREDENTIALS at that file.
	EnvStorageServiceAccountJSON = "APR_STORAGE_SERVICE_ACCOUNT_JSON"
)

// Secret keys we expect to read from the user-supplied Secret objects
// referenced by DatabaseConnection and StorageBackend.
const (
	SecretKeyUsername           = "username"
	SecretKeyPassword           = "password"
	SecretKeyAccessKeyID        = "access_key_id"
	SecretKeySecretAccessKey    = "secret_access_key"
	SecretKeyServiceAccountJSON = "service_account_json"
)

// Standard label keys applied to operator-managed objects.
const (
	LabelAppName     = "app.kubernetes.io/name"
	LabelManagedBy   = "app.kubernetes.io/managed-by"
	LabelArchiveRule = "apr.dev/archive-rule"

	AppName       = "apr"
	ManagerName   = "apr-manager"
	cronJobPrefix = "archiverule-"
)

// AnnotationMetricsEmitted is set on a Job after the reconciler has
// observed it terminate and emitted its archive/restore_runs_total
// counter. Used as a per-Job watermark so re-reconciles of the same
// finished Job don't double-count.
const AnnotationMetricsEmitted = "apr.dev/metrics-emitted"

// Standard condition types and reasons used across reconcilers.
const (
	ConditionReady = "Ready"

	ReasonReady                      = "Ready"
	ReasonDatabaseConnectionNotFound = "DatabaseConnectionNotFound"
	ReasonStorageBackendNotFound     = "StorageBackendNotFound"
	ReasonArchiveRuleNotFound        = "ArchiveRuleNotFound"
	ReasonInvalidSchedule            = "InvalidSchedule"
	ReasonJobReconcileError          = "JobReconcileError"
	ReasonMaxFailuresReached         = "MaxFailuresReached"
	ReasonSuspended                  = "Suspended"
	ReasonSecretNotFound             = "SecretNotFound"
	ReasonSecretMissingKeys          = "SecretMissingKeys"
	ReasonNotImplemented             = "NotImplemented"
)

// Defaults applied to ArchiveRule.spec when the user leaves fields zero.
const (
	DefaultMaxFailures  = 5
	DefaultHistoryLimit = 5
)
