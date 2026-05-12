// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

// Package webhook contains validating admission webhook handlers for the
// APR custom resources. Each handler implements
// sigs.k8s.io/controller-runtime/pkg/webhook.CustomValidator so the
// manager can register it via builder.WebhookManagedBy.
//
// Validation falls into two buckets:
//
//  1. Schema-shaped checks that the CRD's OpenAPI v3 schema can't easily
//     express — cron expression parseability, RestoreRequest spec
//     immutability, type-specific Secret key presence.
//
//  2. Reference existence — the referenced DatabaseConnection /
//     StorageBackend / ArchiveRule / Secret must exist in the same
//     namespace at the time of admission. (Readiness, by contrast, is a
//     runtime concern and stays with the reconciler — webhooks fire
//     during create/update, often before the dependency is Ready.)
//
// Errors are returned as field.ErrorList aggregates so users see precise
// "spec.schedule: invalid cron expression \"garbage\"" messages from
// `kubectl apply`.
package webhook
