// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package webhook

import (
	"context"
	"fmt"

	"github.com/robfig/cron/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// ArchiveRuleValidator validates ArchiveRule CRs at admission time.
//
// Most field-shape checks are handled by the CRD's OpenAPI schema; this
// validator covers what the schema can't:
//
//   - The cron expression in spec.schedule must parse via robfig/cron.
//   - The DatabaseConnection and StorageBackend referenced by spec must
//     exist in the same namespace at the moment of admission.
type ArchiveRuleValidator struct {
	Client client.Reader
}

// Compile-time check that we implement the controller-runtime contract.
var _ admission.Validator[*aprv1alpha1.ArchiveRule] = (*ArchiveRuleValidator)(nil)

func (v *ArchiveRuleValidator) ValidateCreate(ctx context.Context, rule *aprv1alpha1.ArchiveRule) (admission.Warnings, error) {
	return nil, v.validate(ctx, rule).ToAggregate()
}

func (v *ArchiveRuleValidator) ValidateUpdate(ctx context.Context, _, newRule *aprv1alpha1.ArchiveRule) (admission.Warnings, error) {
	return nil, v.validate(ctx, newRule).ToAggregate()
}

func (v *ArchiveRuleValidator) ValidateDelete(_ context.Context, _ *aprv1alpha1.ArchiveRule) (admission.Warnings, error) {
	return nil, nil
}

func (v *ArchiveRuleValidator) validate(ctx context.Context, rule *aprv1alpha1.ArchiveRule) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	if _, err := cron.ParseStandard(rule.Spec.Schedule); err != nil {
		errs = append(errs, field.Invalid(
			specPath.Child("schedule"),
			rule.Spec.Schedule,
			fmt.Sprintf("invalid cron expression: %v", err),
		))
	}

	if rule.Spec.DatabaseRef.Name != "" {
		var dbc aprv1alpha1.DatabaseConnection
		key := types.NamespacedName{Namespace: rule.Namespace, Name: rule.Spec.DatabaseRef.Name}
		if err := v.Client.Get(ctx, key, &dbc); err != nil {
			if apierrors.IsNotFound(err) {
				errs = append(errs, field.NotFound(
					specPath.Child("databaseRef").Child("name"),
					rule.Spec.DatabaseRef.Name,
				))
			} else {
				errs = append(errs, field.InternalError(
					specPath.Child("databaseRef"),
					fmt.Errorf("looking up DatabaseConnection: %w", err),
				))
			}
		}
	}

	if rule.Spec.StorageRef.Name != "" {
		var sb aprv1alpha1.StorageBackend
		key := types.NamespacedName{Namespace: rule.Namespace, Name: rule.Spec.StorageRef.Name}
		if err := v.Client.Get(ctx, key, &sb); err != nil {
			if apierrors.IsNotFound(err) {
				errs = append(errs, field.NotFound(
					specPath.Child("storageRef").Child("name"),
					rule.Spec.StorageRef.Name,
				))
			} else {
				errs = append(errs, field.InternalError(
					specPath.Child("storageRef"),
					fmt.Errorf("looking up StorageBackend: %w", err),
				))
			}
		}
	}

	return errs
}
