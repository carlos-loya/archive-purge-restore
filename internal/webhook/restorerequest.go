// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package webhook

import (
	"context"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// RestoreRequestValidator validates RestoreRequest CRs at admission time.
//
//   - The referenced ArchiveRule must exist in the same namespace.
//   - Spec is immutable after creation — restores are one-shot. Editing
//     spec mid-run would change which archived rows the Job is supposed
//     to restore, which is almost certainly a user mistake.
type RestoreRequestValidator struct {
	Client client.Reader
}

var _ admission.Validator[*aprv1alpha1.RestoreRequest] = (*RestoreRequestValidator)(nil)

func (v *RestoreRequestValidator) ValidateCreate(ctx context.Context, rr *aprv1alpha1.RestoreRequest) (admission.Warnings, error) {
	return nil, v.validateCreate(ctx, rr).ToAggregate()
}

func (v *RestoreRequestValidator) ValidateUpdate(ctx context.Context, oldRR, newRR *aprv1alpha1.RestoreRequest) (admission.Warnings, error) {
	return nil, v.validateUpdate(ctx, oldRR, newRR).ToAggregate()
}

func (v *RestoreRequestValidator) ValidateDelete(_ context.Context, _ *aprv1alpha1.RestoreRequest) (admission.Warnings, error) {
	return nil, nil
}

func (v *RestoreRequestValidator) validateCreate(ctx context.Context, rr *aprv1alpha1.RestoreRequest) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	if rr.Spec.ArchiveRuleRef.Name != "" {
		var rule aprv1alpha1.ArchiveRule
		key := types.NamespacedName{Namespace: rr.Namespace, Name: rr.Spec.ArchiveRuleRef.Name}
		if err := v.Client.Get(ctx, key, &rule); err != nil {
			if apierrors.IsNotFound(err) {
				errs = append(errs, field.NotFound(
					specPath.Child("archiveRuleRef").Child("name"),
					rr.Spec.ArchiveRuleRef.Name,
				))
			} else {
				errs = append(errs, field.InternalError(
					specPath.Child("archiveRuleRef"),
					err,
				))
			}
		}
	}

	return errs
}

func (v *RestoreRequestValidator) validateUpdate(ctx context.Context, oldRR, newRR *aprv1alpha1.RestoreRequest) field.ErrorList {
	var errs field.ErrorList

	if !reflect.DeepEqual(oldRR.Spec, newRR.Spec) {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec"),
			"RestoreRequest spec is immutable; create a new RestoreRequest to re-run a restore",
		))
	}

	// Defensive: even on update, re-check that the ref still resolves.
	// If a user deleted the ArchiveRule mid-update, surfacing that here
	// gives a clearer message than the reconciler later setting Failed.
	errs = append(errs, v.validateCreate(ctx, newRR)...)

	return errs
}
