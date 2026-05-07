// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package webhook

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// StorageBackendValidator validates StorageBackend CRs:
//
//   - r2 backends must set spec.accountID.
//   - When the backend type requires credentials (s3, r2, gcs), the
//     referenced Secret must exist with type-appropriate keys.
//   - Filesystem backends require no Secret; if one is provided, we
//     ignore it.
type StorageBackendValidator struct {
	Client client.Reader
}

const (
	sbSecretKeyAccessKeyID        = "access_key_id"
	sbSecretKeySecretAccessKey    = "secret_access_key"
	sbSecretKeyServiceAccountJSON = "service_account_json"
)

var _ admission.Validator[*aprv1alpha1.StorageBackend] = (*StorageBackendValidator)(nil)

func (v *StorageBackendValidator) ValidateCreate(ctx context.Context, sb *aprv1alpha1.StorageBackend) (admission.Warnings, error) {
	return nil, v.validate(ctx, sb).ToAggregate()
}

func (v *StorageBackendValidator) ValidateUpdate(ctx context.Context, _, newSB *aprv1alpha1.StorageBackend) (admission.Warnings, error) {
	return nil, v.validate(ctx, newSB).ToAggregate()
}

func (v *StorageBackendValidator) ValidateDelete(_ context.Context, _ *aprv1alpha1.StorageBackend) (admission.Warnings, error) {
	return nil, nil
}

func (v *StorageBackendValidator) validate(ctx context.Context, sb *aprv1alpha1.StorageBackend) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	requiredKeys := requiredSecretKeysFor(sb.Spec.Type)

	if sb.Spec.Type == aprv1alpha1.StorageR2 && sb.Spec.AccountID == "" {
		errs = append(errs, field.Required(
			specPath.Child("accountID"),
			"required when spec.type is r2",
		))
	}

	if len(requiredKeys) == 0 {
		// Filesystem (or other future no-secret types): nothing more to
		// check. Bucket non-emptiness is enforced by the CRD.
		return errs
	}

	if sb.Spec.CredentialsSecretRef == nil {
		errs = append(errs, field.Required(
			specPath.Child("credentialsSecretRef"),
			fmt.Sprintf("required when spec.type is %q", sb.Spec.Type),
		))
		return errs
	}

	var secret corev1.Secret
	key := types.NamespacedName{Namespace: sb.Namespace, Name: sb.Spec.CredentialsSecretRef.Name}
	if err := v.Client.Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			errs = append(errs, field.NotFound(
				specPath.Child("credentialsSecretRef").Child("name"),
				sb.Spec.CredentialsSecretRef.Name,
			))
			return errs
		}
		errs = append(errs, field.InternalError(
			specPath.Child("credentialsSecretRef"),
			err,
		))
		return errs
	}

	for _, k := range requiredKeys {
		if v, ok := secret.Data[k]; !ok || len(v) == 0 {
			errs = append(errs, field.Invalid(
				specPath.Child("credentialsSecretRef").Child("name"),
				sb.Spec.CredentialsSecretRef.Name,
				fmt.Sprintf("Secret is missing required key %q", k),
			))
		}
	}
	return errs
}

func requiredSecretKeysFor(typ aprv1alpha1.StorageType) []string {
	switch typ {
	case aprv1alpha1.StorageS3, aprv1alpha1.StorageR2:
		return []string{sbSecretKeyAccessKeyID, sbSecretKeySecretAccessKey}
	case aprv1alpha1.StorageGCS:
		return []string{sbSecretKeyServiceAccountJSON}
	default:
		return nil
	}
}
