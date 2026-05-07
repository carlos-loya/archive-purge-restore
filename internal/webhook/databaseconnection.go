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

// DatabaseConnectionValidator validates DatabaseConnection CRs:
//
//   - The Secret named in spec.credentialsSecretRef must exist in the same
//     namespace and contain non-empty `username` and `password` keys.
type DatabaseConnectionValidator struct {
	Client client.Reader
}

const (
	dbcSecretKeyUsername = "username"
	dbcSecretKeyPassword = "password"
)

var _ admission.Validator[*aprv1alpha1.DatabaseConnection] = (*DatabaseConnectionValidator)(nil)

func (v *DatabaseConnectionValidator) ValidateCreate(ctx context.Context, dbc *aprv1alpha1.DatabaseConnection) (admission.Warnings, error) {
	return nil, v.validate(ctx, dbc).ToAggregate()
}

func (v *DatabaseConnectionValidator) ValidateUpdate(ctx context.Context, _, newDBC *aprv1alpha1.DatabaseConnection) (admission.Warnings, error) {
	return nil, v.validate(ctx, newDBC).ToAggregate()
}

func (v *DatabaseConnectionValidator) ValidateDelete(_ context.Context, _ *aprv1alpha1.DatabaseConnection) (admission.Warnings, error) {
	return nil, nil
}

func (v *DatabaseConnectionValidator) validate(ctx context.Context, dbc *aprv1alpha1.DatabaseConnection) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	if dbc.Spec.CredentialsSecretRef.Name == "" {
		// CRD already enforces required field, but defensively guard.
		return errs
	}

	var secret corev1.Secret
	key := types.NamespacedName{Namespace: dbc.Namespace, Name: dbc.Spec.CredentialsSecretRef.Name}
	if err := v.Client.Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			errs = append(errs, field.NotFound(
				specPath.Child("credentialsSecretRef").Child("name"),
				dbc.Spec.CredentialsSecretRef.Name,
			))
			return errs
		}
		errs = append(errs, field.InternalError(
			specPath.Child("credentialsSecretRef"),
			err,
		))
		return errs
	}

	for _, k := range []string{dbcSecretKeyUsername, dbcSecretKeyPassword} {
		if v, ok := secret.Data[k]; !ok || len(v) == 0 {
			errs = append(errs, field.Invalid(
				specPath.Child("credentialsSecretRef").Child("name"),
				dbc.Spec.CredentialsSecretRef.Name,
				fmt.Sprintf("Secret is missing required key %q", k),
			))
		}
	}
	return errs
}
