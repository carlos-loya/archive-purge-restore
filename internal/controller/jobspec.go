// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	corev1 "k8s.io/api/core/v1"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// buildJobEnv assembles the env-var list shared between the archive CronJob
// and the restore Job. Names follow the ABI in common.go; Secret references
// are resolved by kubelet at pod start.
func buildJobEnv(
	namespace, crName string,
	dbc *aprv1alpha1.DatabaseConnection,
	sb *aprv1alpha1.StorageBackend,
) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: EnvCRNamespace, Value: namespace},
		{Name: EnvCRName, Value: crName},
		{
			Name: EnvDatabaseUsername,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: dbc.Spec.CredentialsSecretRef,
					Key:                  SecretKeyUsername,
				},
			},
		},
		{
			Name: EnvDatabasePassword,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: dbc.Spec.CredentialsSecretRef,
					Key:                  SecretKeyPassword,
				},
			},
		},
	}

	if sb.Spec.CredentialsSecretRef != nil {
		ref := *sb.Spec.CredentialsSecretRef
		switch sb.Spec.Type {
		case aprv1alpha1.StorageS3, aprv1alpha1.StorageR2:
			env = append(env,
				corev1.EnvVar{
					Name: EnvStorageAccessKeyID,
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: ref,
							Key:                  SecretKeyAccessKeyID,
						},
					},
				},
				corev1.EnvVar{
					Name: EnvStorageSecretAccessKey,
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: ref,
							Key:                  SecretKeySecretAccessKey,
						},
					},
				},
			)
		case aprv1alpha1.StorageGCS:
			env = append(env, corev1.EnvVar{
				Name: EnvStorageServiceAccountJSON,
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: ref,
						Key:                  SecretKeyServiceAccountJSON,
					},
				},
			})
		}
	}

	return env
}
