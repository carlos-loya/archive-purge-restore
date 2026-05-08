// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// StorageBackendReconciler reconciles a StorageBackend by validating its
// referenced Secret (when one is required for the backend type) has the
// expected keys.
//
// Filesystem backends require no Secret. S3/R2 expect access_key_id +
// secret_access_key. GCS expects service_account_json.
//
// As with DatabaseConnection, true reachability probing (a HEAD against the
// bucket) is deferred to keep the manager image small. Connection issues
// will surface on the first archive Job.
type StorageBackendReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apr.dev,resources=storagebackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apr.dev,resources=storagebackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apr.dev,resources=storagebackends/finalizers,verbs=update

func (r *StorageBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var sb aprv1alpha1.StorageBackend
	if err := r.Get(ctx, req.NamespacedName, &sb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	res, err := r.validate(ctx, &sb)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Status().Update(ctx, &sb); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	logger.V(1).Info("reconciled StorageBackend",
		"type", sb.Spec.Type, "bucket", sb.Spec.Bucket, "ready", isReady(sb.Status.Conditions))
	return res, nil
}

func (r *StorageBackendReconciler) validate(ctx context.Context, sb *aprv1alpha1.StorageBackend) (ctrl.Result, error) {
	requiredKeys := requiredSecretKeys(sb.Spec.Type)

	// Filesystem backend doesn't require a Secret. If one is provided we
	// silently ignore it.
	if len(requiredKeys) == 0 {
		setReady(&sb.Status.Conditions, sb.Generation, "no credentials required for this backend type")
		sb.Status.ObservedGeneration = sb.Generation
		return ctrl.Result{}, nil
	}

	if sb.Spec.CredentialsSecretRef == nil {
		setNotReady(&sb.Status.Conditions, sb.Generation, ReasonSecretNotFound,
			fmt.Sprintf("backend type %q requires a credentialsSecretRef", sb.Spec.Type))
		sb.Status.ObservedGeneration = sb.Generation
		return ctrl.Result{RequeueAfter: requeueAfterMissingSecret}, nil
	}

	secretKey := types.NamespacedName{Namespace: sb.Namespace, Name: sb.Spec.CredentialsSecretRef.Name}
	var secret corev1.Secret
	if err := r.Get(ctx, secretKey, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			setNotReady(&sb.Status.Conditions, sb.Generation, ReasonSecretNotFound,
				fmt.Sprintf("Secret %q not found", secretKey.Name))
			sb.Status.ObservedGeneration = sb.Generation
			return ctrl.Result{RequeueAfter: requeueAfterMissingSecret}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting Secret: %w", err)
	}

	missing := missingKeys(&secret, requiredKeys...)
	if len(missing) > 0 {
		setNotReady(&sb.Status.Conditions, sb.Generation, ReasonSecretMissingKeys,
			fmt.Sprintf("Secret %q is missing key(s): %v", secretKey.Name, missing))
		sb.Status.ObservedGeneration = sb.Generation
		return ctrl.Result{RequeueAfter: requeueAfterMissingSecret}, nil
	}

	setReady(&sb.Status.Conditions, sb.Generation, "credentials Secret is well-formed")
	sb.Status.ObservedGeneration = sb.Generation
	return ctrl.Result{}, nil
}

// requiredSecretKeys returns the keys we expect a credentials Secret to
// contain for the given backend type, or nil if no Secret is required.
func requiredSecretKeys(typ aprv1alpha1.StorageType) []string {
	switch typ {
	case aprv1alpha1.StorageS3, aprv1alpha1.StorageR2:
		return []string{SecretKeyAccessKeyID, SecretKeySecretAccessKey}
	case aprv1alpha1.StorageGCS:
		return []string{SecretKeyServiceAccountJSON}
	default:
		return nil
	}
}

func (r *StorageBackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aprv1alpha1.StorageBackend{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.sbsUsingSecret),
		).
		Named("storagebackend").
		Complete(r)
}

func (r *StorageBackendReconciler) sbsUsingSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var sbs aprv1alpha1.StorageBackendList
	if err := r.List(ctx, &sbs, client.InNamespace(obj.GetNamespace())); err != nil {
		log.FromContext(ctx).Error(err, "listing StorageBackends for Secret enqueue")
		return nil
	}
	var requests []reconcile.Request
	for i := range sbs.Items {
		sb := &sbs.Items[i]
		if sb.Spec.CredentialsSecretRef != nil && sb.Spec.CredentialsSecretRef.Name == obj.GetName() {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: sb.Namespace, Name: sb.Name},
			})
		}
	}
	return requests
}
