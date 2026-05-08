// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// DatabaseConnectionReconciler reconciles a DatabaseConnection by validating
// that its referenced Secret exists and contains the expected username /
// password keys.
//
// True connectivity probing (a real DB ping) would require pulling DB
// drivers into the manager binary; that's deferred so the manager stays
// lean. Connection failures will surface on the first archive Job.
type DatabaseConnectionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// requeueAfterMissingSecret is how long to wait before re-checking a
// missing or invalid Secret. Watches handle the create/update case, but a
// requeue catches the rare cases where Watch notifications are missed.
const requeueAfterMissingSecret = 30 * time.Second

// +kubebuilder:rbac:groups=apr.dev,resources=databaseconnections,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apr.dev,resources=databaseconnections/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apr.dev,resources=databaseconnections/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *DatabaseConnectionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var dbc aprv1alpha1.DatabaseConnection
	if err := r.Get(ctx, req.NamespacedName, &dbc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	res, err := r.validate(ctx, &dbc)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Status().Update(ctx, &dbc); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	logger.V(1).Info("reconciled DatabaseConnection",
		"engine", dbc.Spec.Engine, "host", dbc.Spec.Host, "ready", isReady(dbc.Status.Conditions))
	return res, nil
}

func (r *DatabaseConnectionReconciler) validate(ctx context.Context, dbc *aprv1alpha1.DatabaseConnection) (ctrl.Result, error) {
	secretKey := types.NamespacedName{Namespace: dbc.Namespace, Name: dbc.Spec.CredentialsSecretRef.Name}

	var secret corev1.Secret
	if err := r.Get(ctx, secretKey, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			setNotReady(&dbc.Status.Conditions, dbc.Generation, ReasonSecretNotFound,
				fmt.Sprintf("Secret %q not found", secretKey.Name))
			dbc.Status.ObservedGeneration = dbc.Generation
			return ctrl.Result{RequeueAfter: requeueAfterMissingSecret}, nil
		}
		return ctrl.Result{}, fmt.Errorf("getting Secret: %w", err)
	}

	missing := missingKeys(&secret, SecretKeyUsername, SecretKeyPassword)
	if len(missing) > 0 {
		setNotReady(&dbc.Status.Conditions, dbc.Generation, ReasonSecretMissingKeys,
			fmt.Sprintf("Secret %q is missing key(s): %v", secretKey.Name, missing))
		dbc.Status.ObservedGeneration = dbc.Generation
		return ctrl.Result{RequeueAfter: requeueAfterMissingSecret}, nil
	}

	setReady(&dbc.Status.Conditions, dbc.Generation, "credentials Secret is well-formed")
	dbc.Status.ObservedGeneration = dbc.Generation
	return ctrl.Result{}, nil
}

func (r *DatabaseConnectionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aprv1alpha1.DatabaseConnection{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.dbcsUsingSecret),
		).
		Named("databaseconnection").
		Complete(r)
}

// dbcsUsingSecret returns reconcile requests for every DatabaseConnection
// in the same namespace that references the given Secret.
func (r *DatabaseConnectionReconciler) dbcsUsingSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var dbcs aprv1alpha1.DatabaseConnectionList
	if err := r.List(ctx, &dbcs, client.InNamespace(obj.GetNamespace())); err != nil {
		log.FromContext(ctx).Error(err, "listing DatabaseConnections for Secret enqueue")
		return nil
	}
	var requests []reconcile.Request
	for i := range dbcs.Items {
		dbc := &dbcs.Items[i]
		if dbc.Spec.CredentialsSecretRef.Name == obj.GetName() {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: dbc.Namespace, Name: dbc.Name},
			})
		}
	}
	return requests
}

// missingKeys returns the subset of `keys` that are absent or empty in the
// given Secret's Data map.
func missingKeys(secret *corev1.Secret, keys ...string) []string {
	var missing []string
	for _, k := range keys {
		if v, ok := secret.Data[k]; !ok || len(v) == 0 {
			missing = append(missing, k)
		}
	}
	return missing
}

func setReady(conds *[]metav1.Condition, gen int64, message string) {
	apimeta.SetStatusCondition(conds, metav1.Condition{
		Type:               ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonReady,
		Message:            message,
		ObservedGeneration: gen,
	})
}

func setNotReady(conds *[]metav1.Condition, gen int64, reason, message string) {
	apimeta.SetStatusCondition(conds, metav1.Condition{
		Type:               ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
	})
}

func isReady(conds []metav1.Condition) bool {
	c := apimeta.FindStatusCondition(conds, ConditionReady)
	return c != nil && c.Status == metav1.ConditionTrue
}
