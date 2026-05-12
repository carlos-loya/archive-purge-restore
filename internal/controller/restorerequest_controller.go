// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// RestoreRequestReconciler implements one-shot restore execution: each RR
// owns a Job that runs `apr restore --from-cr`. Lifecycle is exposed via
// the standard Conditions: Progressing flips True when the Job is active,
// then Succeeded or Failed becomes True when the Job reaches a terminal
// state.
//
// Spec is treated as immutable after the Job is created; to re-run a
// restore, create a new RR. Detecting / rejecting spec mutation is left to
// the validating webhook (see issue #62).
type RestoreRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// ArchiveImage is the container image launched by spawned restore Jobs.
	ArchiveImage string

	// RunnerSA is the ServiceAccount restore Job pods run as.
	RunnerSA string
}

// +kubebuilder:rbac:groups=apr.dev,resources=restorerequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apr.dev,resources=restorerequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apr.dev,resources=restorerequests/finalizers,verbs=update
// +kubebuilder:rbac:groups=apr.dev,resources=archiverules,verbs=get;list;watch
// +kubebuilder:rbac:groups=apr.dev,resources=databaseconnections,verbs=get;list;watch
// +kubebuilder:rbac:groups=apr.dev,resources=storagebackends,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

func (r *RestoreRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var rr aprv1alpha1.RestoreRequest
	if err := r.Get(ctx, req.NamespacedName, &rr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !rr.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Resolve referenced ArchiveRule.
	var rule aprv1alpha1.ArchiveRule
	ruleKey := types.NamespacedName{Namespace: rr.Namespace, Name: rr.Spec.ArchiveRuleRef.Name}
	if err := r.Get(ctx, ruleKey, &rule); err != nil {
		if apierrors.IsNotFound(err) {
			return r.markFailed(ctx, &rr, ReasonArchiveRuleNotFound,
				fmt.Sprintf("ArchiveRule %q not found", ruleKey.Name))
		}
		return ctrl.Result{}, fmt.Errorf("getting ArchiveRule: %w", err)
	}

	// Resolve DBC + SB through the ArchiveRule (a RestoreRequest reuses
	// whatever DB and storage the rule already declared).
	var dbc aprv1alpha1.DatabaseConnection
	if err := r.Get(ctx, types.NamespacedName{Namespace: rr.Namespace, Name: rule.Spec.DatabaseRef.Name}, &dbc); err != nil {
		if apierrors.IsNotFound(err) {
			return r.markFailed(ctx, &rr, ReasonDatabaseConnectionNotFound,
				fmt.Sprintf("DatabaseConnection %q not found", rule.Spec.DatabaseRef.Name))
		}
		return ctrl.Result{}, fmt.Errorf("getting DatabaseConnection: %w", err)
	}
	var sb aprv1alpha1.StorageBackend
	if err := r.Get(ctx, types.NamespacedName{Namespace: rr.Namespace, Name: rule.Spec.StorageRef.Name}, &sb); err != nil {
		if apierrors.IsNotFound(err) {
			return r.markFailed(ctx, &rr, ReasonStorageBackendNotFound,
				fmt.Sprintf("StorageBackend %q not found", rule.Spec.StorageRef.Name))
		}
		return ctrl.Result{}, fmt.Errorf("getting StorageBackend: %w", err)
	}

	// Reconcile the owned Job.
	job, err := r.reconcileJob(ctx, &rr, &dbc, &sb)
	if err != nil {
		return r.markFailed(ctx, &rr, ReasonJobReconcileError, err.Error())
	}

	// Emit restore_runs_total / duration / rows metrics on terminal Jobs.
	if err := emitRestoreMetricsForJob(ctx, r.Client, &rr, job); err != nil {
		logger.Error(err, "emitting restore metrics")
	}

	// Reflect Job state into Conditions. The sink writes RowsRestored /
	// times directly from inside the pod; we only manage conditions and
	// JobRef here.
	r.applyJobStatus(&rr, job)
	setCondition(&rr.Status.Conditions, ConditionReady, metav1.ConditionTrue,
		ReasonReady, "Job reconciled", rr.Generation)
	rr.Status.ObservedGeneration = rr.Generation
	if err := r.Status().Update(ctx, &rr); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	logger.V(1).Info("reconciled RestoreRequest",
		"job", job.Name)
	return ctrl.Result{}, nil
}

func (r *RestoreRequestReconciler) reconcileJob(
	ctx context.Context,
	rr *aprv1alpha1.RestoreRequest,
	dbc *aprv1alpha1.DatabaseConnection,
	sb *aprv1alpha1.StorageBackend,
) (*batchv1.Job, error) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreJobName(rr.Name),
			Namespace: rr.Namespace,
		},
	}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, job, func() error {
		// Job spec is immutable on most fields after creation. Only set
		// the spec on initial create (when ResourceVersion is empty);
		// otherwise leave it alone — the owner ref + labels are already
		// set, and we should not try to mutate an existing Job.
		if job.ResourceVersion != "" {
			return nil
		}
		if job.Labels == nil {
			job.Labels = map[string]string{}
		}
		job.Labels[LabelAppName] = AppName
		job.Labels[LabelManagedBy] = ManagerName
		job.Spec = r.desiredJobSpec(rr, dbc, sb)
		return controllerutil.SetControllerReference(rr, job, r.Scheme)
	})
	if err != nil {
		return nil, fmt.Errorf("reconciling Job: %w", err)
	}
	if op != controllerutil.OperationResultNone {
		log.FromContext(ctx).Info("Job reconciled", "name", job.Name, "operation", op)
	}
	return job, nil
}

func (r *RestoreRequestReconciler) desiredJobSpec(
	rr *aprv1alpha1.RestoreRequest,
	dbc *aprv1alpha1.DatabaseConnection,
	sb *aprv1alpha1.StorageBackend,
) batchv1.JobSpec {
	envVars := buildJobEnv(rr.Namespace, rr.Name, dbc, sb)

	podLabels := map[string]string{
		LabelAppName:   AppName,
		LabelManagedBy: ManagerName,
	}

	return batchv1.JobSpec{
		BackoffLimit: ptr.To[int32](2),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
			Spec: corev1.PodSpec{
				ServiceAccountName: r.RunnerSA,
				RestartPolicy:      corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:            "apr",
					Image:           r.ArchiveImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command:         []string{"/apr"},
					Args: []string{
						"restore",
						"--from-cr",
						fmt.Sprintf("%s/%s", rr.Namespace, rr.Name),
					},
					Env: envVars,
				}},
			},
		},
	}
}

// applyJobStatus derives the RestoreRequest's Progressing/Succeeded/Failed
// conditions from the Job's terminal state. The Job pod itself writes
// RowsRestored / StartTime / CompletionTime via cluster.RecordRestoreResult
// — those fields are not reflected here.
//
// Conditions are set so that exactly one of Succeeded / Failed is True
// after the Job terminates (and neither is True until then). Progressing
// is True only while the Job is actively running.
func (r *RestoreRequestReconciler) applyJobStatus(rr *aprv1alpha1.RestoreRequest, job *batchv1.Job) {
	rr.Status.JobRef = &corev1.LocalObjectReference{Name: job.Name}

	switch {
	case job.Status.Succeeded > 0:
		setCondition(&rr.Status.Conditions, ConditionProgressing, metav1.ConditionFalse,
			ReasonRestoreSucceeded, "restore Job completed successfully", rr.Generation)
		setCondition(&rr.Status.Conditions, ConditionSucceeded, metav1.ConditionTrue,
			ReasonRestoreSucceeded, "restore Job completed successfully", rr.Generation)
		setCondition(&rr.Status.Conditions, ConditionFailed, metav1.ConditionFalse,
			ReasonRestoreSucceeded, "restore Job completed successfully", rr.Generation)
	case job.Status.Failed > 0 && jobIsTerminal(job):
		// Backoff limit exhausted — sink may not have run.
		setCondition(&rr.Status.Conditions, ConditionProgressing, metav1.ConditionFalse,
			ReasonRestoreFailed, "restore Job failed terminally", rr.Generation)
		setCondition(&rr.Status.Conditions, ConditionSucceeded, metav1.ConditionFalse,
			ReasonRestoreFailed, "restore Job failed terminally", rr.Generation)
		setCondition(&rr.Status.Conditions, ConditionFailed, metav1.ConditionTrue,
			ReasonRestoreFailed, "restore Job failed terminally", rr.Generation)
	case job.Status.Active > 0:
		setCondition(&rr.Status.Conditions, ConditionProgressing, metav1.ConditionTrue,
			ReasonRestoreRunning, fmt.Sprintf("restore Job %q is running", job.Name), rr.Generation)
	default:
		setCondition(&rr.Status.Conditions, ConditionProgressing, metav1.ConditionFalse,
			ReasonRestorePending, "restore Job has not started yet", rr.Generation)
	}
}

// jobIsTerminal returns true if the Job has reached a terminal Failed
// condition (backoff exhausted or deadline exceeded). A non-terminal Failed
// count just means a pod failed and the Job is retrying.
func jobIsTerminal(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobFailed || c.Type == batchv1.JobFailureTarget) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *RestoreRequestReconciler) markFailed(
	ctx context.Context,
	rr *aprv1alpha1.RestoreRequest,
	reason, message string,
) (ctrl.Result, error) {
	setCondition(&rr.Status.Conditions, ConditionReady, metav1.ConditionFalse,
		reason, message, rr.Generation)
	setCondition(&rr.Status.Conditions, ConditionFailed, metav1.ConditionTrue,
		reason, message, rr.Generation)
	setCondition(&rr.Status.Conditions, ConditionProgressing, metav1.ConditionFalse,
		reason, message, rr.Generation)
	rr.Status.ObservedGeneration = rr.Generation
	if err := r.Status().Update(ctx, rr); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}
	return ctrl.Result{}, nil
}

func restoreJobName(rrName string) string {
	const maxBase = 52
	name := "restorerequest-" + rrName
	if len(name) > maxBase {
		name = name[:maxBase]
	}
	return name
}

func (r *RestoreRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aprv1alpha1.RestoreRequest{}).
		Owns(&batchv1.Job{}).
		Named("restorerequest").
		Complete(r)
}
