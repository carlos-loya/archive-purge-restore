// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// ArchiveRuleReconciler manages the lifecycle of ArchiveRule custom
// resources. The reconciler implements its own cron-style scheduler — it
// does NOT delegate to a Kubernetes CronJob — so each reconcile decides
// whether to spawn a Job directly. Concurrency is forbidden (at most one
// active Job per rule). After more than spec.maxFailures consecutive Job
// failures the reconciler auto-suspends the rule until a manual fix or
// trigger.
//
// Manual triggering: setting the annotation `apr.dev/trigger-time` to an
// RFC3339 timestamp newer than status.lastTriggerTime causes the next
// reconcile to fire immediately, bypassing the cron schedule.
type ArchiveRuleReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// ArchiveImage is the container image launched by spawned Job pods.
	ArchiveImage string

	// RunnerSA is the ServiceAccount Job pods run as.
	RunnerSA string
}

// requeueWhileBusy is the requeue delay when the rule is suspended or has
// an active Job — we don't poll aggressively; spec/Owns watches will wake
// us when something actually changes. This is just a safety net.
const requeueWhileBusy = 60 * time.Second

// minFireDelay caps how short a RequeueAfter we ever return after firing.
// Without this, time.Until(sched.Next(now)) can produce a negative or
// near-zero value if the schedule is "every second" and we just fired,
// causing controller-runtime to busy-loop.
const minFireDelay = time.Second

// +kubebuilder:rbac:groups=apr.dev,resources=archiverules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apr.dev,resources=archiverules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apr.dev,resources=archiverules/finalizers,verbs=update
// +kubebuilder:rbac:groups=apr.dev,resources=databaseconnections,verbs=get;list;watch
// +kubebuilder:rbac:groups=apr.dev,resources=storagebackends,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

func (r *ArchiveRuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var rule aprv1alpha1.ArchiveRule
	if err := r.Get(ctx, req.NamespacedName, &rule); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !rule.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	dbc, sb, res, err := r.resolveRefs(ctx, &rule)
	if res != nil || err != nil {
		return *res, err
	}

	sched, err := cron.ParseStandard(rule.Spec.Schedule)
	if err != nil {
		return r.setNotReady(ctx, &rule, ReasonInvalidSchedule,
			fmt.Sprintf("invalid cron expression %q: %v", rule.Spec.Schedule, err))
	}

	ownedJobs, err := r.listOwnedJobs(ctx, &rule)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing owned Jobs: %w", err)
	}

	// Reflect Job observations into status before any decisions.
	active, lastFinished := classifyJobs(ownedJobs)
	r.applyJobStatus(&rule, active, lastFinished)
	rule.Status.ConsecutiveFailures = countConsecutiveFailures(ownedJobs)

	// Garbage-collect old Jobs.
	if err := r.pruneJobs(ctx, &rule, ownedJobs); err != nil {
		logger.Error(err, "pruning old Jobs")
		// Don't fail reconcile on prune errors; status is more important.
	}

	now := time.Now()
	failureLimit := DefaultMaxFailures
	if rule.Spec.MaxFailures != 0 {
		failureLimit = int(rule.Spec.MaxFailures)
	}
	autoSuspended := failureLimit > 0 && int(rule.Status.ConsecutiveFailures) >= failureLimit
	suspended := rule.Spec.Suspend || autoSuspended

	canFire := !suspended && active == nil

	triggered := false
	if canFire {
		triggered = r.processManualTrigger(&rule, now)
	}
	scheduledFire := canFire &&
		rule.Status.NextScheduledTime != nil &&
		!now.Before(rule.Status.NextScheduledTime.Time)

	if triggered || scheduledFire {
		job, spawnErr := r.spawnJob(ctx, &rule, dbc, sb)
		if spawnErr != nil {
			return r.setNotReady(ctx, &rule, ReasonJobReconcileError, spawnErr.Error())
		}
		rule.Status.ActiveJobRef = &corev1.LocalObjectReference{Name: job.Name}
		active = job
		logger.Info("spawned archive Job", "job", job.Name, "trigger", triggerSource(triggered, scheduledFire))
	}

	// Compute next scheduled fire and decide our requeue delay.
	//
	// While suspended we don't have a next firing — leave NextScheduledTime
	// nil so users don't see a misleading time ticking toward an event that
	// won't actually happen.
	//
	// While a Job is active we DO compute NextScheduledTime — it's
	// informative for the user. Our requeue is short though, because the
	// Owns(&Job{}) watch will wake us as soon as the Job completes; we
	// don't want to wait until the scheduled time to react.
	requeueAfter := requeueWhileBusy
	if suspended {
		rule.Status.NextScheduledTime = nil
	} else {
		nextFire := sched.Next(now)
		t := metav1.NewTime(nextFire)
		rule.Status.NextScheduledTime = &t
		if active == nil {
			delay := time.Until(nextFire)
			if delay < minFireDelay {
				delay = minFireDelay
			}
			requeueAfter = delay
		}
	}

	// Set the Ready condition based on our decision.
	switch {
	case autoSuspended:
		apimeta.SetStatusCondition(&rule.Status.Conditions, metav1.Condition{
			Type:               ConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             ReasonMaxFailuresReached,
			Message: fmt.Sprintf("ConsecutiveFailures=%d reached MaxFailures=%d; clear the annotation %s or fix the underlying cause to resume",
				rule.Status.ConsecutiveFailures, failureLimit, aprv1alpha1.AnnotationTriggerTime),
			ObservedGeneration: rule.Generation,
		})
	case rule.Spec.Suspend:
		apimeta.SetStatusCondition(&rule.Status.Conditions, metav1.Condition{
			Type:               ConditionReady,
			Status:             metav1.ConditionTrue,
			Reason:             ReasonSuspended,
			Message:            "rule suspended by spec.suspend",
			ObservedGeneration: rule.Generation,
		})
	default:
		apimeta.SetStatusCondition(&rule.Status.Conditions, metav1.Condition{
			Type:               ConditionReady,
			Status:             metav1.ConditionTrue,
			Reason:             ReasonReady,
			Message:            "rule scheduled",
			ObservedGeneration: rule.Generation,
		})
	}
	rule.Status.ObservedGeneration = rule.Generation

	if err := r.Status().Update(ctx, &rule); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	logger.V(1).Info("reconciled ArchiveRule",
		"active", rule.Status.ActiveJobRef != nil,
		"nextScheduled", rule.Status.NextScheduledTime,
		"consecutiveFailures", rule.Status.ConsecutiveFailures,
		"requeueAfter", requeueAfter)
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// resolveRefs fetches the DatabaseConnection and StorageBackend the rule
// references. If either is missing, returns a Result that should be
// returned directly from Reconcile (signalling a NotReady status update +
// requeue).
func (r *ArchiveRuleReconciler) resolveRefs(
	ctx context.Context,
	rule *aprv1alpha1.ArchiveRule,
) (*aprv1alpha1.DatabaseConnection, *aprv1alpha1.StorageBackend, *ctrl.Result, error) {
	var dbc aprv1alpha1.DatabaseConnection
	dbcKey := types.NamespacedName{Namespace: rule.Namespace, Name: rule.Spec.DatabaseRef.Name}
	if err := r.Get(ctx, dbcKey, &dbc); err != nil {
		if apierrors.IsNotFound(err) {
			res, e := r.setNotReady(ctx, rule, ReasonDatabaseConnectionNotFound,
				fmt.Sprintf("DatabaseConnection %q not found", dbcKey.Name))
			return nil, nil, &res, e
		}
		return nil, nil, nil, fmt.Errorf("getting DatabaseConnection: %w", err)
	}
	var sb aprv1alpha1.StorageBackend
	sbKey := types.NamespacedName{Namespace: rule.Namespace, Name: rule.Spec.StorageRef.Name}
	if err := r.Get(ctx, sbKey, &sb); err != nil {
		if apierrors.IsNotFound(err) {
			res, e := r.setNotReady(ctx, rule, ReasonStorageBackendNotFound,
				fmt.Sprintf("StorageBackend %q not found", sbKey.Name))
			return nil, nil, &res, e
		}
		return nil, nil, nil, fmt.Errorf("getting StorageBackend: %w", err)
	}
	return &dbc, &sb, nil, nil
}

// listOwnedJobs returns Jobs owned by the given rule. We filter by both
// label (fast indexed lookup) and ownerRef (correctness — labels are
// user-spoofable).
func (r *ArchiveRuleReconciler) listOwnedJobs(ctx context.Context, rule *aprv1alpha1.ArchiveRule) ([]batchv1.Job, error) {
	var all batchv1.JobList
	if err := r.List(ctx, &all,
		client.InNamespace(rule.Namespace),
		client.MatchingLabels{LabelArchiveRule: rule.Name},
	); err != nil {
		return nil, err
	}
	owned := make([]batchv1.Job, 0, len(all.Items))
	for i := range all.Items {
		j := &all.Items[i]
		for _, ref := range j.OwnerReferences {
			if ref.UID == rule.UID && ref.Controller != nil && *ref.Controller {
				owned = append(owned, *j)
				break
			}
		}
	}
	sort.Slice(owned, func(i, j int) bool {
		return owned[i].CreationTimestamp.Before(&owned[j].CreationTimestamp)
	})
	return owned, nil
}

// classifyJobs returns the currently-active Job (at most one expected)
// and the most recent finished Job. Input must be sorted ascending by
// creationTimestamp.
func classifyJobs(jobs []batchv1.Job) (active, lastFinished *batchv1.Job) {
	for i := range jobs {
		j := &jobs[i]
		if isJobFinished(j) {
			lastFinished = j
		} else if active == nil {
			active = j
		}
	}
	return active, lastFinished
}

// isJobFinished returns true when a Job has reached either its Complete
// or Failed terminal condition.
func isJobFinished(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		if c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed {
			return true
		}
	}
	return false
}

// didJobSucceed assumes the Job is finished and returns whether it
// succeeded vs failed.
func didJobSucceed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// countConsecutiveFailures walks finished Jobs newest-first and counts
// how many failed before the first success. Pure function over the input
// — does not depend on prior state, which keeps reconciles idempotent.
func countConsecutiveFailures(jobs []batchv1.Job) int32 {
	var count int32
	for i := len(jobs) - 1; i >= 0; i-- {
		j := &jobs[i]
		if !isJobFinished(j) {
			continue
		}
		if didJobSucceed(j) {
			return count
		}
		count++
	}
	return count
}

// applyJobStatus reflects observed Job state into the rule's status.
// Conventions:
//
//   - The Job pod's sink (cluster.RecordArchiveResult) is the source of
//     truth for LastRunResult/LastRunRowsArchived/LastRunID/LastRunTime
//     when the engine actually ran. The reconciler does NOT touch those
//     fields on a successful Job.
//   - When a Job terminated with Failed and the sink may not have run
//     (pod crashed before the engine returned), we override
//     LastRunResult to Failed so users see the correct outcome.
//   - LastJobRef and ActiveJobRef are owned by the reconciler.
func (r *ArchiveRuleReconciler) applyJobStatus(rule *aprv1alpha1.ArchiveRule, active, lastFinished *batchv1.Job) {
	if active != nil {
		rule.Status.ActiveJobRef = &corev1.LocalObjectReference{Name: active.Name}
	} else {
		rule.Status.ActiveJobRef = nil
	}
	if lastFinished != nil {
		rule.Status.LastJobRef = &corev1.LocalObjectReference{Name: lastFinished.Name}
		if !didJobSucceed(lastFinished) {
			rule.Status.LastRunResult = aprv1alpha1.ArchiveRunFailed
			if lastFinished.Status.StartTime != nil &&
				(rule.Status.LastRunTime == nil || lastFinished.Status.StartTime.Time.After(rule.Status.LastRunTime.Time)) {
				rule.Status.LastRunTime = lastFinished.Status.StartTime
			}
		}
	}
}

// pruneJobs deletes the oldest finished Jobs once their count exceeds
// HistoryLimit (defaulted via DefaultHistoryLimit when spec leaves it 0).
// HistoryLimit=0 in spec means "unbounded retention" (use with care).
func (r *ArchiveRuleReconciler) pruneJobs(ctx context.Context, rule *aprv1alpha1.ArchiveRule, jobs []batchv1.Job) error {
	limit := DefaultHistoryLimit
	if rule.Spec.HistoryLimit > 0 {
		limit = int(rule.Spec.HistoryLimit)
	}
	finished := make([]batchv1.Job, 0, len(jobs))
	for i := range jobs {
		if isJobFinished(&jobs[i]) {
			finished = append(finished, jobs[i])
		}
	}
	excess := len(finished) - limit
	if excess <= 0 {
		return nil
	}
	bg := metav1.DeletePropagationBackground
	for i := 0; i < excess; i++ {
		j := &finished[i]
		if err := r.Delete(ctx, j, &client.DeleteOptions{PropagationPolicy: &bg}); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("deleting Job %s: %w", j.Name, err)
		}
	}
	return nil
}

// processManualTrigger consumes the apr.dev/trigger-time annotation and
// returns true if this reconcile should fire because of it. Updates
// rule.Status.LastTriggerTime in-memory so the same trigger isn't honored
// twice.
func (r *ArchiveRuleReconciler) processManualTrigger(rule *aprv1alpha1.ArchiveRule, now time.Time) bool {
	val, ok := rule.Annotations[aprv1alpha1.AnnotationTriggerTime]
	if !ok {
		return false
	}
	trig, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return false
	}
	// Don't honor far-future triggers: a user typo shouldn't fire
	// immediately just because the timestamp was newer.
	if trig.After(now.Add(time.Minute)) {
		return false
	}
	var last time.Time
	if rule.Status.LastTriggerTime != nil {
		last = rule.Status.LastTriggerTime.Time
	}
	if !trig.After(last) {
		return false
	}
	t := metav1.NewTime(trig)
	rule.Status.LastTriggerTime = &t
	return true
}

// spawnJob constructs and creates a new archive Job owned by this rule.
// The Job's metadata uses GenerateName so two near-simultaneous fires
// don't collide on the same name.
func (r *ArchiveRuleReconciler) spawnJob(
	ctx context.Context,
	rule *aprv1alpha1.ArchiveRule,
	dbc *aprv1alpha1.DatabaseConnection,
	sb *aprv1alpha1.StorageBackend,
) (*batchv1.Job, error) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: rule.Name + "-",
			Namespace:    rule.Namespace,
			Labels: map[string]string{
				LabelAppName:     AppName,
				LabelManagedBy:   ManagerName,
				LabelArchiveRule: rule.Name,
			},
		},
		Spec: r.desiredJobSpec(rule, dbc, sb),
	}
	if err := controllerutil.SetControllerReference(rule, job, r.Scheme); err != nil {
		return nil, fmt.Errorf("setting owner reference: %w", err)
	}
	if err := r.Create(ctx, job); err != nil {
		return nil, fmt.Errorf("creating Job: %w", err)
	}
	return job, nil
}

func (r *ArchiveRuleReconciler) desiredJobSpec(
	rule *aprv1alpha1.ArchiveRule,
	dbc *aprv1alpha1.DatabaseConnection,
	sb *aprv1alpha1.StorageBackend,
) batchv1.JobSpec {
	envVars := buildJobEnv(rule.Namespace, rule.Name, dbc, sb)

	podLabels := map[string]string{
		LabelAppName:     AppName,
		LabelManagedBy:   ManagerName,
		LabelArchiveRule: rule.Name,
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
						"archive",
						"--from-cr",
						fmt.Sprintf("%s/%s", rule.Namespace, rule.Name),
					},
					Env: envVars,
				}},
			},
		},
	}
}

// setNotReady writes a Ready=False condition with the given reason and
// requeues. Used for unrecoverable spec errors (missing refs, bad cron)
// that we want the user to see immediately.
func (r *ArchiveRuleReconciler) setNotReady(
	ctx context.Context,
	rule *aprv1alpha1.ArchiveRule,
	reason, message string,
) (ctrl.Result, error) {
	apimeta.SetStatusCondition(&rule.Status.Conditions, metav1.Condition{
		Type:               ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: rule.Generation,
	})
	rule.Status.ObservedGeneration = rule.Generation
	if err := r.Status().Update(ctx, rule); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}
	return ctrl.Result{RequeueAfter: requeueWhileBusy}, nil
}

func triggerSource(manual, scheduled bool) string {
	switch {
	case manual && scheduled:
		return "manual+scheduled"
	case manual:
		return "manual"
	case scheduled:
		return "scheduled"
	default:
		return "none"
	}
}

func (r *ArchiveRuleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aprv1alpha1.ArchiveRule{}).
		Owns(&batchv1.Job{}).
		Watches(
			&aprv1alpha1.DatabaseConnection{},
			handler.EnqueueRequestsFromMapFunc(r.rulesUsingDatabase),
		).
		Watches(
			&aprv1alpha1.StorageBackend{},
			handler.EnqueueRequestsFromMapFunc(r.rulesUsingStorage),
		).
		Named("archiverule").
		Complete(r)
}

func (r *ArchiveRuleReconciler) rulesUsingDatabase(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.rulesMatching(ctx, obj.GetNamespace(), func(rule *aprv1alpha1.ArchiveRule) bool {
		return rule.Spec.DatabaseRef.Name == obj.GetName()
	})
}

func (r *ArchiveRuleReconciler) rulesUsingStorage(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.rulesMatching(ctx, obj.GetNamespace(), func(rule *aprv1alpha1.ArchiveRule) bool {
		return rule.Spec.StorageRef.Name == obj.GetName()
	})
}

func (r *ArchiveRuleReconciler) rulesMatching(
	ctx context.Context,
	namespace string,
	predicate func(*aprv1alpha1.ArchiveRule) bool,
) []reconcile.Request {
	var rules aprv1alpha1.ArchiveRuleList
	if err := r.List(ctx, &rules, client.InNamespace(namespace)); err != nil {
		log.FromContext(ctx).Error(err, "listing ArchiveRules for cross-resource enqueue")
		return nil
	}
	var requests []reconcile.Request
	for i := range rules.Items {
		rule := &rules.Items[i]
		if predicate(rule) {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: rule.Namespace, Name: rule.Name},
			})
		}
	}
	return requests
}
