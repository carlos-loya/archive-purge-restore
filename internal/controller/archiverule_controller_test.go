// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// --- Reference resolution + validation ---

func TestArchiveRule_MissingDatabaseRef(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-missing-dbc")

	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "missing-dbc", "missing-sb"))

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getArchiveRule(t, ctx, ns, "rule1")
	requireReady(t, got.Status.Conditions, metav1.ConditionFalse, ReasonDatabaseConnectionNotFound)
}

func TestArchiveRule_MissingStorageRef(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-missing-sb")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "missing-sb"))

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getArchiveRule(t, ctx, ns, "rule1")
	requireReady(t, got.Status.Conditions, metav1.ConditionFalse, ReasonStorageBackendNotFound)
}

func TestArchiveRule_InvalidSchedule(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-bad-sched")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	rule := newArchiveRule(ns, "rule1", "dbc1", "sb1")
	rule.Spec.Schedule = "definitely-not-cron"
	mustCreate(t, ctx, rule)

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getArchiveRule(t, ctx, ns, "rule1")
	requireReady(t, got.Status.Conditions, metav1.ConditionFalse, ReasonInvalidSchedule)
}

// --- Scheduling: do not fire before NextScheduledTime ---

func TestArchiveRule_NoFireBeforeSchedule(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-no-early-fire")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))

	r := newArchiveRuleReconciler()
	res, err := r.Reconcile(ctx, reqFor(ns, "rule1"))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getArchiveRule(t, ctx, ns, "rule1")
	requireReady(t, got.Status.Conditions, metav1.ConditionTrue, ReasonReady)

	// First reconcile must not spawn a Job: status.NextScheduledTime is
	// nil before the first reconcile, so the canFire-via-schedule branch
	// requires a SECOND reconcile after NextScheduledTime is set in the
	// past. Schedule is "0 2 * * *" so the next fire is far future.
	if got.Status.ActiveJobRef != nil {
		t.Errorf("expected no Job spawned on first reconcile, got ActiveJobRef=%v", got.Status.ActiveJobRef)
	}
	if got.Status.NextScheduledTime == nil {
		t.Fatal("expected NextScheduledTime to be set")
	}
	if got.Status.NextScheduledTime.Time.Before(time.Now()) {
		t.Errorf("NextScheduledTime should be in the future, got %v", got.Status.NextScheduledTime)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected positive RequeueAfter, got %v", res.RequeueAfter)
	}

	// Confirm no Jobs were created.
	if jobs := listJobs(t, ctx, ns); len(jobs) != 0 {
		t.Errorf("expected zero Jobs, got %d", len(jobs))
	}
}

// --- Scheduling: fire when NextScheduledTime is past ---

func TestArchiveRule_FireWhenScheduleDue(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-fire-due")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))

	r := newArchiveRuleReconciler()
	// First reconcile sets NextScheduledTime in the future.
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	// Backdate NextScheduledTime to 1 hour ago to simulate the schedule
	// having already fired.
	rule := getArchiveRule(t, ctx, ns, "rule1")
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	rule.Status.NextScheduledTime = &past
	if err := testClient.Status().Update(ctx, rule); err != nil {
		t.Fatalf("backdating NextScheduledTime: %v", err)
	}

	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	got := getArchiveRule(t, ctx, ns, "rule1")
	if got.Status.ActiveJobRef == nil {
		t.Fatal("expected a Job to have been spawned")
	}

	jobs := listJobs(t, ctx, ns)
	if len(jobs) != 1 {
		t.Fatalf("expected exactly one Job, got %d", len(jobs))
	}
	job := jobs[0]
	if !ownedBy(&job, rule) {
		t.Errorf("Job not owned by rule: %v", job.OwnerReferences)
	}
	if job.Spec.Template.Spec.Containers[0].Args[0] != "archive" {
		t.Errorf("Job container should be running archive: %v", job.Spec.Template.Spec.Containers[0].Args)
	}

	// And NextScheduledTime should have advanced.
	if got.Status.NextScheduledTime == nil || got.Status.NextScheduledTime.Time.Before(time.Now()) {
		t.Errorf("expected NextScheduledTime to advance to the future, got %v", got.Status.NextScheduledTime)
	}
}

// --- Scheduling: don't double-fire while a Job is already active ---

func TestArchiveRule_NoDoubleFireWhenActive(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-no-double")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))

	r := newArchiveRuleReconciler()

	// First reconcile.
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	// Backdate, fire once.
	rule := getArchiveRule(t, ctx, ns, "rule1")
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	rule.Status.NextScheduledTime = &past
	if err := testClient.Status().Update(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("fire reconcile: %v", err)
	}
	// Backdate again — but the Job from the previous fire is still active.
	rule = getArchiveRule(t, ctx, ns, "rule1")
	rule.Status.NextScheduledTime = &past
	if err := testClient.Status().Update(ctx, rule); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("second fire reconcile: %v", err)
	}

	jobs := listJobs(t, ctx, ns)
	if len(jobs) != 1 {
		t.Errorf("expected exactly one Job (no double-fire), got %d", len(jobs))
	}
}

// --- Manual trigger via annotation ---

func TestArchiveRule_AnnotationTriggersImmediateRun(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-trigger")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	rule := newArchiveRule(ns, "rule1", "dbc1", "sb1")
	rule.Annotations = map[string]string{
		aprv1alpha1.AnnotationTriggerTime: time.Now().Format(time.RFC3339),
	}
	mustCreate(t, ctx, rule)

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getArchiveRule(t, ctx, ns, "rule1")
	if got.Status.ActiveJobRef == nil {
		t.Fatal("expected annotation trigger to spawn a Job on first reconcile")
	}
	if got.Status.LastTriggerTime == nil {
		t.Error("LastTriggerTime should be set after honoring trigger")
	}

	if jobs := listJobs(t, ctx, ns); len(jobs) != 1 {
		t.Errorf("expected one Job, got %d", len(jobs))
	}
}

func TestArchiveRule_AnnotationTriggerDeduped(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-trigger-dedup")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	trig := time.Now().Format(time.RFC3339)
	rule := newArchiveRule(ns, "rule1", "dbc1", "sb1")
	rule.Annotations = map[string]string{aprv1alpha1.AnnotationTriggerTime: trig}
	mustCreate(t, ctx, rule)

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	// Mark the Job finished so a second fire could happen.
	jobs := listJobs(t, ctx, ns)
	if len(jobs) != 1 {
		t.Fatalf("expected one Job after first reconcile, got %d", len(jobs))
	}
	markJobSucceeded(t, ctx, &jobs[0])

	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	// Same annotation value → must not fire again.
	if jobs := listJobs(t, ctx, ns); len(jobs) != 1 {
		t.Errorf("expected exactly one Job (dedup by trigger value), got %d", len(jobs))
	}
}

// --- Suspend ---

func TestArchiveRule_SuspendBlocksScheduling(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-suspend")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	rule := newArchiveRule(ns, "rule1", "dbc1", "sb1")
	rule.Spec.Suspend = true
	mustCreate(t, ctx, rule)

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getArchiveRule(t, ctx, ns, "rule1")
	if got.Status.NextScheduledTime != nil {
		t.Errorf("NextScheduledTime should be nil while suspended, got %v", got.Status.NextScheduledTime)
	}
	if got.Status.ActiveJobRef != nil {
		t.Error("expected no active Job while suspended")
	}
	if jobs := listJobs(t, ctx, ns); len(jobs) != 0 {
		t.Errorf("expected zero Jobs while suspended, got %d", len(jobs))
	}

	cond := apimeta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonSuspended {
		t.Errorf("expected Ready=True/Suspended, got %+v", cond)
	}
}

// --- Failure tracking ---

func TestArchiveRule_FailedJobIncrementsFailures(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-fail-count")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))

	r := newArchiveRuleReconciler()
	// Reconcile to set NextScheduledTime.
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile init: %v", err)
	}

	rule := getArchiveRule(t, ctx, ns, "rule1")

	// Manually create a finished failed Job owned by the rule.
	failed := newOwnedJob(t, "fail-1", ns, rule)
	mustCreate(t, ctx, failed)
	markJobFailed(t, ctx, failed)

	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile after failed job: %v", err)
	}
	got := getArchiveRule(t, ctx, ns, "rule1")
	if got.Status.ConsecutiveFailures != 1 {
		t.Errorf("ConsecutiveFailures = %d, want 1", got.Status.ConsecutiveFailures)
	}
	if got.Status.LastRunResult != aprv1alpha1.ArchiveRunFailed {
		t.Errorf("LastRunResult = %q, want Failed (sink would not have run for crashed pod)", got.Status.LastRunResult)
	}
}

func TestArchiveRule_SuccessResetsFailureCount(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-success-reset")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatal(err)
	}
	rule := getArchiveRule(t, ctx, ns, "rule1")

	// Three fails, then a success.
	for i := 0; i < 3; i++ {
		j := newOwnedJob(t, fmt.Sprintf("fail-%d", i), ns, rule)
		mustCreate(t, ctx, j)
		markJobFailed(t, ctx, j)
		// Stagger creation timestamps so sort order is stable.
		time.Sleep(20 * time.Millisecond)
	}
	success := newOwnedJob(t, "success-1", ns, rule)
	mustCreate(t, ctx, success)
	markJobSucceeded(t, ctx, success)

	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := getArchiveRule(t, ctx, ns, "rule1")
	if got.Status.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0 (most recent Job succeeded)", got.Status.ConsecutiveFailures)
	}
}

func TestArchiveRule_MaxFailuresAutoSuspends(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-max-fail")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	rule := newArchiveRule(ns, "rule1", "dbc1", "sb1")
	rule.Spec.MaxFailures = 2
	mustCreate(t, ctx, rule)

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatal(err)
	}
	rule = getArchiveRule(t, ctx, ns, "rule1")

	for i := 0; i < 2; i++ {
		j := newOwnedJob(t, fmt.Sprintf("fail-%d", i), ns, rule)
		mustCreate(t, ctx, j)
		markJobFailed(t, ctx, j)
		time.Sleep(20 * time.Millisecond)
	}
	// Backdate NextScheduledTime to ensure the reconciler would otherwise want to fire.
	rule = getArchiveRule(t, ctx, ns, "rule1")
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	rule.Status.NextScheduledTime = &past
	if err := testClient.Status().Update(ctx, rule); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := getArchiveRule(t, ctx, ns, "rule1")

	requireReady(t, got.Status.Conditions, metav1.ConditionFalse, ReasonMaxFailuresReached)
	// No new Job should have been spawned (only the 2 failed ones).
	if jobs := listJobs(t, ctx, ns); len(jobs) != 2 {
		t.Errorf("expected exactly 2 Jobs (no new fire after auto-suspend), got %d", len(jobs))
	}
}

// --- History pruning ---

func TestArchiveRule_PrunesOldFinishedJobs(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-prune")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	rule := newArchiveRule(ns, "rule1", "dbc1", "sb1")
	rule.Spec.HistoryLimit = 2
	mustCreate(t, ctx, rule)

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatal(err)
	}
	rule = getArchiveRule(t, ctx, ns, "rule1")

	// Create 5 finished Jobs; expect the 3 oldest to be pruned.
	for i := 0; i < 5; i++ {
		j := newOwnedJob(t, fmt.Sprintf("job-%d", i), ns, rule)
		mustCreate(t, ctx, j)
		markJobSucceeded(t, ctx, j)
		time.Sleep(20 * time.Millisecond)
	}

	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	jobs := listJobs(t, ctx, ns)
	if len(jobs) != 2 {
		t.Errorf("expected exactly 2 Jobs after pruning to HistoryLimit=2, got %d", len(jobs))
	}
}

// --- Regression: no CronJob ever created ---

func TestArchiveRule_NeverCreatesCronJob(t *testing.T) {
	requireEnvtest(t)
	ctx := t.Context()
	ns := mustCreateNamespace(t, ctx, "ar-no-cronjob")

	mustCreate(t, ctx, newDatabaseConnection(ns, "dbc1"))
	mustCreate(t, ctx, newStorageBackend(ns, "sb1"))
	mustCreate(t, ctx, newArchiveRule(ns, "rule1", "dbc1", "sb1"))

	r := newArchiveRuleReconciler()
	if _, err := r.Reconcile(ctx, reqFor(ns, "rule1")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var cronjobs batchv1.CronJobList
	if err := testClient.List(ctx, &cronjobs, client.InNamespace(ns)); err != nil {
		t.Fatalf("listing CronJobs: %v", err)
	}
	if len(cronjobs.Items) != 0 {
		t.Errorf("expected zero CronJobs (controller-driven scheduling), got %d", len(cronjobs.Items))
	}
}

// --- Pure-function tests for the helpers ---

func TestCountConsecutiveFailures(t *testing.T) {
	cases := []struct {
		name string
		jobs []batchv1.Job
		want int32
	}{
		{name: "empty", jobs: nil, want: 0},
		{
			name: "all-success",
			jobs: []batchv1.Job{finishedJob("j1", true), finishedJob("j2", true)},
			want: 0,
		},
		{
			name: "all-fail",
			jobs: []batchv1.Job{finishedJob("j1", false), finishedJob("j2", false), finishedJob("j3", false)},
			want: 3,
		},
		{
			name: "fail-then-success",
			jobs: []batchv1.Job{finishedJob("j1", false), finishedJob("j2", false), finishedJob("j3", true)},
			want: 0,
		},
		{
			name: "success-then-fail",
			jobs: []batchv1.Job{finishedJob("j1", true), finishedJob("j2", false), finishedJob("j3", false)},
			want: 2,
		},
		{
			name: "active-skipped",
			jobs: []batchv1.Job{finishedJob("j1", false), finishedJob("j2", false), unfinishedJob("j3")},
			want: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := countConsecutiveFailures(tc.jobs); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestClassifyJobs(t *testing.T) {
	jobs := []batchv1.Job{
		finishedJob("old-fail", false),
		finishedJob("recent-success", true),
		unfinishedJob("active"),
	}
	active, last := classifyJobs(jobs)
	if active == nil || active.Name != "active" {
		t.Errorf("active = %v, want active", active)
	}
	if last == nil || last.Name != "recent-success" {
		t.Errorf("lastFinished = %v, want recent-success", last)
	}
}

// --- Helpers ---

func newArchiveRuleReconciler() *ArchiveRuleReconciler {
	return &ArchiveRuleReconciler{
		Client:       testClient,
		Scheme:       testScheme,
		ArchiveImage: "test/apr:dev",
		RunnerSA:     "default",
	}
}

func newArchiveRule(ns, name, dbcName, sbName string) *aprv1alpha1.ArchiveRule {
	return &aprv1alpha1.ArchiveRule{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aprv1alpha1.ArchiveRuleSpec{
			DatabaseRef: corev1.LocalObjectReference{Name: dbcName},
			StorageRef:  corev1.LocalObjectReference{Name: sbName},
			Table:       "orders",
			DateColumn:  "created_at",
			DaysOnline:  30,
			Schedule:    "0 2 * * *",
		},
	}
}

func newDatabaseConnection(ns, name string) *aprv1alpha1.DatabaseConnection {
	return &aprv1alpha1.DatabaseConnection{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aprv1alpha1.DatabaseConnectionSpec{
			Engine:               aprv1alpha1.EnginePostgres,
			Host:                 "pg.example.com",
			Database:             "orders",
			CredentialsSecretRef: corev1.LocalObjectReference{Name: name + "-creds"},
		},
	}
}

func newStorageBackend(ns, name string) *aprv1alpha1.StorageBackend {
	return &aprv1alpha1.StorageBackend{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aprv1alpha1.StorageBackendSpec{
			Type:                 aprv1alpha1.StorageS3,
			Bucket:               "test-archive",
			Region:               "us-west-2",
			CredentialsSecretRef: &corev1.LocalObjectReference{Name: name + "-creds"},
		},
	}
}

// newOwnedJob constructs a Job whose ownerRef points at the given rule.
// We attach the standard archive-rule label so listOwnedJobs finds it.
// Note: rule.TypeMeta is empty after a Get (API server strips it), so we
// hard-code the APIVersion and Kind here.
func newOwnedJob(t *testing.T, name, ns string, rule *aprv1alpha1.ArchiveRule) *batchv1.Job {
	t.Helper()
	tr := true
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{LabelArchiveRule: rule.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: aprv1alpha1.GroupVersion.String(),
				Kind:       "ArchiveRule",
				Name:       rule.Name,
				UID:        rule.UID,
				Controller: &tr,
			}},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "apr",
						Image: "test/apr:dev",
					}},
				},
			},
		},
	}
}

// markJobSucceeded transitions a Job to terminal Complete=True.
// Kubernetes 1.32+ enforces that SuccessCriteriaMet=true must precede
// Complete=true, so we set both and update the status in a single call.
func markJobSucceeded(t *testing.T, ctx context.Context, job *batchv1.Job) {
	t.Helper()
	now := metav1.Now()
	job.Status.Succeeded = 1
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
		batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
	)
	if err := testClient.Status().Update(ctx, job); err != nil {
		t.Fatalf("marking Job succeeded: %v", err)
	}
}

// markJobFailed transitions a Job to terminal Failed=True. Same K8s 1.32+
// rule applies: FailureTarget must precede Failed. K8s only sets
// CompletionTime on Complete=True Jobs, never on Failed ones, so we leave
// it nil here.
func markJobFailed(t *testing.T, ctx context.Context, job *batchv1.Job) {
	t.Helper()
	now := metav1.Now()
	job.Status.Failed = 1
	job.Status.StartTime = &now
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now},
		batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now},
	)
	if err := testClient.Status().Update(ctx, job); err != nil {
		t.Fatalf("marking Job failed: %v", err)
	}
}

func finishedJob(name string, success bool) batchv1.Job {
	condType := batchv1.JobFailed
	if success {
		condType = batchv1.JobComplete
	}
	return batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: condType, Status: corev1.ConditionTrue}},
		},
	}
}

func unfinishedJob(name string) batchv1.Job {
	return batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func listJobs(t *testing.T, ctx context.Context, ns string) []batchv1.Job {
	t.Helper()
	var jobs batchv1.JobList
	if err := testClient.List(ctx, &jobs, client.InNamespace(ns)); err != nil {
		t.Fatalf("listing Jobs: %v", err)
	}
	return jobs.Items
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ownedBy(job *batchv1.Job, rule *aprv1alpha1.ArchiveRule) bool {
	for _, ref := range job.OwnerReferences {
		if ref.UID == rule.UID && ref.Controller != nil && *ref.Controller {
			return true
		}
	}
	return false
}

func mustCreateNamespace(t *testing.T, ctx context.Context, prefix string) string {
	t.Helper()
	name := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	if err := testClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	return name
}

func mustCreate(t *testing.T, ctx context.Context, obj client.Object) {
	t.Helper()
	if err := testClient.Create(ctx, obj); err != nil {
		t.Fatalf("create %T %s/%s: %v", obj, obj.GetNamespace(), obj.GetName(), err)
	}
}

func getArchiveRule(t *testing.T, ctx context.Context, ns, name string) *aprv1alpha1.ArchiveRule {
	t.Helper()
	var rule aprv1alpha1.ArchiveRule
	if err := testClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &rule); err != nil {
		t.Fatalf("get ArchiveRule: %v", err)
	}
	return &rule
}

func reqFor(ns, name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
}

func requireReady(t *testing.T, conds []metav1.Condition, status metav1.ConditionStatus, reason string) {
	t.Helper()
	cond := apimeta.FindStatusCondition(conds, ConditionReady)
	if cond == nil {
		t.Fatalf("Ready condition not set")
	}
	if cond.Status != status || cond.Reason != reason {
		t.Fatalf("Ready condition = {Status:%s, Reason:%s}, want {Status:%s, Reason:%s}",
			cond.Status, cond.Reason, status, reason)
	}
}
