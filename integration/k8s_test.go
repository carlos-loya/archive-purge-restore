// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

//go:build k8s

// Package integration's k8s_test exercises the full APR operator loop
// against a real Kubernetes cluster (kind), an in-cluster Postgres + MinIO
// data plane, and a Helm-installed operator.
//
// Prereqs: the cluster must already be running and the operator installed.
// Use `make test-k8s` (assumes you've run kind-up + kind-data-plane +
// kind-load + kind-install) or `make test-k8s-clean` to bring everything
// up from scratch.
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors" //nolint:unused // referenced in webhook smoke test
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// Connection settings — these match what `dev/kind/kind-config.yaml`
// exposes via NodePort to the host, plus the credentials baked into the
// in-cluster manifests under dev/kind/.
const (
	pgHostExternal = "127.0.0.1"
	pgPortExternal = "15432"
	pgUser         = "postgres"
	pgPassword     = "postgres"
	pgDatabase     = "orders"

	minioEndpointExternal = "http://127.0.0.1:19000"
	minioAccessKey        = "minioadmin"
	minioSecretKey        = "minioadmin"
	minioBucket           = "test-archive"

	// In-cluster service DNS — what the CR specs reference so pods inside
	// the cluster reach the data plane.
	pgHostInternal    = "postgres.data.svc.cluster.local"
	minioHostInternal = "http://minio.data.svc.cluster.local:9000"

	// Default Helm install names (assumes `helm install apr ./charts/apr`).
	runnerSAName    = "apr-runner"
	runnerRoleName  = "apr-runner"
	managerSelector = "app.kubernetes.io/name=apr,app.kubernetes.io/component=manager"
)

// TestEndToEnd_WebhookRejectsBadCRs is a smoke test that the validating
// admission webhook is wired up and reachable. We don't replicate the
// per-field unit-test matrix here — that lives in internal/webhook/_test.
// We just confirm the webhook is being called at all by submitting CRs we
// know it should reject and asserting the API server returns a Forbidden
// admission denial (rather than the API server admitting the CR).
func TestEndToEnd_WebhookRejectsBadCRs(t *testing.T) {
	ctx := t.Context()
	c := newK8sClient(t)
	ns := setupTestNamespace(t, ctx, c)

	// Bad cron expression: should be rejected by the ArchiveRule webhook.
	bad := &aprv1alpha1.ArchiveRule{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-cron", Namespace: ns},
		Spec: aprv1alpha1.ArchiveRuleSpec{
			DatabaseRef: corev1.LocalObjectReference{Name: "nonexistent-dbc"},
			StorageRef:  corev1.LocalObjectReference{Name: "nonexistent-sb"},
			Table:       "orders",
			DateColumn:  "created_at",
			DaysOnline:  30,
			Schedule:    "this is not a cron expression",
		},
	}
	if err := c.Create(ctx, bad); err == nil {
		t.Fatal("expected webhook to reject bad cron, but Create succeeded")
	} else {
		t.Logf("webhook correctly rejected bad cron: %v", err)
		if !apierrors.IsInvalid(err) && !apierrors.IsForbidden(err) {
			t.Errorf("expected Invalid or Forbidden, got %T: %v", err, err)
		}
	}

	// RestoreRequest pointing at a missing ArchiveRule: rejected by the
	// RestoreRequest webhook.
	bad2 := &aprv1alpha1.RestoreRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-restore", Namespace: ns},
		Spec: aprv1alpha1.RestoreRequestSpec{
			ArchiveRuleRef: corev1.LocalObjectReference{Name: "no-such-rule"},
		},
	}
	if err := c.Create(ctx, bad2); err == nil {
		t.Fatal("expected webhook to reject RestoreRequest with missing ArchiveRule")
	}
}

func TestEndToEnd_ArchiveAndRestore(t *testing.T) {
	ctx := t.Context()

	c := newK8sClient(t)
	db := openPostgres(t)
	defer db.Close()
	s3c := newMinioClient(t, ctx)

	// Cleanup any leftover state from a previous run so the test is
	// repeatable against a long-lived cluster.
	cleanMinioBucket(t, ctx, s3c)
	resetPostgresSchema(t, ctx, db)

	initialCount := countOrders(t, ctx, db)
	if initialCount == 0 {
		t.Fatal("expected non-empty seed data; check dev/kind/postgres.yaml")
	}
	t.Logf("initial row count: %d", initialCount)

	ns := setupTestNamespace(t, ctx, c)
	t.Logf("test namespace: %s", ns)

	provisionRunnerRBAC(t, ctx, c, ns)
	applyAprResources(t, ctx, c, ns)

	// Phase 1: ArchiveRule should reach Ready=True after the operator
	// resolves DBC + SB. With controller-driven scheduling there is no
	// CronJob — the operator owns the schedule itself.
	waitForArchiveRuleReady(t, ctx, c, ns, "test-archive", 60*time.Second)
	if cjs := listCronJobs(t, ctx, c, ns); len(cjs) != 0 {
		t.Errorf("expected zero CronJobs (controller-driven scheduling), got %d", len(cjs))
	}

	// Phase 2: trigger an immediate run via the apr.dev/trigger-time
	// annotation. The operator picks up the change on its next reconcile
	// and spawns a Job directly.
	triggerArchiveRule(t, ctx, c, ns, "test-archive")
	finalRule := waitForArchiveSucceeded(t, ctx, c, ns, "test-archive", 180*time.Second)
	t.Logf("archive recorded: rows=%d, runID=%s", finalRule.Status.LastRunRowsArchived, finalRule.Status.LastRunID)
	if finalRule.Status.LastJobRef == nil {
		t.Error("expected status.lastJobRef set after successful archive")
	}

	if finalRule.Status.LastRunRowsArchived == 0 {
		t.Error("LastRunRowsArchived = 0; expected old rows to be archived")
	}

	// Phase 3: rows older than 90 days should be gone from Postgres.
	afterArchiveCount := countOrders(t, ctx, db)
	if afterArchiveCount >= initialCount {
		t.Errorf("expected row count to decrease after archive; before=%d after=%d", initialCount, afterArchiveCount)
	}
	t.Logf("post-archive row count: %d (delta: %d)", afterArchiveCount, initialCount-afterArchiveCount)

	// Phase 4: Parquet objects should exist in MinIO.
	files := listMinioObjects(t, ctx, s3c)
	if len(files) == 0 {
		t.Error("expected parquet objects in MinIO bucket; found none")
	}
	t.Logf("MinIO contains %d archive object(s)", len(files))

	// Phase 5: apply a RestoreRequest and wait for completion.
	applyRestoreRequest(t, ctx, c, ns, "test-restore", "test-archive")
	rr := waitForRestoreSucceeded(t, ctx, c, ns, "test-restore", 180*time.Second)
	t.Logf("restore recorded: rows=%d", rr.Status.RowsRestored)

	if rr.Status.RowsRestored == 0 {
		t.Error("RowsRestored = 0; expected restore to insert archived rows back")
	}

	// Phase 6: Postgres should be back to the initial row count.
	afterRestoreCount := countOrders(t, ctx, db)
	if afterRestoreCount != initialCount {
		t.Errorf("expected restore to bring count back to %d, got %d",
			initialCount, afterRestoreCount)
	}
}


// --- K8s client setup ---

func newK8sClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aprv1alpha1.AddToScheme(scheme))
	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("building client: %v", err)
	}
	return c
}

func setupTestNamespace(t *testing.T, ctx context.Context, c client.Client) string {
	t.Helper()
	name := fmt.Sprintf("apr-e2e-%d", time.Now().UnixNano())
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	t.Cleanup(func() {
		// Best-effort cleanup; ignore errors so the test still reports
		// the real failure.
		_ = c.Delete(context.Background(), ns)
	})
	return name
}

// provisionRunnerRBAC creates the runner ServiceAccount in the test
// namespace and binds the chart's runner ClusterRole to it. Required
// because the chart only creates the SA in its release namespace.
func provisionRunnerRBAC(t *testing.T, ctx context.Context, c client.Client, ns string) {
	t.Helper()
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: runnerSAName, Namespace: ns},
	}
	mustCreate(t, ctx, c, sa)

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: runnerSAName, Namespace: ns},
		Subjects: []rbacv1.Subject{{
			Kind: "ServiceAccount", Name: runnerSAName, Namespace: ns,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     runnerRoleName,
		},
	}
	mustCreate(t, ctx, c, rb)
}

// --- CR fixtures ---

func applyAprResources(t *testing.T, ctx context.Context, c client.Client, ns string) {
	t.Helper()

	mustCreate(t, ctx, c, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-creds", Namespace: ns},
		StringData: map[string]string{"username": pgUser, "password": pgPassword},
	})

	mustCreate(t, ctx, c, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-creds", Namespace: ns},
		StringData: map[string]string{
			"access_key_id":     minioAccessKey,
			"secret_access_key": minioSecretKey,
		},
	})

	mustCreate(t, ctx, c, &aprv1alpha1.DatabaseConnection{
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: ns},
		Spec: aprv1alpha1.DatabaseConnectionSpec{
			Engine:               aprv1alpha1.EnginePostgres,
			Host:                 pgHostInternal,
			Port:                 5432,
			Database:             pgDatabase,
			SSLMode:              "disable",
			CredentialsSecretRef: corev1.LocalObjectReference{Name: "pg-creds"},
		},
	})

	mustCreate(t, ctx, c, &aprv1alpha1.StorageBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "minio", Namespace: ns},
		Spec: aprv1alpha1.StorageBackendSpec{
			Type:                 aprv1alpha1.StorageS3,
			Bucket:               minioBucket,
			Region:               "us-east-1",
			Endpoint:             minioHostInternal,
			CredentialsSecretRef: &corev1.LocalObjectReference{Name: "minio-creds"},
		},
	})

	mustCreate(t, ctx, c, &aprv1alpha1.ArchiveRule{
		ObjectMeta: metav1.ObjectMeta{Name: "test-archive", Namespace: ns},
		Spec: aprv1alpha1.ArchiveRuleSpec{
			DatabaseRef: corev1.LocalObjectReference{Name: "pg"},
			StorageRef:  corev1.LocalObjectReference{Name: "minio"},
			Table:       "orders",
			DateColumn:  "created_at",
			DaysOnline:  90,
			Schedule:    "0 0 31 2 *", // never fires (Feb 31st); we trigger manually
			BatchSize:   1000,
		},
	})
}

func applyRestoreRequest(t *testing.T, ctx context.Context, c client.Client, ns, name, ruleName string) {
	t.Helper()
	mustCreate(t, ctx, c, &aprv1alpha1.RestoreRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aprv1alpha1.RestoreRequestSpec{
			ArchiveRuleRef: corev1.LocalObjectReference{Name: ruleName},
		},
	})
}

// --- Wait helpers ---

func waitForArchiveRuleReady(t *testing.T, ctx context.Context, c client.Client, ns, name string, timeout time.Duration) *aprv1alpha1.ArchiveRule {
	t.Helper()
	var got aprv1alpha1.ArchiveRule
	waitFor(t, ctx, timeout, func() bool {
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &got); err != nil {
			return false
		}
		cond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
		return cond != nil && cond.Status == metav1.ConditionTrue
	}, fmt.Sprintf("ArchiveRule %s/%s Ready=True", ns, name))
	return &got
}

// waitForArchiveSucceeded waits until BOTH LastRunResult=Succeeded (set by
// the Job pod's sink) AND LastJobRef is populated (set by the operator's
// reconciler when it observes the finished Job). These two writes happen
// independently; without waiting for both we'd race the reconciler.
func waitForArchiveSucceeded(t *testing.T, ctx context.Context, c client.Client, ns, name string, timeout time.Duration) *aprv1alpha1.ArchiveRule {
	t.Helper()
	var got aprv1alpha1.ArchiveRule
	waitFor(t, ctx, timeout, func() bool {
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &got); err != nil {
			return false
		}
		return got.Status.LastRunResult == aprv1alpha1.ArchiveRunSucceeded &&
			got.Status.LastJobRef != nil
	}, fmt.Sprintf("ArchiveRule %s/%s LastRunResult=Succeeded with LastJobRef", ns, name))
	return &got
}

func waitForRestoreSucceeded(t *testing.T, ctx context.Context, c client.Client, ns, name string, timeout time.Duration) *aprv1alpha1.RestoreRequest {
	t.Helper()
	var got aprv1alpha1.RestoreRequest
	waitFor(t, ctx, timeout, func() bool {
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &got); err != nil {
			return false
		}
		return got.Status.Phase == aprv1alpha1.RestoreSucceeded
	}, fmt.Sprintf("RestoreRequest %s/%s Phase=Succeeded", ns, name))
	return &got
}

// triggerArchiveRule patches the apr.dev/trigger-time annotation onto an
// ArchiveRule. The operator's reconciler picks up the new annotation on
// its next reconcile and spawns an immediate archive Job, bypassing the
// cron schedule.
func triggerArchiveRule(t *testing.T, ctx context.Context, c client.Client, ns, name string) {
	t.Helper()
	var rule aprv1alpha1.ArchiveRule
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &rule); err != nil {
		t.Fatalf("get ArchiveRule: %v", err)
	}
	patch := client.MergeFrom(rule.DeepCopy())
	if rule.Annotations == nil {
		rule.Annotations = map[string]string{}
	}
	rule.Annotations[aprv1alpha1.AnnotationTriggerTime] = time.Now().Format(time.RFC3339)
	if err := c.Patch(ctx, &rule, patch); err != nil {
		t.Fatalf("patching trigger annotation: %v", err)
	}
	t.Logf("triggered archive rule %s/%s via annotation", ns, name)
}

func listCronJobs(t *testing.T, ctx context.Context, c client.Client, ns string) []batchv1.CronJob {
	t.Helper()
	var cjs batchv1.CronJobList
	if err := c.List(ctx, &cjs, client.InNamespace(ns)); err != nil {
		t.Fatalf("listing CronJobs: %v", err)
	}
	return cjs.Items
}

func waitFor(t *testing.T, ctx context.Context, timeout time.Duration, predicate func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for %s: %v", what, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

// --- Postgres helpers ---

func openPostgres(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		pgHostExternal, pgPortExternal, pgUser, pgPassword, pgDatabase)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("opening postgres: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("postgres unreachable at %s:%s: %v (is `make kind-data-plane` done?)",
			pgHostExternal, pgPortExternal, err)
	}
	return db
}

func countOrders(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM orders").Scan(&n); err != nil {
		t.Fatalf("counting orders: %v", err)
	}
	return n
}

// resetPostgresSchema drops + reseeds the orders table so the test is
// idempotent against a long-lived cluster.
func resetPostgresSchema(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TABLE IF EXISTS orders`,
		`CREATE TABLE orders (
			id          SERIAL PRIMARY KEY,
			customer    TEXT NOT NULL,
			total       NUMERIC(10,2) NOT NULL,
			created_at  TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
		`INSERT INTO orders (customer, total, created_at)
			SELECT 'old-' || g, (random()*1000)::numeric(10,2),
				NOW() - INTERVAL '200 days' + (g || ' days')::interval
			FROM generate_series(1, 50) g`,
		`INSERT INTO orders (customer, total, created_at)
			SELECT 'recent-' || g, (random()*1000)::numeric(10,2),
				NOW() - (g || ' days')::interval
			FROM generate_series(1, 50) g`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("seed exec %q: %v", firstLine(s), err)
		}
	}
}

func firstLine(s string) string {
	if i := strings.Index(s, "\n"); i >= 0 {
		return s[:i]
	}
	return s
}

// --- MinIO helpers ---

func newMinioClient(t *testing.T, ctx context.Context) *s3.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(minioAccessKey, minioSecretKey, "")),
	)
	if err != nil {
		t.Fatalf("loading aws config for MinIO: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpointExternal)
		o.UsePathStyle = true
	})
}

func listMinioObjects(t *testing.T, ctx context.Context, c *s3.Client) []string {
	t.Helper()
	out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(minioBucket)})
	if err != nil {
		t.Fatalf("listing minio objects: %v", err)
	}
	keys := make([]string, 0, len(out.Contents))
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	return keys
}

func cleanMinioBucket(t *testing.T, ctx context.Context, c *s3.Client) {
	t.Helper()
	keys := listMinioObjects(t, ctx, c)
	for _, k := range keys {
		if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(minioBucket),
			Key:    aws.String(k),
		}); err != nil {
			t.Logf("cleanup: deleting %s failed: %v", k, err)
		}
	}
}

// --- Generic helpers ---

func mustCreate(t *testing.T, ctx context.Context, c client.Client, obj client.Object) {
	t.Helper()
	if err := c.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create %T %s/%s: %v", obj, obj.GetNamespace(), obj.GetName(), err)
	}
}
