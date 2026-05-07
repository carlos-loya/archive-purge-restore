// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

// Command apr-manager is the Kubernetes operator that watches APR custom
// resources (DatabaseConnection, StorageBackend, ArchiveRule, RestoreRequest)
// and reconciles them into native Kubernetes workloads (CronJobs, Jobs).
//
// The CLI binary in cmd/apr remains the supported tool for non-Kubernetes
// environments. Both binaries share the engine implementation in
// internal/engine.
package main

import (
	"flag"
	"fmt"
	"os"

	// Ensure auth provider plugins (e.g. GCP, OIDC) are linked in for
	// out-of-cluster kubeconfig contexts that need them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	webhookserver "sigs.k8s.io/controller-runtime/pkg/webhook"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
	"github.com/carlos-loya/archive-purge-restore/internal/controller"
	aprwebhook "github.com/carlos-loya/archive-purge-restore/internal/webhook"
)

var (
	version  = "dev"
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aprv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		printVersion         bool
		archiveImage         string
		archiveRunnerSA      string
		enableWebhooks       bool
		webhookPort          int
		webhookCertDir       string
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		"address the metric endpoint binds to")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081",
		"address the probe endpoint binds to")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"enable leader election for controller manager")
	flag.BoolVar(&printVersion, "version", false, "print version and exit")
	flag.StringVar(&archiveImage, "archive-image",
		"ghcr.io/carlos-loya/archive-purge-restore:dev",
		"container image used by spawned archive Job pods")
	flag.StringVar(&archiveRunnerSA, "archive-runner-service-account", "apr-runner",
		"ServiceAccount name used by spawned archive Job pods")
	flag.BoolVar(&enableWebhooks, "enable-webhooks", false,
		"enable validating admission webhooks (requires a TLS cert mounted at --webhook-cert-dir)")
	flag.IntVar(&webhookPort, "webhook-port", 9443,
		"port the webhook server binds to")
	flag.StringVar(&webhookCertDir, "webhook-cert-dir",
		"/tmp/k8s-webhook-server/serving-certs",
		"directory containing the webhook server's tls.crt and tls.key")

	zapOpts := zap.Options{Development: true}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	if printVersion {
		fmt.Printf("apr-manager version %s\n", version)
		return
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "apr-manager.apr.dev",
	}
	if enableWebhooks {
		mgrOpts.WebhookServer = webhookserver.NewServer(webhookserver.Options{
			Port:    webhookPort,
			CertDir: webhookCertDir,
		})
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := (&controller.DatabaseConnectionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DatabaseConnection")
		os.Exit(1)
	}
	if err := (&controller.StorageBackendReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "StorageBackend")
		os.Exit(1)
	}
	if err := (&controller.ArchiveRuleReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		ArchiveImage: archiveImage,
		RunnerSA:     archiveRunnerSA,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ArchiveRule")
		os.Exit(1)
	}
	if err := (&controller.RestoreRequestReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		ArchiveImage: archiveImage,
		RunnerSA:     archiveRunnerSA,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RestoreRequest")
		os.Exit(1)
	}

	if enableWebhooks {
		if err := registerWebhooks(mgr); err != nil {
			setupLog.Error(err, "registering webhooks")
			os.Exit(1)
		}
		setupLog.Info("validating admission webhooks enabled", "port", webhookPort, "certDir", webhookCertDir)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager", "version", version)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited with error")
		os.Exit(1)
	}
}

// registerWebhooks wires every CRD's validator into the manager's webhook
// server. Each validator is registered at the standard kubebuilder-generated
// path (/validate-<group>-<version>-<kind>) — the chart's
// ValidatingWebhookConfiguration must reference these same paths.
func registerWebhooks(mgr ctrl.Manager) error {
	c := mgr.GetClient()
	if err := builder.WebhookManagedBy(mgr, &aprv1alpha1.ArchiveRule{}).
		WithValidator(&aprwebhook.ArchiveRuleValidator{Client: c}).
		Complete(); err != nil {
		return fmt.Errorf("ArchiveRule webhook: %w", err)
	}
	if err := builder.WebhookManagedBy(mgr, &aprv1alpha1.RestoreRequest{}).
		WithValidator(&aprwebhook.RestoreRequestValidator{Client: c}).
		Complete(); err != nil {
		return fmt.Errorf("RestoreRequest webhook: %w", err)
	}
	if err := builder.WebhookManagedBy(mgr, &aprv1alpha1.DatabaseConnection{}).
		WithValidator(&aprwebhook.DatabaseConnectionValidator{Client: c}).
		Complete(); err != nil {
		return fmt.Errorf("DatabaseConnection webhook: %w", err)
	}
	if err := builder.WebhookManagedBy(mgr, &aprv1alpha1.StorageBackend{}).
		WithValidator(&aprwebhook.StorageBackendValidator{Client: c}).
		Complete(); err != nil {
		return fmt.Errorf("StorageBackend webhook: %w", err)
	}
	return nil
}
