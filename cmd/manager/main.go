// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

// Command apr-manager is the Kubernetes operator that watches APR custom
// resources (DatabaseConnection, StorageBackend, ArchiveRule, RestoreRequest)
// and reconciles them into native Kubernetes workloads (Jobs).
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
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
	"github.com/carlos-loya/archive-purge-restore/internal/controller"
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

	zapOpts := zap.Options{Development: true}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	if printVersion {
		fmt.Printf("apr-manager version %s\n", version)
		return
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "apr-manager.apr.dev",
	})
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
