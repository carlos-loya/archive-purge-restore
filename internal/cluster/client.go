// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

// Package cluster contains the runtime glue that lets the apr CLI execute
// against APR custom resources instead of YAML config. It is loaded into the
// apr binary so that pods spawned by the operator can run `apr archive
// --from-cr <ns>/<name>` (and the equivalent restore command).
//
// internal/engine and internal/format MUST NOT import this package — they
// stay K8s-unaware. This package only depends on engine/config types in one
// direction: it produces them.
package cluster

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// NewClient returns a client.Client wired with the APR scheme. It uses the
// in-cluster ServiceAccount when running inside a pod, or KUBECONFIG /
// ~/.kube/config when running locally.
func NewClient() (client.Client, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(aprv1alpha1.AddToScheme(scheme))

	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubernetes config: %w", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("building kubernetes client: %w", err)
	}
	return c, nil
}
