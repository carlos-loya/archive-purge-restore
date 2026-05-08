// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package controller

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	aprv1alpha1 "github.com/carlos-loya/archive-purge-restore/api/v1alpha1"
)

// envtest harness shared by every test in this package.
//
// envtest spins up a real kube-apiserver + etcd from the binary bundle that
// `setup-envtest` provides. We do this once per package via TestMain because
// each Start/Stop is in the multi-second range. Tests get isolation by
// creating a fresh namespace per test (see test helpers).
//
// If KUBEBUILDER_ASSETS is unset, TestMain runs without setting up envtest
// and individual tests skip via requireEnvtest. Run `make test-envtest` to
// install the assets and execute these tests.
var (
	testEnv    *envtest.Environment
	testCfg    *rest.Config
	testClient client.Client
	testScheme = runtime.NewScheme()
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		utilruntime.Must(clientgoscheme.AddToScheme(testScheme))
		utilruntime.Must(aprv1alpha1.AddToScheme(testScheme))

		repoRoot, err := filepath.Abs("../..")
		if err != nil {
			fmt.Fprintf(os.Stderr, "envtest setup: %v\n", err)
			os.Exit(1)
		}
		testEnv = &envtest.Environment{
			CRDDirectoryPaths:     []string{filepath.Join(repoRoot, "config", "crd", "bases")},
			ErrorIfCRDPathMissing: true,
		}
		cfg, err := testEnv.Start()
		if err != nil {
			fmt.Fprintf(os.Stderr, "envtest start: %v\n", err)
			os.Exit(1)
		}
		testCfg = cfg
		testClient, err = client.New(testCfg, client.Options{Scheme: testScheme})
		if err != nil {
			fmt.Fprintf(os.Stderr, "envtest client: %v\n", err)
			os.Exit(1)
		}
	}

	code := m.Run()

	if testEnv != nil {
		_ = testEnv.Stop()
	}
	os.Exit(code)
}

// requireEnvtest skips the test if the envtest control plane wasn't started.
func requireEnvtest(t *testing.T) {
	t.Helper()
	if testCfg == nil {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make test-envtest` to enable controller tests")
	}
}
