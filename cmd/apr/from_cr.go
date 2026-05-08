// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/carlos-loya/archive-purge-restore/internal/cluster"
)

// runArchiveFromCR is invoked when `apr archive --from-cr <ns>/<name>` is
// used. It bypasses YAML config loading entirely — the CR is the source of
// truth — and writes the run outcome back to the CR's status.
func runArchiveFromCR(cmd *cobra.Command, ref string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c, err := cluster.NewClient()
	if err != nil {
		return err
	}
	return cluster.RunArchiveFromCR(ctx, c, ref, buildLogger())
}

// runRestoreFromCR is invoked when `apr restore --from-cr <ns>/<name>` is
// used. The RestoreRequest CR carries all the parameters (table/date/runID
// filters and the referenced ArchiveRule).
func runRestoreFromCR(cmd *cobra.Command, ref string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c, err := cluster.NewClient()
	if err != nil {
		return err
	}
	return cluster.RunRestoreFromCR(ctx, c, ref, buildLogger())
}
