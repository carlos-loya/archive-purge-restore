// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package storage

import (
	"context"
	"io"

	"github.com/carlos-loya/archive-purge-restore/internal/metrics"
)

// Instrument wraps a Provider so every operation is timed and recorded
// against apr_storage_operation_duration_seconds with the given backend
// type label (e.g. "s3", "r2", "gcs", "filesystem"). The decorator is a
// pass-through — it makes no decisions of its own — so wrapping a provider
// is always a safe addition.
//
// Caller convention: the Job pod's runtime (internal/cluster) is what
// wraps the underlying provider so the CLI and operator both produce
// identical counters when run under instrumentation. The CLI doesn't
// expose /metrics, so its observations are simply discarded at exit.
func Instrument(p Provider, backendType string) Provider {
	return &instrumented{inner: p, typ: backendType}
}

type instrumented struct {
	inner Provider
	typ   string
}

func (i *instrumented) Put(ctx context.Context, key string, reader io.Reader) (err error) {
	defer metrics.ObserveStorageOp(i.typ, "put")(&err)
	err = i.inner.Put(ctx, key, reader)
	return
}

func (i *instrumented) Get(ctx context.Context, key string) (rc io.ReadCloser, err error) {
	defer metrics.ObserveStorageOp(i.typ, "get")(&err)
	rc, err = i.inner.Get(ctx, key)
	return
}

func (i *instrumented) Delete(ctx context.Context, key string) (err error) {
	defer metrics.ObserveStorageOp(i.typ, "delete")(&err)
	err = i.inner.Delete(ctx, key)
	return
}

func (i *instrumented) List(ctx context.Context, prefix string) (objs []ObjectInfo, err error) {
	defer metrics.ObserveStorageOp(i.typ, "list")(&err)
	objs, err = i.inner.List(ctx, prefix)
	return
}

func (i *instrumented) Exists(ctx context.Context, key string) (ok bool, err error) {
	defer metrics.ObserveStorageOp(i.typ, "exists")(&err)
	ok, err = i.inner.Exists(ctx, key)
	return
}

func (i *instrumented) Rename(ctx context.Context, oldKey, newKey string) (err error) {
	defer metrics.ObserveStorageOp(i.typ, "rename")(&err)
	err = i.inner.Rename(ctx, oldKey, newKey)
	return
}
