// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Carlos Loya

package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// fakeProvider records every method call so the decorator test can
// confirm the wrapper forwards arguments faithfully.
type fakeProvider struct {
	calls  []string
	putErr error
}

func (f *fakeProvider) Put(_ context.Context, key string, _ io.Reader) error {
	f.calls = append(f.calls, "put:"+key)
	return f.putErr
}
func (f *fakeProvider) Get(_ context.Context, key string) (io.ReadCloser, error) {
	f.calls = append(f.calls, "get:"+key)
	return io.NopCloser(strings.NewReader("data")), nil
}
func (f *fakeProvider) Delete(_ context.Context, key string) error {
	f.calls = append(f.calls, "delete:"+key)
	return nil
}
func (f *fakeProvider) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	f.calls = append(f.calls, "list:"+prefix)
	return nil, nil
}
func (f *fakeProvider) Exists(_ context.Context, key string) (bool, error) {
	f.calls = append(f.calls, "exists:"+key)
	return true, nil
}
func (f *fakeProvider) Rename(_ context.Context, oldKey, newKey string) error {
	f.calls = append(f.calls, "rename:"+oldKey+"->"+newKey)
	return nil
}

func TestInstrument_ForwardsAllOperations(t *testing.T) {
	fake := &fakeProvider{}
	wrapped := Instrument(fake, "test")
	ctx := context.Background()

	if err := wrapped.Put(ctx, "a", strings.NewReader("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := wrapped.Get(ctx, "a"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := wrapped.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := wrapped.List(ctx, "p/"); err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, err := wrapped.Exists(ctx, "a"); err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if err := wrapped.Rename(ctx, "a", "b"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	want := []string{
		"put:a", "get:a", "delete:a", "list:p/", "exists:a", "rename:a->b",
	}
	if len(fake.calls) != len(want) {
		t.Fatalf("got %d forwarded calls, want %d: %v", len(fake.calls), len(want), fake.calls)
	}
	for i, w := range want {
		if fake.calls[i] != w {
			t.Errorf("call %d = %q, want %q", i, fake.calls[i], w)
		}
	}
}

func TestInstrument_PropagatesErrors(t *testing.T) {
	fake := &fakeProvider{putErr: errors.New("simulated put failure")}
	wrapped := Instrument(fake, "test")
	if err := wrapped.Put(context.Background(), "a", strings.NewReader("x")); err == nil {
		t.Fatal("expected error from inner provider to propagate")
	}
}
