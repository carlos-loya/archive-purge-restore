package filesystem

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	dir := t.TempDir()
	p, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	data := []byte("hello world")
	if err := p.Put(ctx, "test/file.txt", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	rc, err := p.Get(ctx, "test/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Get() = %q, want %q", got, data)
	}
}

func TestExists(t *testing.T) {
	dir := t.TempDir()
	p, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	exists, err := p.Exists(ctx, "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("Exists() = true for nonexistent key")
	}

	if err := p.Put(ctx, "exists.txt", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatal(err)
	}
	exists, err = p.Exists(ctx, "exists.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("Exists() = false for existing key")
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	p, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := p.Put(ctx, "to-delete.txt", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatal(err)
	}

	if err := p.Delete(ctx, "to-delete.txt"); err != nil {
		t.Fatal(err)
	}

	exists, err := p.Exists(ctx, "to-delete.txt")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("file still exists after Delete()")
	}

	// Deleting nonexistent file should not error.
	if err := p.Delete(ctx, "nonexistent"); err != nil {
		t.Errorf("Delete(nonexistent) error: %v", err)
	}
}

func TestList(t *testing.T) {
	dir := t.TempDir()
	p, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	files := []string{
		"db/orders/2025-01-01/run1.parquet",
		"db/orders/2025-01-02/run2.parquet",
		"db/users/2025-01-01/run3.parquet",
	}
	for _, f := range files {
		if err := p.Put(ctx, f, bytes.NewReader([]byte("data"))); err != nil {
			t.Fatal(err)
		}
	}

	objects, err := p.List(ctx, "db/orders/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Errorf("List(db/orders/) returned %d objects, want 2", len(objects))
	}

	objects, err = p.List(ctx, "db/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 3 {
		t.Errorf("List(db/) returned %d objects, want 3", len(objects))
	}
}

func TestRename(t *testing.T) {
	dir := t.TempDir()
	p, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	data := []byte("rename me")
	if err := p.Put(ctx, "old.txt", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	if err := p.Rename(ctx, "old.txt", "subdir/new.txt"); err != nil {
		t.Fatal(err)
	}

	exists, _ := p.Exists(ctx, "old.txt")
	if exists {
		t.Error("old key still exists after Rename()")
	}

	rc, err := p.Get(ctx, "subdir/new.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("renamed file content = %q, want %q", got, data)
	}
}

// errReader is an io.Reader that returns an error after reading n bytes.
type errReader struct {
	data []byte
	pos  int
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.err
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, r.err
	}
	return n, nil
}

func TestPutWriteErrorCleansUpFile(t *testing.T) {
	dir := t.TempDir()
	p, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	readErr := errors.New("simulated read failure")
	reader := &errReader{data: []byte("partial"), err: readErr}

	err = p.Put(ctx, "sub/fail.txt", reader)
	if err == nil {
		t.Fatal("Put() should have returned an error")
	}
	if !errors.Is(err, readErr) {
		t.Errorf("Put() error = %v, want wrapped %v", err, readErr)
	}

	// The partial file should have been cleaned up.
	path := filepath.Join(dir, "sub", "fail.txt")
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("partial file %s was not cleaned up after write error", path)
	}
}

func TestPutNoDoubleClose(t *testing.T) {
	dir := t.TempDir()
	p, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	data := []byte("test content for close check")
	if err := p.Put(ctx, "close-test.txt", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// Verify the file was written correctly (no double-close corruption).
	rc, err := p.Get(ctx, "close-test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Get() = %q, want %q", got, data)
	}
}
