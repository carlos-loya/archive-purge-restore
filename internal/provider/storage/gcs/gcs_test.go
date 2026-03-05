package gcs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

type mockGCSClient struct {
	objects map[string][]byte

	putErr      error
	getErr      error
	deleteErr   error
	deleteFailN int // fail this many times before succeeding (0 = use deleteErr for all)
	deleteCalls int
	listErr     error
	existsErr   error
	copyErr     error
}

func newMockClient() *mockGCSClient {
	return &mockGCSClient{objects: make(map[string][]byte)}
}

func (m *mockGCSClient) Put(_ context.Context, key string, reader io.Reader) error {
	if m.putErr != nil {
		return m.putErr
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.objects[key] = data
	return nil
}

func (m *mockGCSClient) Get(_ context.Context, key string) (io.ReadCloser, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	data, ok := m.objects[key]
	if !ok {
		return nil, errors.New("storage: object doesn't exist")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockGCSClient) Delete(_ context.Context, key string) error {
	m.deleteCalls++
	if m.deleteErr != nil {
		if m.deleteFailN > 0 && m.deleteCalls > m.deleteFailN {
			// Stop failing after N calls.
		} else {
			return m.deleteErr
		}
	}
	delete(m.objects, key)
	return nil
}

func (m *mockGCSClient) List(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	now := time.Now()
	var objects []storage.ObjectInfo
	for key, data := range m.objects {
		if strings.HasPrefix(key, prefix) {
			objects = append(objects, storage.ObjectInfo{
				Key:          key,
				Size:         int64(len(data)),
				LastModified: now,
			})
		}
	}
	return objects, nil
}

func (m *mockGCSClient) Exists(_ context.Context, key string) (bool, error) {
	if m.existsErr != nil {
		return false, m.existsErr
	}
	_, ok := m.objects[key]
	return ok, nil
}

func (m *mockGCSClient) Copy(_ context.Context, srcKey, dstKey string) error {
	if m.copyErr != nil {
		return m.copyErr
	}
	data, ok := m.objects[srcKey]
	if !ok {
		return errors.New("storage: object doesn't exist")
	}
	m.objects[dstKey] = append([]byte{}, data...)
	return nil
}

func newTestProvider(mock *mockGCSClient, prefix string) *Provider {
	return &Provider{
		client: mock,
		bucket: "test-bucket",
		prefix: prefix,
	}
}

func TestFullKey(t *testing.T) {
	tests := []struct {
		prefix string
		key    string
		want   string
	}{
		{"", "file.txt", "file.txt"},
		{"archives/", "file.txt", "archives/file.txt"},
		{"pre/", "db/table/data.parquet", "pre/db/table/data.parquet"},
	}
	for _, tt := range tests {
		p := &Provider{prefix: tt.prefix}
		if got := p.fullKey(tt.key); got != tt.want {
			t.Errorf("fullKey(%q) with prefix %q = %q, want %q", tt.key, tt.prefix, got, tt.want)
		}
	}
}

func TestPutAndGet(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "")
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

func TestPutWithPrefix(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "archives/")
	ctx := context.Background()

	data := []byte("prefixed data")
	if err := p.Put(ctx, "file.txt", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	if _, ok := mock.objects["archives/file.txt"]; !ok {
		t.Error("object not stored with prefixed key")
	}

	rc, err := p.Get(ctx, "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("Get() = %q, want %q", got, data)
	}
}

func TestPutError(t *testing.T) {
	mock := newMockClient()
	mock.putErr = errors.New("access denied")
	p := newTestProvider(mock, "")
	ctx := context.Background()

	err := p.Put(ctx, "file.txt", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("Put() should have returned an error")
	}
	if !strings.Contains(err.Error(), "putting object") {
		t.Errorf("Put() error = %v, want error containing 'putting object'", err)
	}
}

func TestGetNonexistent(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "")
	ctx := context.Background()

	_, err := p.Get(ctx, "nonexistent")
	if err == nil {
		t.Fatal("Get() should have returned an error for nonexistent key")
	}
	if !strings.Contains(err.Error(), "getting object") {
		t.Errorf("Get() error = %v, want error containing 'getting object'", err)
	}
}

func TestDelete(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "")
	ctx := context.Background()

	if err := p.Put(ctx, "to-delete.txt", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatal(err)
	}

	if err := p.Delete(ctx, "to-delete.txt"); err != nil {
		t.Fatal(err)
	}

	if _, ok := mock.objects["to-delete.txt"]; ok {
		t.Error("object still exists after Delete()")
	}
}

func TestDeleteError(t *testing.T) {
	mock := newMockClient()
	mock.deleteErr = errors.New("forbidden")
	p := newTestProvider(mock, "")
	ctx := context.Background()

	err := p.Delete(ctx, "file.txt")
	if err == nil {
		t.Fatal("Delete() should have returned an error")
	}
	if !strings.Contains(err.Error(), "deleting object") {
		t.Errorf("Delete() error = %v, want error containing 'deleting object'", err)
	}
}

func TestList(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "")
	ctx := context.Background()

	files := map[string]string{
		"db/orders/2025-01-01/run1.parquet": "data1",
		"db/orders/2025-01-02/run2.parquet": "data2",
		"db/users/2025-01-01/run3.parquet":  "data3",
	}
	for key, data := range files {
		if err := p.Put(ctx, key, bytes.NewReader([]byte(data))); err != nil {
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

func TestListWithPrefix(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "archives/")
	ctx := context.Background()

	if err := p.Put(ctx, "db/file1.parquet", bytes.NewReader([]byte("a"))); err != nil {
		t.Fatal(err)
	}
	if err := p.Put(ctx, "db/file2.parquet", bytes.NewReader([]byte("b"))); err != nil {
		t.Fatal(err)
	}

	objects, err := p.List(ctx, "db/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Errorf("List(db/) returned %d objects, want 2", len(objects))
	}

	for _, obj := range objects {
		if strings.HasPrefix(obj.Key, "archives/") {
			t.Errorf("List() returned key with prefix: %s", obj.Key)
		}
	}
}

func TestListError(t *testing.T) {
	mock := newMockClient()
	mock.listErr = errors.New("throttled")
	p := newTestProvider(mock, "")
	ctx := context.Background()

	_, err := p.List(ctx, "prefix/")
	if err == nil {
		t.Fatal("List() should have returned an error")
	}
	if !strings.Contains(err.Error(), "listing objects") {
		t.Errorf("List() error = %v, want error containing 'listing objects'", err)
	}
}

func TestExists(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "")
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

func TestExistsError(t *testing.T) {
	mock := newMockClient()
	mock.existsErr = errors.New("internal server error")
	p := newTestProvider(mock, "")
	ctx := context.Background()

	_, err := p.Exists(ctx, "file.txt")
	if err == nil {
		t.Fatal("Exists() should have returned an error")
	}
	if !strings.Contains(err.Error(), "checking existence") {
		t.Errorf("Exists() error = %v, want error containing 'checking existence'", err)
	}
}

func TestRename(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "")
	ctx := context.Background()

	data := []byte("rename me")
	if err := p.Put(ctx, "old.txt", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	if err := p.Rename(ctx, "old.txt", "new.txt"); err != nil {
		t.Fatal(err)
	}

	if _, ok := mock.objects["old.txt"]; ok {
		t.Error("old key still exists after Rename()")
	}

	rc, err := p.Get(ctx, "new.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("renamed content = %q, want %q", got, data)
	}
}

func TestRenameCopyError(t *testing.T) {
	mock := newMockClient()
	mock.copyErr = errors.New("copy failed")
	p := newTestProvider(mock, "")
	ctx := context.Background()

	mock.objects["old.txt"] = []byte("data")
	err := p.Rename(ctx, "old.txt", "new.txt")
	if err == nil {
		t.Fatal("Rename() should have returned an error")
	}
	if !strings.Contains(err.Error(), "copying") {
		t.Errorf("Rename() error = %v, want error containing 'copying'", err)
	}

	if _, ok := mock.objects["old.txt"]; !ok {
		t.Error("old key was deleted despite copy failure")
	}
}

func TestRenameDeleteError(t *testing.T) {
	mock := newMockClient()
	mock.deleteErr = errors.New("delete failed")
	p := newTestProvider(mock, "")
	ctx := context.Background()

	mock.objects["old.txt"] = []byte("data")
	err := p.Rename(ctx, "old.txt", "new.txt")
	if err == nil {
		t.Fatal("Rename() should have returned an error on delete failure")
	}
	if !strings.Contains(err.Error(), "deleting old key") {
		t.Errorf("Rename() error = %v, want error containing 'deleting old key'", err)
	}
	if !strings.Contains(err.Error(), "retried") {
		t.Errorf("Rename() error should mention retries, got: %v", err)
	}
}

func TestRenameDeleteRetriesAndSucceeds(t *testing.T) {
	mock := newMockClient()
	mock.deleteErr = errors.New("temporary failure")
	mock.deleteFailN = 2
	p := newTestProvider(mock, "")
	ctx := context.Background()

	mock.objects["old.txt"] = []byte("data")
	err := p.Rename(ctx, "old.txt", "new.txt")
	if err != nil {
		t.Fatalf("Rename() should have succeeded after retry, got: %v", err)
	}

	if _, ok := mock.objects["old.txt"]; ok {
		t.Error("old key still exists after successful Rename()")
	}
	if _, ok := mock.objects["new.txt"]; !ok {
		t.Error("new key should exist after Rename()")
	}
}

func TestRenameDeleteCleansUpCopyOnFailure(t *testing.T) {
	mock := newMockClient()
	mock.deleteFailN = 4
	mock.deleteErr = errors.New("persistent failure")
	p := newTestProvider(mock, "")
	ctx := context.Background()

	mock.objects["old.txt"] = []byte("data")
	err := p.Rename(ctx, "old.txt", "new.txt")
	if err == nil {
		t.Fatal("Rename() should have returned an error")
	}

	if _, ok := mock.objects["old.txt"]; !ok {
		t.Error("old key should still exist when delete fails")
	}
}
