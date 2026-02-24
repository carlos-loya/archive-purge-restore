package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

// mockS3Client implements s3API for testing.
type mockS3Client struct {
	objects map[string][]byte // key -> data

	putErr       error
	getErr       error
	deleteErr    error
	listErr      error
	headErr      error
	copyErr      error
	lifecycleErr error

	lastLifecycleInput *s3.PutBucketLifecycleConfigurationInput
}

func newMockClient() *mockS3Client {
	return &mockS3Client{objects: make(map[string][]byte)}
}

func (m *mockS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*input.Key] = data
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	data, ok := m.objects[*input.Key]
	if !ok {
		return nil, errors.New("NoSuchKey: the specified key does not exist")
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *mockS3Client) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	delete(m.objects, *input.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var contents []types.Object
	now := time.Now()
	for key, data := range m.objects {
		if strings.HasPrefix(key, *input.Prefix) {
			size := int64(len(data))
			contents = append(contents, types.Object{
				Key:          aws.String(key),
				Size:         &size,
				LastModified: &now,
			})
		}
	}
	return &s3.ListObjectsV2Output{
		Contents: contents,
	}, nil
}

func (m *mockS3Client) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.headErr != nil {
		return nil, m.headErr
	}
	_, ok := m.objects[*input.Key]
	if !ok {
		return nil, errors.New("NotFound: the specified key does not exist")
	}
	return &s3.HeadObjectOutput{}, nil
}

func (m *mockS3Client) CopyObject(_ context.Context, input *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	if m.copyErr != nil {
		return nil, m.copyErr
	}
	// CopySource is "bucket/key"
	source := *input.CopySource
	parts := strings.SplitN(source, "/", 2)
	if len(parts) != 2 {
		return nil, errors.New("invalid CopySource")
	}
	srcKey := parts[1]
	data, ok := m.objects[srcKey]
	if !ok {
		return nil, errors.New("NoSuchKey: source key does not exist")
	}
	m.objects[*input.Key] = append([]byte{}, data...)
	return &s3.CopyObjectOutput{}, nil
}

func (m *mockS3Client) PutBucketLifecycleConfiguration(_ context.Context, input *s3.PutBucketLifecycleConfigurationInput, _ ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	if m.lifecycleErr != nil {
		return nil, m.lifecycleErr
	}
	m.lastLifecycleInput = input
	return &s3.PutBucketLifecycleConfigurationOutput{}, nil
}

func newTestProvider(mock *mockS3Client, prefix string) *Provider {
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

	// Verify the object was stored with the full prefixed key.
	if _, ok := mock.objects["archives/file.txt"]; !ok {
		t.Error("object not stored with prefixed key")
	}

	// Get should work with the unprefixed key.
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

	// Verify the object was removed from the mock store.
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

	// Store objects with prefixed keys via the provider.
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

	// Keys should have the prefix stripped.
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
	mock.headErr = errors.New("internal server error")
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

	// Old key should be gone.
	if _, ok := mock.objects["old.txt"]; ok {
		t.Error("old key still exists after Rename()")
	}

	// New key should have the data.
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

	// Old key should still exist since copy failed.
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

	// New key should exist (copy succeeded).
	if _, ok := mock.objects["test-bucket/new.txt"]; !ok {
		// The copy uses bucket/key as the source format, but stores at the dest key.
		if _, ok := mock.objects["new.txt"]; !ok {
			t.Error("new key should exist after successful copy")
		}
	}
}

func TestSetLifecyclePolicy(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "archives/")
	ctx := context.Background()

	policy := storage.LifecyclePolicy{
		TransitionDays: 30,
		ExpirationDays: 365,
	}

	if err := p.SetLifecyclePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}

	if mock.lastLifecycleInput == nil {
		t.Fatal("PutBucketLifecycleConfiguration was not called")
	}

	rules := mock.lastLifecycleInput.LifecycleConfiguration.Rules
	if len(rules) != 2 {
		t.Fatalf("expected 2 lifecycle rules, got %d", len(rules))
	}

	// Check transition rule.
	if *rules[0].ID != "apr-glacier-transition" {
		t.Errorf("rule[0].ID = %q, want %q", *rules[0].ID, "apr-glacier-transition")
	}
	if *rules[0].Transitions[0].Days != 30 {
		t.Errorf("transition days = %d, want 30", *rules[0].Transitions[0].Days)
	}

	// Check expiration rule.
	if *rules[1].ID != "apr-expiration" {
		t.Errorf("rule[1].ID = %q, want %q", *rules[1].ID, "apr-expiration")
	}
	if *rules[1].Expiration.Days != 365 {
		t.Errorf("expiration days = %d, want 365", *rules[1].Expiration.Days)
	}
}

func TestSetLifecyclePolicyEmpty(t *testing.T) {
	mock := newMockClient()
	p := newTestProvider(mock, "")
	ctx := context.Background()

	// Zero values should be a no-op.
	if err := p.SetLifecyclePolicy(ctx, storage.LifecyclePolicy{}); err != nil {
		t.Fatal(err)
	}

	if mock.lastLifecycleInput != nil {
		t.Error("PutBucketLifecycleConfiguration should not be called for empty policy")
	}
}

func TestSetLifecyclePolicyError(t *testing.T) {
	mock := newMockClient()
	mock.lifecycleErr = errors.New("access denied")
	p := newTestProvider(mock, "")
	ctx := context.Background()

	err := p.SetLifecyclePolicy(ctx, storage.LifecyclePolicy{TransitionDays: 30})
	if err == nil {
		t.Fatal("SetLifecyclePolicy() should have returned an error")
	}
	if !strings.Contains(err.Error(), "setting lifecycle policy") {
		t.Errorf("error = %v, want error containing 'setting lifecycle policy'", err)
	}
}

func TestIsNotFoundError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{errors.New("NotFound: key does not exist"), true},
		{errors.New("404: not found"), true},
		{errors.New("NoSuchKey: the specified key does not exist"), true},
		{errors.New("access denied"), false},
		{nil, false},
	}
	for _, tt := range tests {
		if got := isNotFoundError(tt.err, nil); got != tt.want {
			t.Errorf("isNotFoundError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
