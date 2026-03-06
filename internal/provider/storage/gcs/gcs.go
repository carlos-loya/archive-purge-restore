package gcs

import (
	"context"
	"fmt"
	"io"

	gcsclient "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

// gcsAPI abstracts the GCS client operations used by Provider.
type gcsAPI interface {
	Put(ctx context.Context, key string, reader io.Reader) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error)
	Exists(ctx context.Context, key string) (bool, error)
	Copy(ctx context.Context, srcKey, dstKey string) error
}

// gcsClient wraps the real GCS client and implements gcsAPI.
type gcsClient struct {
	bucket *gcsclient.BucketHandle
}

func (c *gcsClient) Put(ctx context.Context, key string, reader io.Reader) error {
	w := c.bucket.Object(key).NewWriter(ctx)
	if _, err := io.Copy(w, reader); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (c *gcsClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return c.bucket.Object(key).NewReader(ctx)
}

func (c *gcsClient) Delete(ctx context.Context, key string) error {
	return c.bucket.Object(key).Delete(ctx)
}

func (c *gcsClient) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	var objects []storage.ObjectInfo
	it := c.bucket.Objects(ctx, &gcsclient.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		objects = append(objects, storage.ObjectInfo{
			Key:          attrs.Name,
			Size:         attrs.Size,
			LastModified: attrs.Updated,
		})
	}
	return objects, nil
}

func (c *gcsClient) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.bucket.Object(key).Attrs(ctx)
	if err == gcsclient.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *gcsClient) Copy(ctx context.Context, srcKey, dstKey string) error {
	_, err := c.bucket.Object(dstKey).CopierFrom(c.bucket.Object(srcKey)).Run(ctx)
	return err
}

// Provider implements storage.Provider using Google Cloud Storage.
type Provider struct {
	client gcsAPI
	bucket string
	prefix string
}

// New creates a new GCS storage provider.
func New(ctx context.Context, bucket, prefix string) (*Provider, error) {
	client, err := gcsclient.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	return &Provider{
		client: &gcsClient{bucket: client.Bucket(bucket)},
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (p *Provider) fullKey(key string) string {
	if p.prefix == "" {
		return key
	}
	return p.prefix + key
}

func (p *Provider) stripPrefix(key string) string {
	if p.prefix != "" && len(key) > len(p.prefix) {
		return key[len(p.prefix):]
	}
	return key
}

func (p *Provider) Put(ctx context.Context, key string, reader io.Reader) error {
	if err := p.client.Put(ctx, p.fullKey(key), reader); err != nil {
		return fmt.Errorf("putting object %s: %w", key, err)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := p.client.Get(ctx, p.fullKey(key))
	if err != nil {
		return nil, fmt.Errorf("getting object %s: %w", key, err)
	}
	return rc, nil
}

func (p *Provider) Delete(ctx context.Context, key string) error {
	if err := p.client.Delete(ctx, p.fullKey(key)); err != nil {
		return fmt.Errorf("deleting object %s: %w", key, err)
	}
	return nil
}

func (p *Provider) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objects, err := p.client.List(ctx, p.fullKey(prefix))
	if err != nil {
		return nil, fmt.Errorf("listing objects with prefix %s: %w", prefix, err)
	}

	for i := range objects {
		objects[i].Key = p.stripPrefix(objects[i].Key)
	}
	return objects, nil
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := p.client.Exists(ctx, p.fullKey(key))
	if err != nil {
		return false, fmt.Errorf("checking existence of %s: %w", key, err)
	}
	return exists, nil
}

func (p *Provider) Rename(ctx context.Context, oldKey, newKey string) error {
	if err := p.client.Copy(ctx, p.fullKey(oldKey), p.fullKey(newKey)); err != nil {
		return fmt.Errorf("copying %s to %s: %w", oldKey, newKey, err)
	}

	const maxRetries = 3
	var deleteErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		deleteErr = p.client.Delete(ctx, p.fullKey(oldKey))
		if deleteErr == nil {
			return nil
		}
	}

	// Delete failed after retries — remove the copy to avoid orphans.
	p.client.Delete(ctx, p.fullKey(newKey))
	return fmt.Errorf("deleting old key %s after copy (retried %d times): %w", oldKey, maxRetries, deleteErr)
}
