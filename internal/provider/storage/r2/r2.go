package r2

import (
	"context"
	"fmt"
	"io"

	s3store "github.com/carlos-loya/archive-purge-restore/internal/provider/storage/s3"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

// Provider implements storage.Provider using Cloudflare R2.
// R2 is S3-compatible, so this delegates to the S3 provider with the R2 endpoint.
type Provider struct {
	s3 *s3store.Provider
}

// New creates a new Cloudflare R2 storage provider.
// The R2 endpoint is derived from the account ID: https://{accountID}.r2.cloudflarestorage.com
func New(ctx context.Context, accountID, bucket, region, prefix string) (*Provider, error) {
	endpoint := fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)

	if region == "" {
		region = "auto"
	}

	s3Provider, err := s3store.New(ctx, bucket, region, prefix, endpoint)
	if err != nil {
		return nil, fmt.Errorf("creating R2 storage provider: %w", err)
	}

	return &Provider{s3: s3Provider}, nil
}

func (p *Provider) Put(ctx context.Context, key string, reader io.Reader) error {
	return p.s3.Put(ctx, key, reader)
}

func (p *Provider) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return p.s3.Get(ctx, key)
}

func (p *Provider) Delete(ctx context.Context, key string) error {
	return p.s3.Delete(ctx, key)
}

func (p *Provider) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	return p.s3.List(ctx, prefix)
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	return p.s3.Exists(ctx, key)
}

func (p *Provider) Rename(ctx context.Context, oldKey, newKey string) error {
	return p.s3.Rename(ctx, oldKey, newKey)
}
