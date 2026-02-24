package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

// s3API abstracts the S3 client operations used by Provider.
type s3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(ctx context.Context, input *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	PutBucketLifecycleConfiguration(ctx context.Context, input *s3.PutBucketLifecycleConfigurationInput, opts ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error)
}

// Provider implements storage.Provider using Amazon S3.
type Provider struct {
	client s3API
	bucket string
	prefix string
}

// New creates a new S3 storage provider.
func New(ctx context.Context, bucket, region, prefix, endpoint string) (*Provider, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var clientOpts []func(*s3.Options)
	if endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)

	return &Provider{
		client: client,
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

func (p *Provider) Put(ctx context.Context, key string, reader io.Reader) error {
	_, err := p.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.fullKey(key)),
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("putting object %s: %w", key, err)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	output, err := p.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.fullKey(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("getting object %s: %w", key, err)
	}
	return output.Body, nil
}

func (p *Provider) Delete(ctx context.Context, key string) error {
	_, err := p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.fullKey(key)),
	})
	if err != nil {
		return fmt.Errorf("deleting object %s: %w", key, err)
	}
	return nil
}

func (p *Provider) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	fullPrefix := p.fullKey(prefix)
	var objects []storage.ObjectInfo

	paginator := s3.NewListObjectsV2Paginator(p.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(p.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing objects with prefix %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			key := *obj.Key
			if p.prefix != "" && len(key) > len(p.prefix) {
				key = key[len(p.prefix):]
			}
			objects = append(objects, storage.ObjectInfo{
				Key:          key,
				Size:         *obj.Size,
				LastModified: *obj.LastModified,
			})
		}
	}
	return objects, nil
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.fullKey(key)),
	})
	if err != nil {
		// Check if it's a NotFound error.
		var nsk *types.NotFound
		if isNotFoundError(err, nsk) {
			return false, nil
		}
		return false, fmt.Errorf("checking existence of %s: %w", key, err)
	}
	return true, nil
}

func (p *Provider) Rename(ctx context.Context, oldKey, newKey string) error {
	// S3 doesn't have a native rename - copy then delete.
	copySource := p.bucket + "/" + p.fullKey(oldKey)
	_, err := p.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(p.bucket),
		CopySource: aws.String(copySource),
		Key:        aws.String(p.fullKey(newKey)),
	})
	if err != nil {
		return fmt.Errorf("copying %s to %s: %w", oldKey, newKey, err)
	}

	// Retry delete to avoid orphaned copies if the first attempt fails.
	const maxRetries = 3
	var deleteErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		_, deleteErr = p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(p.bucket),
			Key:    aws.String(p.fullKey(oldKey)),
		})
		if deleteErr == nil {
			return nil
		}
	}

	// Delete failed after retries — remove the copy to avoid orphans.
	p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(p.fullKey(newKey)),
	})
	return fmt.Errorf("deleting old key %s after copy (retried %d times): %w", oldKey, maxRetries, deleteErr)
}

// SetLifecyclePolicy configures S3 lifecycle rules on the bucket.
func (p *Provider) SetLifecyclePolicy(ctx context.Context, policy storage.LifecyclePolicy) error {
	var rules []types.LifecycleRule

	if policy.TransitionDays > 0 {
		rules = append(rules, types.LifecycleRule{
			ID:     aws.String("apr-glacier-transition"),
			Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String(p.prefix)},
			Transitions: []types.Transition{
				{
					Days:         aws.Int32(int32(policy.TransitionDays)),
					StorageClass: types.TransitionStorageClassGlacier,
				},
			},
		})
	}

	if policy.ExpirationDays > 0 {
		rules = append(rules, types.LifecycleRule{
			ID:     aws.String("apr-expiration"),
			Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{Prefix: aws.String(p.prefix)},
			Expiration: &types.LifecycleExpiration{
				Days: aws.Int32(int32(policy.ExpirationDays)),
			},
		})
	}

	if len(rules) == 0 {
		return nil
	}

	_, err := p.client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(p.bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})
	if err != nil {
		return fmt.Errorf("setting lifecycle policy: %w", err)
	}
	return nil
}

func isNotFoundError(err error, _ *types.NotFound) bool {
	// Simple string-based check since error wrapping varies.
	return err != nil && (contains(err.Error(), "NotFound") || contains(err.Error(), "404") || contains(err.Error(), "NoSuchKey"))
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
