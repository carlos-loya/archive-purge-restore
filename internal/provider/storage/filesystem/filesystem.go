package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
)

// Provider implements storage.Provider using the local filesystem.
type Provider struct {
	basePath string
}

// New creates a new filesystem storage provider.
func New(basePath string) (*Provider, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("creating base path %s: %w", basePath, err)
	}
	return &Provider{basePath: basePath}, nil
}

func (p *Provider) fullPath(key string) string {
	return filepath.Join(p.basePath, filepath.FromSlash(key))
}

func (p *Provider) Put(_ context.Context, key string, reader io.Reader) error {
	path := p.fullPath(key)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("creating directories for %s: %w", key, err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating file %s: %w", key, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, reader); err != nil {
		return fmt.Errorf("writing file %s: %w", key, err)
	}
	return f.Close()
}

func (p *Provider) Get(_ context.Context, key string) (io.ReadCloser, error) {
	path := p.fullPath(key)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %w", key, err)
	}
	return f, nil
}

func (p *Provider) Delete(_ context.Context, key string) error {
	path := p.fullPath(key)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting file %s: %w", key, err)
	}
	return nil
}

func (p *Provider) List(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	searchDir := p.fullPath(prefix)
	var objects []storage.ObjectInfo

	err := filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(p.basePath, path)
		if err != nil {
			return err
		}

		key := filepath.ToSlash(relPath)
		if strings.HasPrefix(key, prefix) {
			objects = append(objects, storage.ObjectInfo{
				Key:          key,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing files with prefix %s: %w", prefix, err)
	}

	return objects, nil
}

func (p *Provider) Exists(_ context.Context, key string) (bool, error) {
	path := p.fullPath(key)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("checking existence of %s: %w", key, err)
}

func (p *Provider) Rename(_ context.Context, oldKey, newKey string) error {
	oldPath := p.fullPath(oldKey)
	newPath := p.fullPath(newKey)

	if err := os.MkdirAll(filepath.Dir(newPath), 0755); err != nil {
		return fmt.Errorf("creating directories for %s: %w", newKey, err)
	}

	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", oldKey, newKey, err)
	}
	return nil
}
