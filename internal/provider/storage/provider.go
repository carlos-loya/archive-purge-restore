package storage

import (
	"context"
	"io"
	"time"
)

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// LifecyclePolicy defines storage lifecycle rules.
type LifecyclePolicy struct {
	TransitionDays int // days before transitioning to cold storage
	ExpirationDays int // days before permanent deletion
}

// Provider abstracts archive storage operations.
type Provider interface {
	// Put writes data to the given key.
	Put(ctx context.Context, key string, reader io.Reader) error
	// Get retrieves data for the given key.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Delete removes the object at the given key.
	Delete(ctx context.Context, key string) error
	// List returns objects matching the given prefix.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	// Exists checks if an object exists at the given key.
	Exists(ctx context.Context, key string) (bool, error)
	// Rename atomically renames an object from oldKey to newKey.
	Rename(ctx context.Context, oldKey, newKey string) error
}
