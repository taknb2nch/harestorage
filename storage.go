package harestorage

import (
	"context"
	"io"
	"time"
)

// Storage defines the interface for a set of operations on object storage.
type Storage interface {
	// Name returns the name (identifier) of this storage.
	Name() string
	// Get returns an io.ReadCloser to read the object with the specified name.
	Get(ctx context.Context, name string) (io.ReadCloser, error)
	// Put saves data to the storage with the specified name.
	Put(ctx context.Context, name string, r io.Reader, opts *PutOptions) (int64, error)
	// List returns a list of objects matching the specified prefix.
	List(ctx context.Context, prefix string) ([]*ObjectInfo, error)
	// Copy copies an object from the source path to the destination path.
	Copy(ctx context.Context, src string, dst string) error
	// Move moves (renames) an object from the source path to the destination path.
	Move(ctx context.Context, src string, dst string) error
	// Delete deletes the object with the specified name.
	Delete(ctx context.Context, name string) error
	// DeleteAll deletes all objects matching the specified prefix.
	DeleteAll(ctx context.Context, prefix string) error
	// PathJoin joins multiple path elements according to the storage's format.
	PathJoin(elem ...string) string
}

// PutOptions holds optional settings for saving an object.
type PutOptions struct {
	// ContentType specifies the MIME type of the object.
	ContentType string
	// Metadata is arbitrary metadata associated with the object.
	Metadata map[string]string
}

// ObjectInfo holds detailed information about an object in the storage.
type ObjectInfo struct {
	// Name is the full path (name) of the object.
	Name string
	// Size is the data size of the object in bytes.
	Size int64
	// UpdatedAt is the time when the object was last updated.
	UpdatedAt time.Time
	// Metadata is the metadata associated with the object.
	Metadata map[string]string
}
