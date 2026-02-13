package harestorage

import (
	"context"
	"fmt"
	"io"
	"path"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

var _ Storage = (*GCSStorage)(nil)

// GCSStorage implements the Storage interface for Google Cloud Storage (GCS).
type GCSStorage struct {
	client     *storage.Client
	bucketName string
	name       string
}

// NewGCSStorage creates a new GCSStorage instance with the specified client, bucket name, and storage name.
func NewGCSStorage(client *storage.Client, bucketName string, name string) *GCSStorage {
	return &GCSStorage{
		client:     client,
		bucketName: bucketName,
		name:       name,
	}
}

// Name returns the name (identifier) of this storage.
func (s *GCSStorage) Name() string {
	return s.name
}

// Get returns an io.ReadCloser to read the object with the specified name.
func (s *GCSStorage) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	err := s.checkClientAndBucket()
	if err != nil {
		return nil, fmt.Errorf("storage client: %w", err)
	}

	if name == "" {
		return nil, fmt.Errorf("name required")
	}

	r, err := s.client.Bucket(s.bucketName).Object(name).NewReader(ctx)
	if err != nil {
		fullPath := s.PathJoin(s.bucketName, name)

		return nil, fmt.Errorf("failed to open file %q: %w", fullPath, err)
	}

	return r, err
}

// Put saves data to the storage with the specified name.
func (s *GCSStorage) Put(ctx context.Context, name string, r io.Reader, opts *PutOptions) (int64, error) {
	err := s.checkClientAndBucket()
	if err != nil {
		return 0, fmt.Errorf("invalid storage client: %w", err)
	}

	if name == "" {
		return 0, fmt.Errorf("name required")
	}

	w := s.client.Bucket(s.bucketName).Object(name).NewWriter(ctx)

	defer w.Close()

	if opts != nil {
		if opts.ContentType != "" {
			w.ContentType = opts.ContentType
		}

		if len(opts.Metadata) > 0 {
			w.Metadata = opts.Metadata
		}
	}

	size, err := io.Copy(w, r)
	if err != nil {
		fullPath := s.PathJoin(s.bucketName, name)

		return 0, fmt.Errorf("failed to write file %q: %w", fullPath, err)
	}

	return size, nil
}

// List returns a list of objects matching the specified prefix.
func (s *GCSStorage) List(ctx context.Context, prefix string) ([]*ObjectInfo, error) {
	err := s.checkClientAndBucket()
	if err != nil {
		return nil, fmt.Errorf("invalid storage client: %w", err)
	}

	if prefix == "" {
		return nil, fmt.Errorf("prefix required")
	}

	it := s.client.Bucket(s.bucketName).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	var objects []*ObjectInfo

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			fullPath := s.PathJoin(s.bucketName, prefix)

			return nil, fmt.Errorf("failed to list %q: %w", fullPath, err)
		}

		objects = append(objects, &ObjectInfo{
			Name:      s.PathJoin(prefix, attrs.Name),
			Size:      attrs.Size,
			UpdatedAt: attrs.Updated,
			Metadata:  attrs.Metadata,
		})
	}

	return objects, nil
}

// Copy copies an object from the source path to the destination path.
func (s *GCSStorage) Copy(ctx context.Context, src string, dst string) error {
	err := s.checkClientAndBucket()
	if err != nil {
		return fmt.Errorf("invalid storage client: %w", err)
	}

	if src == "" {
		return fmt.Errorf("src required")
	}

	if dst == "" {
		return fmt.Errorf("dst required")
	}

	srcObj := s.client.Bucket(s.bucketName).Object(src)
	dstObj := s.client.Bucket(s.bucketName).Object(dst)

	if _, err := dstObj.CopierFrom(srcObj).Run(ctx); err != nil {
		return fmt.Errorf("failed to copy object from %q to %q: %w", src, dst, err)
	}

	return nil
}

// Move moves (renames) an object from the source path to the destination path.
func (s *GCSStorage) Move(ctx context.Context, src string, dst string) error {
	err := s.checkClientAndBucket()
	if err != nil {
		return fmt.Errorf("invalid storage client: %w", err)
	}

	if src == "" {
		return fmt.Errorf("src required")
	}

	if dst == "" {
		return fmt.Errorf("dst required")
	}

	err = s.Copy(ctx, src, dst)
	if err != nil {
		return fmt.Errorf("failed to copy file %q to %q: %w", src, dst, err)
	}

	err = s.Delete(ctx, src)
	if err != nil {
		return fmt.Errorf("failed to delete file %q: %w", src, err)
	}

	return nil
}

// Delete deletes the object with the specified name.
func (s *GCSStorage) Delete(ctx context.Context, name string) error {
	err := s.checkClientAndBucket()
	if err != nil {
		return fmt.Errorf("invalid storage client: %w", err)
	}

	if name == "" {
		return fmt.Errorf("name required")
	}

	err = s.client.Bucket(s.bucketName).Object(name).Delete(ctx)
	if err != nil {
		fullPath := s.PathJoin(s.bucketName, name)

		return fmt.Errorf("failed to delete file %q: %w", fullPath, err)
	}

	return nil
}

// DeleteAll deletes all objects matching the specified prefix.
func (s *GCSStorage) DeleteAll(ctx context.Context, prefix string) error {
	err := s.checkClientAndBucket()
	if err != nil {
		return fmt.Errorf("invalid storage client: %w", err)
	}

	if prefix == "" {
		return fmt.Errorf("prefix required")
	}

	query := &storage.Query{
		Prefix:     prefix,
		Projection: storage.ProjectionNoACL,
	}

	it := s.client.Bucket(s.bucketName).Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			fullPath := s.PathJoin(s.bucketName, prefix)

			return fmt.Errorf("failed to list %q: %w", fullPath, err)
		}

		err = s.client.Bucket(s.bucketName).Object(attrs.Name).Delete(ctx)
		if err != nil {
			return fmt.Errorf("failed to delete %s: %w", attrs.Name, err)
		}
	}

	return nil
}

// PathJoin joins multiple path elements according to the storage's format.
func (s *GCSStorage) PathJoin(elem ...string) string {
	return path.Join(elem...)
}

func (s *GCSStorage) checkClientAndBucket() error {
	if s.client == nil {
		return fmt.Errorf("storage client required")
	}

	if s.bucketName == "" {
		return fmt.Errorf("bucketName required")
	}

	return nil
}
