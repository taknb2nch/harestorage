package harestorage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var _ Storage = (*LocalStorage)(nil)

// LocalStorage implements the Storage interface for the local filesystem.
type LocalStorage struct {
	rootDir string
	name    string
}

// NewLocalStorage creates a new LocalStorage instance with the specified root directory and name.
func NewLocalStorage(rootDir string, name string) *LocalStorage {
	return &LocalStorage{
		rootDir: rootDir,
		name:    name,
	}
}

// Name returns the name (identifier) of this storage.
func (s *LocalStorage) Name() string {
	return s.name
}

// Get returns an io.ReadCloser to read the object with the specified name.
func (s *LocalStorage) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	err := s.checkRootDir()
	if err != nil {
		return nil, fmt.Errorf("invalid root path: %w", err)
	}

	if name == "" {
		return nil, fmt.Errorf("name required")
	}

	fullPath := s.PathJoin(s.rootDir, name)

	f, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %q: %w", fullPath, err)
	}

	return f, err
}

// Put saves data to the storage with the specified name.
func (s *LocalStorage) Put(ctx context.Context, name string, r io.Reader, opts *PutOptions) (int64, error) {
	err := s.checkRootDir()
	if err != nil {
		return 0, fmt.Errorf("invalid root path: %w", err)
	}

	if name == "" {
		return 0, fmt.Errorf("name required")
	}

	fullPath := s.PathJoin(s.rootDir, name)
	dir := filepath.Dir(fullPath)

	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return 0, fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file %q: %w", fullPath, err)
	}

	defer f.Close()

	size, err := io.Copy(f, r)
	if err != nil {
		return 0, fmt.Errorf("failed to write file %q: %w", fullPath, err)
	}

	return size, nil
}

// List returns a list of objects matching the specified prefix.
func (s *LocalStorage) List(ctx context.Context, prefix string) ([]*ObjectInfo, error) {
	err := s.checkRootDir()
	if err != nil {
		return nil, fmt.Errorf("invalid root path: %w", err)
	}

	if prefix == "" {
		return nil, fmt.Errorf("prefix required")
	}

	fullPath := s.PathJoin(s.rootDir, prefix)

	files, err := os.ReadDir(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*ObjectInfo{}, nil
		}

		return nil, fmt.Errorf("failed to list files %q: %w", fullPath, err)
	}

	objects := make([]*ObjectInfo, 0, len(files))

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		info, err := file.Info()
		if err != nil {
			// ファイルが消えている可能性などは無視
			continue
		}

		objects = append(objects, &ObjectInfo{
			Name:      filepath.ToSlash(s.PathJoin(prefix, file.Name())),
			Size:      info.Size(),
			UpdatedAt: info.ModTime(),
			Metadata:  map[string]string{},
		})
	}

	return objects, nil
}

// Copy copies an object from the source path to the destination path.
func (s *LocalStorage) Copy(ctx context.Context, src string, dst string) error {
	err := s.checkRootDir()
	if err != nil {
		return fmt.Errorf("invalid storage client: %w", err)
	}

	if src == "" {
		return fmt.Errorf("src required")
	}

	if dst == "" {
		return fmt.Errorf("dst required")
	}

	srcPath := filepath.Join(s.rootDir, src)
	dstPath := filepath.Join(s.rootDir, dst)

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open src file %q: %w", srcPath, err)
	}

	defer srcFile.Close()

	err = os.MkdirAll(filepath.Dir(dstPath), 0755)
	if err != nil {
		return fmt.Errorf("failed to create dst directory %q: %w", filepath.Dir(dstPath), err)
	}

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create dst file %q: %w", dstPath, err)
	}

	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("failed to copy file %q to %q: %w", srcPath, dstPath, err)
	}

	return nil
}

// Move moves (renames) an object from the source path to the destination path.
func (s *LocalStorage) Move(ctx context.Context, src string, dst string) error {
	err := s.checkRootDir()
	if err != nil {
		return fmt.Errorf("invalid storage client: %w", err)
	}

	if src == "" {
		return fmt.Errorf("src required")
	}

	if dst == "" {
		return fmt.Errorf("dst required")
	}

	srcPath := filepath.Join(s.rootDir, src)
	dstPath := filepath.Join(s.rootDir, dst)

	err = os.MkdirAll(filepath.Dir(dstPath), 0755)
	if err != nil {
		return fmt.Errorf("failed to create dst directory %q: %w", filepath.Dir(dstPath), err)
	}

	err = os.Rename(srcPath, dstPath)
	if err == nil {
		// NOTE: 正常終了
		return nil
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
func (s *LocalStorage) Delete(ctx context.Context, name string) error {
	err := s.checkRootDir()
	if err != nil {
		return fmt.Errorf("invalid root path: %w", err)
	}

	if name == "" {
		return fmt.Errorf("name required")
	}

	fullPath := s.PathJoin(s.rootDir, name)

	err = os.Remove(fullPath)
	if err != nil {
		return fmt.Errorf("failed to delete file %q: %w", fullPath, err)
	}

	return nil
}

// DeleteAll deletes all objects matching the specified prefix.
func (s *LocalStorage) DeleteAll(ctx context.Context, prefix string) error {
	err := s.checkRootDir()
	if err != nil {
		return fmt.Errorf("invalid root path: %w", err)
	}

	if prefix == "" {
		return fmt.Errorf("prefix required")
	}

	fullPath := s.PathJoin(s.rootDir, prefix)

	err = os.RemoveAll(fullPath)
	if err != nil {
		return fmt.Errorf("failed to delete path %q: %w", fullPath, err)
	}

	return nil
}

// PathJoin joins multiple path elements according to the storage's format.
func (s *LocalStorage) PathJoin(elem ...string) string {
	return filepath.Join(elem...)
}

func (s *LocalStorage) checkRootDir() error {
	if s.rootDir == "" {
		return fmt.Errorf("root dir required")
	}

	return nil
}
