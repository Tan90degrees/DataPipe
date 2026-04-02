package storage

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"datapipe/internal/common/errors"
)

type FileContent struct {
	Path       string
	Name       string
	Size       int64
	Content    []byte
	Metadata   map[string]interface{}
	ModTime    time.Time
	Checksum   string
	Format     string
	IsCompressed bool
}

func (f *FileContent) GetMetadata(key string) interface{} {
	if f.Metadata == nil {
		return nil
	}
	return f.Metadata[key]
}

func (f *FileContent) SetMetadata(key string, value interface{}) {
	if f.Metadata == nil {
		f.Metadata = make(map[string]interface{})
	}
	f.Metadata[key] = value
}

func (f *FileContent) ToJSON() ([]byte, error) {
	return json.Marshal(f)
}

func FileContentFromJSON(data []byte) (*FileContent, error) {
	var content FileContent
	if err := json.Unmarshal(data, &content); err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to unmarshal FileContent")
	}
	return &content, nil
}

type StorageAdapter interface {
	Save(ctx context.Context, content *FileContent) error
	Load(ctx context.Context, path string) (*FileContent, error)
	Delete(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) (bool, error)
	List(ctx context.Context, dir string) ([]*FileContent, error)
	Stat(ctx context.Context, path string) (*FileContent, error)
}

type StorageConfig struct {
	BasePath      string
	MaxFileSize   int64
	AllowedExts   []string
	TempDir       string
	AutoCreateDir bool
}

func NewStorageConfig(basePath string) *StorageConfig {
	return &StorageConfig{
		BasePath:      basePath,
		MaxFileSize:   100 * 1024 * 1024 * 1024,
		AutoCreateDir: true,
	}
}

type LocalFileAdapter struct {
	mu    sync.RWMutex
	basePath string
	config  *StorageConfig
}

func NewLocalFileAdapter(config *StorageConfig) (*LocalFileAdapter, error) {
	if config == nil {
		return nil, errors.New(errors.ErrCodeInvalidConfiguration, "storage config is nil")
	}

	if config.BasePath == "" {
		return nil, errors.New(errors.ErrCodeInvalidConfiguration, "base path is empty")
	}

	if config.AutoCreateDir {
		if err := os.MkdirAll(config.BasePath, 0755); err != nil {
			return nil, errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to create base directory")
		}
	}

	return &LocalFileAdapter{
		basePath: config.BasePath,
		config:   config,
	}, nil
}

func (a *LocalFileAdapter) resolvePath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(a.basePath, path)
}

func (a *LocalFileAdapter) Save(ctx context.Context, content *FileContent) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if content == nil {
		return errors.New(errors.ErrCodeInvalidParameter, "content is nil")
	}

	fullPath := a.resolvePath(content.Path)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to create directory")
	}

	file, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to open file")
	}
	defer file.Close()

	if _, err := file.Write(content.Content); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to write content")
	}

	if err := file.Sync(); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to sync file")
	}

	return nil
}

func (a *LocalFileAdapter) Load(ctx context.Context, path string) (*FileContent, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	fullPath := a.resolvePath(path)

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Wrap(err, errors.ErrCodeFileNotFound, "file not found: "+path)
		}
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to open file")
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read file")
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to get file stat")
	}

	ext := filepath.Ext(fullPath)
	name := filepath.Base(fullPath)

	return &FileContent{
		Path:    path,
		Name:    name,
		Size:    stat.Size(),
		Content: content,
		Metadata: make(map[string]interface{}),
		ModTime: stat.ModTime(),
		Format:  ext,
	}, nil
}

func (a *LocalFileAdapter) Delete(ctx context.Context, path string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	fullPath := a.resolvePath(path)

	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to delete file")
	}

	return nil
}

func (a *LocalFileAdapter) Exists(ctx context.Context, path string) (bool, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	fullPath := a.resolvePath(path)

	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to stat file")
	}

	return true, nil
}

func (a *LocalFileAdapter) List(ctx context.Context, dir string) ([]*FileContent, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	fullPath := a.resolvePath(dir)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*FileContent{}, nil
		}
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read directory")
	}

	contents := make([]*FileContent, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		entryPath := filepath.Join(dir, entry.Name())
		fullEntryPath := a.resolvePath(entryPath)

		stat, err := entry.Info()
		if err != nil {
			continue
		}

		content, err := os.ReadFile(fullEntryPath)
		if err != nil {
			continue
		}

		ext := filepath.Ext(entry.Name())

		contents = append(contents, &FileContent{
			Path:     entryPath,
			Name:     entry.Name(),
			Size:     stat.Size(),
			Content:  content,
			Metadata: make(map[string]interface{}),
			ModTime:  stat.ModTime(),
			Format:   ext,
		})
	}

	return contents, nil
}

func (a *LocalFileAdapter) Stat(ctx context.Context, path string) (*FileContent, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	fullPath := a.resolvePath(path)

	stat, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Wrap(err, errors.ErrCodeFileNotFound, "file not found: "+path)
		}
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to stat file")
	}

	ext := filepath.Ext(fullPath)
	name := filepath.Base(fullPath)

	return &FileContent{
		Path:    path,
		Name:    name,
		Size:    stat.Size(),
		Content: nil,
		Metadata: make(map[string]interface{}),
		ModTime: stat.ModTime(),
		Format:  ext,
	}, nil
}

type MemoryStorageAdapter struct {
	mu       sync.RWMutex
	files    map[string]*FileContent
	maxSize  int64
	curSize  int64
}

func NewMemoryStorageAdapter(maxSize int64) *MemoryStorageAdapter {
	if maxSize <= 0 {
		maxSize = 100 * 1024 * 1024 * 1024
	}

	return &MemoryStorageAdapter{
		files:   make(map[string]*FileContent),
		maxSize: maxSize,
	}
}

func (m *MemoryStorageAdapter) Save(ctx context.Context, content *FileContent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if content == nil {
		return errors.New(errors.ErrCodeInvalidParameter, "content is nil")
	}

	content.Size = int64(len(content.Content))

	if m.curSize+content.Size > m.maxSize {
		return errors.New(errors.ErrCodeResourceExhausted, "memory storage capacity exceeded")
	}

	m.files[content.Path] = content
	m.curSize += content.Size

	return nil
}

func (m *MemoryStorageAdapter) Load(ctx context.Context, path string) (*FileContent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	content, ok := m.files[path]
	if !ok {
		return nil, errors.Wrap(nil, errors.ErrCodeFileNotFound, "file not found: "+path)
	}

	copy := &FileContent{
		Path:       content.Path,
		Name:       content.Name,
		Size:       content.Size,
		Content:    make([]byte, len(content.Content)),
		Metadata:   make(map[string]interface{}),
		ModTime:    content.ModTime,
		Checksum:   content.Checksum,
		Format:     content.Format,
		IsCompressed: content.IsCompressed,
	}
	copy.Content = content.Content
	for k, v := range content.Metadata {
		copy.Metadata[k] = v
	}

	return copy, nil
}

func (m *MemoryStorageAdapter) Delete(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	content, ok := m.files[path]
	if ok {
		m.curSize -= content.Size
		delete(m.files, path)
	}

	return nil
}

func (m *MemoryStorageAdapter) Exists(ctx context.Context, path string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.files[path]
	return ok, nil
}

func (m *MemoryStorageAdapter) List(ctx context.Context, dir string) ([]*FileContent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	contents := make([]*FileContent, 0)
	for _, content := range m.files {
		if dir == "" || filepath.Dir(content.Path) == dir {
			copy := &FileContent{
				Path:       content.Path,
				Name:       content.Name,
				Size:       content.Size,
				Content:    content.Content,
				Metadata:   content.Metadata,
				ModTime:    content.ModTime,
				Checksum:   content.Checksum,
				Format:     content.Format,
				IsCompressed: content.IsCompressed,
			}
			contents = append(contents, copy)
		}
	}

	return contents, nil
}

func (m *MemoryStorageAdapter) Stat(ctx context.Context, path string) (*FileContent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	content, ok := m.files[path]
	if !ok {
		return nil, errors.Wrap(nil, errors.ErrCodeFileNotFound, "file not found: "+path)
	}

	return &FileContent{
		Path:     content.Path,
		Name:     content.Name,
		Size:     content.Size,
		Content:  nil,
		Metadata: content.Metadata,
		ModTime:  content.ModTime,
		Format:   content.Format,
	}, nil
}

func (m *MemoryStorageAdapter) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files = make(map[string]*FileContent)
	m.curSize = 0
}

func (m *MemoryStorageAdapter) CurrentSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.curSize
}

func (m *MemoryStorageAdapter) UsagePercent() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return float64(m.curSize) / float64(m.maxSize) * 100
}

type BufferedStorageAdapter struct {
	mu       sync.RWMutex
	cache    *MemoryStorageAdapter
	backend  StorageAdapter
	cacheSize int64
}

func NewBufferedStorageAdapter(backend StorageAdapter, cacheSize int64) *BufferedStorageAdapter {
	if cacheSize <= 0 {
		cacheSize = 10 * 1024 * 1024
	}

	return &BufferedStorageAdapter{
		cache:    NewMemoryStorageAdapter(cacheSize),
		backend:  backend,
		cacheSize: cacheSize,
	}
}

func (b *BufferedStorageAdapter) Save(ctx context.Context, content *FileContent) error {
	if err := b.backend.Save(ctx, content); err != nil {
		return err
	}

	b.cache.Save(ctx, content)
	return nil
}

func (b *BufferedStorageAdapter) Load(ctx context.Context, path string) (*FileContent, error) {
	if content, err := b.cache.Load(ctx, path); err == nil {
		return content, nil
	}

	return b.backend.Load(ctx, path)
}

func (b *BufferedStorageAdapter) Delete(ctx context.Context, path string) error {
	b.cache.Delete(ctx, path)
	return b.backend.Delete(ctx, path)
}

func (b *BufferedStorageAdapter) Exists(ctx context.Context, path string) (bool, error) {
	if exists, _ := b.cache.Exists(ctx, path); exists {
		return true, nil
	}
	return b.backend.Exists(ctx, path)
}

func (b *BufferedStorageAdapter) List(ctx context.Context, dir string) ([]*FileContent, error) {
	return b.backend.List(ctx, dir)
}

func (b *BufferedStorageAdapter) Stat(ctx context.Context, path string) (*FileContent, error) {
	if content, err := b.cache.Stat(ctx, path); err == nil {
		return content, nil
	}
	return b.backend.Stat(ctx, path)
}

func (b *BufferedStorageAdapter) Flush() error {
	return nil
}

func (b *BufferedStorageAdapter) ClearCache() {
	b.cache.Clear()
}

type StorageAdapterFactory struct{}

func (f *StorageAdapterFactory) CreateLocalAdapter(config *StorageConfig) (*LocalFileAdapter, error) {
	return NewLocalFileAdapter(config)
}

func (f *StorageAdapterFactory) CreateMemoryAdapter(maxSize int64) *MemoryStorageAdapter {
	return NewMemoryStorageAdapter(maxSize)
}

func (f *StorageAdapterFactory) CreateBufferedAdapter(backend StorageAdapter, cacheSize int64) *BufferedStorageAdapter {
	return NewBufferedStorageAdapter(backend, cacheSize)
}
