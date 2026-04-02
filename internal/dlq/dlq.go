package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"datapipe/internal/common/errors"
)

type Message struct {
	ID        string
	Payload   []byte
	Metadata  map[string]interface{}
	Error     *errors.Error
	Timestamp time.Time
	RetryCount int
	Source    string
}

func NewMessage(id string, payload []byte, source string) *Message {
	return &Message{
		ID:        id,
		Payload:   payload,
		Metadata:  make(map[string]interface{}),
		Timestamp: time.Now(),
		RetryCount: 0,
		Source:    source,
	}
}

func (m *Message) SetError(err *errors.Error) {
	m.Error = err
}

func (m *Message) SetMetadata(key string, value interface{}) {
	m.Metadata[key] = value
}

func (m *Message) GetMetadata(key string) interface{} {
	return m.Metadata[key]
}

func (m *Message) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

func MessageFromJSON(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to unmarshal DLQ message")
	}
	return &msg, nil
}

type DLQ interface {
	Enqueue(ctx context.Context, msg *Message) error
	Dequeue(ctx context.Context) (*Message, error)
	Peek(ctx context.Context) (*Message, error)
	Size(ctx context.Context) (int, error)
	Clear(ctx context.Context) error
	List(ctx context.Context, offset, limit int) ([]*Message, error)
	Remove(ctx context.Context, id string) error
	Requeue(ctx context.Context, id string) error
}

type DLQConfig struct {
	BasePath     string
	MaxSize      int
	RetentionDays int
	AutoCreate   bool
}

func NewDLQConfig(basePath string) *DLQConfig {
	return &DLQConfig{
		BasePath:     basePath,
		MaxSize:      10000,
		RetentionDays: 7,
		AutoCreate:   true,
	}
}

type FileBasedDLQ struct {
	mu     sync.RWMutex
	config *DLQConfig
	index  *DLQIndex
}

type DLQIndex struct {
	mu      sync.RWMutex
	entries map[string]*DLQEntry
	queue   []string
}

type DLQEntry struct {
	ID        string
	FilePath  string
	Timestamp time.Time
	Retries   int
}

func NewFileBasedDLQ(config *DLQConfig) (*FileBasedDLQ, error) {
	if config == nil {
		config = NewDLQConfig("./dlq")
	}

	if config.AutoCreate {
		if err := os.MkdirAll(config.BasePath, 0755); err != nil {
			return nil, errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to create DLQ directory")
		}
	}

	dlq := &FileBasedDLQ{
		config: config,
		index: &DLQIndex{
			entries: make(map[string]*DLQEntry),
			queue:   make([]string, 0),
		},
	}

	dlq.rebuildIndex()

	return dlq, nil
}

func (d *FileBasedDLQ) rebuildIndex() {
	entries, err := os.ReadDir(d.config.BasePath)
	if err != nil {
		return
	}

	d.index.mu.Lock()
	defer d.index.mu.Unlock()

	d.index.entries = make(map[string]*DLQEntry)
	d.index.queue = make([]string, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".dlq" {
			continue
		}

		id := entry.Name()[:len(entry.Name())-4]
		filePath := filepath.Join(d.config.BasePath, entry.Name())

		info, err := entry.Info()
		if err != nil {
			continue
		}

		d.index.entries[id] = &DLQEntry{
			ID:        id,
			FilePath:  filePath,
			Timestamp: info.ModTime(),
			Retries:   0,
		}
		d.index.queue = append(d.index.queue, id)
	}
}

func (d *FileBasedDLQ) Enqueue(ctx context.Context, msg *Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	currentSize := d.index.Size()
	if currentSize >= d.config.MaxSize {
		return errors.Newf(errors.ErrCodeResourceExhausted, "DLQ capacity exceeded: max %d", d.config.MaxSize)
	}

	data, err := msg.ToJSON()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSerializationFailed, "failed to serialize message")
	}

	fileName := msg.ID + ".dlq"
	filePath := filepath.Join(d.config.BasePath, fileName)

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to write DLQ message")
	}

	d.index.mu.Lock()
	d.index.entries[msg.ID] = &DLQEntry{
		ID:        msg.ID,
		FilePath:  filePath,
		Timestamp: time.Now(),
		Retries:   msg.RetryCount,
	}
	d.index.queue = append(d.index.queue, msg.ID)
	d.index.mu.Unlock()

	return nil
}

func (d *FileBasedDLQ) Dequeue(ctx context.Context) (*Message, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.index.mu.Lock()
	if len(d.index.queue) == 0 {
		d.index.mu.Unlock()
		return nil, errors.New(errors.ErrCodeResourceExhausted, "DLQ is empty")
	}

	id := d.index.queue[0]
	entry, ok := d.index.entries[id]
	if !ok {
		d.index.mu.Unlock()
		return nil, errors.Newf(errors.ErrCodeFileNotFound, "message not found: %s", id)
	}
	d.index.mu.Unlock()

	data, err := os.ReadFile(entry.FilePath)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read DLQ message")
	}

	msg, err := MessageFromJSON(data)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to deserialize message")
	}

	return msg, nil
}

func (d *FileBasedDLQ) Peek(ctx context.Context) (*Message, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	d.index.mu.RLock()
	if len(d.index.queue) == 0 {
		d.index.mu.RUnlock()
		return nil, errors.New(errors.ErrCodeResourceExhausted, "DLQ is empty")
	}

	id := d.index.queue[0]
	entry, ok := d.index.entries[id]
	if !ok {
		d.index.mu.RUnlock()
		return nil, errors.Newf(errors.ErrCodeFileNotFound, "message not found: %s", id)
	}
	d.index.mu.RUnlock()

	data, err := os.ReadFile(entry.FilePath)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read DLQ message")
	}

	msg, err := MessageFromJSON(data)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to deserialize message")
	}

	return msg, nil
}

func (d *FileBasedDLQ) Size(ctx context.Context) (int, error) {
	d.index.mu.RLock()
	defer d.index.mu.RUnlock()
	return len(d.index.queue), nil
}

func (d *FileBasedDLQ) Clear(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.index.mu.Lock()
	defer d.index.mu.Unlock()

	var errs []error
	for id, entry := range d.index.entries {
		if err := os.Remove(entry.FilePath); err != nil {
			errs = append(errs, errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to delete: "+id))
			continue
		}
		delete(d.index.entries, id)
	}

	d.index.queue = make([]string, 0)

	if len(errs) > 0 {
		return errors.Wrap(errs[0], errors.ErrCodeFileWriteFailed, "failed to clear DLQ")
	}

	return nil
}

func (d *FileBasedDLQ) List(ctx context.Context, offset, limit int) ([]*Message, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	d.index.mu.RLock()
	defer d.index.mu.RUnlock()

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}

	if offset >= len(d.index.queue) {
		return []*Message{}, nil
	}

	end := offset + limit
	if end > len(d.index.queue) {
		end = len(d.index.queue)
	}

	ids := d.index.queue[offset:end]
	messages := make([]*Message, 0, len(ids))

	for _, id := range ids {
		entry, ok := d.index.entries[id]
		if !ok {
			continue
		}

		data, err := os.ReadFile(entry.FilePath)
		if err != nil {
			continue
		}

		msg, err := MessageFromJSON(data)
		if err != nil {
			continue
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

func (d *FileBasedDLQ) Remove(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.index.mu.Lock()
	defer d.index.mu.Unlock()

	entry, ok := d.index.entries[id]
	if !ok {
		return errors.Newf(errors.ErrCodeFileNotFound, "message not found: %s", id)
	}

	if err := os.Remove(entry.FilePath); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to delete: "+id)
	}

	delete(d.index.entries, id)

	for i, qid := range d.index.queue {
		if qid == id {
			d.index.queue = append(d.index.queue[:i], d.index.queue[i+1:]...)
			break
		}
	}

	return nil
}

func (d *FileBasedDLQ) Requeue(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.index.mu.RLock()
	entry, ok := d.index.entries[id]
	if !ok {
		d.index.mu.RUnlock()
		return errors.Newf(errors.ErrCodeFileNotFound, "message not found: %s", id)
	}
	d.index.mu.RUnlock()

	data, err := os.ReadFile(entry.FilePath)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read DLQ message")
	}

	msg, err := MessageFromJSON(data)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to deserialize message")
	}

	msg.RetryCount++
	msg.Timestamp = time.Now()
	msg.Error = nil

	newData, err := msg.ToJSON()
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeSerializationFailed, "failed to serialize message")
	}

	if err := os.WriteFile(entry.FilePath, newData, 0644); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to update DLQ message")
	}

	d.index.mu.Lock()
	d.index.entries[id].Retries = msg.RetryCount
	d.index.entries[id].Timestamp = time.Now()
	d.index.mu.Unlock()

	return nil
}

func (d *DLQIndex) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.queue)
}

type MemoryBasedDLQ struct {
	mu     sync.RWMutex
	config *DLQConfig
	messages map[string]*Message
	queue   []string
}

func NewMemoryBasedDLQ(config *DLQConfig) (*MemoryBasedDLQ, error) {
	if config == nil {
		config = NewDLQConfig("memory")
	}

	return &MemoryBasedDLQ{
		config:   config,
		messages: make(map[string]*Message),
		queue:    make([]string, 0),
	}, nil
}

func (m *MemoryBasedDLQ) Enqueue(ctx context.Context, msg *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.messages) >= m.config.MaxSize {
		return errors.Newf(errors.ErrCodeResourceExhausted, "DLQ capacity exceeded: max %d", m.config.MaxSize)
	}

	m.messages[msg.ID] = msg
	m.queue = append(m.queue, msg.ID)

	return nil
}

func (m *MemoryBasedDLQ) Dequeue(ctx context.Context) (*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.queue) == 0 {
		return nil, errors.New(errors.ErrCodeResourceExhausted, "DLQ is empty")
	}

	id := m.queue[0]
	msg, ok := m.messages[id]
	if !ok {
		m.queue = m.queue[1:]
		return nil, errors.Newf(errors.ErrCodeFileNotFound, "message not found: %s", id)
	}

	return msg, nil
}

func (m *MemoryBasedDLQ) Peek(ctx context.Context) (*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.queue) == 0 {
		return nil, errors.New(errors.ErrCodeResourceExhausted, "DLQ is empty")
	}

	id := m.queue[0]
	msg, ok := m.messages[id]
	if !ok {
		return nil, errors.Newf(errors.ErrCodeFileNotFound, "message not found: %s", id)
	}

	return msg, nil
}

func (m *MemoryBasedDLQ) Size(ctx context.Context) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.queue), nil
}

func (m *MemoryBasedDLQ) Clear(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages = make(map[string]*Message)
	m.queue = make([]string, 0)

	return nil
}

func (m *MemoryBasedDLQ) List(ctx context.Context, offset, limit int) ([]*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}

	if offset >= len(m.queue) {
		return []*Message{}, nil
	}

	end := offset + limit
	if end > len(m.queue) {
		end = len(m.queue)
	}

	ids := m.queue[offset:end]
	messages := make([]*Message, 0, len(ids))

	for _, id := range ids {
		if msg, ok := m.messages[id]; ok {
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

func (m *MemoryBasedDLQ) Remove(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.messages[id]; !ok {
		return errors.Newf(errors.ErrCodeFileNotFound, "message not found: %s", id)
	}

	delete(m.messages, id)

	for i, qid := range m.queue {
		if qid == id {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			break
		}
	}

	return nil
}

func (m *MemoryBasedDLQ) Requeue(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg, ok := m.messages[id]
	if !ok {
		return errors.Newf(errors.ErrCodeFileNotFound, "message not found: %s", id)
	}

	msg.RetryCount++
	msg.Timestamp = time.Now()
	msg.Error = nil

	return nil
}

type DLQManager struct {
	mu     sync.RWMutex
	queues map[string]DLQ
}

func NewDLQManager() *DLQManager {
	return &DLQManager{
		queues: make(map[string]DLQ),
	}
}

func (m *DLQManager) RegisterDLQ(name string, dlq DLQ) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queues[name] = dlq
}

func (m *DLQManager) GetDLQ(name string) (DLQ, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	dlq, ok := m.queues[name]
	return dlq, ok
}

func (m *DLQManager) CreateFileBasedDLQ(name string, config *DLQConfig) (DLQ, error) {
	dlq, err := NewFileBasedDLQ(config)
	if err != nil {
		return nil, err
	}

	m.RegisterDLQ(name, dlq)
	return dlq, nil
}

func (m *DLQManager) CreateMemoryBasedDLQ(name string, config *DLQConfig) (DLQ, error) {
	dlq, err := NewMemoryBasedDLQ(config)
	if err != nil {
		return nil, err
	}

	m.RegisterDLQ(name, dlq)
	return dlq, nil
}

func (m *DLQManager) EnqueueTo(ctx context.Context, dlqName string, msg *Message) error {
	dlq, ok := m.GetDLQ(dlqName)
	if !ok {
		return errors.Newf(errors.ErrCodeResourceNotFound, "DLQ not found: %s", dlqName)
	}
	return dlq.Enqueue(ctx, msg)
}

func (m *DLQManager) DequeueFrom(ctx context.Context, dlqName string) (*Message, error) {
	dlq, ok := m.GetDLQ(dlqName)
	if !ok {
		return nil, errors.Newf(errors.ErrCodeResourceNotFound, "DLQ not found: %s", dlqName)
	}
	return dlq.Dequeue(ctx)
}

func (m *DLQManager) GetQueueStats(dlqName string) (*DLQStats, error) {
	dlq, ok := m.GetDLQ(dlqName)
	if !ok {
		return nil, errors.Newf(errors.ErrCodeResourceNotFound, "DLQ not found: %s", dlqName)
	}

	size, err := dlq.Size(context.Background())
	if err != nil {
		return nil, err
	}

	return &DLQStats{
		Name:      dlqName,
		Size:      size,
		MaxSize:   10000,
		CreatedAt: time.Now(),
	}, nil
}

type DLQStats struct {
	Name      string
	Size      int
	MaxSize   int
	CreatedAt time.Time
}

func (s *DLQStats) UsagePercent() float64 {
	if s.MaxSize == 0 {
		return 0
	}
	return float64(s.Size) / float64(s.MaxSize) * 100
}

func (s *DLQStats) IsNearCapacity() bool {
	return s.UsagePercent() > 80.0
}

func (s *DLQStats) IsFull() bool {
	return s.Size >= s.MaxSize
}

type DLQEventHandler func(event *DLQEvent)

type DLQEvent struct {
	Type    DLQEventType
	Message *Message
	DLQName string
	Time    time.Time
	Error   error
}

type DLQEventType string

const (
	DLQEventEnqueued  DLQEventType = "enqueued"
	DLQEventDequeued  DLQEventType = "dequeued"
	DLQEventRemoved   DLQEventType = "removed"
	DLQEventRequeued  DLQEventType = "requeued"
	DLQEventCleared   DLQEventType = "cleared"
)

type DLQEventPublisher struct {
	mu       sync.RWMutex
handlers map[DLQEventType][]DLQEventHandler
}

func NewDLQEventPublisher() *DLQEventPublisher {
	return &DLQEventPublisher{
		handlers: make(map[DLQEventType][]DLQEventHandler),
	}
}

func (p *DLQEventPublisher) Subscribe(eventType DLQEventType, handler DLQEventHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[eventType] = append(p.handlers[eventType], handler)
}

func (p *DLQEventPublisher) Publish(event *DLQEvent) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	handlers, ok := p.handlers[event.Type]
	if !ok {
		return
	}

	for _, handler := range handlers {
		go handler(event)
	}
}

func DLQEventNew(t DLQEventType, msg *Message, dlqName string) *DLQEvent {
	return &DLQEvent{
		Type:    t,
		Message: msg,
		DLQName: dlqName,
		Time:    time.Now(),
	}
}

func formatDLQMessage(msg *Message) string {
	if len(msg.Payload) > 100 {
		return fmt.Sprintf("ID: %s, Payload: %s..., Error: %v", msg.ID, string(msg.Payload[:100]), msg.Error)
	}
	return fmt.Sprintf("ID: %s, Payload: %s, Error: %v", msg.ID, string(msg.Payload), msg.Error)
}
