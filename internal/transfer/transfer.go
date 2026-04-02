package transfer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"datapipe/internal/common/errors"
)

type TransferMode string

const (
	TransferModeStream TransferMode = "stream"
	TransferModeBatch  TransferMode = "batch"
	TransferModeHybrid TransferMode = "hybrid"
)

type TransferStats struct {
	BytesTransferred uint64
	RecordsTransferred int
	StartTime        time.Time
	EndTime          time.Time
	Errors           int
	CompressionRatio float64
}

func (s *TransferStats) Duration() time.Duration {
	if s.EndTime.IsZero() {
		return time.Since(s.StartTime)
	}
	return s.EndTime.Sub(s.StartTime)
}

func (s *TransferStats) Throughput() float64 {
	duration := s.Duration()
	if duration == 0 {
		return 0
	}
	return float64(s.BytesTransferred) / duration.Seconds()
}

type Transfer interface {
	Send(ctx context.Context, data []byte) error
	Receive(ctx context.Context) ([]byte, error)
	SendStream(ctx context.Context, reader io.Reader) error
	ReceiveStream(ctx context.Context, writer io.Writer) error
	Close() error
	Stats() *TransferStats
}

type TransferConfig struct {
	Mode           TransferMode `json:"mode"`
	BatchSize      int          `json:"batch_size"`
	BufferSize     int          `json:"buffer_size"`
	CompressorType string       `json:"compressor_type"`
	CompressorLevel int         `json:"compressor_level"`
	Timeout        time.Duration `json:"timeout"`
	MaxRetries     int          `json:"max_retries"`
}

func NewTransferConfig() *TransferConfig {
	return &TransferConfig{
		Mode:            TransferModeStream,
		BatchSize:       1024,
		BufferSize:      8192,
		CompressorType: "none",
		CompressorLevel: 6,
		Timeout:         5 * time.Minute,
		MaxRetries:      3,
	}
}

type StreamTransfer struct {
	mu         sync.Mutex
	reader     io.Reader
	writer     io.Writer
	compressor Compressor
	stats      *TransferStats
	bufferSize int
	timeout    time.Duration
	closed     bool
}

func NewStreamTransfer(reader io.Reader, writer io.Writer, compressor Compressor, config *TransferConfig) *StreamTransfer {
	if config == nil {
		config = NewTransferConfig()
	}

	return &StreamTransfer{
		reader:     reader,
		writer:     writer,
		compressor: compressor,
		stats: &TransferStats{
			StartTime: time.Now(),
		},
		bufferSize: config.BufferSize,
		timeout:    config.Timeout,
		closed:     false,
	}
}

func (t *StreamTransfer) Send(ctx context.Context, data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return errors.New(errors.ErrCodeOperationCancelled, "transfer is closed")
	}

	var dataToSend []byte
	if t.compressor != nil {
		compressed, err := t.compressor.Compress(data)
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeSerializationFailed, "compression failed")
		}
		dataToSend = compressed
	} else {
		dataToSend = data
	}

	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, uint64(len(dataToSend)))

	if _, err := t.writer.Write(header); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to write header")
	}

	if _, err := t.writer.Write(dataToSend); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to write data")
	}

	t.stats.BytesTransferred += uint64(len(header) + len(dataToSend))
	t.stats.RecordsTransferred++

	return nil
}

func (t *StreamTransfer) Receive(ctx context.Context) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, errors.New(errors.ErrCodeOperationCancelled, "transfer is closed")
	}

	header := make([]byte, 8)
	if _, err := io.ReadFull(t.reader, header); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read header")
	}

	length := binary.BigEndian.Uint64(header)
	data := make([]byte, length)
	if _, err := io.ReadFull(t.reader, data); err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read data")
	}

	t.stats.BytesTransferred += uint64(len(header) + len(data))

	if t.compressor != nil {
		decompressed, err := t.compressor.Decompress(data)
		if err != nil {
			return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "decompression failed")
		}
		return decompressed, nil
	}

	return data, nil
}

func (t *StreamTransfer) SendStream(ctx context.Context, reader io.Reader) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return errors.New(errors.ErrCodeOperationCancelled, "transfer is closed")
	}

	buffer := make([]byte, t.bufferSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			n, err := reader.Read(buffer)
			if n > 0 {
				data := buffer[:n]
				var dataToSend []byte

				if t.compressor != nil {
					compressed, err := t.compressor.Compress(data)
					if err != nil {
						return errors.Wrap(err, errors.ErrCodeSerializationFailed, "compression failed")
					}
					dataToSend = compressed
				} else {
					dataToSend = data
				}

				header := make([]byte, 8)
				binary.BigEndian.PutUint64(header, uint64(len(dataToSend)))

				if _, err := t.writer.Write(header); err != nil {
					return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to write header")
				}

				if _, err := t.writer.Write(dataToSend); err != nil {
					return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to write data")
				}

				t.stats.BytesTransferred += uint64(len(header) + len(dataToSend))
				t.stats.RecordsTransferred++
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read from reader")
			}
		}
	}

	return nil
}

func (t *StreamTransfer) ReceiveStream(ctx context.Context, writer io.Writer) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return errors.New(errors.ErrCodeOperationCancelled, "transfer is closed")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			header := make([]byte, 8)
			n, err := t.reader.Read(header)
			if n == 0 && err == io.EOF {
				break
			}
			if err != nil && err != io.EOF {
				return errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read header")
			}

			if n > 0 {
				length := binary.BigEndian.Uint64(header)
				data := make([]byte, length)

				readBytes := 0
				for readBytes < int(length) {
					n, err := t.reader.Read(data[readBytes:])
					if err != nil && err != io.EOF {
						return errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read data")
					}
					if n == 0 {
						break
					}
					readBytes += n
				}

				t.stats.BytesTransferred += uint64(len(header) + len(data))

				var dataToWrite []byte
				if t.compressor != nil {
					decompressed, err := t.compressor.Decompress(data)
					if err != nil {
						return errors.Wrap(err, errors.ErrCodeDeserializationFailed, "decompression failed")
					}
					dataToWrite = decompressed
				} else {
					dataToWrite = data
				}

				if _, err := writer.Write(dataToWrite); err != nil {
					return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to write data")
				}
			}

			if n > 0 && n < 8 {
				continue
			}
			if n == 0 || (n == 8 && len(header) == 8 && binary.BigEndian.Uint64(header) == 0) {
				break
			}
		}
	}

	return nil
}

func (t *StreamTransfer) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true
	t.stats.EndTime = time.Now()

	if closer, ok := t.writer.(io.Closer); ok {
		closer.Close()
	}

	return nil
}

func (t *StreamTransfer) Stats() *TransferStats {
	return t.stats
}

type BatchTransfer struct {
	mu          sync.Mutex
	batchSize   int
	compressor  Compressor
	stats       *TransferStats
	pendingData [][]byte
	timeout     time.Duration
	closed      bool
}

func NewBatchTransfer(compressor Compressor, config *TransferConfig) *BatchTransfer {
	if config == nil {
		config = NewTransferConfig()
	}

	return &BatchTransfer{
		batchSize:   config.BatchSize,
		compressor:  compressor,
		stats: &TransferStats{
			StartTime: time.Now(),
		},
		pendingData: make([][]byte, 0),
		timeout:    config.Timeout,
		closed:     false,
	}
}

func (b *BatchTransfer) Send(ctx context.Context, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errors.New(errors.ErrCodeOperationCancelled, "transfer is closed")
	}

	b.pendingData = append(b.pendingData, data)
	b.stats.RecordsTransferred++

	if len(b.pendingData) >= b.batchSize {
		return b.flush()
	}

	return nil
}

func (b *BatchTransfer) flush() error {
	if len(b.pendingData) == 0 {
		return nil
	}

	var combinedData []byte
	for _, data := range b.pendingData {
		combinedData = append(combinedData, data...)
	}

	if b.compressor != nil {
		compressed, err := b.compressor.Compress(combinedData)
		if err != nil {
			return errors.Wrap(err, errors.ErrCodeSerializationFailed, "compression failed")
		}
		combinedData = compressed
	}

	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, uint64(len(combinedData)))

	b.stats.BytesTransferred += uint64(len(header) + len(combinedData))
	b.pendingData = make([][]byte, 0)

	return nil
}

func (b *BatchTransfer) Receive(ctx context.Context) ([]byte, error) {
	return nil, errors.New(errors.ErrCodeOperationNotSupported, "batch transfer receive not supported")
}

func (b *BatchTransfer) SendStream(ctx context.Context, reader io.Reader) error {
	return errors.New(errors.ErrCodeOperationNotSupported, "batch transfer send stream not supported")
}

func (b *BatchTransfer) ReceiveStream(ctx context.Context, writer io.Writer) error {
	return errors.New(errors.ErrCodeOperationNotSupported, "batch transfer receive stream not supported")
}

func (b *BatchTransfer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.closed = true
	b.stats.EndTime = time.Now()

	return b.flush()
}

func (b *BatchTransfer) Stats() *TransferStats {
	return b.stats
}

type HybridTransfer struct {
	streamTransfer *StreamTransfer
	batchTransfer  *BatchTransfer
	mu             sync.Mutex
	mode           TransferMode
	threshold      int
}

func NewHybridTransfer(reader io.Reader, writer io.Writer, compressor Compressor, config *TransferConfig) *HybridTransfer {
	if config == nil {
		config = NewTransferConfig()
	}

	stream := NewStreamTransfer(reader, writer, compressor, config)
	batch := NewBatchTransfer(compressor, config)

	return &HybridTransfer{
		streamTransfer: stream,
		batchTransfer:  batch,
		mode:           config.Mode,
		threshold:      config.BatchSize,
	}
}

func (h *HybridTransfer) Send(ctx context.Context, data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(data) > h.threshold {
		return h.batchTransfer.Send(ctx, data)
	}
	return h.streamTransfer.Send(ctx, data)
}

func (h *HybridTransfer) Receive(ctx context.Context) ([]byte, error) {
	return h.streamTransfer.Receive(ctx)
}

func (h *HybridTransfer) SendStream(ctx context.Context, reader io.Reader) error {
	return h.streamTransfer.SendStream(ctx, reader)
}

func (h *HybridTransfer) ReceiveStream(ctx context.Context, writer io.Writer) error {
	return h.streamTransfer.ReceiveStream(ctx, writer)
}

func (h *HybridTransfer) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.batchTransfer.Close()
	return h.streamTransfer.Close()
}

func (h *HybridTransfer) Stats() *TransferStats {
	streamStats := h.streamTransfer.Stats()
	return &TransferStats{
		BytesTransferred: streamStats.BytesTransferred + h.batchTransfer.Stats().BytesTransferred,
		RecordsTransferred: streamStats.RecordsTransferred + h.batchTransfer.Stats().RecordsTransferred,
		StartTime: streamStats.StartTime,
		EndTime: streamStats.EndTime,
		Errors: streamStats.Errors + h.batchTransfer.Stats().Errors,
	}
}

type TransferManager struct {
	mu        sync.RWMutex
	transfers map[string]Transfer
	config    *TransferConfig
}

func NewTransferManager(config *TransferConfig) *TransferManager {
	if config == nil {
		config = NewTransferConfig()
	}

	return &TransferManager{
		transfers: make(map[string]Transfer),
		config:    config,
	}
}

func (tm *TransferManager) CreateTransfer(id string, reader io.Reader, writer io.Writer, compressor Compressor) Transfer {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	var transfer Transfer
	switch tm.config.Mode {
	case TransferModeStream:
		transfer = NewStreamTransfer(reader, writer, compressor, tm.config)
	case TransferModeBatch:
		transfer = NewBatchTransfer(compressor, tm.config)
	case TransferModeHybrid:
		transfer = NewHybridTransfer(reader, writer, compressor, tm.config)
	default:
		transfer = NewStreamTransfer(reader, writer, compressor, tm.config)
	}

	tm.transfers[id] = transfer
	return transfer
}

func (tm *TransferManager) GetTransfer(id string) (Transfer, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	t, ok := tm.transfers[id]
	return t, ok
}

func (tm *TransferManager) RemoveTransfer(id string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.transfers, id)
}

func (tm *TransferManager) CloseAll() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	var errs []error
	for id, t := range tm.transfers {
		if err := t.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close transfer %s: %w", id, err))
		}
	}

	if len(errs) > 0 {
		return errors.New(errors.ErrCodeInternalServerError, fmt.Sprintf("failed to close transfers: %v", errs))
	}

	tm.transfers = make(map[string]Transfer)
	return nil
}

func NewCompressor(compressorType string, level int) (Compressor, error) {
	switch compressorType {
	case "gzip", "gz":
		return NewGzipCompressor(level), nil
	case "lz4":
		return NewLz4Compressor(), nil
	case "zstd":
		return NewZstdCompressor(level), nil
	case "none", "":
		return nil, nil
	default:
		return nil, errors.Newf(errors.ErrCodeInvalidParameter, "unsupported compressor type: %s", compressorType)
	}
}
