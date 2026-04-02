package transfer

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"

	"datapipe/internal/common/errors"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	CompressStream(reader io.Reader) (io.Reader, error)
	DecompressStream(reader io.Reader) (io.Reader, error)
	Name() string
}

type CompressionType string

const (
	CompressionTypeGzip CompressionType = "gzip"
	CompressionTypeLz4 CompressionType = "lz4"
	CompressionTypeZstd CompressionType = "zstd"
	CompressionTypeNone CompressionType = "none"
)

type GzipCompressor struct {
	level int
}

func NewGzipCompressor(level int) *GzipCompressor {
	if level <= 0 {
		level = gzip.DefaultCompression
	}
	if level > gzip.BestCompression {
		level = gzip.BestCompression
	}

	return &GzipCompressor{
		level: level,
	}
}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
	if data == nil || len(data) == 0 {
		return data, nil
	}

	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, g.level)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSerializationFailed, "failed to create gzip writer")
	}

	if _, err := writer.Write(data); err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSerializationFailed, "failed to write data for compression")
	}

	if err := writer.Close(); err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSerializationFailed, "failed to close gzip writer")
	}

	return buf.Bytes(), nil
}

func (g *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	if data == nil || len(data) == 0 {
		return data, nil
	}

	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to create gzip reader")
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to read decompressed data")
	}

	return result, nil
}

func (g *GzipCompressor) CompressStream(reader io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()

	writer, err := gzip.NewWriterLevel(pw, g.level)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSerializationFailed, "failed to create gzip writer")
	}

	go func() {
		defer func() {
			writer.Close()
			pw.Close()
		}()
		io.Copy(writer, reader)
	}()

	return pr, nil
}

func (g *GzipCompressor) DecompressStream(reader io.Reader) (io.Reader, error) {
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to create gzip reader")
	}
	return gzipReader, nil
}

func (g *GzipCompressor) Name() string {
	return "gzip"
}

type Lz4Compressor struct {
	bufferSize int
}

func NewLz4Compressor() *Lz4Compressor {
	return &Lz4Compressor{
		bufferSize: 65536,
	}
}

func (l *Lz4Compressor) Compress(data []byte) ([]byte, error) {
	if data == nil || len(data) == 0 {
		return data, nil
	}

	var buf bytes.Buffer

	compressed := simpleLz4Encode(data)
	buf.Write(compressed)

	return buf.Bytes(), nil
}

func (l *Lz4Compressor) Decompress(data []byte) ([]byte, error) {
	if data == nil || len(data) == 0 {
		return data, nil
	}

	decompressed, err := simpleLz4Decode(data)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to decompress lz4 data")
	}

	return decompressed, nil
}

func (l *Lz4Compressor) CompressStream(reader io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		io.Copy(pw, reader)
	}()

	return pr, nil
}

func (l *Lz4Compressor) DecompressStream(reader io.Reader) (io.Reader, error) {
	return reader, nil
}

func (l *Lz4Compressor) Name() string {
	return "lz4"
}

func simpleLz4Encode(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	var result bytes.Buffer

	header := []byte{0x04, 0x22, 0x4d, 0x18}
	result.Write(header)

	chunks := splitIntoChunks(data, 65536)
	for _, chunk := range chunks {
		sizeBytes := make([]byte, 4)
		sizeBytes[0] = byte(len(chunk) >> 24)
		sizeBytes[1] = byte(len(chunk) >> 16)
		sizeBytes[2] = byte(len(chunk) >> 8)
		sizeBytes[3] = byte(len(chunk))
		result.Write(sizeBytes)
		result.Write(chunk)
	}

	return result.Bytes()
}

func simpleLz4Decode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	if len(data) < 4 {
		return nil, errors.New(errors.ErrCodeDeserializationFailed, "data too short for lz4")
	}

	if !bytes.Equal(data[:4], []byte{0x04, 0x22, 0x4d, 0x18}) {
		return nil, errors.New(errors.ErrCodeDeserializationFailed, "invalid lz4 magic header")
	}

	offset := 4
	var result []byte

	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}

		size := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		if offset+size > len(data) {
			break
		}

		result = append(result, data[offset:offset+size]...)
		offset += size
	}

	return result, nil
}

func splitIntoChunks(data []byte, chunkSize int) [][]byte {
	var chunks [][]byte
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}
	return chunks
}

type ZstdCompressor struct {
	level int
}

func NewZstdCompressor(level int) *ZstdCompressor {
	if level <= 0 {
		level = 3
	}
	if level > 22 {
		level = 22
	}

	return &ZstdCompressor{
		level: level,
	}
}

func (z *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	if data == nil || len(data) == 0 {
		return data, nil
	}

	compressed := simpleZstdEncode(data, z.level)
	return compressed, nil
}

func (z *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	if data == nil || len(data) == 0 {
		return data, nil
	}

	decompressed, err := simpleZstdDecode(data)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeDeserializationFailed, "failed to decompress zstd data")
	}

	return decompressed, nil
}

func (z *ZstdCompressor) CompressStream(reader io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		io.Copy(pw, reader)
	}()

	return pr, nil
}

func (z *ZstdCompressor) DecompressStream(reader io.Reader) (io.Reader, error) {
	return reader, nil
}

func (z *ZstdCompressor) Name() string {
	return "zstd"
}

func simpleZstdEncode(data []byte, level int) []byte {
	if len(data) == 0 {
		return data
	}

	var result bytes.Buffer

	header := []byte{0x28, 0xb5, 0x2f, 0xfd}
	result.Write(header)

	chunks := splitIntoChunks(data, 65536)
	for _, chunk := range chunks {
		sizeBytes := make([]byte, 4)
		sizeBytes[0] = byte(len(chunk) >> 24)
		sizeBytes[1] = byte(len(chunk) >> 16)
		sizeBytes[2] = byte(len(chunk) >> 8)
		sizeBytes[3] = byte(len(chunk))
		result.Write(sizeBytes)
		result.Write(chunk)
	}

	return result.Bytes()
}

func simpleZstdDecode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	if len(data) < 4 {
		return nil, errors.New(errors.ErrCodeDeserializationFailed, "data too short for zstd")
	}

	if !bytes.Equal(data[:4], []byte{0x28, 0xb5, 0x2f, 0xfd}) {
		return nil, errors.New(errors.ErrCodeDeserializationFailed, "invalid zstd magic header")
	}

	offset := 4
	var result []byte

	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}

		size := int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		if offset+size > len(data) {
			break
		}

		result = append(result, data[offset:offset+size]...)
		offset += size
	}

	return result, nil
}

type NoOpCompressor struct{}

func NewNoOpCompressor() *NoOpCompressor {
	return &NoOpCompressor{}
}

func (n *NoOpCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (n *NoOpCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

func (n *NoOpCompressor) CompressStream(reader io.Reader) (io.Reader, error) {
	return reader, nil
}

func (n *NoOpCompressor) DecompressStream(reader io.Reader) (io.Reader, error) {
	return reader, nil
}

func (n *NoOpCompressor) Name() string {
	return "none"
}

type CompressorPool struct {
	compressors map[string]func() Compressor
	mu          sync.RWMutex
}

func NewCompressorPool() *CompressorPool {
	return &CompressorPool{
		compressors: map[string]func() Compressor{
			"gzip": func() Compressor { return NewGzipCompressor(gzip.DefaultCompression) },
			"none": func() Compressor { return NewNoOpCompressor() },
		},
	}
}

func (p *CompressorPool) Get(name string, level int) Compressor {
	p.mu.RLock()
	defer p.mu.RUnlock()

	switch name {
	case "gzip":
		return NewGzipCompressor(level)
	case "lz4":
		return NewLz4Compressor()
	case "zstd":
		return NewZstdCompressor(level)
	case "none", "":
		return NewNoOpCompressor()
	default:
		return NewNoOpCompressor()
	}
}

func (p *CompressorPool) Register(name string, factory func() Compressor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.compressors[name] = factory
}
