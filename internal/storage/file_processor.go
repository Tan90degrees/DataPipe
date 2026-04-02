package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"

	"datapipe/internal/common/errors"
)

type FileType string

const (
	FileTypePDF       FileType = "pdf"
	FileTypeWord      FileType = "word"
	FileTypeExcel     FileType = "excel"
	FileTypePowerPoint FileType = "ppt"
	FileTypeMarkdown  FileType = "markdown"
	FileTypeHTML      FileType = "html"
	FileTypePNG       FileType = "png"
	FileTypeJPG       FileType = "jpg"
	FileTypeGIF       FileType = "gif"
	FileTypeBMP       FileType = "bmp"
	FileTypeTIFF      FileType = "tiff"
	FileTypeWebP      FileType = "webp"
	FileTypeUnknown   FileType = "unknown"
)

type ParsedContent struct {
	Text     string
	Metadata map[string]interface{}
	FileType FileType
	Pages    int
	Images   []image.Image
}

type FileProcessor interface {
	Process(ctx context.Context, path string) (*ParsedContent, error)
	ProcessBytes(ctx context.Context, data []byte, fileType FileType) (*ParsedContent, error)
	SupportedTypes() []FileType
	CanProcess(fileType FileType) bool
}

type DefaultFileProcessor struct {
	basePath  string
	maxSize   int64
	extractImages bool
}

func NewDefaultFileProcessor(basePath string, maxSize int64) *DefaultFileProcessor {
	return &DefaultFileProcessor{
		basePath:      basePath,
		maxSize:       maxSize,
		extractImages: false,
	}
}

func (p *DefaultFileProcessor) Process(ctx context.Context, path string) (*ParsedContent, error) {
	absPath := path
	if !filepath.IsAbs(path) {
		absPath = filepath.Join(p.basePath, path)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read file: "+path)
	}

	if int64(len(data)) > p.maxSize {
		return nil, errors.Newf(errors.ErrCodeResourceExhausted, "file size %d exceeds max size %d", len(data), p.maxSize)
	}

	fileType := p.detectFileType(absPath)

	return p.ProcessBytes(ctx, data, fileType)
}

func (p *DefaultFileProcessor) ProcessBytes(ctx context.Context, data []byte, fileType FileType) (*ParsedContent, error) {
	switch fileType {
	case FileTypePDF:
		return p.processPDF(ctx, data)
	case FileTypeWord:
		return p.processWord(ctx, data)
	case FileTypeExcel:
		return p.processExcel(ctx, data)
	case FileTypePowerPoint:
		return p.processPowerPoint(ctx, data)
	case FileTypeMarkdown:
		return p.processMarkdown(ctx, data)
	case FileTypeHTML:
		return p.processHTML(ctx, data)
	case FileTypePNG:
		return p.processPNG(ctx, data)
	case FileTypeJPG:
		return p.processJPEG(ctx, data)
	case FileTypeGIF:
		return p.processGIF(ctx, data)
	case FileTypeBMP:
		return p.processBMP(ctx, data)
	case FileTypeTIFF:
		return p.processTIFF(ctx, data)
	case FileTypeWebP:
		return p.processWebP(ctx, data)
	default:
		return p.processText(ctx, data)
	}
}

func (p *DefaultFileProcessor) SupportedTypes() []FileType {
	return []FileType{
		FileTypePDF, FileTypeWord, FileTypeExcel, FileTypePowerPoint,
		FileTypeMarkdown, FileTypeHTML, FileTypePNG, FileTypeJPG,
		FileTypeGIF, FileTypeBMP, FileTypeTIFF, FileTypeWebP,
	}
}

func (p *DefaultFileProcessor) CanProcess(fileType FileType) bool {
	for _, t := range p.SupportedTypes() {
		if t == fileType {
			return true
		}
	}
	return false
}

func (p *DefaultFileProcessor) detectFileType(path string) FileType {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".pdf":
		return FileTypePDF
	case ".doc", ".docx":
		return FileTypeWord
	case ".xls", ".xlsx":
		return FileTypeExcel
	case ".ppt", ".pptx":
		return FileTypePowerPoint
	case ".md", ".markdown":
		return FileTypeMarkdown
	case ".html", ".htm":
		return FileTypeHTML
	case ".png":
		return FileTypePNG
	case ".jpg", ".jpeg":
		return FileTypeJPG
	case ".gif":
		return FileTypeGIF
	case ".bmp":
		return FileTypeBMP
	case ".tiff", ".tif":
		return FileTypeTIFF
	case ".webp":
		return FileTypeWebP
	default:
		return FileTypeUnknown
	}
}

func (p *DefaultFileProcessor) processPDF(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypePDF,
		Text:     string(data),
	}

	signatures := []string{"%PDF", "%pdf"}
	for _, sig := range signatures {
		if bytes.HasPrefix(data, []byte(sig)) {
			result.Metadata["is_valid_pdf"] = true
			result.Text = "[PDF content - " + fmt.Sprintf("%d bytes", len(data)) + "]"
			return result, nil
		}
	}

	result.Metadata["is_valid_pdf"] = false
	result.Text = string(data)
	return result, nil
}

func (p *DefaultFileProcessor) processWord(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeWord,
		Text:     string(data),
	}

	if bytes.HasPrefix(data, []byte("PK")) {
		result.Metadata["is_office_docx"] = true
		result.Text = "[Word document - " + fmt.Sprintf("%d bytes", len(data)) + "]"
	} else if bytes.HasPrefix(data, []byte{0xD0, 0xCF, 0x11, 0xE0}) {
		result.Metadata["is_office_doc"] = true
		result.Text = "[Word document (legacy) - " + fmt.Sprintf("%d bytes", len(data)) + "]"
	}

	return result, nil
}

func (p *DefaultFileProcessor) processExcel(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeExcel,
		Text:     string(data),
	}

	if bytes.HasPrefix(data, []byte("PK")) {
		result.Metadata["is_office_xlsx"] = true
		result.Text = "[Excel spreadsheet - " + fmt.Sprintf("%d bytes", len(data)) + "]"
	}

	return result, nil
}

func (p *DefaultFileProcessor) processPowerPoint(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypePowerPoint,
		Text:     string(data),
	}

	if bytes.HasPrefix(data, []byte("PK")) {
		result.Metadata["is_office_pptx"] = true
		result.Text = "[PowerPoint presentation - " + fmt.Sprintf("%d bytes", len(data)) + "]"
	}

	return result, nil
}

func (p *DefaultFileProcessor) processMarkdown(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeMarkdown,
		Text:     string(data),
	}

	lines := strings.Split(string(data), "\n")
	result.Metadata["line_count"] = len(lines)
	result.Metadata["char_count"] = len(data)

	return result, nil
}

func (p *DefaultFileProcessor) processHTML(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeHTML,
		Text:     string(data),
	}

	if bytes.Contains(data, []byte("<!DOCTYPE")) || bytes.Contains(data, []byte("<html")) {
		result.Metadata["is_valid_html"] = true
	}

	return result, nil
}

func (p *DefaultFileProcessor) processPNG(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypePNG,
	}

	reader := bytes.NewReader(data)
	cfg, err := png.DecodeConfig(reader)
	if err != nil {
		result.Metadata["decode_error"] = err.Error()
		result.Text = "[PNG image - " + fmt.Sprintf("%d bytes", len(data)) + "]"
		return result, nil
	}

	result.Metadata["width"] = cfg.Width
	result.Metadata["height"] = cfg.Height

	if p.extractImages {
		img, err := png.Decode(reader)
		if err == nil {
			result.Images = append(result.Images, img)
		}
	}

	result.Text = fmt.Sprintf("[PNG image %dx%d - %d bytes]", cfg.Width, cfg.Height, len(data))

	return result, nil
}

func (p *DefaultFileProcessor) processJPEG(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeJPG,
	}

	reader := bytes.NewReader(data)
	cfg, err := jpeg.DecodeConfig(reader)
	if err != nil {
		result.Metadata["decode_error"] = err.Error()
		result.Text = "[JPEG image - " + fmt.Sprintf("%d bytes", len(data)) + "]"
		return result, nil
	}

	result.Metadata["width"] = cfg.Width
	result.Metadata["height"] = cfg.Height

	if p.extractImages {
		img, err := jpeg.Decode(reader)
		if err == nil {
			result.Images = append(result.Images, img)
		}
	}

	result.Text = fmt.Sprintf("[JPEG image %dx%d - %d bytes]", cfg.Width, cfg.Height, len(data))

	return result, nil
}

func (p *DefaultFileProcessor) processGIF(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeGIF,
	}

	reader := bytes.NewReader(data)
	cfg, err := gif.DecodeConfig(reader)
	if err != nil {
		result.Metadata["decode_error"] = err.Error()
		result.Text = "[GIF image - " + fmt.Sprintf("%d bytes", len(data)) + "]"
		return result, nil
	}

	result.Metadata["width"] = cfg.Width
	result.Metadata["height"] = cfg.Height

	if p.extractImages {
		img, err := gif.Decode(reader)
		if err == nil {
			result.Images = append(result.Images, img)
		}
	}

	result.Text = fmt.Sprintf("[GIF image %dx%d - %d bytes]", cfg.Width, cfg.Height, len(data))

	return result, nil
}

func (p *DefaultFileProcessor) processBMP(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeBMP,
	}

	if len(data) < 54 {
		result.Text = "[BMP image - insufficient data]"
		return result, nil
	}

	width := int32(data[18]) | int32(data[19])<<8 | int32(data[20])<<16 | int32(data[21])<<24
	height := int32(data[22]) | int32(data[23])<<8 | int32(data[24])<<16 | int32(data[25])<<24

	result.Metadata["width"] = width
	result.Metadata["height"] = height
	result.Text = fmt.Sprintf("[BMP image %dx%d - %d bytes]", width, height, len(data))

	return result, nil
}

func (p *DefaultFileProcessor) processTIFF(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeTIFF,
	}

	result.Text = "[TIFF image - " + fmt.Sprintf("%d bytes", len(data)) + "]"
	return result, nil
}

func (p *DefaultFileProcessor) processWebP(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeWebP,
	}

	result.Text = "[WebP image - " + fmt.Sprintf("%d bytes", len(data)) + "]"
	return result, nil
}

func (p *DefaultFileProcessor) processText(ctx context.Context, data []byte) (*ParsedContent, error) {
	result := &ParsedContent{
		Metadata: make(map[string]interface{}),
		FileType: FileTypeUnknown,
		Text:     string(data),
	}

	lines := strings.Split(string(data), "\n")
	result.Metadata["line_count"] = len(lines)
	result.Metadata["char_count"] = len(data)

	return result, nil
}

type BatchFileProcessor struct {
	processor FileProcessor
	batchSize int
}

func NewBatchFileProcessor(processor FileProcessor, batchSize int) *BatchFileProcessor {
	if batchSize <= 0 {
		batchSize = 10
	}

	return &BatchFileProcessor{
		processor: processor,
		batchSize: batchSize,
	}
}

func (b *BatchFileProcessor) ProcessBatch(ctx context.Context, paths []string) ([]*ParsedContent, error) {
	results := make([]*ParsedContent, 0, len(paths))

	for i := 0; i < len(paths); i += b.batchSize {
		end := i + b.batchSize
		if end > len(paths) {
			end = len(paths)
		}

		batch := paths[i:end]
		for _, path := range batch {
			result, err := b.processor.Process(ctx, path)
			if err != nil {
				continue
			}
			results = append(results, result)
		}
	}

	return results, nil
}

type ImageConverter struct {
	quality int
}

func NewImageConverter(quality int) *ImageConverter {
	if quality <= 0 {
		quality = 85
	}
	return &ImageConverter{
		quality: quality,
	}
}

func (c *ImageConverter) ConvertToPNG(img image.Image) ([]byte, error) {
	var buf bytes.Buffer
	err := png.Encode(&buf, img)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSerializationFailed, "failed to encode PNG")
	}
	return buf.Bytes(), nil
}

func (c *ImageConverter) ConvertToJPEG(img image.Image) ([]byte, error) {
	var buf bytes.Buffer
	err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: c.quality})
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeSerializationFailed, "failed to encode JPEG")
	}
	return buf.Bytes(), nil
}

func (c *ImageConverter) ConvertToBase64(img image.Image, format string) (string, error) {
	var data []byte
	var err error

	switch format {
	case "png":
		data, err = c.ConvertToPNG(img)
	case "jpg", "jpeg":
		data, err = c.ConvertToJPEG(img)
	default:
		data, err = c.ConvertToPNG(img)
	}

	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

func ReadFileContent(path string) (*FileContent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to read file: "+path)
	}

	stat, err := os.Stat(path)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to stat file: "+path)
	}

	return &FileContent{
		Path:    path,
		Name:    filepath.Base(path),
		Size:    stat.Size(),
		Content: data,
		Metadata: make(map[string]interface{}),
		ModTime: stat.ModTime(),
		Format:  filepath.Ext(path),
	}, nil
}

func WriteFileContent(path string, content *FileContent) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to create directory")
	}

	if err := os.WriteFile(path, content.Content, 0644); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to write file: "+path)
	}

	return nil
}

func CopyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeFileReadFailed, "failed to open source file")
	}
	defer sourceFile.Close()

	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to create destination directory")
	}

	destFile, err := os.Create(dst)
	if err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to create destination file")
	}
	defer destFile.Close()

	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to copy file content")
	}

	if err := destFile.Sync(); err != nil {
		return errors.Wrap(err, errors.ErrCodeFileWriteFailed, "failed to sync file")
	}

	return nil
}
