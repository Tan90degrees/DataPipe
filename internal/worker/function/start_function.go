package function

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"datapipe/internal/models"
)

type StartFunctionImpl struct {
	*BaseFunction
	directory    string
	recursive    bool
	fileTypes    []string
	excludeDirs  []string
}

func NewStartFunction(name, version string, config map[string]interface{}) *StartFunctionImpl {
	base := NewBaseFunction(
		name,
		version,
		models.FunctionTypeStart,
		models.InputType{Type: "none"},
		models.OutputType{Type: "array"},
		config,
	)

	fn := &StartFunctionImpl{
		BaseFunction: base,
		directory:    ".",
		recursive:    true,
		fileTypes:    []string{".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".md", ".html", ".htm", ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp"},
		excludeDirs:  []string{".git", "node_modules", "__pycache__", ".venv"},
	}

	if dir, ok := config["directory"].(string); ok {
		fn.directory = dir
	}
	if rec, ok := config["recursive"].(bool); ok {
		fn.recursive = rec
	}
	if types, ok := config["file_types"].([]interface{}); ok {
		fn.fileTypes = make([]string, 0)
		for _, t := range types {
			if s, ok := t.(string); ok {
				fn.fileTypes = append(fn.fileTypes, s)
			}
		}
	}
	if exclude, ok := config["exclude_dirs"].([]interface{}); ok {
		fn.excludeDirs = make([]string, 0)
		for _, e := range exclude {
			if s, ok := e.(string); ok {
				fn.excludeDirs = append(fn.excludeDirs, s)
			}
		}
	}

	return fn
}

func (s *StartFunctionImpl) Scan(ctx context.Context, execCtx *ExecutionContext) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	err := filepath.Walk(s.directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if info.IsDir() {
			if s.shouldExcludeDir(info.Name()) {
				return filepath.SkipDir
			}
			return nil
		}

		if !s.shouldIncludeFile(info.Name()) {
			return nil
		}

		relPath, err := filepath.Rel(s.directory, path)
		if err != nil {
			return nil
		}

		results = append(results, map[string]interface{}{
			"path":         path,
			"name":         info.Name(),
			"relative_path": relPath,
			"size":         info.Size(),
			"extension":   strings.ToLower(filepath.Ext(info.Name())),
			"is_dir":       false,
		})

		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

func (s *StartFunctionImpl) Execute(ctx context.Context, execCtx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
	files, err := s.Scan(ctx, execCtx)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"files": files,
		"count": len(files),
	}, nil
}

func (s *StartFunctionImpl) shouldExcludeDir(name string) bool {
	for _, excluded := range s.excludeDirs {
		if name == excluded {
			return true
		}
	}
	return false
}

func (s *StartFunctionImpl) shouldIncludeFile(name string) bool {
	if len(s.fileTypes) == 0 {
		return true
	}

	ext := strings.ToLower(filepath.Ext(name))
	for _, ft := range s.fileTypes {
		if strings.HasPrefix(ft, ".") {
			if ext == strings.ToLower(ft) {
				return true
			}
		} else {
			if strings.Contains(strings.ToLower(name), strings.ToLower(ft)) {
				return true
			}
		}
	}
	return false
}

func (s *StartFunctionImpl) ValidateConfig() error {
	if s.directory == "" {
		s.directory = "."
	}
	return nil
}

func (s *StartFunctionImpl) Initialize() error {
	info, err := os.Stat(s.directory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return nil
	}
	return nil
}
