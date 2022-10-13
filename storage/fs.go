package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nyaruka/gocommon/uuids"
)

type fsStorage struct {
	directory string
	perms     os.FileMode
}

// NewFS creates a new file system storage service suitable for use in tests
func NewFS(directory string, perms os.FileMode) Storage {
	return &fsStorage{directory: directory, perms: perms}
}

func (s *fsStorage) Name() string {
	return "file system"
}

func (s *fsStorage) Test(ctx context.Context) error {
	// write randomly named file
	path := fmt.Sprintf("%s.txt", uuids.New())
	fullPath, err := s.Put(ctx, path, "text/plain", []byte(`test`))
	if err != nil {
		return err
	}

	os.Remove(fullPath)
	return nil
}

func (s *fsStorage) Get(ctx context.Context, path string) (string, []byte, error) {
	fullPath := filepath.Join(s.directory, path)
	body, err := os.ReadFile(fullPath)
	return "", body, err
}

func (s *fsStorage) Put(ctx context.Context, path string, contentType string, body []byte) (string, error) {
	fullPath := filepath.Join(s.directory, path)

	err := os.MkdirAll(filepath.Dir(fullPath), s.perms)
	if err != nil {
		return "", err
	}

	err = os.WriteFile(fullPath, body, s.perms)
	if err != nil {
		return "", err
	}

	return fullPath, nil
}

func (s *fsStorage) BatchPut(ctx context.Context, us []*Upload) error {
	for _, upload := range us {
		url, err := s.Put(ctx, upload.Path, upload.ContentType, upload.Body)
		if err != nil {
			upload.Error = err
			return err
		}
		upload.URL = url
	}
	return nil
}
