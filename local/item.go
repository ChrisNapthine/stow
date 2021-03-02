package local

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Metadata constants describe the metadata available
// for a local Item.
const (
	MetadataPath       = "path"
	MetadataIsDir      = "is_dir"
	MetadataDir        = "dir"
	MetadataName       = "name"
	MetadataMode       = "mode"
	MetadataModeD      = "mode_d"
	MetadataPerm       = "perm"
	MetadataINode      = "inode"
	MetadataSize       = "size"
	MetadataIsHardlink = "is_hardlink"
	MetadataIsSymlink  = "is_symlink"
	MetadataLink       = "link"
	MetadataUser       = "user_data"
)

const MetadataFileExt = "._meta"

type item struct {
	path          string
	metaPath      string
	contPrefixLen int
	infoOnce      sync.Once // protects info
	info          os.FileInfo
	infoErr       error
	metadata      map[string]interface{}
}

func (i *item) ID() string {
	return i.path
}

func (i *item) Name() string {
	return filepath.ToSlash(i.path[i.contPrefixLen:])
}

func (i *item) Size() (int64, error) {
	err := i.ensureInfo()
	if err != nil {
		return 0, err
	}
	return i.info.Size(), nil
}

func (i *item) URL() *url.URL {
	return &url.URL{
		Scheme: "file",
		Path:   filepath.Clean(i.path),
	}
}

func (i *item) ETag() (string, error) {
	err := i.ensureInfo()
	if err != nil {
		return "", nil
	}
	return i.info.ModTime().String(), nil
}

// Open opens the file for reading.
func (i *item) Open() (io.ReadCloser, error) {
	return os.Open(i.path)
}

func (i *item) LastMod() (time.Time, error) {
	err := i.ensureInfo()
	if err != nil {
		return time.Time{}, nil
	}

	return i.info.ModTime(), nil
}

func (i *item) ensureInfo() error {
	i.infoOnce.Do(func() {
		i.info, i.infoErr = os.Lstat(i.path) // retrieve item file info

		if i.infoErr != nil {
			return
		}
		i.setMetadata(i.info) // merge file and metadata maps
		var md map[string]interface{}
		md, i.infoErr = i.readMeta()
		if i.infoErr != nil {
			return
		}
		if md != nil {
			i.metadata[MetadataUser] = md
		}
	})
	return i.infoErr
}

func (i *item) setMetadata(info os.FileInfo) {
	fileMetadata := getFileMetadata(i.path, info) // retrieve file metadata
	i.metadata = fileMetadata
}

// Metadata gets stat information for the file.
func (i *item) Metadata() (map[string]interface{}, error) {
	err := i.ensureInfo()
	if err != nil {
		return nil, err
	}
	return i.metadata, nil
}

func (i *item) readMeta() (map[string]interface{}, error) {
	if len(i.metaPath) == 0 {
		return nil, nil
	}
	b, err := i.readFileBytes(i.metaPath)
	if err != nil {
		return nil, err
	}
	var metadata map[string]interface{}
	err = json.Unmarshal(b, &metadata)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (i *item) readFileBytes(FileName string) (b []byte, err error) {
	f, err := os.Open(FileName)
	if err != nil {
		return
	}
	defer f.Close()

	b, err = ioutil.ReadAll(f)
	return
}
