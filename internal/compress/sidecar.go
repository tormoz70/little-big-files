package compress

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Sidecar struct{ dir string }

type currentMeta struct {
	DictID   int    `json:"dict_id"`
	SHA256   string `json:"sha256"`
	Filename string `json:"filename"`
}

func NewSidecar(dataRoot string) *Sidecar {
	return &Sidecar{dir: filepath.Join(dataRoot, "dictionaries")}
}

func dictFilename(dictID int, sha string) string {
	short := sha
	if len(short) > 16 {
		short = short[:16]
	}
	return fmt.Sprintf("dict_%04d_%s.zdict", dictID, short)
}

func (s *Sidecar) Save(dictID int, dict []byte) error {
	if len(dict) == 0 {
		return nil
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	sum := sha256.Sum256(dict)
	sha := hex.EncodeToString(sum[:])
	name := dictFilename(dictID, sha)
	final := filepath.Join(s.dir, name)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, dict, 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	meta := currentMeta{DictID: dictID, SHA256: sha, Filename: name}
	mb, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	curTmp := filepath.Join(s.dir, "current.json.tmp")
	cur := filepath.Join(s.dir, "current.json")
	if err := os.WriteFile(curTmp, mb, 0o644); err != nil {
		return err
	}
	return os.Rename(curTmp, cur)
}

func (s *Sidecar) LoadCurrent() (dictID int, dict []byte, err error) {
	cur := filepath.Join(s.dir, "current.json")
	mb, err := os.ReadFile(cur)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, nil
		}
		return 0, nil, err
	}
	var meta currentMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return 0, nil, err
	}
	path := filepath.Join(s.dir, meta.Filename)
	dict, err = os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}
	sum := sha256.Sum256(dict)
	if hex.EncodeToString(sum[:]) != meta.SHA256 {
		return 0, nil, fmt.Errorf("dictionary checksum mismatch")
	}
	return meta.DictID, dict, nil
}
