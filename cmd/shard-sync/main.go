package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/little-big-files/little-big-files/internal/config"
)

type segmentFile struct {
	Name         string `json:"name"`
	Size         int64  `json:"size"`
	SHA256       string `json:"sha256"`
	ModifiedUnix int64  `json:"modified_unix"`
}

func main() {
	cfg := config.Load()
	primary := os.Getenv("SYNC_PRIMARY_URL")
	if primary == "" {
		slog.Error("SYNC_PRIMARY_URL required")
		os.Exit(1)
	}
	interval := 30 * time.Second
	if v := os.Getenv("SYNC_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}

	clusterKey := cfg.ClusterKey
	if clusterKey == "" {
		clusterKey = cfg.ShardClusterKey
	}

	client := &http.Client{Timeout: 120 * time.Second}
	ctx := context.Background()

	for {
		if err := syncOnce(ctx, client, primary, clusterKey, cfg.DataDir); err != nil {
			slog.Warn("segment sync failed", "err", err)
		}
		time.Sleep(interval)
	}
}

func setClusterKey(req *http.Request, key string) {
	if key != "" {
		req.Header.Set("X-Cluster-Key", key)
	}
}

func syncOnce(ctx context.Context, client *http.Client, primaryURL, clusterKey, dataDir string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, primaryURL+"/v1/internal/segments", nil)
	if err != nil {
		return err
	}
	setClusterKey(req, clusterKey)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errStr(fmt.Sprintf("list segments %s: %s", resp.Status, string(body)))
	}
	var files []segmentFile
	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	for _, f := range files {
		if !allowedSyncFile(f.Name) {
			slog.Warn("skip unexpected segment artifact", "name", f.Name)
			continue
		}
		dest := filepath.Join(dataDir, f.Name)
		inSync, err := isFileInSync(dest, f)
		if err == nil && inSync {
			continue
		}
		slog.Info("sync segment", "file", f.Name)
		if err := downloadSegment(ctx, client, primaryURL, clusterKey, f.Name, dest); err != nil {
			return err
		}
	}
	return nil
}

func downloadSegment(ctx context.Context, client *http.Client, primaryURL, clusterKey, name, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, primaryURL+"/v1/internal/segments/"+url.PathEscape(name), nil)
	if err != nil {
		return err
	}
	setClusterKey(req, clusterKey)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errStr("download " + name + ": " + string(body))
	}
	tmp := dest + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dest)
}

func isFileInSync(path string, remote segmentFile) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.Size() != remote.Size {
		return false, nil
	}
	if remote.SHA256 == "" {
		return false, nil
	}
	sum, err := fileSHA256(path)
	if err != nil {
		return false, err
	}
	return strings.EqualFold(sum, remote.SHA256), nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func allowedSyncFile(name string) bool {
	if strings.HasPrefix(name, "segment_") && (strings.HasSuffix(name, ".dat") || strings.HasSuffix(name, ".idx")) {
		return true
	}
	return name == "ingest_journal.ndjson"
}

type errStr string

func (e errStr) Error() string { return string(e) }
