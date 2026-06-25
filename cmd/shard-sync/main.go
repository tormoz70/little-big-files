package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/little-big-files/little-big-files/internal/config"
)

type segmentFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
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

	client := &http.Client{Timeout: 120 * time.Second}
	ctx := context.Background()

	for {
		if err := syncOnce(ctx, client, primary, cfg.DataDir); err != nil {
			slog.Warn("segment sync failed", "err", err)
		}
		time.Sleep(interval)
	}
}

func syncOnce(ctx context.Context, client *http.Client, primaryURL, dataDir string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, primaryURL+"/v1/internal/segments", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var files []segmentFile
	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	for _, f := range files {
		dest := filepath.Join(dataDir, f.Name)
		info, err := os.Stat(dest)
		if err == nil && info.Size() == f.Size {
			continue
		}
		slog.Info("sync segment", "file", f.Name)
		if err := downloadSegment(ctx, client, primaryURL, f.Name, dest); err != nil {
			return err
		}
	}
	return nil
}

func downloadSegment(ctx context.Context, client *http.Client, primaryURL, name, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, primaryURL+"/v1/internal/segments/"+name, nil)
	if err != nil {
		return err
	}
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
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dest)
}

type errStr string

func (e errStr) Error() string { return string(e) }
