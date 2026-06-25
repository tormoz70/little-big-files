package api

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/little-big-files/little-big-files/internal/storage"
)

// ShardGuard holds mutable shard role/read-only state for F4.
type ShardGuard struct {
	ShardID   int
	Role      string
	readOnly  bool
	segments  *storage.SegmentManager
}

func NewShardGuard(shardID int, role string, readOnly bool, segments *storage.SegmentManager) *ShardGuard {
	return &ShardGuard{ShardID: shardID, Role: role, readOnly: readOnly, segments: segments}
}

func (g *ShardGuard) SetReadOnly(v bool) { g.readOnly = v }
func (g *ShardGuard) ReadOnly() bool     { return g.readOnly }

func (g *ShardGuard) MetricsShardID() int { return g.ShardID }
func (g *ShardGuard) MetricsRole() string { return g.Role }

func (g *ShardGuard) writeAllowed() bool {
	if g == nil {
		return true
	}
	if g.readOnly {
		return false
	}
	if strings.EqualFold(g.Role, "replica") {
		return false
	}
	return true
}

func (g *ShardGuard) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/packages") && !g.writeAllowed() {
			writeJSON(w, http.StatusForbidden, errorResponse{Error: "shard is read-only"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

type shardStatsResponse struct {
	ShardID    int    `json:"shard_id"`
	Role       string `json:"role"`
	ReadOnly   bool   `json:"read_only"`
	TotalBytes int64  `json:"total_bytes"`
}

type segmentFileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (s *Server) mountInternal(r chi.Router, guard *ShardGuard) {
	if guard == nil {
		return
	}
	r.Get("/v1/internal/stats", func(w http.ResponseWriter, r *http.Request) {
		total, err := guard.segments.TotalBytes()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, shardStatsResponse{
			ShardID:    guard.ShardID,
			Role:       guard.Role,
			ReadOnly:   guard.ReadOnly(),
			TotalBytes: total,
		})
	})

	r.Post("/v1/internal/seal", func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(guard.Role, "primary") {
			writeJSON(w, http.StatusForbidden, errorResponse{Error: "only primary can seal"})
			return
		}
		guard.SetReadOnly(true)
		writeJSON(w, http.StatusOK, map[string]string{"status": "sealed"})
	})

	r.Get("/v1/internal/segments", func(w http.ResponseWriter, r *http.Request) {
		dir := guard.segments.SegmentDir()
		entries, err := os.ReadDir(dir)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		var files []segmentFileInfo
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			files = append(files, segmentFileInfo{Name: e.Name(), Size: info.Size()})
		}
		writeJSON(w, http.StatusOK, files)
	})

	r.Get("/v1/internal/segments/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := filepath.Base(chi.URLParam(r, "name"))
		path := filepath.Join(guard.segments.SegmentDir(), name)
		data, err := os.ReadFile(path)
		if err != nil {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "segment not found"})
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
}
