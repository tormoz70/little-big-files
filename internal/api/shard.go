package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/little-big-files/little-big-files/internal/storage"
)

// ShardGuard holds mutable shard role/read-only state for F4.
type ShardGuard struct {
	ShardID  int
	Role     string
	readOnly bool
	segments *storage.SegmentManager
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
	Name         string `json:"name"`
	Size         int64  `json:"size"`
	SHA256       string `json:"sha256"`
	ModifiedUnix int64  `json:"modified_unix"`
}

// internalAuthKey returns the shared secret used to protect /v1/internal/*.
// Prefers CLUSTER_KEY, falling back to SHARD_CLUSTER_KEY (the value a shard
// already holds in sharded deployments).
func (s *Server) internalAuthKey() string {
	if k := strings.TrimSpace(s.cfg.ClusterKey); k != "" {
		return k
	}
	return strings.TrimSpace(s.cfg.ShardClusterKey)
}

func internalKeyFromRequest(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-Cluster-Key")); v != "" {
		return v
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[len("bearer "):])
	}
	return ""
}

// requireClusterKey wraps internal handlers with constant-time cluster-key auth.
// Fails closed: if no key is configured the endpoints are disabled entirely.
func (s *Server) requireClusterKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expected := s.internalAuthKey()
		if expected == "" {
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "internal endpoints disabled: cluster key not configured"})
			return
		}
		provided := internalKeyFromRequest(r)
		if subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
			writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "invalid or missing cluster key"})
			return
		}
		next(w, r)
	}
}

func (s *Server) mountInternal(r chi.Router, guard *ShardGuard) {
	if guard == nil {
		return
	}
	r.Get("/v1/internal/stats", s.requireClusterKey(func(w http.ResponseWriter, r *http.Request) {
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
	}))

	r.Post("/v1/internal/seal", s.requireClusterKey(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(guard.Role, "primary") {
			writeJSON(w, http.StatusForbidden, errorResponse{Error: "only primary can seal"})
			return
		}
		guard.SetReadOnly(true)
		writeJSON(w, http.StatusOK, map[string]string{"status": "sealed"})
	}))

	r.Get("/v1/internal/segments", s.requireClusterKey(func(w http.ResponseWriter, r *http.Request) {
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
			if !allowedInternalSyncFile(e.Name()) {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			path := filepath.Join(dir, e.Name())
			sum, err := fileSHA256(path)
			if err != nil {
				continue
			}
			files = append(files, segmentFileInfo{
				Name:         e.Name(),
				Size:         info.Size(),
				SHA256:       sum,
				ModifiedUnix: info.ModTime().Unix(),
			})
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
		writeJSON(w, http.StatusOK, files)
	}))

	r.Get("/v1/internal/segments/{name}", s.requireClusterKey(func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")
		if filepath.Base(name) != name || !allowedInternalSyncFile(name) {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "segment not found"})
			return
		}
		path := filepath.Join(guard.segments.SegmentDir(), name)
		data, err := os.ReadFile(path)
		if err != nil {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "segment not found"})
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
}

func allowedInternalSyncFile(name string) bool {
	if strings.HasPrefix(name, "segment_") && (strings.HasSuffix(name, ".dat") || strings.HasSuffix(name, ".idx")) {
		return true
	}
	return name == "ingest_journal.ndjson"
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
