package coordinator

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/metrics"
	"github.com/little-big-files/little-big-files/internal/supplier"
)

type Server struct {
	cfg      config.Config
	registry *Registry
}

func NewServer(cfg config.Config, registry *Registry) *Server {
	return &Server{cfg: cfg, registry: registry}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Logger, middleware.Recoverer, metrics.Middleware)

	r.Post("/v1/packages", s.postPackage)
	r.Get("/v1/packages/{id}", s.getPackage)
	r.Get("/v1/packages/{id}/files/{file_id}", s.getFile)
	r.Get("/v1/packages/{id}/original", s.getOriginal)

	r.Get("/v1/admin/shards", s.listShards)
	r.Post("/v1/admin/shards", s.registerShard)
	r.Post("/v1/admin/seal-rotate", s.sealRotate)
	r.Patch("/v1/admin/shards/{id}/state", s.patchShardState)
	r.Handle("/metrics", metrics.CoordinatorHandler())
	return r
}

func (s *Server) postPackage(w http.ResponseWriter, r *http.Request) {
	supplierID, err := supplier.ParseQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	body, err := readBody(r, s.cfg.MaxBodyBytes)
	if err != nil {
		status := http.StatusBadRequest
		if err.Error() == "payload too large" {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	filename := r.URL.Query().Get("filename")
	data, status, err := s.registry.ProxyPost(r.Context(), supplierID, body, filename)
	if err != nil {
		var statusErr *StatusError
		if errors.As(err, &statusErr) {
			writeJSON(w, statusErr.Status, map[string]string{"error": statusErr.Code})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func (s *Server) getPackage(w http.ResponseWriter, r *http.Request) {
	id, err := parseGlobalID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	data, status, ct, err := s.registry.ProxyGet(r.Context(), id, "")
	if err != nil {
		var statusErr *StatusError
		if errors.As(err, &statusErr) {
			writeJSON(w, statusErr.Status, map[string]string{"error": statusErr.Code})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeRaw(w, status, ct, data)
}

func (s *Server) getFile(w http.ResponseWriter, r *http.Request) {
	id, err := parseGlobalID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	fileID := chi.URLParam(r, "file_id")
	data, status, ct, err := s.registry.ProxyGet(r.Context(), id, "/files/"+fileID)
	if err != nil {
		var statusErr *StatusError
		if errors.As(err, &statusErr) {
			writeJSON(w, statusErr.Status, map[string]string{"error": statusErr.Code})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeRaw(w, status, ct, data)
}

func (s *Server) getOriginal(w http.ResponseWriter, r *http.Request) {
	id, err := parseGlobalID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	data, status, ct, err := s.registry.ProxyGet(r.Context(), id, "/original")
	if err != nil {
		var statusErr *StatusError
		if errors.As(err, &statusErr) {
			writeJSON(w, statusErr.Status, map[string]string{"error": statusErr.Code})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeRaw(w, status, ct, data)
}

func (s *Server) listShards(w http.ResponseWriter, r *http.Request) {
	shards, err := s.registry.repo.ListShards(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, shards)
}

func (s *Server) sealRotate(w http.ResponseWriter, r *http.Request) {
	var req SealRotateRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
			return
		}
	}
	if err := s.validateClusterKey(clusterKeyFromRequest(req.ClusterKey, r)); err != nil {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
		return
	}
	if err := s.registry.SealAndRotate(r.Context()); err != nil {
		if errors.Is(err, ErrStateConflict) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		if errors.Is(err, ErrRotationIncomplete) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rotated"})
}

func (s *Server) registerShard(w http.ResponseWriter, r *http.Request) {
	var req RegisterShardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}
	if err := s.validateClusterKey(req.ClusterKey); err != nil {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
		return
	}
	if !isUUID(req.ShardUUID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid shard_uuid"})
		return
	}
	if strings.TrimSpace(req.PrimaryURL) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "primary_url is required"})
		return
	}
	state, err := startupRegistrationState(req.StartupState)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	shard, created, err := s.registry.RegisterShard(r.Context(), req.ShardUUID, state, req.PrimaryURL, req.ReplicaURL)
	if err != nil {
		if errors.Is(err, ErrStateConflict) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, RegisterShardResponse{
		Shard:      *shard,
		Registered: created,
	})
}

func startupRegistrationState(raw string) (ShardState, error) {
	if strings.TrimSpace(raw) == "" {
		return ShardStandby, nil
	}
	parsed, err := ParseShardState(raw)
	if err != nil {
		return "", err
	}
	if parsed != ShardStandby {
		return "", errStr("startup_state must be standby")
	}
	return ShardStandby, nil
}

func (s *Server) patchShardState(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid shard id"})
		return
	}
	var req PatchShardStateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}
	if err := s.validateClusterKey(req.ClusterKey); err != nil {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
		return
	}
	state, err := ParseShardState(req.State)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	shard, err := s.registry.PatchShardState(r.Context(), id, state, req.Confirm)
	if err != nil {
		switch {
		case errors.Is(err, ErrShardNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrStateConflict):
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		default:
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}
	writeJSON(w, http.StatusOK, shard)
}

func (s *Server) RunSealLoop(ctx context.Context) {
	if s.cfg.IsSingleNode() || s.cfg.SealCheckInterval <= 0 || s.cfg.ShardMaxBytes <= 0 {
		return
	}
	ticker := time.NewTicker(s.cfg.SealCheckInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				if err := s.registry.CheckSeal(ctx); err != nil {
					slog.Warn("seal check failed", "err", err)
				}
			}
		}
	}()
}

func parseGlobalID(r *http.Request) (int64, error) {
	return strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
}

func readBody(r *http.Request, max int64) ([]byte, error) {
	if r.Body == nil {
		return nil, errStr("empty body")
	}
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, max+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, errStr("payload too large")
	}
	if len(data) == 0 {
		return nil, errStr("empty body")
	}
	return data, nil
}

type errStr string

func (e errStr) Error() string { return string(e) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeRaw(w http.ResponseWriter, status int, ct string, data []byte) {
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func (s *Server) validateClusterKey(provided string) error {
	expected := s.cfg.EffectiveClusterKey()
	if expected == "" {
		return errStr("cluster key is not configured")
	}
	if subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		return errStr("invalid cluster key")
	}
	return nil
}

func clusterKeyFromRequest(explicit string, r *http.Request) string {
	if key := strings.TrimSpace(explicit); key != "" {
		return key
	}
	if key := strings.TrimSpace(r.Header.Get("X-Cluster-Key")); key != "" {
		return key
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[len("bearer "):])
	}
	return ""
}

func isUUID(value string) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	if len(v) != 36 {
		return false
	}
	for i, ch := range v {
		switch i {
		case 8, 13, 18, 23:
			if ch != '-' {
				return false
			}
		default:
			if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
				return false
			}
		}
	}
	return true
}
