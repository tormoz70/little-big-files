package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
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
	r.Post("/v1/admin/seal-rotate", s.sealRotate)
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
	if err := s.registry.SealAndRotate(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rotated"})
}

func (s *Server) RunSealLoop(ctx context.Context) {
	if s.cfg.SealCheckInterval <= 0 || s.cfg.ShardMaxBytes <= 0 {
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
