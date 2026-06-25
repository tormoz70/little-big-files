package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/metrics"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/little-big-files/little-big-files/internal/supplier"
)

type Server struct {
	cfg    config.Config
	ingest *ingestion.Service
	repo   metadata.Repository
	blobs  *storage.BlobStore
	guard  *ShardGuard
}

func NewServer(cfg config.Config, ingest *ingestion.Service, repo metadata.Repository, blobs *storage.BlobStore) *Server {
	return &Server{cfg: cfg, ingest: ingest, repo: repo, blobs: blobs}
}

// NewShardServer creates a server with shard guard and internal endpoints.
func NewShardServer(cfg config.Config, ingest *ingestion.Service, repo metadata.Repository, blobs *storage.BlobStore, segments *storage.SegmentManager) *Server {
	s := NewServer(cfg, ingest, repo, blobs)
	s.guard = NewShardGuard(cfg.ShardID, cfg.ShardRole, cfg.ShardReadOnly, segments)
	return s
}

func (s *Server) ShardGuard() *ShardGuard { return s.guard }

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Logger, middleware.Recoverer, metrics.Middleware)
	if s.guard != nil {
		r.Use(s.guard.middleware)
	}

	r.Post("/v1/packages", s.postPackage)
	r.Get("/v1/packages/{id}", s.getPackage)
	r.Get("/v1/packages/{id}/files/{file_id}", s.getFile)
	r.Get("/v1/packages/{id}/original", s.getOriginal)
	s.mountInternal(r, s.guard)
	r.Handle("/metrics", metrics.Handler())
	return r
}

type errorResponse struct {
	Error string `json:"error"`
}

type fileResponse struct {
	FileID      int64   `json:"file_id"`
	Role        string  `json:"role"`
	Filename    string  `json:"filename"`
	Size        int     `json:"size"`
	Sequence    *int    `json:"sequence,omitempty"`
	DownloadURL string  `json:"download_url"`
}

type linksResponse struct {
	Self     string `json:"self"`
	Original string `json:"original"`
}

type packageResponse struct {
	PackageID        int64           `json:"package_id"`
	SupplierID       int             `json:"supplier_id"`
	PayloadType      string          `json:"payload_type"`
	StorageMode      string          `json:"storage_mode"`
	ReceivedAt       time.Time       `json:"received_at"`
	OriginalFilename *string         `json:"original_filename,omitempty"`
	FileCount        int             `json:"file_count"`
	Files            []fileResponse  `json:"files"`
	Links            linksResponse   `json:"links"`
	UnpackStatus     string          `json:"unpack_status,omitempty"`
	UnpackError      string          `json:"unpack_error,omitempty"`
}

func (s *Server) postPackage(w http.ResponseWriter, r *http.Request) {
	supplierID, err := supplier.ParseQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	body, err := readBodyLimited(r, s.cfg.MaxBodyBytes)
	if err != nil {
		if strings.Contains(err.Error(), "too large") {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorResponse{Error: "payload_too_large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	var filename *string
	if fn := r.URL.Query().Get("filename"); fn != "" {
		filename = &fn
	}

	pkg, err := s.ingest.ProcessPackage(r.Context(), supplierID, body, filename)
	if err != nil {
		status := http.StatusInternalServerError
		if isClientError(err) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, errorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, buildPackageResponse(s.cfg, pkg))
}

func (s *Server) getPackage(w http.ResponseWriter, r *http.Request) {
	id, err := parsePackageID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	pkg, err := s.repo.GetPackage(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if pkg == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "package not found"})
		return
	}
	writeJSON(w, http.StatusOK, buildPackageResponse(s.cfg, pkg))
}

func (s *Server) getFile(w http.ResponseWriter, r *http.Request) {
	id, err := parsePackageID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	fileID, err := strconv.ParseInt(chi.URLParam(r, "file_id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid file_id"})
		return
	}
	s.serveFile(w, r, id, fileID)
}

func (s *Server) getOriginal(w http.ResponseWriter, r *http.Request) {
	id, err := parsePackageID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	file, err := s.repo.GetOriginalFile(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if file == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "original not found"})
		return
	}
	s.serveFile(w, r, id, file.ID)
}

func (s *Server) serveFile(w http.ResponseWriter, r *http.Request, packageID, fileID int64) {
	file, err := s.repo.GetPackageFile(r.Context(), packageID, fileID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if file == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "file not found"})
		return
	}

	blob, err := s.repo.GetBlob(r.Context(), file.BlobHash)
	if err != nil || blob == nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "blob not found"})
		return
	}

	data, err := s.blobs.ReadBlob(*blob)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}

	filename := "file"
	if file.OriginalFilename != nil {
		filename = *file.OriginalFilename
	}
	ct := contentTypeFor(file.Role, filename)
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func buildPackageResponse(cfg config.Config, pkg *metadata.Package) packageResponse {
	resp := packageResponse{
		PackageID:        pkg.ID,
		SupplierID:       pkg.SupplierID,
		PayloadType:      pkg.PayloadType,
		StorageMode:      pkg.StorageMode,
		ReceivedAt:       pkg.ReceivedAt,
		OriginalFilename: pkg.OriginalFilename,
		FileCount:        pkg.FileCount,
		Links: linksResponse{
			Self:     fmt.Sprintf("/v1/packages/%d", pkg.ID),
			Original: fmt.Sprintf("/v1/packages/%d/original", pkg.ID),
		},
	}

	if pkg.PayloadType == string(ingestion.PayloadZIP) {
		switch pkg.StorageMode {
		case ingestion.StorageRawLarge:
			if cfg.LargeZipAsyncUnpack {
				resp.UnpackStatus = ingestion.UnpackPending
			} else {
				resp.UnpackStatus = ingestion.UnpackSkipped
			}
		case ingestion.StorageZipWithMembers:
			if pkg.UnpackError != nil {
				resp.UnpackStatus = ingestion.UnpackFailed
				resp.UnpackError = *pkg.UnpackError
			} else {
				resp.UnpackStatus = ingestion.UnpackOK
			}
		}
	}

	for _, f := range pkg.Files {
		fn := ""
		if f.OriginalFilename != nil {
			fn = *f.OriginalFilename
		}
		resp.Files = append(resp.Files, fileResponse{
			FileID:      f.ID,
			Role:        f.Role,
			Filename:    fn,
			Size:        f.Size,
			Sequence:    f.SequenceNumber,
			DownloadURL: fmt.Sprintf("/v1/packages/%d/files/%d", pkg.ID, f.ID),
		})
	}
	return resp
}

func contentTypeFor(role, filename string) string {
	switch role {
	case ingestion.RoleOriginal:
		if strings.HasSuffix(strings.ToLower(filename), ".zip") {
			return "application/zip"
		}
	case ingestion.RoleMember:
		if strings.HasSuffix(strings.ToLower(filename), ".xml") {
			return "application/xml"
		}
	case ingestion.RoleUnpackError:
		return "text/plain; charset=utf-8"
	}
	if strings.HasSuffix(strings.ToLower(filename), ".xml") {
		return "application/xml"
	}
	if strings.HasSuffix(strings.ToLower(filename), ".zip") {
		return "application/zip"
	}
	return "application/octet-stream"
}

func parsePackageID(r *http.Request) (int64, error) {
	return strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func isClientError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "empty") ||
		strings.Contains(msg, "unsupported") ||
		strings.Contains(msg, "too large") ||
		strings.Contains(msg, "supplier")
}
