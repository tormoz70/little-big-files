package coordinator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/stretchr/testify/require"
)

func TestSealRotateRequiresClusterKey(t *testing.T) {
	srv := NewServer(config.Config{ClusterKey: "top-secret"}, &Registry{})

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/seal-rotate", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "invalid cluster key")
}

func TestSealRotateRejectsWrongClusterKey(t *testing.T) {
	srv := NewServer(config.Config{ClusterKey: "top-secret"}, &Registry{})

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/seal-rotate", strings.NewReader(`{"cluster_key":"wrong"}`))
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "invalid cluster key")
}

