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

func TestStartupRegistrationStateDefaultsToStandby(t *testing.T) {
	state, err := startupRegistrationState("")
	require.NoError(t, err)
	require.Equal(t, ShardStandby, state)
}

func TestStartupRegistrationStateRejectsNonStandby(t *testing.T) {
	_, err := startupRegistrationState("active")
	require.Error(t, err)
	require.Contains(t, err.Error(), "startup_state must be standby")

	_, err = startupRegistrationState("sealed")
	require.Error(t, err)
	require.Contains(t, err.Error(), "startup_state must be standby")
}

func TestRegisterShardRejectsActiveStartupState(t *testing.T) {
	srv := NewServer(config.Config{ClusterKey: "top-secret"}, NewRegistry(nil, 0, ""))

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/shards", strings.NewReader(`{
		"cluster_key":"top-secret",
		"shard_uuid":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		"primary_url":"http://shard-1:8080",
		"startup_state":"active"
	}`))
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "startup_state must be standby")
}
