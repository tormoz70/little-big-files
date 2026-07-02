//go:build integration

package coordinator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/coordinator"
	"github.com/stretchr/testify/require"
)

func TestRegisterShardIsIdempotentByUUID(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	uuid := "11111111-1111-1111-1111-111111111111"
	first, created, err := repo.RegisterShard(ctx, uuid, coordinator.ShardStandby, "http://shard-a:8080", nil)
	require.NoError(t, err)
	require.True(t, created)

	second, createdAgain, err := repo.RegisterShard(ctx, uuid, coordinator.ShardStandby, "http://shard-a-updated:8080", nil)
	require.NoError(t, err)
	require.False(t, createdAgain)
	require.Equal(t, first.ShardID, second.ShardID)
	require.Equal(t, "http://shard-a-updated:8080", second.PrimaryURL)
}

func TestRegisterShardForcesStandbyOnCreate(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	shard, created, err := repo.RegisterShard(ctx, "77777777-7777-7777-7777-777777777777", coordinator.ShardActive, "http://shard-force:8080", nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, coordinator.ShardStandby, shard.State)
}

func TestRegisterShardEndpointCreatesStandbyWithoutStartupState(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	reg := coordinator.NewRegistry(repo, 0, "")
	srv := coordinator.NewServer(config.Config{ClusterKey: "test-key"}, reg)

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/shards", strings.NewReader(`{
		"cluster_key":"test-key",
		"shard_uuid":"88888888-8888-8888-8888-888888888888",
		"primary_url":"http://shard-8:8080"
	}`))
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	var resp coordinator.RegisterShardResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, coordinator.ShardStandby, resp.Shard.State)

	created, err := repo.GetShard(ctx, resp.Shard.ShardID)
	require.NoError(t, err)
	require.NotNil(t, created)
	require.Equal(t, coordinator.ShardStandby, created.State)
}

func TestRegisterShardEndpointReturnsActiveWhenAutoPromoted(t *testing.T) {
	_, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	statsSrv := newStatsServer(t)
	defer statsSrv.Close()

	reg := coordinator.NewRegistry(repo, 0, "")
	srv := coordinator.NewServer(config.Config{ClusterKey: "test-key"}, reg)

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/shards", strings.NewReader(`{
		"cluster_key":"test-key",
		"shard_uuid":"8a8a8a8a-8a8a-8a8a-8a8a-8a8a8a8a8a8a",
		"primary_url":"`+statsSrv.URL+`"
	}`))
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	var resp coordinator.RegisterShardResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, coordinator.ShardActive, resp.Shard.State)
}

func TestRegisterShardEndpointReregistrationKeepsExistingState(t *testing.T) {
	_, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	statsSrv := newStatsServer(t)
	defer statsSrv.Close()

	reg := coordinator.NewRegistry(repo, 0, "")
	srv := coordinator.NewServer(config.Config{ClusterKey: "test-key"}, reg)
	uuid := "99999999-9999-9999-9999-999999999999"

	req1 := httptest.NewRequest(http.MethodPost, "/v1/admin/shards", strings.NewReader(`{
		"cluster_key":"test-key",
		"shard_uuid":"`+uuid+`",
		"primary_url":"`+statsSrv.URL+`"
	}`))
	rec1 := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusCreated, rec1.Code)
	var first coordinator.RegisterShardResponse
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&first))
	require.Equal(t, coordinator.ShardActive, first.Shard.State)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/admin/shards", strings.NewReader(`{
		"cluster_key":"test-key",
		"shard_uuid":"`+uuid+`",
		"primary_url":"http://shard-updated:8080"
	}`))
	rec2 := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)

	var second coordinator.RegisterShardResponse
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&second))
	require.Equal(t, first.Shard.ShardID, second.Shard.ShardID)
	require.Equal(t, coordinator.ShardActive, second.Shard.State)
	require.Equal(t, "http://shard-updated:8080", second.Shard.PrimaryURL)
}

func TestBootstrapDoesNotOverwriteRuntimeState(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	statsSrv := newStatsServer(t)
	defer statsSrv.Close()
	standbySrv := newStatsServer(t)
	defer standbySrv.Close()

	active, _, err := repo.RegisterShard(ctx, "aaaaaaa1-1111-1111-1111-111111111111", coordinator.ShardStandby, statsSrv.URL, nil)
	require.NoError(t, err)
	standby, _, err := repo.RegisterShard(ctx, "bbbbbbb2-2222-2222-2222-222222222222", coordinator.ShardStandby, standbySrv.URL, nil)
	require.NoError(t, err)

	reg := coordinator.NewRegistry(repo, 0, "")
	_, err = reg.PatchShardState(ctx, active.ShardID, coordinator.ShardActive, true)
	require.NoError(t, err)
	_, err = reg.PatchShardState(ctx, standby.ShardID, coordinator.ShardActive, true)
	require.NoError(t, err)

	bootstrap := []map[string]any{
		{
			"shard_id":    active.ShardID,
			"shard_uuid":  "aaaaaaa1-1111-1111-1111-111111111111",
			"state":       "active",
			"primary_url": "http://bootstrap-primary",
		},
		{
			"shard_id":    standby.ShardID,
			"shard_uuid":  "bbbbbbb2-2222-2222-2222-222222222222",
			"state":       "standby",
			"primary_url": "http://bootstrap-standby",
		},
	}
	data, err := json.Marshal(bootstrap)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "bootstrap.json")
	require.NoError(t, os.WriteFile(path, data, 0o644))
	require.NoError(t, repo.BootstrapFromFile(ctx, path))

	afterActive, err := repo.GetShard(ctx, active.ShardID)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardSealed, afterActive.State)
	require.Equal(t, statsSrv.URL, afterActive.PrimaryURL)

	afterStandby, err := repo.GetShard(ctx, standby.ShardID)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardActive, afterStandby.State)
	require.Equal(t, standbySrv.URL, afterStandby.PrimaryURL)
}

func TestRegisterShardAssignsNextShardID(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	a, created, err := repo.RegisterShard(ctx, "22222222-2222-2222-2222-222222222222", coordinator.ShardStandby, "http://s-a:8080", nil)
	require.NoError(t, err)
	require.True(t, created)

	b, created, err := repo.RegisterShard(ctx, "33333333-3333-3333-3333-333333333333", coordinator.ShardStandby, "http://s-b:8080", nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, a.ShardID+1, b.ShardID)
}

func TestPatchShardStatePromotesStandbyAndSealsPreviousActive(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	statsSrv := newStatsServer(t)
	defer statsSrv.Close()

	reg := coordinator.NewRegistry(repo, 0, "")
	a, _, err := repo.RegisterShard(ctx, "44444444-4444-4444-4444-444444444444", coordinator.ShardStandby, statsSrv.URL, nil)
	require.NoError(t, err)
	_, err = reg.PatchShardState(ctx, a.ShardID, coordinator.ShardActive, true)
	require.NoError(t, err)

	b, _, err := repo.RegisterShard(ctx, "55555555-5555-5555-5555-555555555555", coordinator.ShardStandby, statsSrv.URL, nil)
	require.NoError(t, err)

	_, err = reg.PatchShardState(ctx, b.ShardID, coordinator.ShardActive, false)
	require.ErrorIs(t, err, coordinator.ErrStateConflict)

	_, err = reg.PatchShardState(ctx, b.ShardID, coordinator.ShardActive, true)
	require.NoError(t, err)

	afterA, err := repo.GetShard(ctx, a.ShardID)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardSealed, afterA.State)

	afterB, err := repo.GetShard(ctx, b.ShardID)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardActive, afterB.State)
}

func TestSealAndRotatePromotesStandby(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	activeSrv := newStatsServer(t)
	defer activeSrv.Close()
	standbySrv := newStatsServer(t)
	defer standbySrv.Close()

	reg := coordinator.NewRegistry(repo, 0, "")
	active, _, err := repo.RegisterShard(ctx, "12121212-1212-1212-1212-121212121212", coordinator.ShardStandby, activeSrv.URL, nil)
	require.NoError(t, err)
	standby, _, err := repo.RegisterShard(ctx, "13131313-1313-1313-1313-131313131313", coordinator.ShardStandby, standbySrv.URL, nil)
	require.NoError(t, err)

	_, err = reg.PatchShardState(ctx, active.ShardID, coordinator.ShardActive, true)
	require.NoError(t, err)
	require.NoError(t, reg.SealAndRotate(ctx))

	afterActive, err := repo.GetShard(ctx, active.ShardID)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardSealed, afterActive.State)

	afterStandby, err := repo.GetShard(ctx, standby.ShardID)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardActive, afterStandby.State)
}

func TestRegisterShardAutoActivatesWhenNoActiveExists(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	statsSrv := newStatsServer(t)
	defer statsSrv.Close()

	reg := coordinator.NewRegistry(repo, 0, "")
	shard, created, err := reg.RegisterShard(ctx, "1a1a1a1a-1a1a-1a1a-1a1a-1a1a1a1a1a1a", coordinator.ShardStandby, statsSrv.URL, nil)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, coordinator.ShardActive, shard.State)

	active, err := repo.ActiveShard(ctx)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Equal(t, shard.ShardID, active.ShardID)
}

func TestRegisterShardSkipsUnreachableStandbyAndActivatesNext(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	reg := coordinator.NewRegistry(repo, 0, "")

	first, _, err := reg.RegisterShard(ctx, "2b2b2b2b-2b2b-2b2b-2b2b-2b2b2b2b2b2b", coordinator.ShardStandby, "http://127.0.0.1:1", nil)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardStandby, first.State)

	statsSrv := newStatsServer(t)
	defer statsSrv.Close()
	second, _, err := reg.RegisterShard(ctx, "3c3c3c3c-3c3c-3c3c-3c3c-3c3c3c3c3c3c", coordinator.ShardStandby, statsSrv.URL, nil)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardActive, second.State)

	active, err := repo.ActiveShard(ctx)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Equal(t, second.ShardID, active.ShardID)

	firstAfter, err := repo.GetShard(ctx, first.ShardID)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardStandby, firstAfter.State)
}

func TestRegisterShardAutoActivatesAfterAllShardsSealed(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	activeSrv := newStatsServer(t)
	defer activeSrv.Close()
	reg := coordinator.NewRegistry(repo, 0, "")
	first, _, err := reg.RegisterShard(ctx, "4d4d4d4d-4d4d-4d4d-4d4d-4d4d4d4d4d4d", coordinator.ShardStandby, activeSrv.URL, nil)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardActive, first.State)

	sealed, err := reg.PatchShardState(ctx, first.ShardID, coordinator.ShardSealed, false)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardSealed, sealed.State)
	active, err := repo.ActiveShard(ctx)
	require.NoError(t, err)
	require.Nil(t, active)

	nextSrv := newStatsServer(t)
	defer nextSrv.Close()
	second, _, err := reg.RegisterShard(ctx, "5e5e5e5e-5e5e-5e5e-5e5e-5e5e5e5e5e5e", coordinator.ShardStandby, nextSrv.URL, nil)
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardActive, second.State)
}

func TestProxyPostReturns503WhenNoActiveAndNoStandby(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	reg := coordinator.NewRegistry(repo, 0, "")
	_, status, err := reg.ProxyPost(ctx, 1, []byte("<?xml version=\"1.0\"?><x/>"), "")
	require.Equal(t, http.StatusServiceUnavailable, status)
	var statusErr *coordinator.StatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, "active_shard_unavailable", statusErr.Code)
}

func TestProxyPostReturns503WhenActiveShardUnavailable(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	statsSrv := newStatsServer(t)
	defer statsSrv.Close()

	reg := coordinator.NewRegistry(repo, 0, "")
	shard, _, err := repo.RegisterShard(ctx, "66666666-6666-6666-6666-666666666666", coordinator.ShardStandby, statsSrv.URL, nil)
	require.NoError(t, err)

	_, err = reg.PatchShardState(ctx, shard.ShardID, coordinator.ShardActive, true)
	require.NoError(t, err)

	// Same UUID -> idempotent update of URL, state remains active.
	_, _, err = repo.RegisterShard(ctx, "66666666-6666-6666-6666-666666666666", coordinator.ShardStandby, "http://127.0.0.1:1", nil)
	require.NoError(t, err)

	_, status, err := reg.ProxyPost(ctx, 1, []byte("<?xml version=\"1.0\"?><x/>"), "")
	require.Equal(t, http.StatusServiceUnavailable, status)
	var statusErr *coordinator.StatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, "active_shard_unavailable", statusErr.Code)
}

func TestCheckSealFailsClosedWhenNoStandby(t *testing.T) {
	ctx, repo := setupCoordinatorRepo(t)
	defer repo.Close()

	statsSrv := newStatsServerWithTotalBytes(t, 10)
	defer statsSrv.Close()

	reg := coordinator.NewRegistry(repo, 1, "")
	shard, _, err := repo.RegisterShard(ctx, "76767676-7676-7676-7676-767676767676", coordinator.ShardStandby, statsSrv.URL, nil)
	require.NoError(t, err)

	require.NoError(t, reg.CheckSeal(ctx))

	after, err := repo.GetShard(ctx, shard.ShardID)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.Equal(t, coordinator.ShardSealed, after.State)

	_, status, err := reg.ProxyPost(ctx, 1, []byte("<?xml version=\"1.0\"?><x/>"), "")
	require.Equal(t, http.StatusServiceUnavailable, status)
	var statusErr *coordinator.StatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, "active_shard_unavailable", statusErr.Code)
}

func setupCoordinatorRepo(t *testing.T) (context.Context, *coordinator.Repository) {
	t.Helper()
	dsn := os.Getenv("COORDINATOR_PG_DSN")
	if dsn == "" {
		dsn = os.Getenv("PG_DSN")
	}
	if dsn == "" {
		t.Skip("COORDINATOR_PG_DSN or PG_DSN not set")
	}

	ctx := context.Background()
	require.NoError(t, coordinator.RunMigrations(ctx, dsn, "../../migrations/coordinator"))

	repo, err := coordinator.NewRepository(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, repo.TruncateAll(ctx))
	return ctx, repo
}

func newStatsServer(t *testing.T) *httptest.Server {
	return newStatsServerWithTotalBytes(t, 0)
}

func newStatsServerWithTotalBytes(t *testing.T, totalBytes int64) *httptest.Server {
	t.Helper()
	sealed := false
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/seal":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			sealed = true
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "sealed"})
			return
		case "/v1/internal/stats":
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"shard_id":    0,
			"role":        "primary",
			"read_only":   sealed,
			"total_bytes": totalBytes,
		})
	}))
}
