//go:build integration

package coordinator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

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
			"total_bytes": 0,
		})
	}))
}
