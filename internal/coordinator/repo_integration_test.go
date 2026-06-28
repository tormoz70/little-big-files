//go:build integration

package coordinator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
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
	return ctx, repo
}

func newStatsServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/internal/stats" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"shard_id":    0,
			"role":        "primary",
			"read_only":   false,
			"total_bytes": 0,
		})
	}))
}
