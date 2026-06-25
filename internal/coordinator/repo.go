package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(ctx context.Context, dsn string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() { r.pool.Close() }

func RunMigrations(ctx context.Context, dsn, dir string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		sql, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (r *Repository) UpsertShard(ctx context.Context, s ShardInfo) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO shard_registry (shard_id, state, primary_url, replica_url, total_bytes, sealed_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (shard_id) DO UPDATE SET
			state = EXCLUDED.state,
			primary_url = EXCLUDED.primary_url,
			replica_url = EXCLUDED.replica_url,
			total_bytes = EXCLUDED.total_bytes,
			sealed_at = EXCLUDED.sealed_at`,
		s.ShardID, string(s.State), s.PrimaryURL, s.ReplicaURL, s.TotalBytes, s.SealedAt)
	return err
}

func (r *Repository) GetShard(ctx context.Context, shardID int) (*ShardInfo, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT shard_id, state, primary_url, replica_url, total_bytes, sealed_at, created_at
		FROM shard_registry WHERE shard_id = $1`, shardID)
	return scanShard(row)
}

func (r *Repository) ActiveShard(ctx context.Context) (*ShardInfo, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT shard_id, state, primary_url, replica_url, total_bytes, sealed_at, created_at
		FROM shard_registry WHERE state = 'active' ORDER BY shard_id LIMIT 1`)
	return scanShard(row)
}

func (r *Repository) StandbyShard(ctx context.Context) (*ShardInfo, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT shard_id, state, primary_url, replica_url, total_bytes, sealed_at, created_at
		FROM shard_registry WHERE state = 'standby' ORDER BY shard_id LIMIT 1`)
	return scanShard(row)
}

func (r *Repository) ListShards(ctx context.Context) ([]ShardInfo, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT shard_id, state, primary_url, replica_url, total_bytes, sealed_at, created_at
		FROM shard_registry ORDER BY shard_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShardInfo
	for rows.Next() {
		s, err := scanShardRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

func scanShard(row pgx.Row) (*ShardInfo, error) {
	var s ShardInfo
	var state string
	var replica *string
	err := row.Scan(&s.ShardID, &state, &s.PrimaryURL, &replica, &s.TotalBytes, &s.SealedAt, &s.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.State = ShardState(state)
	s.ReplicaURL = replica
	return &s, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanShardRow(rows rowScanner) (*ShardInfo, error) {
	var s ShardInfo
	var state string
	var replica *string
	if err := rows.Scan(&s.ShardID, &state, &s.PrimaryURL, &replica, &s.TotalBytes, &s.SealedAt, &s.CreatedAt); err != nil {
		return nil, err
	}
	s.State = ShardState(state)
	s.ReplicaURL = replica
	return &s, nil
}

func (r *Repository) SetShardState(ctx context.Context, shardID int, state ShardState, totalBytes int64, sealedAt *time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE shard_registry SET state = $2, total_bytes = $3, sealed_at = $4 WHERE shard_id = $1`,
		shardID, string(state), totalBytes, sealedAt)
	return err
}

func (r *Repository) InsertGlobalPackage(ctx context.Context, e GlobalPackageIndex) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO global_package_index (global_id, shard_id, local_id, supplier_id, received_at, package_hash)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (global_id) DO NOTHING`,
		e.GlobalID, e.ShardID, e.LocalID, e.SupplierID, e.ReceivedAt, e.PackageHash)
	return err
}

func (r *Repository) InsertGlobalXMLHashes(ctx context.Context, shardID int, hashes [][]byte) error {
	for _, h := range hashes {
		_, err := r.pool.Exec(ctx, `
			INSERT INTO global_xml_index (content_hash, shard_id) VALUES ($1, $2)
			ON CONFLICT (content_hash) DO NOTHING`, h, shardID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) FindShardByXMLHash(ctx context.Context, hash []byte) (*ShardInfo, error) {
	var shardID int
	err := r.pool.QueryRow(ctx, `SELECT shard_id FROM global_xml_index WHERE content_hash = $1`, hash).Scan(&shardID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return r.GetShard(ctx, shardID)
}

// BootstrapFromFile loads shard registry entries from JSON.
func (r *Repository) BootstrapFromFile(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var shards []BootstrapShard
	if err := json.Unmarshal(data, &shards); err != nil {
		return err
	}
	for _, b := range shards {
		var replica *string
		if b.ReplicaURL != "" {
			replica = &b.ReplicaURL
		}
		if err := r.UpsertShard(ctx, ShardInfo{
			ShardID:    b.ShardID,
			State:      ShardState(b.State),
			PrimaryURL: b.PrimaryURL,
			ReplicaURL: replica,
		}); err != nil {
			return err
		}
	}
	return nil
}
