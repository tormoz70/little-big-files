package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

var (
	ErrShardNotFound = errors.New("shard not found")
	ErrStateConflict = errors.New("invalid shard state transition")
)

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
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		sql, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

func (r *Repository) UpsertShard(ctx context.Context, s ShardInfo) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO shard_registry (
			shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (shard_id) DO UPDATE SET
			shard_uuid = COALESCE(EXCLUDED.shard_uuid, shard_registry.shard_uuid),
			state = EXCLUDED.state,
			primary_url = EXCLUDED.primary_url,
			replica_url = EXCLUDED.replica_url,
			total_bytes = EXCLUDED.total_bytes,
			sealed_at = EXCLUDED.sealed_at,
			last_seen_at = EXCLUDED.last_seen_at,
			last_error = EXCLUDED.last_error`,
		s.ShardID,
		s.ShardUUID,
		string(s.State),
		s.PrimaryURL,
		s.ReplicaURL,
		s.TotalBytes,
		s.SealedAt,
		s.LastSeenAt,
		s.LastError,
	)
	return err
}

func (r *Repository) GetShard(ctx context.Context, shardID int) (*ShardInfo, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry WHERE shard_id = $1`, shardID)
	return scanShard(row)
}

func (r *Repository) ActiveShard(ctx context.Context) (*ShardInfo, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry WHERE state = 'active' ORDER BY shard_id LIMIT 1`)
	return scanShard(row)
}

func (r *Repository) StandbyShard(ctx context.Context) (*ShardInfo, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry WHERE state = 'standby' ORDER BY shard_id LIMIT 1`)
	return scanShard(row)
}

func (r *Repository) ListShards(ctx context.Context) ([]ShardInfo, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
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
	var shardUUID *string
	var replica *string
	var lastError *string
	err := row.Scan(
		&s.ShardID,
		&shardUUID,
		&state,
		&s.PrimaryURL,
		&replica,
		&s.TotalBytes,
		&s.SealedAt,
		&s.LastSeenAt,
		&lastError,
		&s.CreatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.State = ShardState(state)
	s.ShardUUID = shardUUID
	s.ReplicaURL = replica
	s.LastError = lastError
	return &s, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanShardRow(rows rowScanner) (*ShardInfo, error) {
	var s ShardInfo
	var state string
	var shardUUID *string
	var replica *string
	var lastError *string
	if err := rows.Scan(
		&s.ShardID,
		&shardUUID,
		&state,
		&s.PrimaryURL,
		&replica,
		&s.TotalBytes,
		&s.SealedAt,
		&s.LastSeenAt,
		&lastError,
		&s.CreatedAt,
	); err != nil {
		return nil, err
	}
	s.State = ShardState(state)
	s.ShardUUID = shardUUID
	s.ReplicaURL = replica
	s.LastError = lastError
	return &s, nil
}

func (r *Repository) SetShardState(ctx context.Context, shardID int, state ShardState, totalBytes int64, sealedAt *time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE shard_registry SET state = $2, total_bytes = $3, sealed_at = $4 WHERE shard_id = $1`,
		shardID, string(state), totalBytes, sealedAt)
	return err
}

type SealedShardTransition struct {
	ShardID    int
	TotalBytes int64
	SealedAt   time.Time
}

func (r *Repository) PromoteStandby(ctx context.Context, targetShardID int, sealedActive *SealedShardTransition) (*ShardInfo, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	target, err := scanShard(tx.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry
		WHERE shard_id = $1
		FOR UPDATE`, targetShardID))
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, ErrShardNotFound
	}
	if target.State != ShardStandby {
		return nil, ErrStateConflict
	}

	currentActive, err := scanShard(tx.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry
		WHERE state = 'active' AND shard_id <> $1
		ORDER BY shard_id
		LIMIT 1
		FOR UPDATE`, targetShardID))
	if err != nil {
		return nil, err
	}

	if sealedActive != nil {
		if currentActive == nil || currentActive.ShardID != sealedActive.ShardID {
			return nil, ErrStateConflict
		}
		if _, err := tx.Exec(ctx, `
			UPDATE shard_registry
			SET state = 'sealed', total_bytes = $2, sealed_at = $3, last_error = NULL
			WHERE shard_id = $1`, sealedActive.ShardID, sealedActive.TotalBytes, sealedActive.SealedAt); err != nil {
			return nil, err
		}
	} else if currentActive != nil {
		return nil, ErrStateConflict
	}

	if _, err := tx.Exec(ctx, `
		UPDATE shard_registry
		SET state = 'active', sealed_at = NULL, last_error = NULL
		WHERE shard_id = $1`, targetShardID); err != nil {
		return nil, err
	}

	updated, err := scanShard(tx.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry WHERE shard_id = $1`, targetShardID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return updated, nil
}

func (r *Repository) GetShardByUUID(ctx context.Context, shardUUID string) (*ShardInfo, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry WHERE shard_uuid = $1`, shardUUID)
	return scanShard(row)
}

func (r *Repository) RegisterShard(ctx context.Context, shardUUID string, state ShardState, primaryURL string, replicaURL *string) (*ShardInfo, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	existing, err := scanShard(tx.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry WHERE shard_uuid = $1 FOR UPDATE`, shardUUID))
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		_, err := tx.Exec(ctx, `
			UPDATE shard_registry
			SET primary_url = $2, replica_url = $3, last_seen_at = NOW(), last_error = NULL
			WHERE shard_uuid = $1`,
			shardUUID, primaryURL, replicaURL)
		if err != nil {
			return nil, false, err
		}
		updated, err := scanShard(tx.QueryRow(ctx, `
			SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
			FROM shard_registry WHERE shard_uuid = $1`, shardUUID))
		if err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return updated, false, nil
	}

	var nextID int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(shard_id), -1) + 1 FROM shard_registry`).Scan(&nextID); err != nil {
		return nil, false, err
	}

	if state == ShardActive {
		alreadyActive, err := scanShard(tx.QueryRow(ctx, `
			SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
			FROM shard_registry
			WHERE state = 'active'
			ORDER BY shard_id
			LIMIT 1
			FOR UPDATE`))
		if err != nil {
			return nil, false, err
		}
		if alreadyActive != nil {
			return nil, false, ErrStateConflict
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO shard_registry (
			shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error
		)
		VALUES ($1, $2, $3, $4, $5, 0, NULL, NOW(), NULL)`,
		nextID, shardUUID, string(state), primaryURL, replicaURL)
	if err != nil {
		return nil, false, err
	}

	created, err := scanShard(tx.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry WHERE shard_id = $1`, nextID))
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return created, true, nil
}

func (r *Repository) PatchShardState(ctx context.Context, shardID int, nextState ShardState, confirm bool) (*ShardInfo, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	target, err := scanShard(tx.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry
		WHERE shard_id = $1
		FOR UPDATE`, shardID))
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, ErrShardNotFound
	}

	switch nextState {
	case ShardSealed:
		if target.State != ShardActive {
			return nil, ErrStateConflict
		}
		now := time.Now().UTC()
		if _, err := tx.Exec(ctx, `
			UPDATE shard_registry
			SET state = 'sealed', sealed_at = $2
			WHERE shard_id = $1`, shardID, &now); err != nil {
			return nil, err
		}
	case ShardActive:
		if target.State != ShardStandby || !confirm {
			return nil, ErrStateConflict
		}
		// Keep single-active invariant: seal previous active, then promote target.
		currentActive, err := scanShard(tx.QueryRow(ctx, `
			SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
			FROM shard_registry
			WHERE state = 'active' AND shard_id <> $1
			ORDER BY shard_id
			LIMIT 1
			FOR UPDATE`, shardID))
		if err != nil {
			return nil, err
		}
		if currentActive != nil {
			now := time.Now().UTC()
			if _, err := tx.Exec(ctx, `
				UPDATE shard_registry
				SET state = 'sealed', sealed_at = $2
				WHERE shard_id = $1`, currentActive.ShardID, &now); err != nil {
				return nil, err
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE shard_registry
			SET state = 'active', sealed_at = NULL, last_error = NULL
			WHERE shard_id = $1`, shardID); err != nil {
			return nil, err
		}
	default:
		return nil, ErrStateConflict
	}

	updated, err := scanShard(tx.QueryRow(ctx, `
		SELECT shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error, created_at
		FROM shard_registry WHERE shard_id = $1`, shardID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return updated, nil
}

func (r *Repository) MarkShardReachable(ctx context.Context, shardID int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE shard_registry
		SET last_seen_at = NOW(), last_error = NULL
		WHERE shard_id = $1`, shardID)
	return err
}

func (r *Repository) MarkShardUnreachable(ctx context.Context, shardID int, lastErr string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE shard_registry
		SET last_error = $2
		WHERE shard_id = $1`, shardID, lastErr)
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

func (r *Repository) TruncateAll(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, `TRUNCATE global_xml_index, global_package_index, shard_registry RESTART IDENTITY CASCADE`)
	return err
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
		var shardUUID *string
		if b.ShardUUID != "" {
			shardUUID = &b.ShardUUID
		}
		var replica *string
		if b.ReplicaURL != "" {
			replica = &b.ReplicaURL
		}
		if err := r.seedShardFromBootstrap(ctx, ShardInfo{
			ShardID:    b.ShardID,
			ShardUUID:  shardUUID,
			State:      ShardState(b.State),
			PrimaryURL: b.PrimaryURL,
			ReplicaURL: replica,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) seedShardFromBootstrap(ctx context.Context, s ShardInfo) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO shard_registry (
			shard_id, shard_uuid, state, primary_url, replica_url, total_bytes, sealed_at, last_seen_at, last_error
		)
		VALUES ($1, $2, $3, $4, $5, 0, NULL, NULL, NULL)
		ON CONFLICT (shard_id) DO UPDATE SET
			shard_uuid = COALESCE(shard_registry.shard_uuid, EXCLUDED.shard_uuid),
			primary_url = CASE
				WHEN NULLIF(BTRIM(shard_registry.primary_url), '') IS NULL THEN EXCLUDED.primary_url
				ELSE shard_registry.primary_url
			END,
			replica_url = COALESCE(shard_registry.replica_url, EXCLUDED.replica_url)`,
		s.ShardID,
		s.ShardUUID,
		string(s.State),
		s.PrimaryURL,
		s.ReplicaURL,
	)
	return err
}
