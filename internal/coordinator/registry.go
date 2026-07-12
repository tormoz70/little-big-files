package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/little-big-files/little-big-files/internal/globalid"
	"github.com/little-big-files/little-big-files/internal/metrics"
)

type shardStats struct {
	ShardID    int    `json:"shard_id"`
	Role       string `json:"role"`
	ReadOnly   bool   `json:"read_only"`
	TotalBytes int64  `json:"total_bytes"`
}

type Registry struct {
	repo          *Repository
	shardMaxBytes int64
	clusterKey    string
	client        *http.Client
	rotationMu    sync.Mutex
}

type StatusError struct {
	Status int
	Code   string
	Cause  error
}

var ErrRotationIncomplete = errors.New("rotation incomplete")
var ErrNoStandbyShard = errors.New("no standby shard available")

func (e *StatusError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Code
}

func (e *StatusError) Unwrap() error { return e.Cause }

func NewRegistry(repo *Repository, shardMaxBytes int64, clusterKey string) *Registry {
	return &Registry{
		repo:          repo,
		shardMaxBytes: shardMaxBytes,
		clusterKey:    clusterKey,
		client:        &http.Client{Timeout: 60 * time.Second},
	}
}

// setInternalAuth attaches the cluster key required by shard /v1/internal/* endpoints.
func (reg *Registry) setInternalAuth(req *http.Request) {
	if reg.clusterKey != "" {
		req.Header.Set("X-Cluster-Key", reg.clusterKey)
	}
}

func (reg *Registry) ActiveShard(ctx context.Context) (*ShardInfo, error) {
	return reg.repo.ActiveShard(ctx)
}

func (reg *Registry) ShardByID(ctx context.Context, id int) (*ShardInfo, error) {
	return reg.repo.GetShard(ctx, id)
}

func (reg *Registry) RegisterShard(ctx context.Context, shardUUID string, state ShardState, primaryURL string, replicaURL *string) (*ShardInfo, bool, error) {
	shard, created, err := reg.repo.RegisterShard(ctx, shardUUID, state, primaryURL, replicaURL)
	if err != nil {
		return nil, false, err
	}
	if shard != nil {
		_ = reg.repo.MarkShardReachable(ctx, shard.ShardID)
		metrics.SetCoordinatorShardUp(strconv.Itoa(shard.ShardID), string(shard.State), true)
	}
	// Keep startup registration standby-only, but auto-recover write path when active is missing.
	ensured, ensureErr := reg.EnsureActiveShard(ctx)
	if ensureErr != nil {
		slog.Warn("failed to auto-activate standby shard", "err", ensureErr)
	} else if ensured != nil && shard != nil && ensured.ShardID == shard.ShardID {
		shard = ensured
	}
	return shard, created, nil
}

func (reg *Registry) PatchShardState(ctx context.Context, shardID int, nextState ShardState, confirm bool) (*ShardInfo, error) {
	reg.rotationMu.Lock()
	defer reg.rotationMu.Unlock()

	target, err := reg.repo.GetShard(ctx, shardID)
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
		if _, _, err := reg.sealShardAndPersist(ctx, target, "manual_seal"); err != nil {
			return nil, err
		}
		updated, err := reg.repo.GetShard(ctx, shardID)
		if err != nil {
			return nil, err
		}
		if updated == nil {
			return nil, ErrShardNotFound
		}
		return updated, nil
	case ShardActive:
		if target.State != ShardStandby || !confirm {
			return nil, ErrStateConflict
		}
		if _, err := reg.FetchStats(ctx, target.PrimaryURL); err != nil {
			_ = reg.repo.MarkShardUnreachable(ctx, target.ShardID, err.Error())
			metrics.SetCoordinatorShardUp(strconv.Itoa(target.ShardID), string(target.State), false)
			metrics.IncCoordinatorShardFailures(strconv.Itoa(target.ShardID), "manual_promote")
			return nil, fmt.Errorf("%w: target standby shard is unavailable", ErrStateConflict)
		}
		_ = reg.repo.MarkShardReachable(ctx, target.ShardID)
		metrics.SetCoordinatorShardUp(strconv.Itoa(target.ShardID), string(target.State), true)

		var sealed *SealedShardTransition
		active, err := reg.repo.ActiveShard(ctx)
		if err != nil {
			return nil, err
		}
		if active != nil && active.ShardID != target.ShardID {
			total, sealedAt, err := reg.sealShardRemote(ctx, active, "manual_promote")
			if err != nil {
				return nil, err
			}
			sealed = &SealedShardTransition{
				ShardID:    active.ShardID,
				TotalBytes: total,
				SealedAt:   sealedAt,
			}
		}

		updated, err := reg.repo.PromoteStandby(ctx, target.ShardID, sealed)
		if err != nil {
			if sealed != nil {
				metrics.IncCoordinatorShardFailures(strconv.Itoa(sealed.ShardID), "manual_promote_commit")
				return nil, fmt.Errorf("%w: %w", ErrRotationIncomplete, err)
			}
			return nil, err
		}
		if sealed != nil {
			metrics.SetCoordinatorShardUp(strconv.Itoa(sealed.ShardID), string(ShardSealed), true)
		}
		_ = reg.repo.MarkShardReachable(ctx, updated.ShardID)
		metrics.SetCoordinatorShardUp(strconv.Itoa(updated.ShardID), string(updated.State), true)
		return updated, nil
	default:
		return nil, ErrStateConflict
	}
}

// ReadURL picks the shard HTTP base for package reads.
// Sealed shards use replica_url when configured; otherwise primary serves reads (including sealed).
func (reg *Registry) ReadURL(shard *ShardInfo) string {
	if shard.State == ShardSealed && shard.ReplicaURL != nil && *shard.ReplicaURL != "" {
		return *shard.ReplicaURL
	}
	return shard.PrimaryURL
}

func (reg *Registry) FetchStats(ctx context.Context, baseURL string) (*shardStats, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/internal/stats", nil)
	if err != nil {
		return nil, err
	}
	reg.setInternalAuth(req)
	resp, err := reg.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("stats %s: %s", resp.Status, string(body))
	}
	var st shardStats
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (reg *Registry) SealShard(ctx context.Context, shard *ShardInfo) error {
	reg.rotationMu.Lock()
	defer reg.rotationMu.Unlock()
	_, _, err := reg.sealShardAndPersist(ctx, shard, "seal")
	return err
}

func (reg *Registry) sealShardRemote(ctx context.Context, shard *ShardInfo, op string) (int64, time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, shard.PrimaryURL+"/v1/internal/seal", nil)
	if err != nil {
		return 0, time.Time{}, err
	}
	reg.setInternalAuth(req)
	resp, err := reg.client.Do(req)
	if err != nil {
		_ = reg.repo.MarkShardUnreachable(ctx, shard.ShardID, err.Error())
		metrics.SetCoordinatorShardUp(strconv.Itoa(shard.ShardID), string(shard.State), false)
		metrics.IncCoordinatorShardFailures(strconv.Itoa(shard.ShardID), op)
		return 0, time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		msg := fmt.Sprintf("seal %s: %s", resp.Status, string(body))
		_ = reg.repo.MarkShardUnreachable(ctx, shard.ShardID, msg)
		metrics.SetCoordinatorShardUp(strconv.Itoa(shard.ShardID), string(shard.State), false)
		metrics.IncCoordinatorShardFailures(strconv.Itoa(shard.ShardID), op)
		return 0, time.Time{}, errors.New(msg)
	}
	now := time.Now().UTC()
	st, _ := reg.FetchStats(ctx, shard.PrimaryURL)
	var total int64
	if st != nil {
		total = st.TotalBytes
	}
	_ = reg.repo.MarkShardReachable(ctx, shard.ShardID)
	metrics.SetCoordinatorShardUp(strconv.Itoa(shard.ShardID), string(shard.State), true)
	return total, now, nil
}

func (reg *Registry) sealShardAndPersist(ctx context.Context, shard *ShardInfo, op string) (int64, time.Time, error) {
	total, sealedAt, err := reg.sealShardRemote(ctx, shard, op)
	if err != nil {
		return 0, time.Time{}, err
	}
	if err := reg.repo.SetShardState(ctx, shard.ShardID, ShardSealed, total, &sealedAt); err != nil {
		metrics.IncCoordinatorShardFailures(strconv.Itoa(shard.ShardID), op+"_persist")
		return 0, time.Time{}, err
	}
	metrics.SetCoordinatorShardUp(strconv.Itoa(shard.ShardID), string(ShardSealed), true)
	return total, sealedAt, nil
}

func (reg *Registry) EnsureActiveShard(ctx context.Context) (*ShardInfo, error) {
	reg.rotationMu.Lock()
	defer reg.rotationMu.Unlock()

	return reg.ensureActiveShardLocked(ctx)
}

func (reg *Registry) ensureActiveShardLocked(ctx context.Context) (*ShardInfo, error) {
	active, err := reg.repo.ActiveShard(ctx)
	if err != nil {
		return nil, err
	}
	if active != nil {
		return active, nil
	}

	standbys, err := reg.repo.StandbyShards(ctx)
	if err != nil {
		return nil, err
	}
	if len(standbys) == 0 {
		return nil, fmt.Errorf("no standby shard available")
	}

	var lastErr error
	for _, standby := range standbys {
		if _, err := reg.FetchStats(ctx, standby.PrimaryURL); err != nil {
			_ = reg.repo.MarkShardUnreachable(ctx, standby.ShardID, err.Error())
			metrics.SetCoordinatorShardUp(strconv.Itoa(standby.ShardID), string(standby.State), false)
			metrics.IncCoordinatorShardFailures(strconv.Itoa(standby.ShardID), "ensure_active")
			lastErr = err
			continue
		}
		_ = reg.repo.MarkShardReachable(ctx, standby.ShardID)
		metrics.SetCoordinatorShardUp(strconv.Itoa(standby.ShardID), string(standby.State), true)

		updated, err := reg.repo.PromoteStandby(ctx, standby.ShardID, nil)
		if err != nil {
			metrics.IncCoordinatorShardFailures(strconv.Itoa(standby.ShardID), "ensure_active_promote")
			lastErr = err
			if errors.Is(err, ErrStateConflict) {
				active, activeErr := reg.repo.ActiveShard(ctx)
				if activeErr != nil {
					return nil, activeErr
				}
				if active != nil {
					return active, nil
				}
			}
			continue
		}
		_ = reg.repo.MarkShardReachable(ctx, updated.ShardID)
		metrics.SetCoordinatorShardUp(strconv.Itoa(updated.ShardID), string(updated.State), true)
		slog.Info("auto-activated standby shard", "shard_id", updated.ShardID)
		return updated, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("no reachable standby shard available: %w", lastErr)
	}
	return nil, fmt.Errorf("no reachable standby shard available")
}

func (reg *Registry) ActivateStandby(ctx context.Context) (*ShardInfo, error) {
	return reg.EnsureActiveShard(ctx)
}

func (reg *Registry) SealAndRotate(ctx context.Context) error {
	reg.rotationMu.Lock()
	defer reg.rotationMu.Unlock()

	return reg.sealAndRotateLocked(ctx)
}

func (reg *Registry) sealAndRotateLocked(ctx context.Context) error {
	active, err := reg.repo.ActiveShard(ctx)
	if err != nil {
		return err
	}
	if active == nil {
		return fmt.Errorf("no active shard")
	}
	standby, err := reg.repo.StandbyShard(ctx)
	if err != nil {
		return err
	}
	if standby == nil {
		return ErrNoStandbyShard
	}
	if _, err := reg.FetchStats(ctx, standby.PrimaryURL); err != nil {
		_ = reg.repo.MarkShardUnreachable(ctx, standby.ShardID, err.Error())
		metrics.SetCoordinatorShardUp(strconv.Itoa(standby.ShardID), string(standby.State), false)
		metrics.IncCoordinatorShardFailures(strconv.Itoa(standby.ShardID), "rotate")
		return fmt.Errorf("standby shard %d is unavailable: %w", standby.ShardID, err)
	}
	_ = reg.repo.MarkShardReachable(ctx, standby.ShardID)
	metrics.SetCoordinatorShardUp(strconv.Itoa(standby.ShardID), string(standby.State), true)

	slog.Info("sealing active shard", "shard_id", active.ShardID)
	total, sealedAt, err := reg.sealShardRemote(ctx, active, "rotate")
	if err != nil {
		return err
	}
	next, err := reg.repo.PromoteStandby(ctx, standby.ShardID, &SealedShardTransition{
		ShardID:    active.ShardID,
		TotalBytes: total,
		SealedAt:   sealedAt,
	})
	if err != nil {
		metrics.IncCoordinatorShardFailures(strconv.Itoa(active.ShardID), "rotate_commit")
		return fmt.Errorf("%w: %w", ErrRotationIncomplete, err)
	}
	_ = reg.repo.MarkShardReachable(ctx, active.ShardID)
	metrics.SetCoordinatorShardUp(strconv.Itoa(active.ShardID), string(ShardSealed), true)
	_ = reg.repo.MarkShardReachable(ctx, next.ShardID)
	metrics.SetCoordinatorShardUp(strconv.Itoa(next.ShardID), string(next.State), true)
	slog.Info("activated standby shard", "shard_id", next.ShardID)
	return nil
}

func (reg *Registry) CheckSeal(ctx context.Context) error {
	active, err := reg.repo.ActiveShard(ctx)
	if err != nil {
		return err
	}
	if active == nil {
		ensured, err := reg.EnsureActiveShard(ctx)
		if err != nil || ensured == nil {
			return nil
		}
		active = ensured
	}
	st, err := reg.FetchStats(ctx, active.PrimaryURL)
	if err != nil {
		_ = reg.repo.MarkShardUnreachable(ctx, active.ShardID, err.Error())
		metrics.SetCoordinatorShardUp(strconv.Itoa(active.ShardID), string(active.State), false)
		metrics.IncCoordinatorShardFailures(strconv.Itoa(active.ShardID), "check")
		return err
	}
	_ = reg.repo.MarkShardReachable(ctx, active.ShardID)
	metrics.SetCoordinatorShardUp(strconv.Itoa(active.ShardID), string(active.State), true)
	_ = reg.repo.SetShardState(ctx, active.ShardID, ShardActive, st.TotalBytes, nil)
	if reg.shardMaxBytes > 0 && st.TotalBytes >= reg.shardMaxBytes {
		if err := reg.SealAndRotate(ctx); err != nil {
			// Fail-closed behavior: if there is no standby to rotate into,
			// seal the current active shard so writes stop with 503 until a new standby appears.
			if errors.Is(err, ErrNoStandbyShard) {
				if _, sealErr := reg.PatchShardState(ctx, active.ShardID, ShardSealed, false); sealErr != nil {
					return sealErr
				}
				slog.Warn("sealed active shard without standby; coordinator is fail-closed", "shard_id", active.ShardID)
				return nil
			}
			return err
		}
	}
	return nil
}

// ProxyPost forwards package ingest to active shard and registers global index.
func (reg *Registry) ProxyPost(ctx context.Context, supplierID int, body []byte, filename string) ([]byte, int, error) {
	active, err := reg.repo.ActiveShard(ctx)
	if err != nil {
		return nil, 0, err
	}
	var noActiveCause error
	if active == nil {
		ensured, ensureErr := reg.EnsureActiveShard(ctx)
		if ensureErr != nil {
			noActiveCause = ensureErr
		}
		if ensured != nil {
			active = ensured
		}
	}
	if active == nil {
		if noActiveCause == nil {
			noActiveCause = fmt.Errorf("no active shard")
		}
		return nil, http.StatusServiceUnavailable, &StatusError{
			Status: http.StatusServiceUnavailable,
			Code:   "active_shard_unavailable",
			Cause:  noActiveCause,
		}
	}
	targetURL, err := url.Parse(active.PrimaryURL + "/v1/packages")
	if err != nil {
		return nil, 0, err
	}
	query := targetURL.Query()
	query.Set("supplier_id", strconv.Itoa(supplierID))
	if filename != "" {
		query.Set("filename", filename)
	}
	targetURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	resp, err := reg.client.Do(req)
	if err != nil {
		_ = reg.repo.MarkShardUnreachable(ctx, active.ShardID, err.Error())
		metrics.SetCoordinatorShardUp(strconv.Itoa(active.ShardID), string(active.State), false)
		metrics.IncCoordinatorShardFailures(strconv.Itoa(active.ShardID), "write")
		return nil, http.StatusServiceUnavailable, &StatusError{
			Status: http.StatusServiceUnavailable,
			Code:   "active_shard_unavailable",
			Cause:  err,
		}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusCreated {
		if resp.StatusCode == http.StatusInsufficientStorage {
			_ = reg.repo.MarkShardReachable(ctx, active.ShardID)
			metrics.SetCoordinatorShardUp(strconv.Itoa(active.ShardID), string(active.State), true)
			return nil, http.StatusInsufficientStorage, &StatusError{
				Status: http.StatusInsufficientStorage,
				Code:   "insufficient_storage",
				Cause:  errors.New("active shard reported insufficient storage"),
			}
		}
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode >= http.StatusInternalServerError {
			msg := fmt.Sprintf("active shard returned status %d", resp.StatusCode)
			_ = reg.repo.MarkShardUnreachable(ctx, active.ShardID, msg)
			metrics.SetCoordinatorShardUp(strconv.Itoa(active.ShardID), string(active.State), false)
			metrics.IncCoordinatorShardFailures(strconv.Itoa(active.ShardID), "write_http")
			return nil, http.StatusServiceUnavailable, &StatusError{
				Status: http.StatusServiceUnavailable,
				Code:   "active_shard_unavailable",
				Cause:  errors.New(msg),
			}
		}
		_ = reg.repo.MarkShardReachable(ctx, active.ShardID)
		metrics.SetCoordinatorShardUp(strconv.Itoa(active.ShardID), string(active.State), true)
		return data, resp.StatusCode, nil
	}
	var pkg map[string]any
	if err := json.Unmarshal(data, &pkg); err != nil {
		return nil, http.StatusBadGateway, &StatusError{
			Status: http.StatusBadGateway,
			Code:   "invalid_shard_response",
			Cause:  err,
		}
	}
	localID, ok := asJSONInt64(pkg["package_id"])
	if !ok {
		return nil, http.StatusBadGateway, &StatusError{
			Status: http.StatusBadGateway,
			Code:   "invalid_shard_response",
			Cause:  fmt.Errorf("missing package_id in shard response"),
		}
	}
	globalID := globalid.Encode(active.ShardID, localID)
	pkg["package_id"] = globalID
	if links, ok := pkg["links"].(map[string]any); ok {
		links["self"] = fmt.Sprintf("/v1/packages/%d", globalID)
		links["original"] = fmt.Sprintf("/v1/packages/%d/original", globalID)
	}
	if files, ok := pkg["files"].([]any); ok {
		for _, f := range files {
			if m, ok := f.(map[string]any); ok {
				if fid, ok := asJSONInt64(m["file_id"]); ok {
					m["download_url"] = fmt.Sprintf("/v1/packages/%d/files/%d", globalID, fid)
				}
			}
		}
	}
	out, err := json.Marshal(pkg)
	if err != nil {
		return data, resp.StatusCode, err
	}
	pkgHash := hashPackage(body)
	supplier64, ok := asJSONInt64(pkg["supplier_id"])
	if !ok {
		return nil, http.StatusBadGateway, &StatusError{
			Status: http.StatusBadGateway,
			Code:   "invalid_shard_response",
			Cause:  fmt.Errorf("missing supplier_id in shard response"),
		}
	}
	supplier := int(supplier64)
	received := time.Now().UTC()
	if ts, ok := pkg["received_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, ts); err == nil {
			received = t
		}
	}
	if err := reg.repo.InsertGlobalPackage(ctx, GlobalPackageIndex{
		GlobalID:    globalID,
		ShardID:     active.ShardID,
		LocalID:     localID,
		SupplierID:  supplier,
		ReceivedAt:  received,
		PackageHash: pkgHash,
	}); err != nil {
		metrics.IncCoordinatorShardFailures(strconv.Itoa(active.ShardID), "global_index")
		return nil, http.StatusInternalServerError, &StatusError{
			Status: http.StatusInternalServerError,
			Code:   "global_index_write_failed",
			Cause:  err,
		}
	}
	_ = reg.repo.MarkShardReachable(ctx, active.ShardID)
	metrics.SetCoordinatorShardUp(strconv.Itoa(active.ShardID), string(active.State), true)
	reg.indexXMLFromPackage(ctx, active.ShardID, active.PrimaryURL, localID)
	return out, http.StatusCreated, nil
}

func asJSONInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

func (reg *Registry) indexXMLFromPackage(ctx context.Context, shardID int, baseURL string, localID int64) {
	// Global XML index is intentionally out of scope for the current MVP.
	// Coordinator keeps per-package global routing only.
	_ = ctx
	_ = shardID
	_ = baseURL
	_ = localID
}

func (reg *Registry) ProxyGet(ctx context.Context, globalID int64, pathSuffix string) ([]byte, int, string, error) {
	shardID, localID := globalid.Decode(globalID)
	shard, err := reg.repo.GetShard(ctx, shardID)
	if err != nil || shard == nil {
		return nil, http.StatusNotFound, "application/json", &StatusError{
			Status: http.StatusNotFound,
			Code:   "shard_not_found",
			Cause:  fmt.Errorf("shard not found"),
		}
	}
	base := reg.ReadURL(shard)
	url := fmt.Sprintf("%s/v1/packages/%d%s", base, localID, pathSuffix)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, "", err
	}
	resp, err := reg.client.Do(req)
	if err != nil {
		_ = reg.repo.MarkShardUnreachable(ctx, shardID, err.Error())
		metrics.SetCoordinatorShardUp(strconv.Itoa(shardID), string(shard.State), false)
		metrics.IncCoordinatorShardFailures(strconv.Itoa(shardID), "read")
		return nil, http.StatusServiceUnavailable, "application/json", &StatusError{
			Status: http.StatusServiceUnavailable,
			Code:   "shard_unavailable",
			Cause:  err,
		}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, "", err
	}
	ct := resp.Header.Get("Content-Type")
	if resp.StatusCode == http.StatusOK && ct == "application/json" {
		var pkg map[string]any
		if err := json.Unmarshal(data, &pkg); err == nil {
			pkg["package_id"] = globalID
			if links, ok := pkg["links"].(map[string]any); ok {
				links["self"] = fmt.Sprintf("/v1/packages/%d", globalID)
				links["original"] = fmt.Sprintf("/v1/packages/%d/original", globalID)
			}
			if files, ok := pkg["files"].([]any); ok {
				for _, f := range files {
					if m, ok := f.(map[string]any); ok {
						if fid, ok := m["file_id"].(float64); ok {
							m["download_url"] = fmt.Sprintf("/v1/packages/%d/files/%d", globalID, int64(fid))
						}
					}
				}
			}
			data, _ = json.Marshal(pkg)
		}
	}
	_ = reg.repo.MarkShardReachable(ctx, shardID)
	metrics.SetCoordinatorShardUp(strconv.Itoa(shardID), string(shard.State), true)
	return data, resp.StatusCode, ct, nil
}

func hashPackage(body []byte) []byte {
	h := sha256Sum(body)
	return h[:]
}

// ShardSnapshots implements metrics.RegistryReader.
func (reg *Registry) ShardSnapshots(ctx context.Context) ([]metrics.ShardSnapshot, error) {
	shards, err := reg.repo.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]metrics.ShardSnapshot, 0, len(shards))
	for _, s := range shards {
		out = append(out, metrics.ShardSnapshot{
			ShardID:    s.ShardID,
			State:      string(s.State),
			TotalBytes: s.TotalBytes,
		})
	}
	return out, nil
}
