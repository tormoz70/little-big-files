package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
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
	client        *http.Client
}

func NewRegistry(repo *Repository, shardMaxBytes int64) *Registry {
	return &Registry{
		repo:          repo,
		shardMaxBytes: shardMaxBytes,
		client:        &http.Client{Timeout: 60 * time.Second},
	}
}

func (reg *Registry) ActiveShard(ctx context.Context) (*ShardInfo, error) {
	return reg.repo.ActiveShard(ctx)
}

func (reg *Registry) ShardByID(ctx context.Context, id int) (*ShardInfo, error) {
	return reg.repo.GetShard(ctx, id)
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, shard.PrimaryURL+"/v1/internal/seal", nil)
	if err != nil {
		return err
	}
	resp, err := reg.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("seal %s: %s", resp.Status, string(body))
	}
	now := time.Now().UTC()
	st, _ := reg.FetchStats(ctx, shard.PrimaryURL)
	var total int64
	if st != nil {
		total = st.TotalBytes
	}
	return reg.repo.SetShardState(ctx, shard.ShardID, ShardSealed, total, &now)
}

func (reg *Registry) ActivateStandby(ctx context.Context) (*ShardInfo, error) {
	standby, err := reg.repo.StandbyShard(ctx)
	if err != nil {
		return nil, err
	}
	if standby == nil {
		return nil, fmt.Errorf("no standby shard available")
	}
	if err := reg.repo.SetShardState(ctx, standby.ShardID, ShardActive, 0, nil); err != nil {
		return nil, err
	}
	return reg.repo.GetShard(ctx, standby.ShardID)
}

func (reg *Registry) SealAndRotate(ctx context.Context) error {
	active, err := reg.repo.ActiveShard(ctx)
	if err != nil {
		return err
	}
	if active == nil {
		return fmt.Errorf("no active shard")
	}
	slog.Info("sealing active shard", "shard_id", active.ShardID)
	if err := reg.SealShard(ctx, active); err != nil {
		return err
	}
	next, err := reg.ActivateStandby(ctx)
	if err != nil {
		return err
	}
	slog.Info("activated standby shard", "shard_id", next.ShardID)
	return nil
}

func (reg *Registry) CheckSeal(ctx context.Context) error {
	active, err := reg.repo.ActiveShard(ctx)
	if err != nil || active == nil {
		return err
	}
	st, err := reg.FetchStats(ctx, active.PrimaryURL)
	if err != nil {
		return err
	}
	_ = reg.repo.SetShardState(ctx, active.ShardID, ShardActive, st.TotalBytes, nil)
	if reg.shardMaxBytes > 0 && st.TotalBytes >= reg.shardMaxBytes {
		return reg.SealAndRotate(ctx)
	}
	return nil
}

// ProxyPost forwards package ingest to active shard and registers global index.
func (reg *Registry) ProxyPost(ctx context.Context, supplierID int, body []byte, filename string) ([]byte, int, error) {
	active, err := reg.repo.ActiveShard(ctx)
	if err != nil {
		return nil, 0, err
	}
	if active == nil {
		return nil, 0, fmt.Errorf("no active shard")
	}
	url := active.PrimaryURL + "/v1/packages?supplier_id=" + strconv.Itoa(supplierID)
	if filename != "" {
		url += "&filename=" + filename
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	resp, err := reg.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusCreated {
		return data, resp.StatusCode, nil
	}
	var pkg map[string]any
	if err := json.Unmarshal(data, &pkg); err != nil {
		return data, resp.StatusCode, err
	}
	localID, ok := pkg["package_id"].(float64)
	if !ok {
		return data, resp.StatusCode, fmt.Errorf("missing package_id in shard response")
	}
	globalID := globalid.Encode(active.ShardID, int64(localID))
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
	out, err := json.Marshal(pkg)
	if err != nil {
		return data, resp.StatusCode, err
	}
	pkgHash := hashPackage(body)
	supplier := int(pkg["supplier_id"].(float64))
	received := time.Now().UTC()
	if ts, ok := pkg["received_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, ts); err == nil {
			received = t
		}
	}
	_ = reg.repo.InsertGlobalPackage(ctx, GlobalPackageIndex{
		GlobalID:    globalID,
		ShardID:     active.ShardID,
		LocalID:     int64(localID),
		SupplierID:  supplier,
		ReceivedAt:  received,
		PackageHash: pkgHash,
	})
	reg.indexXMLFromPackage(ctx, active.ShardID, active.PrimaryURL, int64(localID))
	return out, http.StatusCreated, nil
}

func (reg *Registry) indexXMLFromPackage(ctx context.Context, shardID int, baseURL string, localID int64) {
	url := fmt.Sprintf("%s/v1/packages/%d", baseURL, localID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := reg.client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	defer resp.Body.Close()
	var pkg map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&pkg); err != nil {
		return
	}
	// Blob hashes are internal; index skipped unless shard exposes them later.
	_ = shardID
}

func (reg *Registry) ProxyGet(ctx context.Context, globalID int64, pathSuffix string) ([]byte, int, string, error) {
	shardID, localID := globalid.Decode(globalID)
	shard, err := reg.repo.GetShard(ctx, shardID)
	if err != nil || shard == nil {
		return nil, http.StatusNotFound, "application/json", fmt.Errorf("shard not found")
	}
	base := reg.ReadURL(shard)
	url := fmt.Sprintf("%s/v1/packages/%d%s", base, localID, pathSuffix)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, "", err
	}
	resp, err := reg.client.Do(req)
	if err != nil {
		return nil, 0, "", err
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
