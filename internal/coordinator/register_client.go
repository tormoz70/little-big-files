package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func RegisterShardWithRetry(ctx context.Context, coordinatorURL string, req RegisterShardRequest) (*RegisterShardResponse, error) {
	delay := 500 * time.Millisecond
	for {
		resp, err := RegisterShardOnce(ctx, coordinatorURL, req)
		if err == nil {
			return resp, nil
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

		if delay < 15*time.Second {
			delay *= 2
			if delay > 15*time.Second {
				delay = 15 * time.Second
			}
		}
	}
}

func RegisterShardOnce(ctx context.Context, coordinatorURL string, req RegisterShardRequest) (*RegisterShardResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(coordinatorURL, "/") + "/v1/admin/shards"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 15 * time.Second}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}
	if httpResp.StatusCode != http.StatusOK && httpResp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("register shard failed: status=%d body=%s", httpResp.StatusCode, string(body))
	}
	var resp RegisterShardResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
