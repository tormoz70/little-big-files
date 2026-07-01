package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type RegisterShardStatusError struct {
	StatusCode int
	Body       string
}

func (e *RegisterShardStatusError) Error() string {
	return fmt.Sprintf("register shard failed: status=%d body=%s", e.StatusCode, e.Body)
}

func (e *RegisterShardStatusError) IsRetryable() bool {
	return e.StatusCode == http.StatusRequestTimeout || e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= http.StatusInternalServerError
}

func RegisterShardWithRetry(ctx context.Context, coordinatorURL string, req RegisterShardRequest) (*RegisterShardResponse, error) {
	delay := 500 * time.Millisecond
	for {
		resp, err := RegisterShardOnce(ctx, coordinatorURL, req)
		if err == nil {
			return resp, nil
		}
		if !shouldRetryRegister(err) {
			return nil, err
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
		return nil, &RegisterShardStatusError{
			StatusCode: httpResp.StatusCode,
			Body:       string(body),
		}
	}
	var resp RegisterShardResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func shouldRetryRegister(err error) bool {
	var statusErr *RegisterShardStatusError
	if errors.As(err, &statusErr) {
		return statusErr.IsRetryable()
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return false
}
