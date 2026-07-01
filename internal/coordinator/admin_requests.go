package coordinator

import (
	"fmt"
	"strings"
)

type RegisterShardRequest struct {
	ShardUUID    string  `json:"shard_uuid"`
	ClusterKey   string  `json:"cluster_key"`
	PrimaryURL   string  `json:"primary_url"`
	ReplicaURL   *string `json:"replica_url,omitempty"`
	StartupState string  `json:"startup_state,omitempty"`
}

type RegisterShardResponse struct {
	Shard      ShardInfo `json:"shard"`
	Registered bool      `json:"registered"`
}

type PatchShardStateRequest struct {
	State      string `json:"state"`
	Confirm    bool   `json:"confirm"`
	ClusterKey string `json:"cluster_key"`
}

type SealRotateRequest struct {
	ClusterKey string `json:"cluster_key"`
}

func ParseShardState(raw string) (ShardState, error) {
	state := ShardState(strings.ToLower(strings.TrimSpace(raw)))
	switch state {
	case ShardActive, ShardSealed, ShardStandby:
		return state, nil
	default:
		return "", fmt.Errorf("invalid shard state: %q", raw)
	}
}
