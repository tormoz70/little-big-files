package coordinator

import (
	"time"
)

type ShardState string

const (
	ShardActive  ShardState = "active"
	ShardSealed  ShardState = "sealed"
	ShardStandby ShardState = "standby"
)

type ShardInfo struct {
	ShardID    int        `json:"shard_id"`
	State      ShardState `json:"state"`
	PrimaryURL string     `json:"primary_url"`
	ReplicaURL *string    `json:"replica_url,omitempty"`
	TotalBytes int64      `json:"total_bytes"`
	SealedAt   *time.Time `json:"sealed_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

type GlobalPackageIndex struct {
	GlobalID    int64
	ShardID     int
	LocalID     int64
	SupplierID  int
	ReceivedAt  time.Time
	PackageHash []byte
}

type BootstrapShard struct {
	ShardID    int    `json:"shard_id"`
	State      string `json:"state"`
	PrimaryURL string `json:"primary_url"`
	ReplicaURL string `json:"replica_url,omitempty"`
}
