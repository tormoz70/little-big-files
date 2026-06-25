package globalid_test

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/globalid"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	shardID := 3
	localID := int64(123456789)
	global := globalid.Encode(shardID, localID)
	gotShard, gotLocal := globalid.Decode(global)
	require.Equal(t, shardID, gotShard)
	require.Equal(t, localID, gotLocal)
}

func TestDecodeLargeShardID(t *testing.T) {
	global := globalid.Encode(65535, 1)
	shard, local := globalid.Decode(global)
	require.Equal(t, 65535, shard)
	require.Equal(t, int64(1), local)
}
