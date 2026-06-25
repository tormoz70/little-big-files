package globalid

const ShardShift = 48

const localIDMask uint64 = (1 << ShardShift) - 1

// Encode builds a global package_id: [16 bit shard_id][48 bit local_id].
func Encode(shardID int, localID int64) int64 {
	return int64((uint64(shardID) << ShardShift) | (uint64(localID) & localIDMask))
}

// Decode splits a global package_id into shard and local parts.
func Decode(globalID int64) (shardID int, localID int64) {
	u := uint64(globalID)
	shardID = int(u >> ShardShift)
	localID = int64(u & localIDMask)
	return shardID, localID
}
