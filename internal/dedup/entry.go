package dedup

import (
	"encoding/binary"
	"fmt"
)

// Entry mirrors content_blobs location fields for hot-path lookup.
type Entry struct {
	SegmentID int
	Offset    int64
	Size      int
}

func encodeEntry(e Entry) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(e.SegmentID))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(e.Offset))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(e.Size))
	return buf
}

func decodeEntry(data []byte) (Entry, error) {
	if len(data) < 16 {
		return Entry{}, fmt.Errorf("entry value too short")
	}
	return Entry{
		SegmentID: int(binary.LittleEndian.Uint32(data[0:4])),
		Offset:    int64(binary.LittleEndian.Uint64(data[4:12])),
		Size:      int(binary.LittleEndian.Uint32(data[12:16])),
	}, nil
}
