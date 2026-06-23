package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/little-big-files/little-big-files/internal/metadata"
)

type BlobStore struct {
	segments *SegmentManager
	encoder  *compress.Encoder
}

func NewBlobStore(segments *SegmentManager, encoder *compress.Encoder) *BlobStore {
	return &BlobStore{segments: segments, encoder: encoder}
}

func ContentHash(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func PackageHash(data []byte) []byte {
	return ContentHash(data)
}

func (b *BlobStore) encodeRecord(data []byte, recordType RecordType) []byte {
	magic := recordType.Magic()
	payload := data
	if b.encoder != nil && recordType == RecordXML && b.encoder.ShouldCompress(len(data)) {
		if compressed, err := b.encoder.Compress(data); err == nil && len(compressed) < len(data) {
			magic = MagicXMLC
			payload = compressed
		}
	}
	return EncodeRecord(magic, payload)
}

// StoreOrRef persists new content or increments ref_count for existing blob.
func (b *BlobStore) StoreOrRef(ctx context.Context, tx metadata.Tx, data []byte, recordType RecordType) ([]byte, error) {
	hash := ContentHash(data)
	existing, err := tx.GetBlob(ctx, hash)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if err := tx.IncrementRefCount(ctx, hash); err != nil {
			return nil, err
		}
		return hash, nil
	}

	record := b.encodeRecord(data, recordType)
	loc, err := b.segments.Append(record)
	if err != nil {
		return nil, err
	}

	blob := metadata.ContentBlob{
		ContentHash: hash,
		Size:        len(data),
		SegmentID:   loc.SegmentID,
		Offset:      loc.Offset,
		RefCount:    1,
		FirstSeenAt: time.Now().UTC(),
	}
	if err := tx.InsertBlob(ctx, blob); err != nil {
		return nil, fmt.Errorf("insert blob: %w", err)
	}
	return hash, nil
}

func (b *BlobStore) ReadBlob(blob metadata.ContentBlob) ([]byte, error) {
	magic, payload, err := b.segments.ReadRecord(blob.SegmentID, blob.Offset)
	if err != nil {
		return nil, err
	}
	if IsCompressedXML(magic) {
		if b.encoder == nil {
			return nil, fmt.Errorf("compressed blob but no encoder configured")
		}
		return b.encoder.Decompress(payload)
	}
	return payload, nil
}
