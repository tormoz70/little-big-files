package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/metadata"
)

type BlobStore struct {
	segments     *SegmentManager
	segmentIndex *SegmentIndex
	encoder      *compress.Encoder
	index        *dedup.HotIndex
}

func NewBlobStore(segments *SegmentManager, segmentIndex *SegmentIndex, encoder *compress.Encoder, index *dedup.HotIndex) *BlobStore {
	return &BlobStore{segments: segments, segmentIndex: segmentIndex, encoder: encoder, index: index}
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

// StoreOrRef persists new content or increments ref_count for an existing blob.
// The second return value is true when a new blob was written.
func (b *BlobStore) StoreOrRef(ctx context.Context, tx metadata.Tx, data []byte, recordType RecordType, supplierID int) ([]byte, bool, error) {
	hash := ContentHash(data)

	if b.index != nil && !b.index.MightContain(hash) {
		return b.storeNew(ctx, tx, hash, data, recordType, supplierID)
	}

	if b.index != nil {
		if _, ok := b.index.Lookup(hash); ok {
			if updated, err := tx.IncrementRefCountIfExists(ctx, hash); err != nil {
				return nil, false, err
			} else if updated {
				return hash, false, nil
			}
		}
	}

	existing, err := tx.GetBlob(ctx, hash)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		if err := tx.IncrementRefCount(ctx, hash); err != nil {
			return nil, false, err
		}
		_ = b.indexPut(hash, *existing)
		return hash, false, nil
	}

	return b.storeNew(ctx, tx, hash, data, recordType, supplierID)
}

func (b *BlobStore) storeNew(ctx context.Context, tx metadata.Tx, hash, data []byte, recordType RecordType, supplierID int) ([]byte, bool, error) {
	record := b.encodeRecord(data, recordType)
	loc, err := b.segments.Append(record)
	if err != nil {
		return nil, false, err
	}

	magic, _, _ := DecodeRecordHeader(record)
	var dictID uint32
	if b.encoder != nil && IsCompressedXML(magic) {
		dictID = uint32(b.encoder.DictID())
	}
	if b.segmentIndex != nil {
		var h [32]byte
		copy(h[:], hash)
		_ = b.segmentIndex.Append(loc.SegmentID, IndexEntry{
			Offset:      loc.Offset,
			StoredSize:  uint32(len(record)),
			LogicalSize: uint32(len(data)),
			Magic:       magic,
			Hash:        h,
			SupplierID:  uint32(supplierID),
			DictID:      dictID,
		})
	}

	blob := metadata.ContentBlob{
		ContentHash: hash,
		Size:        len(data),
		StoredSize:  len(record),
		SegmentID:   loc.SegmentID,
		Offset:      loc.Offset,
		RefCount:    1,
		FirstSeenAt: time.Now().UTC(),
	}
	if err := tx.InsertBlob(ctx, blob); err != nil {
		return nil, false, fmt.Errorf("insert blob: %w", err)
	}
	if err := b.indexPut(hash, blob); err != nil {
		return nil, false, err
	}
	return hash, true, nil
}

func (b *BlobStore) indexPut(hash []byte, blob metadata.ContentBlob) error {
	if b.index == nil {
		return nil
	}
	return b.index.Put(hash, dedup.Entry{
		SegmentID: blob.SegmentID,
		Offset:    blob.Offset,
		Size:      blob.Size,
	})
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
