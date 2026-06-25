#!/usr/bin/env python3
import pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]

def patch(rel, old, new, count=1):
    p = ROOT / rel
    text = p.read_text(encoding='utf-8')
    if old not in text:
        raise SystemExit(f'patch miss {rel}: {old[:120]!r}')
    p.write_text(text.replace(old, new, count), encoding='utf-8', newline='\n')
    print('patched', rel)

# encoder DictID
patch('internal/compress/encoder.go', 'type Encoder struct {\n\tminSize int\n\tdict    []byte',
      'type Encoder struct {\n\tdictID  int\n\tminSize int\n\tdict    []byte')
patch('internal/compress/encoder.go', 'func (e *Encoder) ShouldCompress(size int) bool {',
      'func (e *Encoder) DictID() int { return e.dictID }\n\nfunc (e *Encoder) SetDictID(id int) { e.dictID = id }\n\nfunc (e *Encoder) ShouldCompress(size int) bool {')

# bootstrap sidecar
patch('internal/compress/bootstrap.go', '''import (
	"context"
	"log/slog"

	"github.com/little-big-files/little-big-files/internal/config"
)''',
'''import (
	"context"
	"log/slog"
	"path/filepath"

	"github.com/little-big-files/little-big-files/internal/config"
)''')

patch('internal/compress/bootstrap.go', '''func BootstrapEncoder(ctx context.Context, cfg config.Config, repo dictRepo) (*Encoder, error) {
	if !cfg.CompressionEnabled {
		return nil, nil
	}

	dict, _, err := repo.GetLatestDictionary(ctx)
	if err != nil {
		return nil, err
	}''',
'''func BootstrapEncoder(ctx context.Context, cfg config.Config, repo dictRepo) (*Encoder, error) {
	if !cfg.CompressionEnabled {
		return nil, nil
	}

	dataRoot := filepath.Dir(cfg.DataDir)
	sidecar := NewSidecar(dataRoot)
	dictID, dict, err := sidecar.LoadCurrent()
	if err != nil {
		return nil, err
	}
	if len(dict) == 0 {
		dict, _, err = repo.GetLatestDictionary(ctx)
		if err != nil {
			return nil, err
		}
		if len(dict) > 0 {
			dictID = 1
		}
	}''')

patch('internal/compress/bootstrap.go', '''			if len(dict) > 0 {
				if err := repo.SaveDictionary(ctx, dict, len(samples)); err != nil {
					return nil, err
				}
				slog.Info("trained compression dictionary", "samples", len(samples), "dict_bytes", len(dict))''',
'''			if len(dict) > 0 {
				if err := repo.SaveDictionary(ctx, dict, len(samples)); err != nil {
					return nil, err
				}
				if err := sidecar.Save(1, dict); err != nil {
					return nil, err
				}
				dictID = 1
				slog.Info("trained compression dictionary", "samples", len(samples), "dict_bytes", len(dict))''')

patch('internal/compress/bootstrap.go', '''	enc, err := NewEncoder(dict, cfg.CompressionMinSize)
	if err != nil {
		return nil, err
	}''',
'''	enc, err := NewEncoder(dict, cfg.CompressionMinSize)
	if err != nil {
		return nil, err
	}
	if dictID <= 0 && len(dict) > 0 {
		dictID = 1
	}
	enc.SetDictID(dictID)''')

# blob_store
patch('internal/storage/blob_store.go', '''type BlobStore struct {
	segments *SegmentManager
	encoder  *compress.Encoder
	index    *dedup.HotIndex
}''',
'''type BlobStore struct {
	segments     *SegmentManager
	segmentIndex *SegmentIndex
	encoder      *compress.Encoder
	index        *dedup.HotIndex
}''')

patch('internal/storage/blob_store.go', 'func NewBlobStore(segments *SegmentManager, encoder *compress.Encoder, index *dedup.HotIndex) *BlobStore {\n\treturn &BlobStore{segments: segments, encoder: encoder, index: index}\n}',
      'func NewBlobStore(segments *SegmentManager, segmentIndex *SegmentIndex, encoder *compress.Encoder, index *dedup.HotIndex) *BlobStore {\n\treturn &BlobStore{segments: segments, segmentIndex: segmentIndex, encoder: encoder, index: index}\n}')

patch('internal/storage/blob_store.go', 'func (b *BlobStore) StoreOrRef(ctx context.Context, tx metadata.Tx, data []byte, recordType RecordType) ([]byte, bool, error) {',
      'func (b *BlobStore) StoreOrRef(ctx context.Context, tx metadata.Tx, data []byte, recordType RecordType, supplierID int) ([]byte, bool, error) {')

patch('internal/storage/blob_store.go', '\t\treturn b.storeNew(ctx, tx, hash, data, recordType)\n\t}',
      '\t\treturn b.storeNew(ctx, tx, hash, data, recordType, supplierID)\n\t}', count=2)

patch('internal/storage/blob_store.go', 'return b.storeNew(ctx, tx, hash, data, recordType)\n}',
      'return b.storeNew(ctx, tx, hash, data, recordType, supplierID)\n}', count=1)

patch('internal/storage/blob_store.go', 'func (b *BlobStore) storeNew(ctx context.Context, tx metadata.Tx, hash, data []byte, recordType RecordType) ([]byte, bool, error) {',
      'func (b *BlobStore) storeNew(ctx context.Context, tx metadata.Tx, hash, data []byte, recordType RecordType, supplierID int) ([]byte, bool, error) {')

patch('internal/storage/blob_store.go', '''\trecord := b.encodeRecord(data, recordType)
	loc, err := b.segments.Append(record)
	if err != nil {
		return nil, false, err
	}''',
'''\trecord := b.encodeRecord(data, recordType)
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
	}''')

# handlers
patch('internal/api/handlers.go', '\t"github.com/little-big-files/little-big-files/internal/storage"\n)',
      '\t"github.com/little-big-files/little-big-files/internal/storage"\n\t"github.com/little-big-files/little-big-files/internal/supplier"\n)')

patch('internal/api/handlers.go', '\tsupplierID, err := parseSupplierID(r)',
      '\tsupplierID, err := supplier.ParseQuery(r)')

patch('internal/api/handlers.go', '''func parseSupplierID(r *http.Request) (int, error) {
	v := r.URL.Query().Get("supplier_id")
	if v == "" {
		return 0, fmt.Errorf("supplier_id is required")
	}
	id, err := strconv.Atoi(v)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid supplier_id")
	}
	return id, nil
}

func parsePackageID''',
'''func parsePackageID''')

# coordinator
patch('internal/coordinator/server.go', '\t"github.com/little-big-files/little-big-files/internal/metrics"\n)',
      '\t"github.com/little-big-files/little-big-files/internal/metrics"\n\t"github.com/little-big-files/little-big-files/internal/supplier"\n)')

patch('internal/coordinator/server.go', '\tsupplierID, err := parseSupplierID(r)',
      '\tsupplierID, err := supplier.ParseQuery(r)')

patch('internal/coordinator/server.go', '''func parseSupplierID(r *http.Request) (int, error) {
	v := r.URL.Query().Get("supplier_id")
	if v == "" {
		return 0, errStr("supplier_id is required")
	}
	id, err := strconv.Atoi(v)
	if err != nil || id <= 0 {
		return 0, errStr("invalid supplier_id")
	}
	return id, nil
}

func parseGlobalID''',
'''func parseGlobalID''')

print('phase3 done')
