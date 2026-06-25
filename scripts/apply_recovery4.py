#!/usr/bin/env python3
import pathlib, re
ROOT = pathlib.Path(__file__).resolve().parents[1]

def patch(rel, old, new, count=1):
    p = ROOT / rel
    text = p.read_text(encoding='utf-8')
    if old not in text:
        raise SystemExit(f'patch miss {rel}: {old[:100]!r}')
    p.write_text(text.replace(old, new, count), encoding='utf-8', newline='\n')
    print('patched', rel)

# ingestion journal
patch('internal/ingestion/package.go', '''import (
	"context"
	"fmt"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/storage"
)''',
'''import (
	"context"
	"fmt"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/recovery"
	"github.com/little-big-files/little-big-files/internal/storage"
)''')

patch('internal/ingestion/package.go', '''type Service struct {
	cfg         config.Config
	repo        metadata.Repository
	blobs       *storage.BlobStore
	unpackQueue *UnpackQueue
}''',
'''type Service struct {
	cfg         config.Config
	repo        metadata.Repository
	blobs       *storage.BlobStore
	journal     *recovery.Journal
	unpackQueue *UnpackQueue
}''')

patch('internal/ingestion/package.go', 'func (s *Service) SetUnpackQueue(q *UnpackQueue) {\n\ts.unpackQueue = q\n}',
      'func (s *Service) SetUnpackQueue(q *UnpackQueue) {\n\ts.unpackQueue = q\n}\n\nfunc (s *Service) SetJournal(j *recovery.Journal) {\n\ts.journal = j\n}\n\nfunc (s *Service) journalPackage(pkg *metadata.Package) {\n\tif s.journal == nil || pkg == nil {\n\t\treturn\n\t}\n\t_ = s.journal.Append(recovery.EntryFromPackage(pkg))\n}\n\nfunc (s *Service) loadAndJournal(ctx context.Context, packageID int64) (*metadata.Package, error) {\n\tpkg, err := s.repo.GetPackage(ctx, packageID)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\ts.journalPackage(pkg)\n\treturn pkg, nil\n}')

patch('internal/ingestion/package.go', 'return s.repo.GetPackage(ctx, packageID)',
      'return s.loadAndJournal(ctx, packageID)', count=10)

patch('internal/ingestion/package.go', 's.blobs.StoreOrRef(ctx, tx, body, storage.RecordXML)',
      's.blobs.StoreOrRef(ctx, tx, body, storage.RecordXML, supplierID)')
patch('internal/ingestion/package.go', 's.blobs.StoreOrRef(ctx, tx, body, storage.RecordZIP)',
      's.blobs.StoreOrRef(ctx, tx, body, storage.RecordZIP, supplierID)', count=2)

patch('internal/ingestion/package.go', 'persistZipMembers(ctx, tx, s.blobs, packageID, members, unpackErr, &counters)',
      'persistZipMembers(ctx, tx, s.blobs, packageID, supplierID, members, unpackErr, &counters)')

# unpack_large
patch('internal/ingestion/unpack_large.go', 'func persistZipMembers(ctx context.Context, tx metadata.Tx, blobs *storage.BlobStore, packageID int64, members []ZipMember, unpackErr error, counters *ingestCounters)',
      'func persistZipMembers(ctx context.Context, tx metadata.Tx, blobs *storage.BlobStore, packageID int64, supplierID int, members []ZipMember, unpackErr error, counters *ingestCounters)')

patch('internal/ingestion/unpack_large.go', 'blobs.StoreOrRef(ctx, tx, errBytes, storage.RecordError)',
      'blobs.StoreOrRef(ctx, tx, errBytes, storage.RecordError, supplierID)')
patch('internal/ingestion/unpack_large.go', 'blobs.StoreOrRef(ctx, tx, m.Data, storage.RecordXML)',
      'blobs.StoreOrRef(ctx, tx, m.Data, storage.RecordXML, supplierID)')

patch('internal/ingestion/unpack_large.go', 'fc, uet, err := persistZipMembers(ctx, tx, s.blobs, packageID, members, unpackErr, nil)',
      'fc, uet, err := persistZipMembers(ctx, tx, s.blobs, packageID, pkg.SupplierID, members, unpackErr, nil)')

patch('internal/ingestion/unpack_large.go', 'return s.propagateUnpackToClones(ctx, packageID)',
      'if err := s.propagateUnpackToClones(ctx, packageID); err != nil {\n\t\treturn err\n\t}\n\t_, err = s.loadAndJournal(ctx, packageID)\n\treturn err')

# server main
patch('cmd/server/main.go', '\t"github.com/little-big-files/little-big-files/internal/metrics"\n\t"github.com/little-big-files/little-big-files/internal/storage"\n)',
      '\t"github.com/little-big-files/little-big-files/internal/metrics"\n\t"github.com/little-big-files/little-big-files/internal/recovery"\n\t"github.com/little-big-files/little-big-files/internal/storage"\n)')

patch('cmd/server/main.go', '\tdefer segments.Close()\n',
      '\tdefer segments.Close()\n\n\tsegmentIndex := storage.NewSegmentIndex(cfg.DataDir)\n\tdefer segmentIndex.Close()\n')

patch('cmd/server/main.go', '\tblobs := storage.NewBlobStore(segments, encoder, dedupIdx)\n\tingest := ingestion.NewService(cfg, repo, blobs)\n',
      '\tblobs := storage.NewBlobStore(segments, segmentIndex, encoder, dedupIdx)\n\tingest := ingestion.NewService(cfg, repo, blobs)\n\n\tif cfg.ShardRole != "replica" {\n\t\tjournal, err := recovery.NewJournal(cfg.DataDir)\n\t\tif err != nil {\n\t\t\tslog.Error("journal init failed", "err", err)\n\t\t\tos.Exit(1)\n\t\t}\n\t\tdefer journal.Close()\n\t\tingest.SetJournal(journal)\n\t}\n')

# tests NewBlobStore and StoreOrRef
for rel in [
    'internal/storage/blob_store_test.go',
    'internal/api/shard_test.go',
    'internal/api/handlers_errors_test.go',
    'internal/api/handlers_test.go',
    'internal/ingestion/clone_test.go',
    'internal/ingestion/unpack_worker_test.go',
    'internal/ingestion/package_test.go',
    'internal/ingestion/dedup_test.go',
    'internal/ingestion/unpack_large_test.go',
]:
    p = ROOT / rel
    text = p.read_text(encoding='utf-8')
    text2 = text.replace('NewBlobStore(sm, enc, nil)', 'NewBlobStore(sm, nil, enc, nil)')
    text2 = text2.replace('NewBlobStore(sm, nil, idx)', 'NewBlobStore(sm, nil, nil, idx)')
    text2 = text2.replace('NewBlobStore(segments, nil, nil)', 'NewBlobStore(segments, nil, nil, nil)')
    text2 = text2.replace('StoreOrRef(ctx, tx, original, storage.RecordXML)', 'StoreOrRef(ctx, tx, original, storage.RecordXML, 1)')
    text2 = text2.replace('StoreOrRef(ctx, tx, data, storage.RecordXML)', 'StoreOrRef(ctx, tx, data, storage.RecordXML, 1)')
    text2 = text2.replace('StoreOrRef(ctx, tx, zipData, storage.RecordZIP)', 'StoreOrRef(ctx, tx, zipData, storage.RecordZIP, 1)')
    if text2 != text:
        p.write_text(text2, encoding='utf-8', newline='\n')
        print('updated', rel)

# python orgs
(ROOT / 'clients/python/orgs.py').write_text('''"""Ten test orgs from ekb_work2 (folder name -> supplier_id for API)."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class OrgSpec:
    """EKB work directory org folder and Coordinator supplier_id."""

    folder: str
    supplier_id: int

    @property
    def name(self) -> str:
        return self.folder


DEFAULT_ORGS: list[OrgSpec] = [
    OrgSpec("6866", 6866),
    OrgSpec("5879", 5879),
    OrgSpec("2793", 2793),
    OrgSpec("2791", 2791),
    OrgSpec("2451", 2451),
    OrgSpec("2450", 2450),
    OrgSpec("2447", 2447),
    OrgSpec("2107", 2107),
    OrgSpec("2101", 2101),
    OrgSpec("1577-1601", 1577),
]


def org_folders() -> list[str]:
    return [o.folder for o in DEFAULT_ORGS]


def supplier_ids() -> list[int]:
    return [o.supplier_id for o in DEFAULT_ORGS]


def folder_to_supplier_id(folder: str) -> int | None:
    for o in DEFAULT_ORGS:
        if o.folder == folder:
            return o.supplier_id
    return folder_name_to_supplier_id(folder)


def folder_name_to_supplier_id(folder: str) -> int | None:
    if re.fullmatch(r"\\d+", folder):
        return int(folder)
    m = re.fullmatch(r"(\\d+)-(\\d+)", folder)
    if m:
        return int(m.group(1))
    return None
''', encoding='utf-8', newline='\n')
print('wrote clients/python/orgs.py')

patch('clients/python/ekb_work.py', 'from orgs import DEFAULT_ORGS, OrgSpec, folder_to_supplier_id',
      'from orgs import DEFAULT_ORGS, OrgSpec, folder_name_to_supplier_id, folder_to_supplier_id')

patch('clients/python/ekb_work.py', '''def supplier_id_for_folder(folder: str) -> int:
    sid = folder_to_supplier_id(folder)
    if sid is not None:
        return sid
    digits = "".join(c for c in folder if c.isdigit())
    if digits:
        return int(digits)
    raise ValueError(f"cannot map folder to supplier_id: {folder}")''',
'''def supplier_id_for_folder(folder: str) -> int:
    sid = folder_to_supplier_id(folder)
    if sid is not None:
        return sid
    raise ValueError(f"cannot map folder to supplier_id: {folder}")''')

# Makefile
patch('Makefile', '\tgo build -o bin/rebuild-index ./cmd/rebuild-index\n\tgo build -o bin/shard-sync ./cmd/shard-sync',
      '\tgo build -o bin/rebuild-index ./cmd/rebuild-index\n\tgo build -o bin/recovery-tool ./cmd/recovery-tool\n\tgo build -o bin/shard-sync ./cmd/shard-sync')

# supplier test + segment index test + invalid supplier test
(ROOT / 'internal/supplier/id_test.go').write_text('''package supplier_test

import (
	"net/http"
	"testing"

	"github.com/little-big-files/little-big-files/internal/supplier"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	require.NoError(t, supplier.Validate(1))
	require.NoError(t, supplier.Validate(1_000_000))
	require.Error(t, supplier.Validate(0))
	require.Error(t, supplier.Validate(1_000_001))
}

func TestParseQuery(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1577", nil)
	id, err := supplier.ParseQuery(r)
	require.NoError(t, err)
	require.Equal(t, 1577, id)
}
''', encoding='utf-8', newline='\n')

patch('internal/api/handlers_errors_test.go', '''func TestPostPackageInvalidSupplierID(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=0", bytes.NewReader([]byte("x")))
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}''',
'''func TestPostPackageInvalidSupplierID(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=0", bytes.NewReader([]byte("x")))
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1000001", bytes.NewReader([]byte("x")))
	rec2 := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusBadRequest, rec2.Code)
}''')

print('phase4 done')
