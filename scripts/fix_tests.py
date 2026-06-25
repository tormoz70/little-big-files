import pathlib
ROOT = pathlib.Path(r"c:\data\prjs\little-big-files")
for rel in ['internal/storage/blob_store_test.go','internal/ingestion/package_test.go','internal/ingestion/dedup_test.go']:
    p = ROOT / rel
    t = p.read_text(encoding='utf-8')
    t2 = t.replace('NewBlobStore(segments, nil, idx)', 'NewBlobStore(segments, nil, nil, idx)')
    t2 = t2.replace('NewBlobStore(sm, nil, nil)', 'NewBlobStore(sm, nil, nil, nil)')
    if t2 != t:
        p.write_text(t2, encoding='utf-8', newline='\n')
        print('fixed', rel)

# bootstrap: mirror PG dict to sidecar on load
p = ROOT / 'internal/compress/bootstrap.go'
t = p.read_text(encoding='utf-8')
old = '''\tif len(dict) == 0 {
\t\tdict, _, err = repo.GetLatestDictionary(ctx)
\t\tif err != nil {
\t\t\treturn nil, err
\t\t}
\t\tif len(dict) > 0 {
\t\t\tdictID = 1
\t\t}
\t}'''
new = '''\tif len(dict) == 0 {
\t\tdict, _, err = repo.GetLatestDictionary(ctx)
\t\tif err != nil {
\t\t\treturn nil, err
\t\t}
\t\tif len(dict) > 0 {
\t\t\tdictID = 1
\t\t\t_ = sidecar.Save(dictID, dict)
\t\t}
\t}'''
if old in t:
    p.write_text(t.replace(old, new), encoding='utf-8', newline='\n')
    print('patched bootstrap sidecar mirror')

# segment index test
(ROOT / 'internal/storage/segment_index_test.go').write_text('''package storage_test

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestSegmentIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	idx := storage.NewSegmentIndex(dir)
	defer idx.Close()

	var hash [32]byte
	hash[0] = 0xab
	entry := storage.IndexEntry{
		Offset: 128, StoredSize: 200, LogicalSize: 180,
		Magic: storage.MagicZIP, Hash: hash, SupplierID: 1577, DictID: 1,
	}
	require.NoError(t, idx.Append(0, entry))

	path := dir + "/segment_0000.idx"
	entries, err := storage.ReadIndexFile(path)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, entry.Offset, entries[0].Offset)
	require.Equal(t, entry.SupplierID, entries[0].SupplierID)
}
''', encoding='utf-8', newline='\n')
print('wrote segment_index_test.go')
