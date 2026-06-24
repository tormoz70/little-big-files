package dedup

type kvStore interface {
	Get(hash []byte) (Entry, bool, error)
	Put(hash []byte, entry Entry) error
	Clear() error
	Close() error
	Len() (int, error)
}
