package compress

import (
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const DefaultDictSize = 32 * 1024

// Encoder compresses XML payloads with an optional Zstd dictionary.
type Encoder struct {
	dictID  int
	minSize int
	dict    []byte
	enc     *zstd.Encoder
	dec     *zstd.Decoder
	mu      sync.RWMutex
}

func NewEncoder(dict []byte, minSize int) (*Encoder, error) {
	if minSize <= 0 {
		minSize = 64
	}
	e := &Encoder{dict: dict, minSize: minSize}
	if err := e.init(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Encoder) init() error {
	var encOpts []zstd.EOption
	var decOpts []zstd.DOption
	if len(e.dict) > 0 {
		encOpts = append(encOpts, zstd.WithEncoderDict(e.dict))
		decOpts = append(decOpts, zstd.WithDecoderDicts(e.dict))
	}
	enc, err := zstd.NewWriter(nil, encOpts...)
	if err != nil {
		return err
	}
	dec, err := zstd.NewReader(nil, decOpts...)
	if err != nil {
		enc.Close()
		return err
	}
	e.enc = enc
	e.dec = dec
	return nil
}

func (e *Encoder) DictID() int { return e.dictID }

func (e *Encoder) SetDictID(id int) { e.dictID = id }

func (e *Encoder) ShouldCompress(size int) bool {
	return size >= e.minSize
}

func (e *Encoder) Compress(data []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.enc == nil {
		return data, nil
	}
	out := e.enc.EncodeAll(data, make([]byte, 0, len(data)))
	if len(out) >= len(data) {
		return data, nil
	}
	return out, nil
}

func (e *Encoder) Decompress(data []byte) ([]byte, error) {
	if len(data) < 4 || !isZstdFrame(data) {
		return data, nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.dec == nil {
		return data, nil
	}
	return e.dec.DecodeAll(data, nil)
}

func isZstdFrame(data []byte) bool {
	// Zstd frame magic: 0xFD2FB528 (little-endian).
	return data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD
}

func (e *Encoder) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.enc != nil {
		e.enc.Close()
		e.enc = nil
	}
	if e.dec != nil {
		e.dec.Close()
		e.dec = nil
	}
}

// TrainDictionary builds a Zstd dictionary from sample payloads.
func TrainDictionary(samples [][]byte, dictSize int) ([]byte, error) {
	if len(samples) == 0 {
		return nil, nil
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		Contents: samples,
	})
	if err != nil {
		if strings.Contains(err.Error(), "dictionary of size") {
			return nil, nil
		}
		return nil, err
	}
	if len(dict) < 8 {
		return nil, nil
	}
	return dict, nil
}
