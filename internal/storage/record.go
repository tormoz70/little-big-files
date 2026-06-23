package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	MagicXML  uint32 = 0x584D4C31 // "XML1"
	MagicXMLC uint32 = 0x584D4C32 // "XML2" compressed XML
	MagicZIP  uint32 = 0x5A495031 // "ZIP1"
	MagicERR uint32 = 0x45525231 // "ERR1"

	FooterMagic uint32 = 0x464F4F54 // "FOOT"

	HeaderSize  = 8
	FooterSize  = 32
	RecordOverhead = HeaderSize + FooterSize
)

type RecordType int

const (
	RecordXML RecordType = iota
	RecordZIP
	RecordError
)

func (t RecordType) Magic() uint32 {
	switch t {
	case RecordZIP:
		return MagicZIP
	case RecordError:
		return MagicERR
	default:
		return MagicXML
	}
}

func IsCompressedXML(magic uint32) bool { return magic == MagicXMLC }

func EncodeRecord(magic uint32, payload []byte) []byte {
	buf := make([]byte, HeaderSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], magic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[HeaderSize:], payload)
	return buf
}

func DecodeRecordHeader(data []byte) (magic uint32, size uint32, err error) {
	if len(data) < HeaderSize {
		return 0, 0, fmt.Errorf("record header too short")
	}
	magic = binary.LittleEndian.Uint32(data[0:4])
	size = binary.LittleEndian.Uint32(data[4:8])
	return magic, size, nil
}

func EncodeFooter(recordCount uint32, totalSize uint64) []byte {
	buf := make([]byte, FooterSize)
	binary.LittleEndian.PutUint32(buf[0:4], recordCount)
	binary.LittleEndian.PutUint64(buf[4:12], totalSize)
	binary.LittleEndian.PutUint32(buf[28:32], FooterMagic)
	return buf
}

func ValidateFooter(data []byte) (recordCount uint32, totalSize uint64, ok bool) {
	if len(data) < FooterSize {
		return 0, 0, false
	}
	off := len(data) - FooterSize
	if binary.LittleEndian.Uint32(data[off+28:off+32]) != FooterMagic {
		return 0, 0, false
	}
	recordCount = binary.LittleEndian.Uint32(data[off : off+4])
	totalSize = binary.LittleEndian.Uint64(data[off+4 : off+12])
	return recordCount, totalSize, true
}
