package coordinator

import (
	"crypto/sha256"
)

func sha256Sum(data []byte) [32]byte {
	return sha256.Sum256(data)
}
