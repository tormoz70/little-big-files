package recovery

import (
	"encoding/hex"

	"github.com/little-big-files/little-big-files/internal/metadata"
)

func EntryFromPackage(pkg *metadata.Package) JournalEntry {
	e := JournalEntry{
		PackageID:          pkg.ID,
		SupplierID:         pkg.SupplierID,
		ReceivedAt:         FormatTimeUTC(pkg.ReceivedAt),
		PackageHash:        hex.EncodeToString(pkg.PackageHash),
		PayloadType:        pkg.PayloadType,
		StorageMode:        pkg.StorageMode,
		CanonicalPackageID: pkg.CanonicalPackageID,
		OriginalFilename:   pkg.OriginalFilename,
		FileCount:          pkg.FileCount,
		UnpackError:        pkg.UnpackError,
	}
	for _, f := range pkg.Files {
		e.Files = append(e.Files, FileRef{
			FileID:           f.ID,
			BlobHash:         hex.EncodeToString(f.BlobHash),
			Role:             f.Role,
			OriginalFilename: f.OriginalFilename,
			SequenceNumber:   f.SequenceNumber,
		})
	}
	return e
}
