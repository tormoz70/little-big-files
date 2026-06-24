package ingestion

import "context"

type ingestCounters struct {
	newBlobs      int
	duplicateRefs int
}

func (c *ingestCounters) add(created bool) {
	if created {
		c.newBlobs++
	} else {
		c.duplicateRefs++
	}
}

func (s *Service) recordIngest(ctx context.Context, supplierID, fileCount int, counters ingestCounters, packageClone bool) {
	dup := counters.duplicateRefs
	if packageClone {
		dup = fileCount
	}
	_ = s.repo.RecordSupplierIngest(ctx, supplierID, fileCount, counters.newBlobs, dup)
}
