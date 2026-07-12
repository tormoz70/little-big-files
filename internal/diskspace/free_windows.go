//go:build windows

package diskspace

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

func availableBytes(path string) (int64, error) {
	target := filepath.Clean(path)
	if len(target) == 2 && target[1] == ':' {
		target += `\`
	}
	ptr, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return 0, err
	}

	var freeAvailable uint64
	if err := windows.GetDiskFreeSpaceEx(ptr, &freeAvailable, nil, nil); err != nil {
		return 0, err
	}
	return int64(freeAvailable), nil
}
