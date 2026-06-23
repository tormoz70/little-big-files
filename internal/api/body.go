package api

import (
	"fmt"
	"io"
	"net/http"
)

func readBodyLimited(r *http.Request, maxBytes int64) ([]byte, error) {
	if r.Body == nil {
		return nil, fmt.Errorf("empty body")
	}
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("payload too large")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty body")
	}
	return data, nil
}
