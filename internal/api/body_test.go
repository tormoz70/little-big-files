package api

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadBodyLimitedOK(t *testing.T) {
	req := httptestReq("hello")
	data, err := readBodyLimited(req, 1024)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)
}

func TestReadBodyLimitedEmpty(t *testing.T) {
	req := httptestReq("")
	_, err := readBodyLimited(req, 1024)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

func TestReadBodyLimitedTooLarge(t *testing.T) {
	req := httptestReq(strings.Repeat("x", 20))
	_, err := readBodyLimited(req, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "too large")
}

func TestReadBodyLimitedNilBody(t *testing.T) {
	req := &http.Request{Method: http.MethodPost}
	_, err := readBodyLimited(req, 1024)
	require.Error(t, err)
}

func httptestReq(body string) *http.Request {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	} else {
		r = strings.NewReader("")
	}
	req, _ := http.NewRequest(http.MethodPost, "/", r)
	return req
}
