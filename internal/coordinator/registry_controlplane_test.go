package coordinator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestControlPlaneHTTPTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reg := NewRegistry(nil, 0, "")
	start := time.Now()
	httpCtx, cancel := reg.controlPlaneContext(context.Background())
	defer cancel()
	_, err := reg.FetchStats(httpCtx, srv.URL)
	require.Error(t, err)
	require.Less(t, time.Since(start), 15*time.Second)
}
