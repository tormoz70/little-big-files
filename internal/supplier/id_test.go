package supplier_test

import (
	"net/http"
	"testing"

	"github.com/little-big-files/little-big-files/internal/supplier"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	require.NoError(t, supplier.Validate(1))
	require.NoError(t, supplier.Validate(1_000_000))
	require.Error(t, supplier.Validate(0))
	require.Error(t, supplier.Validate(1_000_001))
}

func TestParseQuery(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1577", nil)
	id, err := supplier.ParseQuery(r)
	require.NoError(t, err)
	require.Equal(t, 1577, id)
}

func TestParseQueryMissing(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, "/v1/packages", nil)
	_, err := supplier.ParseQuery(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "required")
}

func TestParseQueryNonNumeric(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, "/v1/packages?supplier_id=abc", nil)
	_, err := supplier.ParseQuery(r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid supplier_id")
}

func TestParseQueryOverMax(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1000001", nil)
	_, err := supplier.ParseQuery(r)
	require.Error(t, err)
}
