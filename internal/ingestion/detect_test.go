package ingestion_test

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/stretchr/testify/require"
)

func TestDetectPayloadXML(t *testing.T) {
	tpe, err := ingestion.DetectPayload([]byte(`<?xml version="1.0"?><seans></seans>`))
	require.NoError(t, err)
	require.Equal(t, ingestion.PayloadXML, tpe)
}

func TestDetectPayloadZIP(t *testing.T) {
	tpe, err := ingestion.DetectPayload([]byte{0x50, 0x4b, 0x03, 0x04})
	require.NoError(t, err)
	require.Equal(t, ingestion.PayloadZIP, tpe)
}

func TestDetectPayloadUnsupported(t *testing.T) {
	_, err := ingestion.DetectPayload([]byte("not a file"))
	require.Error(t, err)
}
