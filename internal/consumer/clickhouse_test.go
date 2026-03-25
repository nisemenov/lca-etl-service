package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/httpclient"
	"github.com/stretchr/testify/require"
)

func TestClickHouseLoader_InsertBatch_OK(t *testing.T) {
	var req *http.Request
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req = r

		var err error
		receivedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	httpClient := httpclient.NewHTTPClient(&http.Client{}, server.URL)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	loader := NewClickHouseLoader(httpClient, "payments", logger)

	payments := []domain.Payment{
		{ID: 1, FullName: "Ivan", Amount: domain.Money(10000)},
		{ID: 2, FullName: "Petr", Amount: domain.Money(20000)},
	}

	err := loader.InsertBatch(context.Background(), payments)
	require.NoError(t, err)
	require.Equal(t, "POST", req.Method)
	require.Equal(t, "/?query=INSERT+INTO+payments+FORMAT+JSONEachRow", req.URL.RequestURI())

	lines := strings.Split(strings.TrimSpace(string(receivedBody)), "\n")
	require.Len(t, lines, 2)

	var r1, r2 map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &r1))
	require.Equal(t, float64(1), r1["id"])
	require.Equal(t, "Ivan", r1["full_name"])
	require.Equal(t, float64(100), r1["amount"])

	require.NoError(t, json.Unmarshal([]byte(lines[1]), &r2))
	require.Equal(t, float64(2), r2["id"])
	require.Equal(t, "Petr", r2["full_name"])
	require.Equal(t, float64(200), r2["amount"])
}

func TestClickHouseLoader_InsertBatch_Empty(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, nil))
	loader := NewClickHouseLoader(nil, "", logger)

	err := loader.InsertBatch(context.Background(), nil)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "empty payments batch for CH InsertBatch")
}

func TestClickHouseLoader_InsertBatch_HTTPError(t *testing.T){
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	var buf bytes.Buffer
	httpClient := httpclient.NewHTTPClient(&http.Client{}, server.URL)
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	loader := NewClickHouseLoader(httpClient, "payments", logger)

	err := loader.InsertBatch(context.Background(), []domain.Payment{{ID: domain.PaymentID(1)}})
	require.Error(t, err)
	require.Contains(t, buf.String(), "CH InsertBatch failed")
}
