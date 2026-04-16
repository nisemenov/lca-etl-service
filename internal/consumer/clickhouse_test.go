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

	"github.com/nisemenov/etl-service/internal/domain"
	"github.com/nisemenov/etl-service/internal/httpclient"
	"github.com/stretchr/testify/require"
)

func TestClickHousePayment_InsertBatch_OK(t *testing.T) {
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	httpClient := httpclient.NewHTTPClient(&http.Client{}, server.URL, logger)
	loader := NewClickHouseLoader(httpClient, "payments", PaymentToClickHouseRow, logger)

	payments := []domain.Payment{
		{ID: 1, FullName: "Ivan", Amount: domain.Money("10000")},
		{ID: 2, FullName: "Petr", Amount: domain.Money("10000")},
	}

	err := loader.InsertBatch(context.Background(), payments)
	require.NoError(t, err)
	require.Equal(t, "POST", req.Method)
	require.Equal(t, "/?query=INSERT+INTO+payments+FORMAT+JSONEachRow", req.URL.RequestURI())

	lines := strings.Split(strings.TrimSpace(string(receivedBody)), "\n")
	require.Len(t, lines, 2)

	var r1, r2 map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &r1))
	require.Equal(t, "Ivan", r1["full_name"])

	require.NoError(t, json.Unmarshal([]byte(lines[1]), &r2))
	require.Equal(t, "Petr", r2["full_name"])
}

func TestClickHousePayment_InsertBatch_Empty(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, nil))
	loader := NewClickHouseLoader(nil, "", PaymentToClickHouseRow, logger)

	err := loader.InsertBatch(context.Background(), nil)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "empty batch for InsertBatch")
}

func TestClickHousePayment_InsertBatch_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	httpClient := httpclient.NewHTTPClient(&http.Client{}, server.URL, logger)
	loader := NewClickHouseLoader(httpClient, "payments", PaymentToClickHouseRow, logger)

	err := loader.InsertBatch(context.Background(), []domain.Payment{{ID: domain.PaymentID(1)}})
	require.Error(t, err)
	require.Contains(t, buf.String(), "InsertBatch failed")
}
