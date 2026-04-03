package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nisemenov/etl_service/internal/domain"
	"github.com/nisemenov/etl_service/internal/httpclient"
	"github.com/stretchr/testify/require"
)

func TestPaymentProducer_Fetch_OK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)
		require.Equal(t, "/payments/", r.URL.Path)
		require.Equal(t, "target=ch", r.URL.RawQuery)

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"data": [
        		{
            		"id": 1,
            		"case_id": 123,
            		"debtor_id": 123,
            		"full_name": "Петров Петр Петрович",
            		"credit_number": "XYZ789",
            		"credit_issue_date": "2023-05-05T00:00:00+00:00",
            		"amount": "100.00",
            		"debt_amount": "100.00",
            		"execution_date_by_system": "2024-07-01T12:00:00Z",
            		"channel": "email"
				}
			]
		}`))
	}))
	defer server.Close()

	payProducer := getPayProducer(server.URL)
	payments, err := payProducer.Fetch(context.Background())

	require.NoError(t, err)
	require.Len(t, payments, 1)

	first := payments[0]
	require.Equal(t, domain.PaymentID(1), first.ID)
	require.Equal(t, "Петров Петр Петрович", first.FullName)
}

func TestPaymentProducer_Fetch_SkipsInvalid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"data": [
				{"id": 1},
        		{
            		"id": 2,
            		"case_id": 123,
            		"debtor_id": 123,
            		"full_name": "Петров Петр Петрович",
            		"credit_number": "XYZ789",
            		"credit_issue_date": "2023-05-05T00:00:00+00:00",
            		"amount": "100.00",
            		"debt_amount": "100.00",
            		"execution_date_by_system": "2024-07-01T12:00:00Z",
            		"channel": "email"
				},
				{"id": 3}
			]
		}`))
	}))
	defer server.Close()

	payProducer := getPayProducer(server.URL)
	payments, err := payProducer.Fetch(context.Background())

	require.NoError(t, err)
	require.Len(t, payments, 1)
	require.Equal(t, domain.PaymentID(2), payments[0].ID)
}

func TestPaymentProducer_Fetch_AllInvalid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"data": [
				{"id": 1},
				{"id": 2},
				{"id": 3}
			]
		}`))
	}))
	defer server.Close()

	payProducer := getPayProducer(server.URL)
	payments, err := payProducer.Fetch(context.Background())

	require.NoError(t, err)
	require.Len(t, payments, 0)
}

func TestPaymentProducer_Fetch_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data": []}`))
	}))
	defer server.Close()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, nil))
	http := httpclient.NewHTTPClient(&http.Client{}, server.URL, logger)
	payProducer := NewPaymentProducer(http, logger)

	payments, err := payProducer.Fetch(context.Background())

	require.NoError(t, err)
	require.Len(t, payments, 0)
	require.Contains(t, buf.String(), "no new payments data to export")
}

func TestPaymentProducer_Fetch_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data": [{"id": 1`))
	}))
	defer server.Close()

	payProducer := getPayProducer(server.URL)
	_, err := payProducer.Fetch(context.Background())

	require.Error(t, err)
}

func TestPaymentProducer_Acknowledge_OK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PATCH", r.Method)
		require.Equal(t, "/payments/", r.URL.Path)
		require.Equal(t, "target=ch", r.URL.RawQuery)

		var body struct {
			IDs []int `json:"ids"`
		}
		err := json.NewDecoder(r.Body).Decode(&body)
		require.NoError(t, err)
		require.Equal(t, []int{1, 2}, body.IDs)
	}))
	defer server.Close()

	payProducer := getPayProducer(server.URL)
	err := payProducer.Acknowledge(context.Background(), []domain.PaymentID{1, 2})
	require.NoError(t, err)
}

func TestPaymentProducer_Acknowledge_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PATCH", r.Method)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"db down"}`))
	}))
	defer server.Close()

	payProducer := getPayProducer(server.URL)
	err := payProducer.Acknowledge(context.Background(), []domain.PaymentID{1, 2})

	require.Error(t, err)
}

func TestPaymentProducer_Acknowledge_Empty(t *testing.T) {
	payProducer := getPayProducer("")

	err := payProducer.Acknowledge(context.Background(), nil)
	require.NoError(t, err)
}

func getPayProducer(baseURL string) *paymentProducer {
	// logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	http := httpclient.NewHTTPClient(&http.Client{}, baseURL, logger)

	return NewPaymentProducer(http, logger)
}
