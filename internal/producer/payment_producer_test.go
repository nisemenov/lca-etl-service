package producer

import (
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
		require.Equal(t, FetchPaymentsPath, r.URL.Path)

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
            		"amount": 100.00,
            		"debt_amount": 100.00,
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
	require.Equal(t, domain.Money(10000), first.Amount)
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
            		"amount": 100.00,
            		"debt_amount": 100.00,
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

	payProducer := getPayProducer(server.URL)
	payments, err := payProducer.Fetch(context.Background())

	require.NoError(t, err)
	require.Len(t, payments, 0)
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
		require.Equal(t, "POST", r.Method)
		require.Equal(t, AckPaymentsPath, r.URL.Path)

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
		require.Equal(t, "POST", r.Method)

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
	http := httpclient.NewHTTPClient(&http.Client{}, baseURL)
	// logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	return NewPaymentProducer(http, logger)
}
