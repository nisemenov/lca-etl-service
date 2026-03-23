package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testURL = "/test"

func TestHTTPClient_Get_OK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)
		require.Equal(t, testURL, r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"value": 42}`))
	}))
	defer server.Close()

	p := NewHTTPClient(&http.Client{}, server.URL)

	var resp struct {
		Value int `json:"value"`
	}

	err := p.Get(context.Background(), testURL, &resp)
	require.NoError(t, err)
	require.Equal(t, 42, resp.Value)
}

func TestHTTPClient_Get_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer server.Close()

	p := NewHTTPClient(&http.Client{}, server.URL)

	var out any
	err := p.Get(context.Background(), testURL, &out)

	require.Error(t, err)
}

func TestHTTPClient_PostRaw(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, testURL, r.URL.Path)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var body map[string]int
		err := json.NewDecoder(r.Body).Decode(&body)
		require.NoError(t, err)
		require.Equal(t, 123, body["id"])
	}))
	defer server.Close()

	p := NewHTTPClient(&http.Client{}, server.URL)
	// convert req body to bytes
	b, _ := json.Marshal(map[string]int{"id": 123})

	err := p.PostRaw(context.Background(), testURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
}

func TestHTTPClient_Post(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, testURL, r.URL.Path)

		var body map[string]int
		err := json.NewDecoder(r.Body).Decode(&body)
		require.NoError(t, err)
		require.Equal(t, 123, body["id"])
	}))
	defer server.Close()

	p := NewHTTPClient(&http.Client{}, server.URL)

	err := p.Post(context.Background(), testURL, map[string]int{"id": 123})
	require.NoError(t, err)
}

func TestHTTPClient_Timeout(t *testing.T) {
	called := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	p := NewHTTPClient(&http.Client{Timeout: 50 * time.Millisecond}, server.URL)

	var out any
	err := p.Get(context.Background(), testURL, &out)

	require.Error(t, err)
	require.True(t, called)
}
