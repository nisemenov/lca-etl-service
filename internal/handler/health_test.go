package handler

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	healthHandler(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	if string(body) != "ok" {
		t.Fatalf("expected body 'ok', got %s", string(body))
	}
}

// e2e
func TestHTTPServer_Health(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)

	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if string(body) != "ok" {
		t.Fatalf("expected 'ok', got %s", string(body))
	}
}
