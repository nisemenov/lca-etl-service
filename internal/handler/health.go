package handler

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/nisemenov/etl-service/internal/config"
)

func NewHTTPServer(cfg *config.Config, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler)

	return &http.Server{
		Addr:         cfg.HTTPAddr(),
		Handler:      loggingMiddleware(mux, logger),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func loggingMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		wrapped := &responseWriterWrapper{
			ResponseWriter: w,
			statusCode:     0,
		}

		next.ServeHTTP(wrapped, r)

		if wrapped.statusCode == 0 {
			wrapped.statusCode = http.StatusOK
		}

		logger.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"duration_ms", time.Since(start).Milliseconds(),
			"remote_addr", r.RemoteAddr,
		)
	})
}

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriterWrapper) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
