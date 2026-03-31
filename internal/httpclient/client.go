// Package httpclient provides clients for fetching data
// from external producer services over HTTP.
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

type HTTPClient struct {
	client     *http.Client
	baseURL    string
	headers    map[string]string
	middleware func(*http.Request)
	logger     *slog.Logger
}

func (c *HTTPClient) Get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+path, nil)
	if err != nil {
		return err
	}

	c.applyHeadersAndMiddleware(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	// c.logger.Debug(
	// 	"raw response",
	// 	"status", resp.StatusCode,
	// 	"body", string(body),
	// )

	if resp.StatusCode >= 300 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}

	return json.Unmarshal(body, out)
}

func (c *HTTPClient) Post(ctx context.Context, path string, body any) error {
	b, _ := json.Marshal(body)
	return c.PostRaw(ctx, path, "application/json", bytes.NewReader(b))
}

func (c *HTTPClient) PostRaw(ctx context.Context, path string, contentType string, body io.Reader) error {
	req, _ := http.NewRequestWithContext(ctx, "POST", c.baseURL+path, body)
	req.Header.Set("Content-Type", contentType)

	c.applyHeadersAndMiddleware(req)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *HTTPClient) applyHeadersAndMiddleware(req *http.Request) {
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	if c.middleware != nil {
		c.middleware(req)
	}
}

func NewHTTPClient(client *http.Client, baseURL string, logger *slog.Logger, opts ...Option) *HTTPClient {
	c := &HTTPClient{
		client:  client,
		baseURL: baseURL,
		headers: make(map[string]string),
		logger:  logger,
	}

	// for modification HTTPClient from high levels
	for _, opt := range opts {
		opt(c)
	}
	return c
}
