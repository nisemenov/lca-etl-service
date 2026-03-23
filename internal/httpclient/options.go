package httpclient

import (
	"maps"
	"net/http"
)

type Option func(*HTTPClient)

// WithHeaders modifies HTTPClient.headers during NewHTTPClient calling
//
// usage: prod := NewHTTPClient(client, baseURL, WithHeaders(...))
func WithHeaders(headers map[string]string) Option {
	return func(p *HTTPClient) {
		maps.Copy(p.headers, headers)
	}
}

// WithMiddleware modifies HTTPClient.middleware during NewHTTPClient calling.
// Without error handling so far.
//
// Only one Middleware!
//
// usage: prod := NewHTTPClient(client, baseURL, WithMiddleware(...))
func WithMiddleware(mw func(*http.Request)) Option {
	return func(p *HTTPClient) {
		p.middleware = mw
	}
}
