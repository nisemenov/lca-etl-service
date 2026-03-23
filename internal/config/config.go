// Package config provides application configuration loading and constants.
//
// It is responsible for:
//   - Loading configuration from environment variables
//   - Providing compile-time constants for API endpoints, limits, etc.
//   - Central place for all configuration-related values
//
// Usage:
//
//	cfg := config.Load()
//	producer := producer.NewHTTPClient(..., cfg.APIBaseURL, ...)
package config

import (
	"os"
	"strconv"

	"github.com/nisemenov/etl_service/internal/validation"
)

type Config struct {
	DBPath     string `validate:"required"`
	APIBaseURL string `validate:"required"`

	XInternalToken string `validate:"required"`

	Debug bool
}

func Load() *Config {
	config := &Config{
		DBPath:         getEnv("DB_PATH", ""),
		APIBaseURL:     getEnv("API_BASE_URL", ""),
		XInternalToken: getEnv("X_INTERNAL_TOKEN", ""),
		Debug:          getEnvBool("DEBUG"),
	}

	if err := validation.Validate.Struct(config); err != nil {
		// TODO: logger?
		panic(err)
	}

	return config
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvBool(key string) bool {
	if v, ok := os.LookupEnv(key); ok {
		b, _ := strconv.ParseBool(v)
		return b
	}
	return false
}
