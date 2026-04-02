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

	"github.com/joho/godotenv"
	"github.com/nisemenov/etl_service/internal/validation"
)

type Config struct {
	DBPath     string `validate:"required"`
	APIBaseURL string `validate:"required"`

	ClickHouseHost     string `validate:"required"`
	ClickHousePort     string `validate:"required"`
	ClickHouseUser     string `validate:"required"`
	ClickHousePassword string `validate:"required"`
	ClickHouseDB       string `validate:"required"`

	PaymentCHTableName    string `validate:"required"`
	YooPaymentCHTableName string `validate:"required"`

	Debug bool
}

func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	config := &Config{
		DBPath:                getEnv("DB_PATH", ""),
		APIBaseURL:            getEnv("API_BASE_URL", ""),
		ClickHouseHost:        getEnv("CLICKHOUSE_HOST", ""),
		ClickHousePort:        getEnv("CLICKHOUSE_PORT", ""),
		ClickHouseUser:        getEnv("CLICKHOUSE_USER", ""),
		ClickHousePassword:    getEnv("CLICKHOUSE_PASSWORD", ""),
		ClickHouseDB:          getEnv("CLICKHOUSE_DB", ""),
		PaymentCHTableName:    getEnv("PAYMENT_CH_TABLE_NAME", ""),
		YooPaymentCHTableName: getEnv("YOO_PAYMENT_CH_TABLE_NAME", ""),
		Debug:                 getEnvBool("DEBUG"),
	}

	if err := validation.Validate.Struct(config); err != nil {
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
