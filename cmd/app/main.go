// The main package is the application entry point.
//
// It loads configuration and starts the application.
package main

import (
	"github.com/nisemenov/etl-service/internal/app"
	"github.com/nisemenov/etl-service/internal/config"
)

func main() {
	cfg := config.Load()

	app := app.New(cfg)
	app.Run()
}
