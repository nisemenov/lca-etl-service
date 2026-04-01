package sqlite

import (
	"database/sql"
	"embed"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

func Migrate(db *sql.DB) error {
	goose.SetBaseFS(migrations)

	if err := goose.SetDialect("sqlite3"); err != nil {
		return fmt.Errorf("failed to set goose dialect: %w", err)
	}

	// Если очень хочется видеть, какие миграции применились:
	// goose.SetVerbose(true)

	return goose.Up(db, "migrations")
}

func InitSQLite(dsn string) *sql.DB {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("PRAGMA journal_mode = WAL;")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("PRAGMA synchronous = NORMAL;")
	if err != nil {
		panic(err)
	}

	err = Migrate(db)
	if err != nil {
		panic(err)
	}

	return db
}
