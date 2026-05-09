package persistence

import (
	"fmt"

	"github.com/golang-migrate/migrate/v4"
)

func RunMigrations(connStr string) error {
	m, err := migrate.New("file://migrations", connStr)
	if err != nil {
		return fmt.Errorf("migrate.New: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migrate.Up: %w", err)
	}
	return nil
}
