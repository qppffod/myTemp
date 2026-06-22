package migrations

import (
	"embed"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed *.sql
var migrationsFS embed.FS

func RunMigrations(connStr string) error {
	// m, err := migrate.New("file://migrations", connStr)
	// if err != nil {
	// 	return fmt.Errorf("migrate.New: %w", err)
	// }
	// defer m.Close()

	// if err := m.Up(); err != nil && err != migrate.ErrNoChange {
	// 	return fmt.Errorf("migrate.Up: %w", err)
	// }
	// return nil

	d, err := iofs.New(migrationsFS, ".") // "." = current embedded dir
	if err != nil {
		return err
	}
	m, err := migrate.NewWithSourceInstance("iofs", d, connStr)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}
