package goose_modules

import (
	"database/sql"
	"sort"

	"github.com/pkg/errors"
)

// Reset rolls back all migrations
func Reset(db *sql.DB, dir string, opts ...OptionsFunc) error {
	option := &options{}
	for _, f := range opts {
		f(option)
	}
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return errors.Wrap(err, "failed to collect migrations")
	}
	if option.noVersioning {
		return DownTo(db, dir, minVersion, opts...)
	}

	statuses, err := dbMigrationsStatus(db)
	if err != nil {
		return errors.Wrap(err, "failed to get status of migrations")
	}
	sort.Sort(sort.Reverse(migrations))

	for _, migration := range migrations {
		if !statuses[migration.Version] {
			continue
		}
		if err = migration.Down(db); err != nil {
			return errors.Wrap(err, "failed to db-down")
		}
	}

	return nil
}

func dbMigrationsStatus(db *sql.DB) (map[int64]bool, error) {
	result := make(map[int64]bool)

	for _, module := range registeredModules {
		rows, err := GetDialect().dbVersionQuery(db, module.Table)
		if err != nil {
			return map[int64]bool{}, nil
		}

		for rows.Next() {
			var row MigrationRecord
			if err = rows.Scan(&row.VersionID, &row.IsApplied); err != nil {
				return nil, errors.Wrap(err, "failed to scan row")
			}

			if _, ok := result[row.VersionID]; ok {
				continue
			}

			result[row.VersionID] = row.IsApplied
		}

		rows.Close()
	}

	return result, nil
}
