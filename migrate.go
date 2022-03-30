package goose_modules

import (
	"database/sql"
	"fmt"
	"io/fs"
	"path"
	"runtime"
	"sort"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrNoCurrentVersion when a current migration version is not found.
	ErrNoCurrentVersion = errors.New("no current version found")
	// ErrNoNextVersion when the next migration version is not found.
	ErrNoNextVersion = errors.New("no next version found")
	// ErrModuleNotFound when requested module is not found
	ErrModuleNotFound = errors.New("module is not found")
	// MaxVersion is the maximum allowed version.
	MaxVersion int64 = 9223372036854775807 // max(int64)

	registeredModules            = make(map[string]MigrationModule)
	registeredGoMigrations       = map[int64]*Migration{}
	registeredModuleGoMigrations = make(map[string]map[int64]*Migration)
)

// MigrationModule describes migration module
type MigrationModule struct {
	Table string
	Dir   string
}

// Migrations slice.
type Migrations []*Migration

// helpers so we can use pkg sort
func (ms Migrations) Len() int      { return len(ms) }
func (ms Migrations) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
func (ms Migrations) Less(i, j int) bool {
	if ms[i].Version == ms[j].Version {
		panic(fmt.Sprintf("goose: duplicate version %v detected:\n%v\n%v", ms[i].Version, ms[i].Source, ms[j].Source))
	}
	return ms[i].Version < ms[j].Version
}

// Current gets the current migration.
func (ms Migrations) Current(current int64) (*Migration, error) {
	for i, migration := range ms {
		if migration.Version == current {
			return ms[i], nil
		}
	}

	return nil, ErrNoCurrentVersion
}

// Next gets the next migration.
func (ms Migrations) Next(current int64) (*Migration, error) {
	for i, migration := range ms {
		if migration.Version > current {
			return ms[i], nil
		}
	}

	return nil, ErrNoNextVersion
}

// Previous : Get the previous migration.
func (ms Migrations) Previous(current int64) (*Migration, error) {
	for i := len(ms) - 1; i >= 0; i-- {
		if ms[i].Version < current {
			return ms[i], nil
		}
	}

	return nil, ErrNoNextVersion
}

// Last gets the last migration.
func (ms Migrations) Last() (*Migration, error) {
	if len(ms) == 0 {
		return nil, ErrNoNextVersion
	}

	return ms[len(ms)-1], nil
}

// Versioned gets versioned migrations.
func (ms Migrations) versioned() (Migrations, error) {
	var migrations Migrations

	// assume that the user will never have more than 19700101000000 migrations
	for _, m := range ms {
		// parse version as timestmap
		versionTime, err := time.Parse(timestampFormat, fmt.Sprintf("%d", m.Version))

		if versionTime.Before(time.Unix(0, 0)) || err != nil {
			migrations = append(migrations, m)
		}
	}

	return migrations, nil
}

// Timestamped gets the timestamped migrations.
func (ms Migrations) timestamped() (Migrations, error) {
	var migrations Migrations

	// assume that the user will never have more than 19700101000000 migrations
	for _, m := range ms {
		// parse version as timestmap
		versionTime, err := time.Parse(timestampFormat, fmt.Sprintf("%d", m.Version))
		if err != nil {
			// probably not a timestamp
			continue
		}

		if versionTime.After(time.Unix(0, 0)) {
			migrations = append(migrations, m)
		}
	}
	return migrations, nil
}

func (ms Migrations) String() string {
	str := ""
	for _, m := range ms {
		str += fmt.Sprintln(m)
	}
	return str
}

// AddMigration adds a migration.
func AddMigration(up func(*sql.Tx) error, down func(*sql.Tx) error) {
	_, filename, _, _ := runtime.Caller(1)
	AddNamedMigration(filename, up, down)
}

// AddNamedMigration : Add a named migration.
func AddNamedMigration(filename string, up func(*sql.Tx) error, down func(*sql.Tx) error) {
	v, _ := NumericComponent(filename)
	migration := &Migration{Version: v, Next: -1, Previous: -1, Registered: true, UpFn: up, DownFn: down, Source: filename}

	if existing, ok := registeredGoMigrations[v]; ok {
		panic(fmt.Sprintf("failed to add migration %q: version conflicts with %q", filename, existing.Source))
	}

	registeredGoMigrations[v] = migration
}

// AddModuleMigration adds a migration in specific module
func AddModuleMigration(module string, up func(*sql.Tx) error, down func(*sql.Tx) error) {
	_, filename, _, _ := runtime.Caller(1)
	AddNamedModuleMigration(module, filename, up, down)
}

// AddNamedModuleMigration adds a migration in specific module
func AddNamedModuleMigration(module string, filename string, up func(*sql.Tx) error, down func(*sql.Tx) error) {
	v, _ := NumericComponent(filename)
	migration := &Migration{
		Version:    v,
		Next:       -1,
		Previous:   -1,
		Registered: true,
		UpFn:       up,
		DownFn:     down,
		Source:     filename,
		Module:     module,
	}

	if _, ok := registeredModuleGoMigrations[module]; !ok {
		registeredModuleGoMigrations[module] = make(map[int64]*Migration)
	}

	if existing, ok := registeredModuleGoMigrations[module][v]; ok {
		panic(fmt.Sprintf("failed to add migration %q: version conflicts with %q", filename, existing.Source))
	}

	registeredModuleGoMigrations[module][v] = migration
}

func collectMigrationsFS(fsys fs.FS, dirpath string, current, target int64) (Migrations, error) {
	if _, err := fs.Stat(fsys, dirpath); errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s directory does not exist", dirpath)
	}

	var migrations Migrations

	// SQL migration files.
	sqlMigrationFiles, err := fs.Glob(fsys, path.Join(dirpath, "*.sql"))
	if err != nil {
		return nil, err
	}
	for _, file := range sqlMigrationFiles {
		v, err := NumericComponent(file)
		if err != nil {
			return nil, err
		}
		if versionFilter(v, current, target) {
			migration := &Migration{Version: v, Next: -1, Previous: -1, Source: file}
			migrations = append(migrations, migration)
		}
	}

	// Go migrations registered via goose.AddMigration().
	for _, migration := range registeredGoMigrations {
		v, err := NumericComponent(migration.Source)
		if err != nil {
			return nil, err
		}
		if versionFilter(v, current, target) {
			migrations = append(migrations, migration)
		}
	}

	// Go migration files
	goMigrationFiles, err := fs.Glob(fsys, path.Join(dirpath, "*.go"))
	if err != nil {
		return nil, err
	}
	for _, file := range goMigrationFiles {
		v, err := NumericComponent(file)
		if err != nil {
			continue // Skip any files that don't have version prefix.
		}

		// Skip migrations already existing migrations registered via goose.AddMigration().
		if _, ok := registeredGoMigrations[v]; ok {
			continue
		}

		if versionFilter(v, current, target) {
			migration := &Migration{Version: v, Next: -1, Previous: -1, Source: file, Registered: false}
			migrations = append(migrations, migration)
		}
	}

	migrations = sortAndConnectMigrations(migrations)

	return migrations, nil
}

// CollectMigrations returns all the valid looking migration scripts in the
// migrations folder and go func registry, and key them by version.
func CollectMigrations(dirpath string, current, target int64) (Migrations, error) {
	return collectMigrationsFS(baseFS, dirpath, current, target)
}

func sortAndConnectMigrations(migrations Migrations) Migrations {
	sort.Sort(migrations)

	// now that we're sorted in the appropriate direction,
	// populate next and previous for each migration
	for i, m := range migrations {
		prev := int64(-1)
		if i > 0 {
			prev = migrations[i-1].Version
			migrations[i-1].Next = m.Version
		}
		migrations[i].Previous = prev
	}

	return migrations
}

func versionFilter(v, current, target int64) bool {

	if target > current {
		return v > current && v <= target
	}

	if target < current {
		return v <= current && v > target
	}

	return false
}

// EnsureDBVersion retrieves the current version for this DB.
// Create and initialize the DB version table if it doesn't exist.
func EnsureDBVersion(db *sql.DB, tableName string) (int64, error) {
	rows, err := GetDialect().dbVersionQuery(db, tableName)
	if err != nil {
		return 0, createVersionTable(db, tableName)
	}
	defer rows.Close()

	// The most recent record for each migration specifies
	// whether it has been applied or rolled back.
	// The first version we find that has been applied is the current version.

	toSkip := make([]int64, 0)

	for rows.Next() {
		var row MigrationRecord
		if err = rows.Scan(&row.VersionID, &row.IsApplied); err != nil {
			return 0, errors.Wrap(err, "failed to scan row")
		}

		// have we already marked this version to be skipped?
		skip := false
		for _, v := range toSkip {
			if v == row.VersionID {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		// if version has been applied we're done
		if row.IsApplied {
			return row.VersionID, nil
		}

		// latest version of migration has not been applied.
		toSkip = append(toSkip, row.VersionID)
	}
	if err := rows.Err(); err != nil {
		return 0, errors.Wrap(err, "failed to get next row")
	}

	return 0, ErrNoNextVersion
}

// Create the db version table
// and insert the initial 0 value into it
func createVersionTable(db *sql.DB, tableName string) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	d := GetDialect()

	if _, err := txn.Exec(d.createVersionTableSQL(tableName)); err != nil {
		txn.Rollback()
		return err
	}

	version := 0
	applied := true
	if _, err := txn.Exec(d.insertVersionSQL(tableName), version, applied); err != nil {
		txn.Rollback()
		return err
	}

	return txn.Commit()
}

// GetDBVersion is an alias for EnsureDBVersion, but returns -1 in error.
func GetDBVersion(db *sql.DB) (int64, error) {
	var version int64
	version = -1

	for _, module := range registeredModules {
		moduleVersion, err := EnsureDBVersion(db, module.Table)
		if err != nil {
			continue
		}

		if moduleVersion > version {
			version = moduleVersion
		}
	}

	return version, nil
}

// LoadModuleMigrations loads specific module migrations into global registered migrations
func LoadModuleMigrations(module string) {
	for key, migration := range registeredModuleGoMigrations[module] {
		registeredGoMigrations[key] = migration
	}
}

// RegisterModule registers new migration module
func RegisterModule(name string, module MigrationModule) {
	registeredModules[name] = module
}

// GetModuleByName returns module by its name
func GetModuleByName(name string) MigrationModule {
	return registeredModules[name]
}
