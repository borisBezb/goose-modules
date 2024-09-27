package goose_modules

import (
	"database/sql"
	"fmt"
)

// SQLDialect abstracts the details of specific SQL dialects
// for goose's few SQL specific statements
type SQLDialect interface {
	createVersionTableSQL(tableName string) string // sql string to create the db version table
	insertVersionSQL(tableName string) string      // sql string to insert the initial version table row
	deleteVersionSQL(tableName string) string      // sql string to delete version
	migrationSQL() string                          // sql string to retrieve migrations
	dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error)
}

var dialect SQLDialect = &PostgresDialect{}

// GetDialect gets the SQLDialect
func GetDialect() SQLDialect {
	return dialect
}

// SetDialect sets the SQLDialect
func SetDialect(d string) error {
	switch d {
	case "postgres", "pgx":
		dialect = &PostgresDialect{}
	case "mysql":
		dialect = &MySQLDialect{}
	case "sqlite3", "sqlite":
		dialect = &Sqlite3Dialect{}
	case "mssql":
		dialect = &SqlServerDialect{}
	case "redshift":
		dialect = &RedshiftDialect{}
	case "tidb":
		dialect = &TiDBDialect{}
	case "clickhouse":
		dialect = &ClickHouseDialect{}
	default:
		return fmt.Errorf("%q: unknown dialect", d)
	}

	return nil
}

////////////////////////////
// Postgres
////////////////////////////

// PostgresDialect struct.
type PostgresDialect struct{}

func (pg PostgresDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
            	id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, tableName)
}

func (pg PostgresDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES ($1, $2);", tableName)
}

func (pg PostgresDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied from %s ORDER BY id DESC", tableName))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m PostgresDialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id=$1 ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (pg PostgresDialect) deleteVersionSQL(tableName string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=$1;", tableName)
}

////////////////////////////
// MySQL
////////////////////////////

// MySQLDialect struct.
type MySQLDialect struct{}

func (m MySQLDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
                id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, tableName)
}

func (m MySQLDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES (?, ?);", tableName)
}

func (m MySQLDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied from %s ORDER BY id DESC", tableName))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m MySQLDialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id=? ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (m MySQLDialect) deleteVersionSQL(tableName string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=?;", tableName)
}

////////////////////////////
// MSSQL
////////////////////////////

// SqlServerDialect struct.
type SqlServerDialect struct{}

func (m SqlServerDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
                id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
                version_id BIGINT NOT NULL,
                is_applied BIT NOT NULL,
                tstamp DATETIME NULL DEFAULT CURRENT_TIMESTAMP
            );`, tableName)
}

func (m SqlServerDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES (@p1, @p2);", tableName)
}

func (m SqlServerDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied FROM %s ORDER BY id DESC", tableName))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m SqlServerDialect) migrationSQL() string {
	const tpl = `
WITH Migrations AS
(
    SELECT tstamp, is_applied,
    ROW_NUMBER() OVER (ORDER BY tstamp) AS 'RowNumber'
    FROM %s
	WHERE version_id=@p1
)
SELECT tstamp, is_applied
FROM Migrations
WHERE RowNumber BETWEEN 1 AND 2
ORDER BY tstamp DESC
`
	return fmt.Sprintf(tpl, TableName())
}

func (m SqlServerDialect) deleteVersionSQL(tableName string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=@p1;", tableName)
}

////////////////////////////
// sqlite3
////////////////////////////

// Sqlite3Dialect struct.
type Sqlite3Dialect struct{}

func (m Sqlite3Dialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                is_applied INTEGER NOT NULL,
                tstamp TIMESTAMP DEFAULT (datetime('now'))
            );`, tableName)
}

func (m Sqlite3Dialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES (?, ?);", tableName)
}

func (m Sqlite3Dialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied from %s ORDER BY id DESC", tableName))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m Sqlite3Dialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id=? ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (m Sqlite3Dialect) deleteVersionSQL(tableName string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=?;", tableName)
}

////////////////////////////
// Redshift
////////////////////////////

// RedshiftDialect struct.
type RedshiftDialect struct{}

func (rs RedshiftDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
            	id integer NOT NULL identity(1, 1),
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default sysdate,
                PRIMARY KEY(id)
            );`, tableName)
}

func (rs RedshiftDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES ($1, $2);", tableName)
}

func (rs RedshiftDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied from %s ORDER BY id DESC", tableName))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m RedshiftDialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id=$1 ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (rs RedshiftDialect) deleteVersionSQL(tableName string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=$1;", tableName)
}

////////////////////////////
// TiDB
////////////////////////////

// TiDBDialect struct.
type TiDBDialect struct{}

func (m TiDBDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, tableName)
}

func (m TiDBDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES (?, ?);", tableName)
}

func (m TiDBDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied from %s ORDER BY id DESC", tableName))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m TiDBDialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id=? ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (m TiDBDialect) deleteVersionSQL(tableName string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=?;", tableName)
}

////////////////////////////
// ClickHouse
////////////////////////////

// ClickHouseDialect struct.
type ClickHouseDialect struct{}

func (m ClickHouseDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
      version_id Int64,
      is_applied UInt8,
      date Date default now(),
      tstamp DateTime default now()
    )
    ENGINE = MergeTree()
	PRIMARY KEY (date)
	ORDER BY (date)
	SETTINGS index_granularity = 8192;
    `, tableName)
}

func (m ClickHouseDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied FROM %s ORDER BY tstamp DESC LIMIT 1", tableName))
	if err != nil {
		return nil, err
	}
	return rows, err
}

func (m ClickHouseDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES (?, ?)", tableName)
}

func (m ClickHouseDialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id = ? ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (m ClickHouseDialect) deleteVersionSQL(tableName string) string {
	return fmt.Sprintf("ALTER TABLE %s DELETE WHERE version_id = ?", tableName)
}
