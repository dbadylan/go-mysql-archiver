package data

import (
	"database/sql"
	"fmt"

	"github.com/dbadylan/go-archiver/internal/config"

	_ "github.com/go-sql-driver/mysql"
)

func NewConn(m config.MySQL) (db *sql.DB, err error) {
	if db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&interpolateParams=true", m.Username, m.Password, m.Host, m.Port, m.Database, m.Charset)); err != nil {
		return
	}
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(2)
	err = db.Ping()
	return
}

func GetColumnNames(db *sql.DB, database string, table string) (columnNames []string, err error) {
	query := `SELECT /* go-archiver */ COLUMN_NAME
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA NOT IN ('VIRTUAL GENERATED', 'STORED GENERATED')
ORDER BY ORDINAL_POSITION`
	var rows *sql.Rows
	if rows, err = db.Query(query, database, table); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var columnName string
		if err = rows.Scan(&columnName); err != nil {
			return
		}
		columnNames = append(columnNames, columnName)
	}

	return
}

type Key struct {
	Name      string
	NonUnique uint8
	Nullable  string
}

func GetKeys(db *sql.DB, database string, table string) (keys []Key, err error) {
	query := `SELECT /* go-archiver */ INDEX_NAME, GROUP_CONCAT(DISTINCT NON_UNIQUE), GROUP_CONCAT(DISTINCT NULLABLE), MAX(CARDINALITY) c
FROM information_schema.STATISTICS
WHERE IS_VISIBLE = 'YES' AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
GROUP BY INDEX_NAME
ORDER BY c DESC`

	var rows *sql.Rows
	if rows, err = db.Query(query, database, table); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			key          Key
			_cardinality uint64
		)
		if err = rows.Scan(&key.Name, &key.NonUnique, &key.Nullable, &_cardinality); err != nil {
			return
		}
		keys = append(keys, key)
	}

	return
}

func GetKeyColumns(db *sql.DB, database string, table string, key string) (columns string, positions string, err error) {
	query := `SELECT /* go-archiver */ GROUP_CONCAT(c.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX),
       GROUP_CONCAT(c.ORDINAL_POSITION-1 ORDER BY s.SEQ_IN_INDEX)
FROM information_schema.STATISTICS s
JOIN information_schema.COLUMNS c
ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME AND s.COLUMN_NAME = c.COLUMN_NAME
WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.INDEX_NAME = ?`
	err = db.QueryRow(query, database, table, key).Scan(&columns, &positions)
	return
}

func CheckSyntax(db *sql.DB, table string, query string) (keyName string, rowsEstimate int64, err error) {
	var rows *sql.Rows
	if rows, err = db.Query("EXPLAIN " + query); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	var (
		_id           int
		_selectType   string
		Table         sql.NullString
		_partitions   sql.NullString
		_type         sql.NullString
		_possibleKeys sql.NullString
		KeyName       sql.NullString
		_keyLen       sql.NullInt32
		_ref          sql.NullString
		Rows          sql.NullInt64
		_filtered     sql.NullFloat64
		_extra        sql.NullString
	)
	for rows.Next() {
		if err = rows.Scan(
			&_id,
			&_selectType,
			&Table,
			&_partitions,
			&_type,
			&_possibleKeys,
			&KeyName,
			&_keyLen,
			&_ref,
			&Rows,
			&_filtered,
			&_extra,
		); err != nil {
			return
		}
		if Table.String != table {
			continue
		}
		rowsEstimate = Rows.Int64
		keyName = KeyName.String
		break
	}

	return
}

func CheckTargetTable(db *sql.DB, database string, table string) (count int, err error) {
	query := "SELECT /* go-archiver */ COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	err = db.QueryRow(query, database, table).Scan(&count)
	return
}

func GetValues(db *sql.DB, query string, keyColumnPositions []int, valueContainer *[]interface{}, keyValueContainer *[]interface{}) (rowQuantity int64, err error) {
	var rows *sql.Rows
	if rows, err = db.Query(query); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	var columns []string
	if columns, err = rows.Columns(); err != nil {
		return
	}
	columnQuantity := len(columns)

	for rows.Next() {
		dest := make([]interface{}, columnQuantity)
		for i := range dest {
			dest[i] = new(interface{})
		}
		if err = rows.Scan(dest...); err != nil {
			return
		}
		*valueContainer = append(*valueContainer, dest...)
		for _, keyColumnPosition := range keyColumnPositions {
			*keyValueContainer = append(*keyValueContainer, dest[keyColumnPosition])
		}
		rowQuantity++
	}

	return
}

func ExecuteDMLStmt(tx *sql.Tx, query *string, values *[]interface{}) (rowsAffected int64, err error) {
	var result sql.Result
	if result, err = tx.Exec(*query, *values...); err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}
