package data

import (
	"database/sql"
	"encoding/json"
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
	query := `SELECT /* go-mysql-archiver */ COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
                AND EXTRA NOT IN ('VIRTUAL GENERATED', 'STORED GENERATED')`
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

type IndexColumn struct {
	Names     []string
	Positions []int
}

func GetUniqueKeyColumns(db *sql.DB, database string, table string) (uniqueKeys map[string]IndexColumn, err error) {
	query := `SELECT /* go-mysql-archiver */ index_name, column_names, column_positions
FROM (
    SELECT s.INDEX_NAME index_name,
           GROUP_CONCAT(DISTINCT c.IS_NULLABLE) is_nullable,
           CONVERT(CONCAT('[', GROUP_CONCAT(CONCAT('"', c.COLUMN_NAME, '"') ORDER BY c.ORDINAL_POSITION SEPARATOR ', '), ']'), JSON) column_names,
           CONVERT(CONCAT('[', GROUP_CONCAT(c.ORDINAL_POSITION-1 ORDER BY c.ORDINAL_POSITION SEPARATOR ', '), ']'), JSON) column_positions
    FROM information_schema.STATISTICS s
    JOIN information_schema.COLUMNS c
    ON s.TABLE_SCHEMA = c.TABLE_SCHEMA
    AND s.TABLE_NAME = c.TABLE_NAME
    AND s.COLUMN_NAME = c.COLUMN_NAME
    WHERE s.NON_UNIQUE = 0
    AND s.IS_VISIBLE = 'YES'
    AND s.TABLE_SCHEMA = ?
    AND s.TABLE_NAME = ?
    GROUP BY s.INDEX_NAME
) t
WHERE is_nullable = 'NO'`

	var rows *sql.Rows
	if rows, err = db.Query(query, database, table); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	uniqueKeys = make(map[string]IndexColumn)
	for rows.Next() {
		var (
			indexName          string
			columnNamesRaw     []byte
			columnPositionsRaw []byte
			indexColumn        IndexColumn
		)
		if err = rows.Scan(&indexName, &columnNamesRaw, &columnPositionsRaw); err != nil {
			return
		}
		if err = json.Unmarshal(columnNamesRaw, &indexColumn.Names); err != nil {
			return
		}
		if err = json.Unmarshal(columnPositionsRaw, &indexColumn.Positions); err != nil {
			return
		}
		uniqueKeys[indexName] = indexColumn
	}

	return
}

func CheckSelectStmt(db *sql.DB, table string, query string) (rowsEstimate int64, err error) {
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
		_key          sql.NullString
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
			&_key,
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
		break
	}

	return
}

func CheckTargetTable(db *sql.DB, database string, table string) (count int, err error) {
	query := "SELECT /* go-mysql-archiver */ COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	err = db.QueryRow(query, database, table).Scan(&count)
	return
}

func GetValues(db *sql.DB, query string, columnQuantity int, ukColumnPositions []int, valueContainer *[]interface{}, ukValueContainer *[]interface{}) (rowQuantity int64, err error) {
	var rows *sql.Rows
	if rows, err = db.Query(query); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		dest := make([]interface{}, columnQuantity)
		for i := range dest {
			dest[i] = new(interface{})
		}
		if err = rows.Scan(dest...); err != nil {
			return
		}
		*valueContainer = append(*valueContainer, dest...)
		for _, ukColumnPosition := range ukColumnPositions {
			*ukValueContainer = append(*ukValueContainer, dest[ukColumnPosition])
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
