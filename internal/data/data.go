package data

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/dbadylan/go-archiver/internal/config"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func NewConn(m config.MySQL) (db *sqlx.DB, err error) {
	if db, err = sqlx.Connect("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&interpolateParams=true", m.Username, m.Password, m.Host, m.Port, m.Database, m.Charset)); err != nil {
		return
	}
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(2)
	return
}

func GetColumnNames(db *sqlx.DB, database string, table string) (columnNames []string, err error) {
	query := `SELECT /* go-mysql-archiver */ COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = ?
                AND TABLE_NAME = ?
                AND EXTRA NOT IN ('VIRTUAL GENERATED', 'STORED GENERATED')`
	err = db.Select(&columnNames, query, database, table)
	return
}

type IndexColumn struct {
	Names     []string
	Positions []int
}

func GetUniqueKeyColumns(db *sqlx.DB, database string, table string) (uniqueKeys map[string]IndexColumn, err error) {
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

	var rows *sqlx.Rows
	if rows, err = db.Queryx(query, database, table); err != nil {
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

func CheckSelectStmt(db *sqlx.DB, table string, query string) (rowsEstimate int64, err error) {
	type Explain struct {
		Id           int             `db:"id"`
		SelectType   string          `db:"select_type"`
		Table        sql.NullString  `db:"table"`
		Partitions   sql.NullString  `db:"partitions"`
		Type         sql.NullString  `db:"type"`
		PossibleKeys sql.NullString  `db:"possible_keys"`
		Key          sql.NullString  `db:"key"`
		KeyLen       sql.NullInt32   `db:"key_len"`
		Ref          sql.NullString  `db:"ref"`
		Rows         sql.NullInt64   `db:"rows"`
		Filtered     sql.NullFloat64 `db:"filtered"`
		Extra        sql.NullString  `db:"Extra"`
	}

	var rows *sqlx.Rows
	if rows, err = db.Queryx("EXPLAIN " + query); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		explain := new(Explain)
		if err = rows.StructScan(explain); err != nil {
			return
		}
		if explain.Table.String != table {
			continue
		}
		rowsEstimate = explain.Rows.Int64
		break
	}

	return
}

func CheckTargetTable(db *sqlx.DB, database string, table string) (count int, err error) {
	query := "SELECT /* go-mysql-archiver */ COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	err = db.QueryRowx(query, database, table).Scan(&count)
	return
}

func GetValues(db *sqlx.DB, query string, ukColPositions []int) (values []interface{}, ukValues []interface{}, rowQuantity int64, err error) {
	var rows *sqlx.Rows
	if rows, err = db.Queryx(query); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var vals []interface{}
		if vals, err = rows.SliceScan(); err != nil {
			return
		}
		values = append(values, vals...)
		for _, ukColPosition := range ukColPositions {
			ukValues = append(ukValues, vals[ukColPosition])
		}
		rowQuantity++
	}

	return
}

func ExecuteDMLStmt(tx *sqlx.Tx, query *string, values []interface{}) (rowsAffected int64, err error) {
	var result sql.Result
	if result, err = tx.Exec(*query, values...); err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}
