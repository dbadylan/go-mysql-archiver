package data

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

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

func Explain(db *sql.DB, table string, where string) (keyName string, rowsEstimate int64, err error) {
	query := fmt.Sprintf("EXPLAIN /* go-archiver */ SELECT * FROM `%s`", table)
	if where != "" {
		query += " WHERE " + where
	}
	var rows *sql.Rows
	if rows, err = db.Query(query); err != nil {
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

type Keys struct {
	Details map[string]Property
	Elected string
}

type Property struct {
	ColumnNames []string
	NonUnique   int
	Nullable    string
}

func GetKeys(db *sql.DB, database string, table string) (keys Keys, err error) {
	query := `SELECT /* go-archiver */ INDEX_NAME, CONVERT(CONCAT('[', GROUP_CONCAT(CONCAT('"', COLUMN_NAME, '"') ORDER BY SEQ_IN_INDEX), ']'), JSON), MAX(NON_UNIQUE) c, MAX(NULLABLE), MAX(CARDINALITY)
FROM information_schema.STATISTICS
WHERE INDEX_TYPE = 'BTREE' AND IS_VISIBLE = 'YES' AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
GROUP BY INDEX_NAME ORDER BY c DESC`
	var rows *sql.Rows
	if rows, err = db.Query(query, database, table); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	details := make(map[string]Property)
	var c int64
	for rows.Next() {
		var (
			indexName   string
			columnNames []byte
			cardinality int64
		)
		var property Property
		if err = rows.Scan(&indexName, &columnNames, &property.NonUnique, &property.Nullable, &cardinality); err != nil {
			return
		}
		if err = json.Unmarshal(columnNames, &property.ColumnNames); err != nil {
			return
		}
		details[indexName] = property
		if cardinality <= c {
			continue
		}
		keys.Elected = indexName
		c = cardinality
	}
	keys.Details = details

	return
}

type SelectParam struct {
	DB         *sql.DB
	Table      string
	Where      string
	OrderBy    string
	Limit      int64
	KeyColumns map[string]struct{}
}

type Insert struct {
	Columns   string
	Values    *string
	ValueList *[]interface{}
}

type Delete struct {
	Where     *string
	ValueList *[]interface{}
}

type SelectResponse struct {
	Insert Insert
	Delete Delete

	Rows int64
}

func SelectRows(param *SelectParam) (resp *SelectResponse, err error) {
	query := fmt.Sprintf("SELECT /* go-archiver */ * FROM `%s`", param.Table)
	if param.Where != "" {
		query += " WHERE " + param.Where
	}
	if param.OrderBy != "" {
		query += " ORDER BY " + param.OrderBy
	}
	query += fmt.Sprintf(" LIMIT %d", param.Limit)

	var rows *sql.Rows
	if rows, err = param.DB.Query(query); err != nil {
		return
	}
	defer func() { _ = rows.Close() }()

	resp = new(SelectResponse)

	var columns []string
	if columns, err = rows.Columns(); err != nil {
		return
	}

	allColQty := len(columns)
	allColMaxIdx := allColQty - 1
	dest := make([]interface{}, allColQty)
	allColDict := make(map[string]struct{}, allColQty)
	var columnBuf bytes.Buffer
	for i := 0; i < allColQty; i++ {
		dest[i] = new([]byte)
		columnName := columns[i]
		allColDict[columnName] = struct{}{}
		columnBuf.WriteString("`")
		columnBuf.WriteString(columnName)
		columnBuf.WriteString("`")
		if i != allColMaxIdx {
			columnBuf.WriteString(", ")
		}
	}
	resp.Insert.Columns = columnBuf.String()

	keyColQty := len(param.KeyColumns)
	if keyColQty == 0 {
		keyColQty = allColQty
		param.KeyColumns = allColDict
	}

	var (
		valuesSubClauses []string
		whereSubClauses  []string
		allValueList     = make([]interface{}, 0, param.Limit*int64(allColQty))
		keyValueList     = make([]interface{}, 0, param.Limit*int64(keyColQty))
	)
	for rows.Next() {
		resp.Rows++

		if err = rows.Scan(dest...); err != nil {
			return
		}

		var (
			valuesSubClauseBuf bytes.Buffer
			columnExpressions  []string
		)
		valuesSubClauseBuf.WriteString("(")
		for i := 0; i < allColQty; i++ {
			value := *(dest[i].(*[]byte))
			allValueList = append(allValueList, value)

			valuesSubClauseBuf.WriteString("?")
			if i != allColMaxIdx {
				valuesSubClauseBuf.WriteString(", ")
			}

			if param.OrderBy != "" {
				continue
			}

			currentColumnName := columns[i]
			if _, exist := param.KeyColumns[currentColumnName]; !exist {
				continue
			}
			var operator string
			if value == nil {
				operator = "IS NULL"
			} else {
				operator = "= ?"
				keyValueList = append(keyValueList, value)
			}
			var colExprBuf bytes.Buffer
			colExprBuf.WriteString("`")
			colExprBuf.WriteString(currentColumnName)
			colExprBuf.WriteString("` ")
			colExprBuf.WriteString(operator)
			columnExpressions = append(columnExpressions, colExprBuf.String())
		}
		valuesSubClauseBuf.WriteString(")")
		valuesSubClauses = append(valuesSubClauses, valuesSubClauseBuf.String())

		if param.OrderBy != "" {
			continue
		}
		whereSubClauses = append(whereSubClauses, "("+strings.Join(columnExpressions, " AND ")+")")
	}

	valuesClause := strings.Join(valuesSubClauses, ", ")
	whereClause := strings.Join(whereSubClauses, " OR ")

	resp.Insert.Values = &valuesClause
	resp.Insert.ValueList = &allValueList
	resp.Delete.Where = &whereClause
	resp.Delete.ValueList = &keyValueList

	return
}

type InsertParam struct {
	Tx        *sql.Tx
	Table     string
	Columns   string
	Values    *string
	ValueList *[]interface{}
}

func InsertRows(param *InsertParam) (rowsAffected int64, err error) {
	query := fmt.Sprintf("INSERT /* go-archiver */ INTO `%s` (%s) VALUES %s", param.Table, param.Columns, *param.Values)
	var result sql.Result
	if result, err = param.Tx.Exec(query, *param.ValueList...); err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}

type DeleteParam struct {
	Tx        *sql.Tx
	Table     string
	Where     *string
	OrderBy   string
	Limit     int64
	ValueList *[]interface{}
}

func DeleteRows(param *DeleteParam) (rowsAffected int64, err error) {
	query := fmt.Sprintf("DELETE /* go-archiver */ FROM `%s`", param.Table)
	if *param.Where != "" {
		query += fmt.Sprintf(" WHERE %s", *param.Where)
	}
	var result sql.Result
	if param.OrderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s LIMIT %d", param.OrderBy, param.Limit)
		result, err = param.Tx.Exec(query)
	} else {
		query += fmt.Sprintf(" LIMIT %d", param.Limit)
		result, err = param.Tx.Exec(query, *param.ValueList...)
	}
	if err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}
