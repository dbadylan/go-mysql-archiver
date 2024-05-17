package data

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dbadylan/go-archiver/internal/config"

	_ "github.com/go-sql-driver/mysql"
)

func NewDB(m config.MySQL) (db *sql.DB, err error) {
	if db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&interpolateParams=true", m.Username, m.Password, m.Address, m.Database, m.Charset)); err != nil {
		return
	}
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(2)
	err = db.Ping()
	return
}

func explain(db *sql.DB, table string, where string) (keyName string, rowsEstimate int64, err error) {
	query := fmt.Sprintf("EXPLAIN /* go-archiver */ SELECT 1 FROM `%s`", table)
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

func getUniqueKey(db *sql.DB, database string, table string) (exist bool, columns []string, positions []int, err error) {
	query := `SELECT /* go-archiver */ CONVERT(CONCAT('[', GROUP_CONCAT(CONCAT('"', c.COLUMN_NAME, '"') ORDER BY s.SEQ_IN_INDEX), ']'), JSON) columns, CONVERT(CONCAT('[', GROUP_CONCAT(c.ORDINAL_POSITION-1 ORDER BY s.SEQ_IN_INDEX), ']'), JSON) positions, MAX(NON_UNIQUE) non_unique, MAX(NULLABLE) nullable, MAX(CARDINALITY) cardinality
FROM information_schema.STATISTICS s
JOIN information_schema.COLUMNS c
ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME AND s.COLUMN_NAME = c.COLUMN_NAME
WHERE s.INDEX_TYPE = 'BTREE' AND non_unique = 0 AND nullable = '' AND s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ?
GROUP BY s.INDEX_NAME
ORDER BY cardinality DESC
LIMIT 1`
	var (
		columnsByte   []byte
		positionsByte []byte
		_nonUnique    interface{}
		_nullable     interface{}
		_cardinality  interface{}
	)
	if err = db.QueryRow(query, database, table).Scan(&columnsByte, &positionsByte, &_nonUnique, &_nullable, &_cardinality); err != nil && errors.Is(err, sql.ErrNoRows) {
		err = nil
		return
	}
	if err = json.Unmarshal(columnsByte, &columns); err != nil {
		return
	}
	if err = json.Unmarshal(positionsByte, &positions); err != nil {
		return
	}
	exist = true
	return
}

func getOtherKey(db *sql.DB, database string, table string) (exist bool, columns []string, positions []int, err error) {
	query := `SELECT /* go-archiver */ CONVERT(CONCAT('[', GROUP_CONCAT(CONCAT('"', c.COLUMN_NAME, '"') ORDER BY SEQ_IN_INDEX), ']'), JSON) columns, CONVERT(CONCAT('[', GROUP_CONCAT(c.ORDINAL_POSITION-1 ORDER BY s.SEQ_IN_INDEX), ']'), JSON) positions, MAX(CARDINALITY) cardinality
FROM information_schema.STATISTICS s
JOIN information_schema.COLUMNS c
ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME AND s.COLUMN_NAME = c.COLUMN_NAME
WHERE s.INDEX_TYPE = 'BTREE' AND s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ?
GROUP BY s.INDEX_NAME
ORDER BY cardinality DESC
LIMIT 1`
	var (
		columnsByte   []byte
		positionsByte []byte
		_cardinality  interface{}
	)
	if err = db.QueryRow(query, database, table).Scan(&columnsByte, &positionsByte, &_cardinality); err != nil && errors.Is(err, sql.ErrNoRows) {
		err = nil
		return
	}
	if err = json.Unmarshal(columnsByte, &columns); err != nil {
		return
	}
	if err = json.Unmarshal(positionsByte, &positions); err != nil {
		return
	}
	exist = true
	return
}

func getKeyByName(db *sql.DB, database string, table string, key string) (exist bool, columns []string, positions []int, err error) {
	query := `SELECT /* go-archiver */ CONVERT(CONCAT('[', GROUP_CONCAT(CONCAT('"', c.COLUMN_NAME, '"') ORDER BY SEQ_IN_INDEX), ']'), JSON) columns, CONVERT(CONCAT('[', GROUP_CONCAT(c.ORDINAL_POSITION-1 ORDER BY s.SEQ_IN_INDEX), ']'), JSON) positions
FROM information_schema.STATISTICS s
JOIN information_schema.COLUMNS c
ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME AND s.COLUMN_NAME = c.COLUMN_NAME
WHERE s.INDEX_TYPE = 'BTREE' AND s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.INDEX_NAME = ?
GROUP BY s.INDEX_NAME`
	var (
		columnsByte   []byte
		positionsByte []byte
	)
	if err = db.QueryRow(query, database, table, key).Scan(&columnsByte, &positionsByte); err != nil && errors.Is(err, sql.ErrNoRows) {
		err = nil
		return
	}
	if err = json.Unmarshal(columnsByte, &columns); err != nil {
		return
	}
	if err = json.Unmarshal(positionsByte, &positions); err != nil {
		return
	}
	exist = true
	return
}

type Analysis struct {
	RowsEstimated int64
	QueryType     int
	Columns       []string
	Positions     []int
}

func AnalyzeQuery(db *sql.DB, database string, table string, where string) (analysis Analysis, err error) {
	var (
		keyName string
		exist   bool
	)
	if keyName, analysis.RowsEstimated, err = explain(db, table, where); err != nil {
		return
	}
	if exist, analysis.Columns, analysis.Positions, err = getUniqueKey(db, database, table); err != nil {
		return
	}
	if exist {
		analysis.QueryType = 1
		return
	}
	if keyName == "" {
		goto F
	}
	if exist, analysis.Columns, analysis.Positions, err = getKeyByName(db, database, table, keyName); err != nil {
		return
	}
	if exist {
		analysis.QueryType = 2
		return
	}
F:
	if exist, analysis.Columns, analysis.Positions, err = getOtherKey(db, database, table); err != nil {
		return
	}
	if exist {
		analysis.QueryType = 2
		return
	}
	analysis.QueryType = 3
	return
}

type SelectParam struct {
	DB       *sql.DB
	Table    string
	Where    string
	Limit    int64
	Analysis Analysis
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
	if param.Analysis.QueryType == 2 {
		query += " ORDER BY " + "`" + strings.Join(param.Analysis.Columns, "`, `") + "`"
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
	var columnBuf bytes.Buffer
	for i := 0; i < allColQty; i++ {
		dest[i] = new([]byte)
		columnName := columns[i]
		columnBuf.WriteString("`")
		columnBuf.WriteString(columnName)
		columnBuf.WriteString("`")
		if i != allColMaxIdx {
			columnBuf.WriteString(", ")
		}
	}
	resp.Insert.Columns = columnBuf.String()

	var keyValueMaxLen int64
	switch param.Analysis.QueryType {
	case 1:
		keyValueMaxLen = param.Limit * int64(len(param.Analysis.Columns))
	case 3:
		keyValueMaxLen = param.Limit * int64(allColQty)
	}

	var (
		valuesSubClauses []string
		whereSubClauses  []string
		allValueList     = make([]interface{}, 0, param.Limit*int64(allColQty))
		keyValueList     = make([]interface{}, 0, keyValueMaxLen)
	)
	for rows.Next() {
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

			if param.Analysis.QueryType == 3 {
				var operator string
				if value == nil {
					operator = "IS NULL"
				} else {
					operator = "= ?"
					keyValueList = append(keyValueList, value)
				}
				var colExprBuf bytes.Buffer
				colExprBuf.WriteString("`")
				colExprBuf.WriteString(columns[i])
				colExprBuf.WriteString("` ")
				colExprBuf.WriteString(operator)
				columnExpressions = append(columnExpressions, colExprBuf.String())
			}
		}

		switch param.Analysis.QueryType {
		case 1:
			var placeholders []string
			for _, position := range param.Analysis.Positions {
				keyValueList = append(keyValueList, allValueList[int64(allColQty)*resp.Rows+int64(position)])
				placeholders = append(placeholders, "?")
			}
			whereSubClauses = append(whereSubClauses, "("+strings.Join(placeholders, ", ")+")")
		case 3:
			whereSubClauses = append(whereSubClauses, "("+strings.Join(columnExpressions, " AND ")+")")
		}

		valuesSubClauseBuf.WriteString(")")
		valuesSubClauses = append(valuesSubClauses, valuesSubClauseBuf.String())

		resp.Rows++
	}

	valuesClause := strings.Join(valuesSubClauses, ", ")

	var whereClause string
	switch param.Analysis.QueryType {
	case 1:
		whereClause = "(`" + strings.Join(param.Analysis.Columns, "`, `") + "`) IN (" + strings.Join(whereSubClauses, ", ") + ")"
	case 2:
		whereClause = param.Where
	case 3:
		whereClause = strings.Join(whereSubClauses, " OR ")
	}

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
	Limit     int64
	ValueList *[]interface{}
	Analysis  Analysis
}

func DeleteRows(param *DeleteParam) (rowsAffected int64, err error) {
	query := fmt.Sprintf("DELETE /* go-archiver */ FROM `%s`", param.Table)
	if *param.Where != "" {
		query += fmt.Sprintf(" WHERE %s", *param.Where)
	}
	var result sql.Result
	switch param.Analysis.QueryType {
	case 1:
		result, err = param.Tx.Exec(query, *param.ValueList...)
	case 2:
		query += fmt.Sprintf(" ORDER BY %s LIMIT %d", "`"+strings.Join(param.Analysis.Columns, "`, `")+"`", param.Limit)
		result, err = param.Tx.Exec(query)
	case 3:
		query += fmt.Sprintf(" LIMIT %d", param.Limit)
		result, err = param.Tx.Exec(query, *param.ValueList...)
	default:
		return
	}
	if err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}
