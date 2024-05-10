package archiver

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbadylan/go-archiver/internal/config"
	"github.com/dbadylan/go-archiver/internal/data"
)

func Run(cfg *config.Config) (err error) {
	sTime := time.Now().Local()

	srcConn, e1 := data.NewConn(cfg.Source.MySQL)
	if e1 != nil {
		err = e1
		return
	}
	defer func() { _ = srcConn.Close() }()

	tgtConn, e2 := data.NewConn(cfg.Target.MySQL)
	if e2 != nil {
		err = e2
		return
	}
	defer func() { _ = tgtConn.Close() }()

	columnNames, e3 := data.GetColumnNames(srcConn, cfg.Source.MySQL.Database, cfg.Source.Table)
	if e3 != nil {
		err = e3
		return
	}
	columnQuantity := len(columnNames)
	if columnQuantity == 0 {
		err = fmt.Errorf("source table `%s` not found", cfg.Source.Table)
		return
	}
	columns, placeholderSet := buildColumnsAndPlaceholderSet(columnNames)
	valueMaxCapacity := columnQuantity * int(cfg.Source.Limit)

	simpleStmt := buildSimpleSelectStmt(cfg.Source.Table, cfg.Source.Where)
	explainKeyName, rowsEstimated, e4 := data.CheckSyntax(srcConn, cfg.Source.Table, simpleStmt)
	if e4 != nil {
		err = e4
		return
	}

	keys, e5 := data.GetKeys(srcConn, cfg.Source.Database, cfg.Source.Table)
	if e5 != nil {
		err = e5
		return
	}
	pickedKeyType, pickedKeyName := getKey(keys)

	var keyName string
	if explainKeyName != "" {
		keyName = explainKeyName
	} else {
		keyName = pickedKeyName
	}

	var (
		keyColumns          string
		keyPlaceholderSet   string
		keyColumnPositions  []int
		keyValueMaxCapacity int
		selectStmt          string
	)
	if keyName != "" {
		keyCols, keyColPoss, e := data.GetKeyColumns(srcConn, cfg.Source.Database, cfg.Source.Table, keyName)
		if e != nil {
			err = e
			return
		}
		keyColumnNames := strings.Split(keyCols, ",")
		keyColumns, keyPlaceholderSet = buildColumnsAndPlaceholderSet(keyColumnNames)
		for _, keyColPos := range strings.Split(keyColPoss, ",") {
			pos, e := strconv.ParseInt(keyColPos, 10, 32)
			if e != nil {
				err = e
				return
			}
			keyColumnPositions = append(keyColumnPositions, int(pos))
		}
		keyValueMaxCapacity = len(keyColumnNames) * int(cfg.Source.Limit)
		selectStmt = buildOrderSelectStmt(cfg.Source.Table, columns, cfg.Source.Where, keyColumns, cfg.Source.Limit)
	} else {
		selectStmt = buildSelectStmt(cfg.Source.Table, columns, cfg.Source.Where, cfg.Source.Limit)
	}

	tgtTableCount, e6 := data.CheckTargetTable(tgtConn, cfg.Target.Database, cfg.Target.Table)
	if e6 != nil {
		err = e6
		return
	}
	if tgtTableCount != 1 {
		err = fmt.Errorf("target table `%s` not found", cfg.Target.Table)
		return
	}

	var (
		rowsSelect int64
		rowsInsert int64
		rowsDelete int64
		exitChan   = make(chan struct{}, 1)
	)
	defer func() { exitChan <- struct{}{} }()
	if cfg.Progress != 0 {
		go func() {
			ticker := time.NewTicker(cfg.Progress)
			defer ticker.Stop()
			for {
				select {
				case ts := <-ticker.C:
					fmt.Printf("[%s] progress: %d/%d\n", ts.Local().Format(config.TimeFormat), rowsSelect, rowsEstimated)
				case <-exitChan:
					return
				}
			}
		}()
	}

	var (
		insertStmt *string
		deleteStmt *string
		firstLoop  = true
		lastLoop   = false
	)
	for {
		valueContainer := make([]interface{}, 0, valueMaxCapacity)
		keyValueContainer := make([]interface{}, 0, keyValueMaxCapacity)
		rowQuantity, e1 := data.GetValues(srcConn, selectStmt, keyColumnPositions, &valueContainer, &keyValueContainer)
		if e1 != nil {
			err = e1
			return
		}
		if rowQuantity == 0 {
			break
		}
		if rowQuantity < int64(cfg.Source.Limit) {
			lastLoop = true
		}

		if firstLoop || lastLoop {
			placeholderSets, keyPlaceholderSets := buildPlaceholders(placeholderSet, keyPlaceholderSet, rowQuantity)
			insertStmt = buildInsertStmt(cfg.Target.Table, columns, placeholderSets)
			switch pickedKeyType {
			case 0:
				deleteStmt = buildDeleteByRowStmt(cfg.Source.Table, columns, placeholderSets, cfg.Source.Limit)
			case 1:
				deleteStmt = buildDeleteByUniqueKeyStmt(cfg.Source.Table, keyColumns, keyPlaceholderSets)
			case 2:
				deleteStmt = buildDeleteByKeyStmt(cfg.Source.Table, keyColumns, keyPlaceholderSets, cfg.Source.Limit)
			}
		}
		firstLoop = false

		srcTx, e2 := srcConn.Begin()
		if e2 != nil {
			err = e2
			return
		}
		tgtTx, e3 := tgtConn.Begin()
		if e3 != nil {
			err = e3
			return
		}

		var (
			waitGrp sync.WaitGroup
			inserts int64
			deletes int64
			errs    []string
		)

		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			var e error
			if inserts, e = data.ExecuteDMLStmt(tgtTx, insertStmt, &valueContainer); e != nil {
				errs = append(errs, e.Error())
			}
		}()

		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			var e error
			switch pickedKeyType {
			case 0:
				deletes, e = data.ExecuteDMLStmt(srcTx, deleteStmt, &valueContainer)
			case 1, 2:
				deletes, e = data.ExecuteDMLStmt(srcTx, deleteStmt, &keyValueContainer)
			}
			if e != nil {
				errs = append(errs, e.Error())
			}
		}()

		waitGrp.Wait()

		if len(errs) != 0 {
			err = fmt.Errorf(strings.Join(errs, "\n"))
			return
		}

		if inserts < deletes {
			err = fmt.Errorf("rows deleted(%d) larger than inserted(%d), rollback and exit", deletes, inserts)
			return
		}

		if err = tgtTx.Commit(); err != nil {
			return
		}
		if err = srcTx.Commit(); err != nil {
			return
		}

		rowsSelect += rowQuantity
		rowsInsert += inserts
		rowsDelete += deletes

		if lastLoop {
			break
		}

		if cfg.Sleep == 0 {
			continue
		}
		time.Sleep(cfg.Sleep)
	}

	eTime := time.Now().Local()

	if !cfg.Statistics {
		return
	}
	fmt.Println()
	fmt.Printf("TIME    start: %s, end: %s, duration: %s\n", sTime.Format(config.TimeFormat), eTime.Format(config.TimeFormat), eTime.Sub(sTime).Truncate(time.Second).String())
	fmt.Printf("SOURCE  host: %s, port: %d, database: %s, table: %s, charset: %s\n", cfg.Source.Host, cfg.Source.Port, cfg.Source.Database, cfg.Source.Table, cfg.Source.Charset)
	fmt.Printf("TARGET  host: %s, port: %d, database: %s, table: %s, charset: %s\n", cfg.Target.Host, cfg.Target.Port, cfg.Target.Database, cfg.Target.Table, cfg.Target.Charset)
	fmt.Printf("ACTION  select: %d, insert: %d, delete: %d\n", rowsSelect, rowsInsert, rowsDelete)

	return
}

// buildColumnsAndPlaceholderSet
//  columns: `c1`, `c2`, `c3`, ...
//  placeholderSet: (?, ?, ?, ...)
func buildColumnsAndPlaceholderSet(columnNames []string) (columns string, placeholderSet string) {
	if len(columnNames) == 0 {
		return
	}
	buf1, buf2 := new(bytes.Buffer), new(bytes.Buffer)
	buf2.WriteString("(")
	for index, columnName := range columnNames {
		if index > 0 {
			buf1.WriteString(", ")
			buf2.WriteString(", ")
		}
		buf1.WriteString("`")
		buf1.WriteString(columnName)
		buf1.WriteString("`")
		buf2.WriteString("?")
	}
	buf2.WriteString(")")
	columns = buf1.String()
	placeholderSet = buf2.String()
	return
}

// buildPlaceholders
//  placeholderSets: (?, ?, ?, ...), (?, ?, ?, ...), ...
//  keyPlaceholderSets: (?, ...), (?, ...), ...
func buildPlaceholders(placeholderSet string, keyPlaceholderSet string, rowQuantity int64) (placeholderSets *string, keyPlaceholderSets *string) {
	f := func(i int64, s1 string, s2 string, b1 *bytes.Buffer, b2 *bytes.Buffer) {
		if i > 0 {
			b1.WriteString(", ")
			b2.WriteString(", ")
		}
		b1.WriteString(s1)
		b2.WriteString(s2)
		return
	}
	if keyPlaceholderSet == "" {
		f = func(i int64, s string, _ string, b *bytes.Buffer, _ *bytes.Buffer) {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(s)
			return
		}
	}
	buf1, buf2 := new(bytes.Buffer), new(bytes.Buffer)
	for i := int64(0); i < rowQuantity; i++ {
		f(i, placeholderSet, keyPlaceholderSet, buf1, buf2)
	}
	str1, str2 := buf1.String(), buf2.String()
	placeholderSets = &str1
	keyPlaceholderSets = &str2
	return
}

// getKey
//  0: no key
//  1: unique key
//  2: key
func getKey(keys []data.Key) (keyType int, keyName string) {
	var (
		uniqueKeys []string
		otherKeys  []string
	)
	for _, key := range keys {
		if key.NonUnique == 0 && key.Nullable == "" {
			uniqueKeys = append(uniqueKeys, key.Name)
			continue
		}
		otherKeys = append(otherKeys, key.Name)
	}

	if len(uniqueKeys) > 0 {
		keyType = 1
		keyName = uniqueKeys[0]
		return
	}
	if len(otherKeys) > 0 {
		keyType = 2
		keyName = otherKeys[0]
		return
	}

	return
}

// buildSimpleSelectStmt
//  SELECT /* go-archiver */ COUNT(*) FROM `tb` WHERE c4 < 'xxx'
func buildSimpleSelectStmt(table string, where string) (stmt string) {
	stmt = fmt.Sprintf("SELECT /* go-archiver */ COUNT(*) FROM `%s` WHERE %s", table, where)
	return
}

// buildSelectStmt
//  SELECT /* go-archiver */ `c1`, `c2`, `c3`, ... FROM `tb` WHERE c4 < 'xxx' LIMIT 1000
func buildSelectStmt(table string, columns string, where string, limit uint) (stmt string) {
	stmt = fmt.Sprintf("SELECT /* go-archiver */ %s FROM `%s` WHERE %s LIMIT %d", columns, table, where, limit)
	return
}

// buildOrderSelectStmt
//  SELECT /* go-archiver */ `c1`, `c2`, `c3`, ... FROM `tb` WHERE c4 < 'xxx' ORDER BY `c4` LIMIT 1000
func buildOrderSelectStmt(table string, columns string, where string, keyColumns string, limit uint) (stmt string) {
	stmt = fmt.Sprintf("SELECT /* go-archiver */ %s FROM `%s` WHERE %s ORDER BY %s LIMIT %d", columns, table, where, keyColumns, limit)
	return
}

// buildInsertStmt
//  INSERT /* go-archiver */ INTO `tb` (`c1`, `c2`, `c3`, ...) VALUES (?, ?, ?, ...), (?, ?, ?, ...), ...
func buildInsertStmt(table string, columns string, placeholderSets *string) (stmt *string) {
	s := fmt.Sprintf("INSERT /* go-archiver */ INTO `%s` (%s) VALUES %s", table, columns, *placeholderSets)
	stmt = &s
	return
}

// buildDeleteByUniqueKeyStmt
//  DELETE /* go-archiver */ FROM `tb` WHERE (`c1`, `c2`, ...) IN ((?, ?, ...), (?, ?, ...), ...)
func buildDeleteByUniqueKeyStmt(table string, keyColumns string, keyPlaceholderSets *string) (stmt *string) {
	s := fmt.Sprintf("DELETE /* go-archiver */ FROM `%s` WHERE (%s) IN (%s)", table, keyColumns, *keyPlaceholderSets)
	stmt = &s
	return
}

// buildDeleteByKeyStmt
//  DELETE /* go-archiver */ FROM `tb` WHERE (`c1`, `c2`, ...) IN ((?, ?, ...), (?, ?, ...), ...) ORDER BY `c1`, `c2`, ... LIMIT 1000
func buildDeleteByKeyStmt(table string, keyColumns string, keyPlaceholderSets *string, limit uint) (stmt *string) {
	s := fmt.Sprintf("DELETE /* go-archiver */ FROM `%s` WHERE (%s) IN (%s) ORDER BY %s LIMIT %d", table, keyColumns, *keyPlaceholderSets, keyColumns, limit)
	stmt = &s
	return
}

// buildDeleteByRowStmt
//  DELETE /* go-archiver */ FROM `tb` WHERE (`c1`, `c2`, `c3`, ...) IN ((?, ?, ?, ...), (?, ?, ?, ...), ...) LIMIT 1000
func buildDeleteByRowStmt(table string, columns string, placeholderSets *string, limit uint) (stmt *string) {
	s := fmt.Sprintf("DELETE /* go-archiver */ FROM `%s` WHERE (%s) IN (%s) LIMIT %d", table, columns, *placeholderSets, limit)
	stmt = &s
	return
}
