package archiver

import (
	"bytes"
	"fmt"
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
	if len(columnNames) == 0 {
		err = fmt.Errorf("source table `%s` not found", cfg.Source.Table)
		return
	}
	columns, placeholderSet := buildColumnsAndPlaceholderSet(columnNames)

	selectStmt := buildSelectStmt(cfg.Source.Table, columns, cfg.Source.Where, cfg.Source.Limit)

	rowsEstimated, e4 := data.CheckSelectStmt(srcConn, cfg.Source.Table, selectStmt)
	if e4 != nil {
		err = e4
		return
	}

	uniqueKeys, e5 := data.GetUniqueKeyColumns(srcConn, cfg.Source.MySQL.Database, cfg.Source.Table)
	if e5 != nil {
		err = e5
		return
	}
	ukName := "PRIMARY"
	ukColumn, ukExists := uniqueKeys[ukName]
	if !ukExists {
		for ukName, ukColumn = range uniqueKeys {
			ukExists = true
			break
		}
	}
	ukColumns, ukPlaceholderSet := buildColumnsAndPlaceholderSet(ukColumn.Names)

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
		values, ukValues, rowQuantity, e1 := data.GetValues(srcConn, selectStmt, ukColumn.Positions)
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
			placeholderSets, ukPlaceholderSets := buildPlaceholders(placeholderSet, ukPlaceholderSet, rowQuantity)
			insertStmt = buildInsertStmt(cfg.Target.Table, columns, placeholderSets)
			if ukExists {
				deleteStmt = buildUniqueIndexDeleteStmt(cfg.Source.Table, ukColumns, ukPlaceholderSets)
			} else {
				deleteStmt = buildEntireMatchDeleteStmt(cfg.Source.Table, columns, placeholderSets, cfg.Source.Limit)
			}
		}
		firstLoop = false

		srcTx, e2 := srcConn.Beginx()
		if e2 != nil {
			err = e2
			return
		}
		tgtTx, e3 := tgtConn.Beginx()
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
			if inserts, e = data.ExecuteDMLStmt(tgtTx, insertStmt, values); e != nil {
				errs = append(errs, e.Error())
			}
		}()

		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			var e error
			if ukExists {
				deletes, e = data.ExecuteDMLStmt(srcTx, deleteStmt, ukValues)
			} else {
				deletes, e = data.ExecuteDMLStmt(srcTx, deleteStmt, values)
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
	fmt.Printf("TIME    start: %s, end: %s, duration: %s\n", sTime.Format(config.TimeFormat), eTime.Format(config.TimeFormat), eTime.Sub(sTime).String())
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
//  ukPlaceholderSets: (?, ...), (?, ...), ...
func buildPlaceholders(placeholderSet string, ukPlaceholderSet string, rowQuantity int64) (placeholderSets *string, ukPlaceholderSets *string) {
	f := func(i int64, s1 string, s2 string, b1 *bytes.Buffer, b2 *bytes.Buffer) {
		if i > 0 {
			b1.WriteString(", ")
			b2.WriteString(", ")
		}
		b1.WriteString(s1)
		b2.WriteString(s2)
		return
	}
	if ukPlaceholderSet == "" {
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
		f(i, placeholderSet, ukPlaceholderSet, buf1, buf2)
	}
	str1, str2 := buf1.String(), buf2.String()
	placeholderSets = &str1
	ukPlaceholderSets = &str2
	return
}

// buildSelectStmt
//  SELECT /* go-mysql-archiver */ `c1`, `c2`, `c3`, ... FROM `tb` WHERE c4 < 'xxx' LIMIT 1000
func buildSelectStmt(table string, columns string, where string, limit uint) (stmt string) {
	stmt = fmt.Sprintf("SELECT /* go-mysql-archiver */ %s FROM `%s` WHERE %s LIMIT %d", columns, table, where, limit)
	return
}

// buildInsertStmt
//  INSERT /* go-mysql-archiver */ INTO `tb` (`c1`, `c2`, `c3`, ...) VALUES (?, ?, ?, ...), (?, ?, ?, ...), ...
func buildInsertStmt(table string, columns string, placeholderSets *string) (stmt *string) {
	s := fmt.Sprintf("INSERT /* go-mysql-archiver */ INTO `%s` (%s) VALUES %s", table, columns, *placeholderSets)
	stmt = &s
	return
}

// buildUniqueIndexDeleteStmt
//  DELETE /* go-mysql-archiver */ FROM `tb` WHERE (`c1`, `c2`, ...) IN ((?, ?, ...), (?, ?, ...), ...)
func buildUniqueIndexDeleteStmt(table string, ukColumns string, ukPlaceholderSets *string) (stmt *string) {
	s := fmt.Sprintf("DELETE /* go-mysql-archiver */ FROM `%s` WHERE (%s) IN (%s)", table, ukColumns, *ukPlaceholderSets)
	stmt = &s
	return
}

// buildEntireMatchDeleteStmt
//  DELETE /* go-mysql-archiver */ FROM `tb` WHERE (`c1`, `c2`, `c3`, ...) IN ((?, ?, ?, ...), (?, ?, ?, ...), ...) LIMIT 1000
func buildEntireMatchDeleteStmt(table string, columns string, placeholderSets *string, limit uint) (stmt *string) {
	s := fmt.Sprintf("DELETE /* go-mysql-archiver */ FROM `%s` WHERE (%s) IN (%s) LIMIT %d", table, columns, *placeholderSets, limit)
	stmt = &s
	return
}
