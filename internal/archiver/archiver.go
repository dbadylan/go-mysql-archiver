package archiver

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dbadylan/go-archiver/internal/config"
	"github.com/dbadylan/go-archiver/internal/data"
)

func Run(cfg *config.Config) (err error) {
	var (
		rowsAffected  int64
		rowsEstimated int64
		errorChan     = make(chan error, 1)
		infoChan      = make(chan string, 1)
		exitChan      = make(chan struct{}, 1)
		interval      = time.NewTicker(cfg.Interval)
	)
	defer func() { exitChan <- struct{}{} }()
	go func() {
		for {
			select {
			case e := <-errorChan:
				fmt.Printf("%+v\n", e)
			case i := <-infoChan:
				if cfg.Quiet {
					continue
				}
				fmt.Printf("[%s] %s\n", time.Now().Local().Format("2006-01-02 15:04:05"), i)
			case t := <-interval.C:
				if cfg.Quiet {
					continue
				}
				fmt.Printf("[%s] progress: %d/%d\n", t.Local().Format("2006-01-02 15:04:05"), rowsAffected, rowsEstimated)
			case <-exitChan:
				return
			}
		}
	}()

	srcConn, e1 := data.NewConn(cfg.Source.MySQL)
	if e1 != nil {
		errorChan <- e1
		return
	}
	defer func() { _ = srcConn.Close() }()
	infoChan <- fmt.Sprintf("source database: %s@%s:%d", cfg.Source.Database, cfg.Source.Host, cfg.Source.Port)

	tgtConn, e2 := data.NewConn(cfg.Target.MySQL)
	if e2 != nil {
		errorChan <- e2
		return
	}
	defer func() { _ = tgtConn.Close() }()
	infoChan <- fmt.Sprintf("target database: %s@%s:%d", cfg.Target.Database, cfg.Target.Host, cfg.Target.Port)

	columnNames, e3 := data.GetColumnNames(srcConn, cfg.Source.MySQL.Database, cfg.Source.Table)
	if e3 != nil {
		errorChan <- e3
		return
	}
	if len(columnNames) == 0 {
		errorChan <- errors.New("source table not found")
		return
	}
	columns, placeholderSet := buildColumnsAndPlaceholderSet(columnNames)

	selectStmt := buildSelectStmt(cfg.Source.Table, columns, cfg.Source.Where, cfg.Source.Limit)

	rowsEstimated, e3 = data.CheckSelectStmt(srcConn, cfg.Source.Table, selectStmt)
	if e3 != nil {
		errorChan <- e3
		return
	}
	infoChan <- fmt.Sprintf("source table: %s, where clause: %s, estimate rows: %d", cfg.Source.Table, cfg.Source.Where, rowsEstimated)

	uniqueKeys, e4 := data.GetUniqueKeyColumns(srcConn, cfg.Source.MySQL.Database, cfg.Source.Table)
	if e4 != nil {
		errorChan <- e4
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
	if ukExists {
		infoChan <- fmt.Sprintf("use non-null unique key: %s", ukName)
	} else {
		infoChan <- "non-null unique key not found"
	}

	tgtTableCount, e5 := data.CheckTargetTable(tgtConn, cfg.Target.Database, cfg.Target.Table)
	if e5 != nil {
		errorChan <- e5
		return
	}
	if tgtTableCount != 1 {
		errorChan <- errors.New("target table not found")
		return
	}
	infoChan <- fmt.Sprintf("target table: %s", cfg.Target.Table)

	var (
		insertStmt *string
		deleteStmt *string
		firstLoop  = true
		lastLoop   = false
	)
	for {
		values, ukValues, rowsFetched, e1 := data.GetValues(srcConn, selectStmt, ukColumn.Positions)
		if e1 != nil {
			errorChan <- e1
			break
		}
		if rowsFetched == 0 {
			break
		}
		if rowsFetched < cfg.Source.Limit {
			lastLoop = true
		}

		if firstLoop || lastLoop {
			placeholderSets, ukPlaceholderSets := buildPlaceholders(placeholderSet, ukPlaceholderSet, rowsFetched)
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
			errorChan <- e2
			break
		}
		tgtTx, e3 := tgtConn.Beginx()
		if e3 != nil {
			errorChan <- e3
			break
		}

		var (
			waitGrp sync.WaitGroup
			inserts int64
			deletes int64
			success = true
		)

		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			var err error
			if inserts, err = data.ExecuteDMLStmt(tgtTx, insertStmt, values); err != nil {
				errorChan <- err
				success = false
			}
		}()

		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			var err error
			if ukExists {
				deletes, err = data.ExecuteDMLStmt(srcTx, deleteStmt, ukValues)
			} else {
				deletes, err = data.ExecuteDMLStmt(srcTx, deleteStmt, values)
			}
			if err != nil {
				errorChan <- err
				success = false
			}
		}()

		waitGrp.Wait()

		if !success {
			break
		}

		if inserts < deletes {
			errorChan <- errors.New("rows deleted larger than inserted, rollback and exit")
			break
		}

		if e3 = tgtTx.Commit(); e3 != nil {
			errorChan <- e3
			break
		}
		if e3 = srcTx.Commit(); e3 != nil {
			errorChan <- e3
			break
		}

		rowsAffected += int64(rowsFetched)

		if lastLoop {
			break
		}

		if cfg.Sleep == 0 {
			continue
		}
		time.Sleep(cfg.Sleep)
	}

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
func buildPlaceholders(placeholderSet string, ukPlaceholderSet string, rowQuantity uint) (placeholderSets *string, ukPlaceholderSets *string) {
	f := func(i uint, s1 string, s2 string, b1 *bytes.Buffer, b2 *bytes.Buffer) {
		if i > 0 {
			b1.WriteString(", ")
			b2.WriteString(", ")
		}
		b1.WriteString(s1)
		b2.WriteString(s2)
		return
	}
	if ukPlaceholderSet == "" {
		f = func(i uint, s string, _ string, b *bytes.Buffer, _ *bytes.Buffer) {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(s)
			return
		}
	}
	buf1, buf2 := new(bytes.Buffer), new(bytes.Buffer)
	for i := uint(0); i < rowQuantity; i++ {
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
func buildInsertStmt(table string, columns string, placeholderSet *string) (stmt *string) {
	*stmt = fmt.Sprintf("INSERT /* go-mysql-archiver */ INTO `%s` (%s) VALUES %s", table, columns, *placeholderSet)
	return
}

// buildUniqueIndexDeleteStmt
//  DELETE /* go-mysql-archiver */ FROM `tb` WHERE (`c1`, `c2`, ...) IN ((?, ?, ...), (?, ?, ...), ...)
func buildUniqueIndexDeleteStmt(table string, ukColumns string, ukPlaceholderSets *string) (stmt *string) {
	*stmt = fmt.Sprintf("DELETE /* go-mysql-archiver */ FROM `%s` WHERE (%s) IN (%s)", table, ukColumns, *ukPlaceholderSets)
	return
}

// buildEntireMatchDeleteStmt
//  DELETE /* go-mysql-archiver */ FROM `tb` WHERE (`c1`, `c2`, `c3`, ...) IN ((?, ?, ?, ...), (?, ?, ?, ...), ...) LIMIT 1000
func buildEntireMatchDeleteStmt(table string, columns string, placeholderSets *string, limit uint) (stmt *string) {
	*stmt = fmt.Sprintf("DELETE /* go-mysql-archiver */ FROM `%s` WHERE (%s) IN (%s) LIMIT %d", table, columns, *placeholderSets, limit)
	return
}
