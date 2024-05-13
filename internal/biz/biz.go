package biz

import (
	"fmt"
	"os"
	"runtime"
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

	keyName, rowsEstimated, e3 := data.Explain(srcConn, cfg.Source.Table, cfg.Source.Where)
	if e3 != nil {
		err = e3
		return
	}
	keys, e4 := data.GetKeys(srcConn, cfg.Source.Database, cfg.Source.Table)
	if e4 != nil {
		err = e4
		return
	}
	if keyName == "" {
		keyName = keys.Elected
	}
	var orderBy string
	keyColumns := make(map[string]struct{})
	if detail, exist := keys.Details[keyName]; exist {
		var columnNames []string
		for _, columnName := range detail.ColumnNames {
			keyColumns[columnName] = struct{}{}
			columnNames = append(columnNames, fmt.Sprintf("`%s`", columnName))
		}
		orderBy = strings.Join(columnNames, ", ")
	}

	var rowsSelect int64
	if cfg.Progress != 0 {
		var exitChan = make(chan struct{}, 1)
		defer func() { exitChan <- struct{}{} }()
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

	if cfg.Memory > 0 {
		var exitChan = make(chan struct{}, 1)
		defer func() { exitChan <- struct{}{} }()
		go func() {
			memStats := new(runtime.MemStats)
			runtime.ReadMemStats(memStats)
			procMem := memStats.Alloc

			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case ts := <-ticker.C:
					runtime.ReadMemStats(memStats)
					increased := memStats.Alloc - procMem
					if increased > uint64(cfg.Memory) {
						fmt.Printf("[%s] the memory usage(%d) of the task has exceeded the limit(%d), you can either reduce the batch size or increase the memory limit", ts.Local().Format(config.TimeFormat), increased, cfg.Memory)
						os.Exit(-1)
					}
				case <-exitChan:
					return
				}
			}
		}()
	}

	var (
		rowsInsert int64
		rowsDelete int64
	)
	for {
		selectParam := &data.SelectParam{
			DB:         srcConn,
			Table:      cfg.Source.Table,
			Where:      cfg.Source.Where,
			OrderBy:    orderBy,
			Limit:      cfg.Source.Limit,
			KeyColumns: keyColumns,
		}

		resp, e1 := data.SelectRows(selectParam)
		if e1 != nil {
			err = e1
			return
		}
		if resp.Rows == 0 {
			break
		}

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
			param := &data.InsertParam{
				Tx:        tgtTx,
				Table:     cfg.Target.Table,
				Columns:   resp.Insert.Columns,
				Values:    resp.Insert.Values,
				ValueList: resp.Insert.ValueList,
			}
			var e error
			if inserts, e = data.InsertRows(param); e != nil {
				errs = append(errs, e.Error())
			}
		}()

		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			var where *string
			if orderBy == "" {
				where = resp.Delete.Where
			} else {
				where = &cfg.Source.Where
			}
			param := &data.DeleteParam{
				Tx:        srcTx,
				Table:     cfg.Source.Table,
				Where:     where,
				OrderBy:   orderBy,
				Limit:     cfg.Source.Limit,
				ValueList: resp.Delete.ValueList,
			}
			var e error
			if deletes, e = data.DeleteRows(param); e != nil {
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

		rowsSelect += resp.Rows
		rowsInsert += inserts
		rowsDelete += deletes

		if resp.Rows < cfg.Source.Limit {
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
