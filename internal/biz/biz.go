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

	srcDB, e1 := data.NewDB(cfg.Source.MySQL)
	if e1 != nil {
		err = e1
		return
	}
	defer func() { _ = srcDB.Close() }()

	tgtDB, e2 := data.NewDB(cfg.Target.MySQL)
	if e2 != nil {
		err = e2
		return
	}
	defer func() { _ = tgtDB.Close() }()

	keyName, rowsEstimated, e3 := data.Explain(srcDB, cfg.Source.Table, cfg.Source.Where)
	if e3 != nil {
		err = e3
		return
	}
	keys, e4 := data.GetKeys(srcDB, cfg.Source.Database, cfg.Source.Table)
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
				case <-ticker.C:
					runtime.ReadMemStats(memStats)
					increased := memStats.Alloc - procMem
					if increased > uint64(cfg.Memory) {
						fmt.Printf("the memory usage(%d) of the task has exceeded the limit(%d), you can either reduce the batch size or increase the memory limit\n", increased, cfg.Memory)
						os.Exit(1)
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
		sleep      = new(time.Ticker)
		runTime    = new(time.Ticker)
	)
	if cfg.Sleep > 0 {
		sleep = time.NewTicker(cfg.Sleep)
		defer sleep.Stop()
	}
	if cfg.RunTime > 0 {
		runTime = time.NewTicker(cfg.RunTime)
		defer runTime.Stop()
	}
L:
	for {
		select {
		case <-runTime.C:
			break L
		default:
			selectParam := &data.SelectParam{
				DB:         srcDB,
				Table:      cfg.Source.Table,
				Where:      cfg.Source.Where,
				OrderBy:    orderBy,
				Limit:      cfg.Source.Limit,
				KeyColumns: keyColumns,
			}
			var resp *data.SelectResponse
			if resp, err = data.SelectRows(selectParam); err != nil {
				return
			}
			if resp.Rows == 0 {
				break L
			}
			rowsSelect += resp.Rows

			var (
				waitGrp    sync.WaitGroup
				errs       []string
				rowQty     = make(chan int64, 1)
				qtyMatched = make(chan bool, 1)
				committed  = make(chan bool, 1)
			)

			waitGrp.Add(1)
			go func() {
				defer waitGrp.Done()
				tgtTx, e1 := tgtDB.Begin()
				if e1 != nil {
					errs = append(errs, e1.Error())
					return
				}
				param := &data.InsertParam{
					Tx:        tgtTx,
					Table:     cfg.Target.Table,
					Columns:   resp.Insert.Columns,
					Values:    resp.Insert.Values,
					ValueList: resp.Insert.ValueList,
				}
				inserts, e2 := data.InsertRows(param)
				if e2 != nil {
					errs = append(errs, e2.Error())
					return
				}
				// 1. 把插入的行的数量通知给 src 事务
				rowQty <- inserts
				// 4. 拿到 src 事务中行数匹配的结果
				ok := <-qtyMatched
				if !ok {
					return
				}
				// 5. 提交事务，把结果通知给 src 事务
				if e2 = tgtTx.Commit(); e2 != nil {
					committed <- false
					errs = append(errs, e2.Error())
					return
				}
				committed <- true
				rowsInsert += inserts
			}()

			waitGrp.Add(1)
			go func() {
				defer waitGrp.Done()
				srcTx, e1 := srcDB.Begin()
				if e1 != nil {
					errs = append(errs, e1.Error())
					return
				}
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
				deletes, e2 := data.DeleteRows(param)
				if e2 != nil {
					errs = append(errs, e2.Error())
					return
				}
				// 2. 拿到 tgt 事务里插入的行的数量
				inserts := <-rowQty
				// 3. 判断行数是否匹配，将结果通知给 tgt 事务
				if inserts < deletes {
					qtyMatched <- false
					errs = append(errs, fmt.Sprintf("rows deleted(%d) larger than inserted(%d), rollback and exit", deletes, inserts))
					return
				}
				qtyMatched <- true
				// 6. 拿到 tgt 事务的提交状态
				ok := <-committed
				if !ok {
					return
				}
				if e2 = srcTx.Commit(); e2 != nil {
					errs = append(errs, e2.Error())
					return
				}
				rowsDelete += deletes
			}()

			waitGrp.Wait()

			if len(errs) != 0 {
				err = fmt.Errorf(strings.Join(errs, "\n"))
				return
			}

			if resp.Rows < cfg.Source.Limit {
				break L
			}

			if cfg.Sleep == 0 {
				continue
			}
			<-sleep.C
		}
	}

	eTime := time.Now().Local()

	if !cfg.Statistics {
		return
	}

	statistics := `
{
    "time": {
        "begin": "%s",
        "finish": "%s",
        "duration": "%s"
    },
    "source": {
        "address": "%s",
        "database": "%s",
        "table": "%s",
        "charset": "%s"
    },
    "target": {
        "address": "%s",
        "database": "%s",
        "table": "%s",
        "charset": "%s"
    },
    "action": {
        "select": %d,
        "insert": %d,
        "delete": %d
    }
}

`
	fmt.Printf(
		statistics,
		sTime.Format(config.TimeFormat), eTime.Format(config.TimeFormat), eTime.Sub(sTime).Truncate(time.Second).String(),
		cfg.Source.Address, cfg.Source.Database, cfg.Source.Table, cfg.Source.Charset,
		cfg.Target.Address, cfg.Target.Database, cfg.Target.Table, cfg.Target.Charset,
		rowsSelect, rowsInsert, rowsDelete,
	)

	return
}
