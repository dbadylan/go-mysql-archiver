package biz

import (
	"fmt"
	"net"
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

	analysis, e3 := data.AnalyzeQuery(srcDB, cfg.Source.Database, cfg.Source.Table, cfg.Source.Where)
	if e3 != nil {
		err = e3
		return
	}

	socketFile := cfg.Socket
	if socketFile == "" {
		socketFile = fmt.Sprintf("/tmp/%s-%s-%s.sock", cfg.Source.Address, cfg.Source.Database, cfg.Source.Table)
	}
	listener, e4 := net.Listen("unix", socketFile)
	if e4 != nil {
		err = e4
		return
	}
	defer func() {
		_ = listener.Close()
		_ = os.Remove(socketFile)
	}()
	var (
		pause  bool
		resume = make(chan struct{}, 1)
	)
	go func() {
		for {
			conn, e1 := listener.Accept()
			if e1 != nil {
				return
			}
			buf := make([]byte, 128)
			n, e2 := conn.Read(buf)
			if e2 != nil {
				fmt.Println(e2.Error())
				continue
			}
			cmd := strings.TrimSpace(string(buf[:n]))
			var response string
			switch cmd {
			case "pause":
				response = "task has been paused\n"
				pause = true
			case "resume":
				response = "task will be resumed\n"
				if pause {
					pause = false
					resume <- struct{}{}
				}
			default:
				response = "unknown command\n"
			}
			if _, e2 = conn.Write([]byte(response)); e2 != nil {
				fmt.Println(e2.Error())
				continue
			}
			_ = conn.Close()
		}
	}()

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
					fmt.Printf("[%s] progress: %d/%d\n", ts.Local().Format(config.TimeFormat), rowsSelect, analysis.RowsEstimated)
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
				DB:       srcDB,
				Table:    cfg.Source.Table,
				Where:    cfg.Source.Where,
				Limit:    cfg.Source.Limit,
				Analysis: analysis,
			}
			resp, e1 := data.SelectRows(selectParam)
			if e1 != nil {
				err = e1
				return
			}
			if resp.Rows == 0 {
				break L
			}
			rowsSelect += resp.Rows

			srcTx, e2 := srcDB.Begin()
			if e2 != nil {
				err = e2
				return
			}
			tgtTx, e3 := tgtDB.Begin()
			if e3 != nil {
				err = e3
				return
			}

			insertParam := &data.InsertParam{
				Tx:        tgtTx,
				Table:     cfg.Target.Table,
				Columns:   resp.Insert.Columns,
				Values:    resp.Insert.Values,
				ValueList: resp.Insert.ValueList,
			}
			deleteParam := &data.DeleteParam{
				Tx:        srcTx,
				Table:     cfg.Source.Table,
				Where:     resp.Delete.Where,
				Limit:     cfg.Source.Limit,
				ValueList: resp.Delete.ValueList,
				Analysis:  analysis,
			}

			var (
				wg      = new(sync.WaitGroup)
				inserts int64
				deletes int64
				errs    []string
			)
			wg.Add(1)
			go func(wg *sync.WaitGroup, param *data.InsertParam, inserts *int64, errs *[]string) {
				defer wg.Done()
				rowsAffected, e := data.InsertRows(param)
				if e != nil {
					*errs = append(*errs, e.Error())
					return
				}
				*inserts = rowsAffected
				return
			}(wg, insertParam, &inserts, &errs)

			wg.Add(1)
			go func(wg *sync.WaitGroup, param *data.DeleteParam, deletes *int64, errs *[]string) {
				defer wg.Done()
				rowsAffected, e := data.DeleteRows(param)
				if e != nil {
					*errs = append(*errs, e.Error())
					return
				}
				*deletes = rowsAffected
			}(wg, deleteParam, &deletes, &errs)

			wg.Wait()

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
			rowsInsert += inserts

			if err = srcTx.Commit(); err != nil {
				return
			}
			rowsDelete += deletes

			if resp.Rows < cfg.Source.Limit {
				break L
			}

			if pause {
				<-resume
				continue
			}

			if cfg.Sleep != 0 {
				<-sleep.C
				continue
			}
		}
	}

	eTime := time.Now().Local()

	if !cfg.Statistics {
		return
	}

	fmt.Printf(
		config.StatisticsTemplate,
		sTime.Format(config.TimeFormat), eTime.Format(config.TimeFormat), eTime.Sub(sTime).Truncate(time.Second).String(),
		cfg.Source.Address, cfg.Source.Database, cfg.Source.Table, cfg.Source.Charset,
		cfg.Target.Address, cfg.Target.Database, cfg.Target.Table, cfg.Target.Charset,
		rowsSelect, rowsInsert, rowsDelete,
	)

	return
}
