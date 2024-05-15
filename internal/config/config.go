package config

import (
	"errors"
	"flag"
	"time"
)

const TimeFormat = "2006-01-02 15:04:05"

type MySQL struct {
	Address  string
	Username string
	Password string
	Database string
	Charset  string
}

type Source struct {
	MySQL
	Table string
	Where string
	Limit int64
}

type Target struct {
	MySQL
	Table string
}

type Config struct {
	Source     Source
	Target     Target
	Progress   time.Duration
	Sleep      time.Duration
	Statistics bool
	Memory     int64
	RunTime    time.Duration
}

func NewFlag() (cfg *Config, err error) {
	srcAddress := flag.String("src-address", "127.0.0.1:3306", "source mysql address")
	srcUsername := flag.String("src-username", "root", "source mysql username")
	srcPassword := flag.String("src-password", "", "source mysql password")
	srcDatabase := flag.String("src-database", "", "source mysql database")
	srcCharset := flag.String("src-charset", "utf8mb4", "source mysql character set")
	srcTable := flag.String("src-table", "", "source mysql table")
	srcWhere := flag.String("src-where", "", "the WHERE clause, if unspecified, it will fetch all rows")
	srcLimit := flag.Uint("src-limit", 500, "the number of rows fetched per round")

	tgtAddress := flag.String("tgt-address", "127.0.0.1:3306", "target mysql address")
	tgtUsername := flag.String("tgt-username", "root", "target mysql username")
	tgtPassword := flag.String("tgt-password", "", "target mysql password")
	tgtDatabase := flag.String("tgt-database", "", "target mysql database, if unspecified, it defaults to the source database")
	tgtCharset := flag.String("tgt-charset", "", "target mysql character set, if unspecified, it defaults to the source character set")
	tgtTable := flag.String("tgt-table", "", "target mysql table, if unspecified, it defaults to the source table")

	progress := flag.Duration("progress", 5*time.Second, "time interval for printing progress, such as 10s, 1m, etc, 0 means disable")
	sleep := flag.Duration("sleep", 0, "time interval for fetching rows, such as 500ms, 1s, etc, if unspecified, it means disable")
	statistics := flag.Bool("statistics", false, "print statistics after task has finished")
	memory := flag.Int64("memory", 0, "max memory usage in bytes, if unspecified, it means unlimited")
	runTime := flag.Duration("run-time", 0, "time to run before exiting, such as 600s, 120m, 5h30m15s, etc")

	flag.Parse()

	if *srcAddress == "" {
		err = errors.New("the source address was specified with an empty value")
		return
	}
	if *srcDatabase == "" {
		err = errors.New("the source database was specified with an empty value")
		return
	}
	if *tgtDatabase == "" {
		tgtDatabase = srcDatabase
	}
	if *srcTable == "" {
		err = errors.New("the source table was specified with an empty value")
		return
	}
	if *tgtTable == "" {
		tgtTable = srcTable
	}
	if *srcAddress == *tgtAddress && *srcDatabase == *tgtDatabase && *srcTable == *tgtTable {
		err = errors.New("the source and target tables are identical")
		return
	}
	if *srcCharset == "" {
		err = errors.New("the source charset was specified with an empty value")
		return
	}
	if *tgtCharset == "" {
		tgtCharset = srcCharset
	}
	if *srcLimit == 0 {
		*srcLimit = 500
	}
	if *progress < time.Second {
		err = errors.New("the value of progress must be equal to 0 or greater than 1s")
		return
	}
	if *sleep > 0 && *sleep < time.Millisecond {
		err = errors.New("the value of sleep must be equal to 0 or greater than 100ms")
		return
	}
	if *memory < 0 {
		err = errors.New("the value of memory cannot be less than 0")
		return
	}
	cfg = &Config{
		Source: Source{
			MySQL: MySQL{
				Address:  *srcAddress,
				Username: *srcUsername,
				Password: *srcPassword,
				Database: *srcDatabase,
				Charset:  *srcCharset,
			},
			Table: *srcTable,
			Where: *srcWhere,
			Limit: int64(*srcLimit),
		},
		Target: Target{
			MySQL: MySQL{
				Address:  *tgtAddress,
				Username: *tgtUsername,
				Password: *tgtPassword,
				Database: *tgtDatabase,
				Charset:  *tgtCharset,
			},
			Table: *tgtTable,
		},
		Progress:   *progress,
		Sleep:      *sleep,
		Statistics: *statistics,
		Memory:     *memory,
		RunTime:    *runTime,
	}

	return
}
