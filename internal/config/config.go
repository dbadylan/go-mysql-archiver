package config

import (
	"errors"
	"flag"
	"time"
)

const TimeFormat = "2006-01-02 15:04:05"

type MySQL struct {
	Host     string
	Port     uint16
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
}

func NewFlag() (cfg *Config, err error) {
	srcHost := flag.String("src.host", "127.0.0.1", "source mysql host")
	srcPort := flag.Uint("src.port", 3306, "source mysql port")
	srcUsername := flag.String("src.username", "root", "source mysql username")
	srcPassword := flag.String("src.password", "", "source mysql password")
	srcDatabase := flag.String("src.database", "", "source mysql database")
	srcCharset := flag.String("src.charset", "utf8mb4", "source mysql character set")
	srcTable := flag.String("src.table", "", "source mysql table")
	srcWhere := flag.String("src.where", "", "the WHERE clause, if unspecified, it will fetch all rows")
	srcLimit := flag.Uint("src.limit", 500, "the number of rows fetched per round")

	tgtHost := flag.String("tgt.host", "127.0.0.1", "target mysql host")
	tgtPort := flag.Uint("tgt.port", 3306, "target mysql port")
	tgtUsername := flag.String("tgt.username", "root", "target mysql username")
	tgtPassword := flag.String("tgt.password", "", "target mysql password")
	tgtDatabase := flag.String("tgt.database", "", "target mysql database, if unspecified, it defaults to the source database")
	tgtCharset := flag.String("tgt.charset", "", "target mysql character set, if unspecified, it defaults to the source character set")
	tgtTable := flag.String("tgt.table", "", "target mysql table, if unspecified, it defaults to the source table")

	progress := flag.Duration("progress", 5*time.Second, "time interval for printing progress, such as 10s, 1m, etc, 0 means disable")
	sleep := flag.Duration("sleep", 0, "time interval for fetching rows, such as 500ms, 1s, etc, if unspecified, it means disable")
	statistics := flag.Bool("statistics", false, "print statistics after task has finished")
	memory := flag.Int64("memory", 0, "max memory usage in bytes, if unspecified, it means unlimited")

	flag.Parse()

	if *srcPort == 0 || *srcPort > 65535 || *tgtPort == 0 || *tgtPort > 65535 {
		err = errors.New("port number out of range")
		return
	}
	if *srcDatabase == "" {
		err = errors.New("source database name unspecified")
		return
	}
	if *tgtDatabase == "" {
		tgtDatabase = srcDatabase
	}
	if *srcTable == "" {
		err = errors.New("source table name unspecified")
		return
	}
	if *tgtTable == "" {
		tgtTable = srcTable
	}
	if *srcHost == *tgtHost && *srcPort == *tgtPort && *srcDatabase == *tgtDatabase && *srcTable == *tgtTable {
		err = errors.New("the source and target tables are identical")
		return
	}
	if *srcCharset == "" {
		err = errors.New("source charset unspecified")
		return
	}
	if *tgtCharset == "" {
		tgtCharset = srcCharset
	}
	if *progress < time.Second {
		err = errors.New("progress must be larger than 1s")
		return
	}
	if *sleep > 0 && *sleep < time.Millisecond {
		err = errors.New("sleep must be larger than 100ms")
		return
	}
	if *srcLimit == 0 {
		*srcLimit = 500
	}
	if *memory < 0 {
		err = errors.New("memory must be larger than 0")
		return
	}

	cfg = &Config{
		Source: Source{
			MySQL: MySQL{
				Host:     *srcHost,
				Port:     uint16(*srcPort),
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
				Host:     *tgtHost,
				Port:     uint16(*tgtPort),
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
	}

	return
}
