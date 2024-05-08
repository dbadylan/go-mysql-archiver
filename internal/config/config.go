package config

import (
	"errors"
	"flag"
	"time"
)

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
	Limit uint
}

type Target struct {
	MySQL
	Table string
}

type Config struct {
	Source   Source
	Target   Target
	Quiet    bool
	Interval time.Duration
	Sleep    time.Duration
}

func NewFlag() (cfg *Config, err error) {
	srcHost := flag.String("src.host", "127.0.0.1", "source mysql host")
	srcPort := flag.Uint("src.port", 3306, "source mysql port")
	srcUsername := flag.String("src.username", "root", "source mysql username")
	srcPassword := flag.String("src.password", "", "source mysql password")
	srcDatabase := flag.String("src.database", "", "source mysql database")
	srcCharset := flag.String("src.charset", "utf8mb4,utf8", "source mysql charset")
	srcTable := flag.String("src.table", "", "source mysql table")
	srcWhere := flag.String("src.where", "1 = 1", "WHERE clause")
	srcLimit := flag.Uint("src.limit", 500, "rows to fetch per round")

	tgtHost := flag.String("tgt.host", "127.0.0.1", "target mysql host")
	tgtPort := flag.Uint("tgt.port", 3306, "target mysql port")
	tgtUsername := flag.String("tgt.username", "root", "target mysql username")
	tgtPassword := flag.String("tgt.password", "", "target mysql password")
	tgtDatabase := flag.String("tgt.database", "", "target mysql database, if unspecified, it defaults to the source database")
	tgtCharset := flag.String("tgt.charset", "", "target mysql charset, if unspecified, it defaults to the source charset")
	tgtTable := flag.String("tgt.table", "", "target mysql table, if unspecified, it defaults to the source table")

	quiet := flag.Bool("quiet", false, "do not print any output")
	interval := flag.Duration("interval", 5*time.Second, "time interval for printing statistics, such as 10s, 1m, etc")
	sleep := flag.Duration("sleep", 100*time.Millisecond, "time interval for fetching rows, such as 0, 500ms, 1s, etc")

	flag.Parse()

	if *srcPort < 0 || *srcPort > 65535 || *tgtPort < 0 || *tgtPort > 65535 {
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
	if *interval < time.Second {
		err = errors.New("interval must be larger than 1s")
		return
	}
	if *sleep != 0 && *sleep < time.Millisecond {
		err = errors.New("sleep must be 0 or larger than 100ms")
		return
	}
	if *srcLimit == 0 {
		*srcLimit = 500
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
			Limit: *srcLimit,
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
		Quiet:    *quiet,
		Interval: *interval,
		Sleep:    *sleep,
	}

	return
}
