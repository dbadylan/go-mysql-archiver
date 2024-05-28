package main

import (
	"fmt"

	"github.com/dbadylan/go-mysql-archiver/internal/biz"
	"github.com/dbadylan/go-mysql-archiver/internal/config"
)

func main() {
	cfg, err := config.NewFlag()
	if err != nil {
		fmt.Println(err)
		return
	}
	if err = biz.Run(cfg); err != nil {
		fmt.Println(err)
		return
	}
}
