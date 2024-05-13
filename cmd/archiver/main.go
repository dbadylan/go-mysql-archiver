package main

import (
	"fmt"

	"github.com/dbadylan/go-archiver/internal/biz"
	"github.com/dbadylan/go-archiver/internal/config"
)

func main() {
	cfg, err := config.NewFlag()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if err = biz.Run(cfg); err != nil {
		fmt.Println(err.Error())
		return
	}
}
