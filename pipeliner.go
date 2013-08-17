// pipeliner project main.go
package main

import (
	"flag"
	"fmt"

	"github.com/brunoga/go-pipeliner/config"
)

var configFile = flag.String("config", "./config.yaml", "path to config file")

func main() {
	flag.Parse()
	config, err := config.New(*configFile)
	if err != nil {
		fmt.Println(err)
	} else {
		err := config.StartPipelines()
		if err != nil {
			fmt.Println(err)
		}
		config.WaitPipelines()
		fmt.Println("Pipelines done.")
	}
}
