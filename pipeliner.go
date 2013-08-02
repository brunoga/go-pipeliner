// pipeliner project main.go
package main

import (
	"fmt"

	"github.com/brunoga/go-pipeliner/config"
)

func main() {
	config, err := config.New("./config.yaml")
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
