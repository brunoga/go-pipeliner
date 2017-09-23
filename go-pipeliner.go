// pipeliner project main.go
package main

import (
	"flag"
	"fmt"

	"github.com/brunoga/go-pipeliner/config"

	modules "gopkg.in/brunoga/go-modules.v1"
)

var configFile = flag.String("config", "./config.yaml", "path to config file")
var listModules = flag.Bool("list-modules", false, "list available modules and exit")

func printModulesByType(moduleType string) {
	fullModuleMap := modules.GetModulesByType(moduleType)
	if len(fullModuleMap) == 0 {
		fmt.Println("[No Available Modules]")
		return
	}
	for _, moduleMap := range fullModuleMap {
		for _, module := range moduleMap {
			fmt.Printf("* %s v%s (%s)\n", module.Name(), module.Version(), module.GenericId())
		}
	}
}

func main() {
	flag.Parse()

	if *listModules {
		fmt.Println("----- Producer  Modules -----\n")
		printModulesByType("pipeliner-producer")
		fmt.Println("\n----- Processor Modules -----\n")
		printModulesByType("pipeliner-processor")
		fmt.Println("\n----- Consumer  Modules -----\n")
		printModulesByType("pipeliner-consumer")
		fmt.Println("")
		return
	}

	config, err := config.New(*configFile)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("* Starting pipelines.")
		config.Dump()
		err := config.StartPipelines()
		if err != nil {
			fmt.Println(err)
		}
		config.WaitPipelines()
		fmt.Println("* Pipelines done.")
	}
}
