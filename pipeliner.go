// pipeliner project main.go
package main

import (
	"flag"
	"fmt"

	"github.com/brunoga/go-pipeliner/config"

	modules "github.com/brunoga/go-modules"
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
		fmt.Println("----- Input  Modules -----\n")
		printModulesByType("pipeliner-input")
		fmt.Println("\n----- Filter Modules -----\n")
		printModulesByType("pipeliner-filter")
		fmt.Println("\n----- Output Modules -----\n")
		printModulesByType("pipeliner-output")
		fmt.Println("")
		return
	}

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
