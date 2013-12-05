package input

import (
	"fmt"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type PrintOutputModule struct {
	*pipeliner_modules.GenericOutputModule
}

func NewPrintOutputModule(specificId string) *PrintOutputModule {
	printOutputModule := &PrintOutputModule{
		pipeliner_modules.NewGenericOutputModule("Print Output Module",
			"1.0.0", "print", specificId, nil),
	}
	printOutputModule.SetConsumerFunc(printOutputModule.printItem)

	return printOutputModule
}

func (m *PrintOutputModule) Duplicate(specificId string) (base_modules.Module,
	error) {
	duplicate := NewPrintOutputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerOutputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *PrintOutputModule) Ready() bool {
	// There is no configuration, so we are always ready.
	return true
}

func (m *PrintOutputModule) printItem(
	consumerChannel <-chan *datatypes.PipelineItem) {
	// Read channel until it is closed by the other end and print the
	// received pipeline items.
	for pipelineItem := range consumerChannel {
		fmt.Println(pipelineItem)
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerOutputModule(
		NewPrintOutputModule(""))
}
