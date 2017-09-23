package input

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"

	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	base_modules "gopkg.in/brunoga/go-modules.v1"
)

type PrintConsumerModule struct {
	*pipeliner_modules.GenericConsumerModule
}

func NewPrintConsumerModule(specificId string) *PrintConsumerModule {
	printConsumerModule := &PrintConsumerModule{
		pipeliner_modules.NewGenericConsumerModule("Print Consumer Module",
			"1.0.0", "print", specificId, nil),
	}
	printConsumerModule.SetConsumerFunc(printConsumerModule.printItem)

	return printConsumerModule
}

func (m *PrintConsumerModule) Duplicate(specificId string) (base_modules.Module,
	error) {
	duplicate := NewPrintConsumerModule(specificId)
	err := pipeliner_modules.RegisterPipelinerConsumerModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *PrintConsumerModule) Ready() bool {
	// There is no configuration, so we are always ready.
	return true
}

func (m *PrintConsumerModule) printItem(
	consumerChannel <-chan *datatypes.PipelineItem,
	waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	// Read channel until it is closed by the other end and print the
	// received pipeline items.
	for pipelineItem := range consumerChannel {
		fmt.Println(pipelineItem)
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerConsumerModule(
		NewPrintConsumerModule(""))
}
