package input

import (
	"fmt"
	"sync"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type PrintOutputModule struct {
	*base_modules.GenericModule

	inputChannel chan interface{}
	quitChannel  chan interface{}
}

func NewPrintOutputModule(specificId string) *PrintOutputModule {
	return &PrintOutputModule{
		base_modules.NewGenericModule("Print Output Module", "1.0.0",
			"print", specificId, "pipeliner-output"),
		make(chan interface{}),
		make(chan interface{}),
	}
}

func (m *PrintOutputModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := NewPrintOutputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerOutputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *PrintOutputModule) GetInputChannel() chan<- interface{} {
	return m.inputChannel
}

func (m *PrintOutputModule) Ready() bool {
	return true
}

func (m *PrintOutputModule) Start(waitGroup *sync.WaitGroup) error {
	if m.inputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("input channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *PrintOutputModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan interface{})
}

func (m *PrintOutputModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
L:
	for {
		select {
		case item, ok := <-m.inputChannel:
			if ok {
				fmt.Println(item)
			} else {
				m.inputChannel = nil
				break L
			}
		case <-m.quitChannel:
			break L
		}
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerOutputModule(NewPrintOutputModule(""))
}
