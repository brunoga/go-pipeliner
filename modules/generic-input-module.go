package modules

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
)

type GenericInputModule struct {
	*base_modules.GenericModule

	outputChannel chan<- *datatypes.PipelineItem
	quitChannel   chan struct{}

	generatorFunc func(chan<- *datatypes.PipelineItem, <-chan struct{})
}

func NewGenericInputModule(name, version, genericId, specificId string,
	generatorFunc func(chan<- *datatypes.PipelineItem,
		<-chan struct{})) *GenericInputModule {
	return &GenericInputModule{
		base_modules.NewGenericModule(name, version,
			genericId, specificId, "pipeliner-input"),
		nil,
		make(chan struct{}),
		generatorFunc,
	}
}

func (m *GenericInputModule) Duplicate(specificId string) (base_modules.Module, error) {
	return nil, fmt.Errorf("generic input module can not be duplicated")
}

func (m *GenericInputModule) SetOutputChannel(outputChannel chan<- *datatypes.PipelineItem) error {
	if outputChannel == nil {
		return fmt.Errorf("can't set output to a nil channel")
	}

	m.outputChannel = outputChannel
	return nil
}

func (m *GenericInputModule) Start(waitGroup *sync.WaitGroup) error {
	if !m.Ready() {
		waitGroup.Done()
		return fmt.Errorf("not ready")
	}

	if m.outputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("output channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *GenericInputModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *GenericInputModule) SetGeneratorFunc(
	generatorFunc func(chan<- *datatypes.PipelineItem, <-chan struct{})) {
	m.generatorFunc = generatorFunc
}

func (m *GenericInputModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	generatorChannel := make(chan *datatypes.PipelineItem)
	generatorControlChannel := make(chan struct{})

	go m.generatorFunc(generatorChannel, generatorControlChannel)

L:
	for {
		select {
		case item, ok := <-generatorChannel:
			if ok {
				m.outputChannel <- item
			} else {
				close(m.outputChannel)
				break L
			}
		case <-m.quitChannel:
			close(generatorControlChannel)
			break L
		}
	}
}

