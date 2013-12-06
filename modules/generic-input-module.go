package modules

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
)

type GenericInputModule struct {
	*GenericPipelineModule

	outputChannel chan<- *datatypes.PipelineItem

	generatorFunc func(chan<- *datatypes.PipelineItem, <-chan struct{})
}

func NewGenericInputModule(name, version, genericId, specificId string,
	generatorFunc func(chan<- *datatypes.PipelineItem,
		<-chan struct{})) *GenericInputModule {
	return &GenericInputModule{
		NewGenericPipelineModule(name, version, genericId, specificId,
			"pipeliner-input"),
		nil,
		generatorFunc,
	}
}

func (m *GenericInputModule) SetOutputChannel(
	outputChannel chan<- *datatypes.PipelineItem) error {
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

