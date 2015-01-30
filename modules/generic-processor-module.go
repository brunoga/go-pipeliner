package modules

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
)

type GenericProcessorModule struct {
	*GenericPipelineModule

	inputChannel  chan *datatypes.PipelineItem
	outputChannel chan<- *datatypes.PipelineItem

	processorFunc func(*datatypes.PipelineItem) bool
}

func NewGenericProcessorModule(name, version, genericId, specificId string,
	processorFunc func(*datatypes.PipelineItem) bool) *GenericProcessorModule {
	return &GenericProcessorModule{
		NewGenericPipelineModule(name, version, genericId, specificId,
			"pipeliner-processor"),
		make(chan *datatypes.PipelineItem),
		nil,
		processorFunc,
	}
}

func (m *GenericProcessorModule) GetInputChannel() chan<- *datatypes.PipelineItem {
	return m.inputChannel
}

func (m *GenericProcessorModule) SetOutputChannel(
	inputChannel chan<- *datatypes.PipelineItem) error {
	if inputChannel == nil {
		return fmt.Errorf("can't set output to a nil channel")
	}

	m.outputChannel = inputChannel

	return nil
}

func (m *GenericProcessorModule) Start(waitGroup *sync.WaitGroup) error {
	if !m.Ready() {
		waitGroup.Done()
		return fmt.Errorf("not ready")
	}

	if m.inputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("input channel not connected")
	}

	if m.outputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("output channel not connected")
	}

	if m.processorFunc == nil {
		waitGroup.Done()
		return fmt.Errorf("processor function must not be nil")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *GenericProcessorModule) SetProcessorFunc(
	processorFunc func(*datatypes.PipelineItem) bool) error {
	if processorFunc == nil {
		return fmt.Errorf("processor function must not be nil")
	}

	m.processorFunc = processorFunc

	return nil
}

func (m *GenericProcessorModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
L:
	for {
		select {
		case item, ok := <-m.inputChannel:
			if ok {
				filtered := m.processorFunc(item)
				if !filtered {
					m.outputChannel <- item
				}
			} else {
				close(m.outputChannel)
				break L
			}
		case <-m.quitChannel:
			break L
		}
	}
}
