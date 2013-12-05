package modules

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
)

type GenericFilterModule struct {
	*base_modules.GenericModule

	inputChannel  chan *datatypes.PipelineItem
	outputChannel chan<- *datatypes.PipelineItem
	quitChannel   chan struct{}

	filterFunc func(*datatypes.PipelineItem) bool
}

func NewGenericFilterModule(name, version, genericId, specificId string,
	filterFunc func(*datatypes.PipelineItem) bool) *GenericFilterModule {
	return &GenericFilterModule{
		base_modules.NewGenericModule(name, version, genericId,
			specificId, "pipeliner-filter"),
		make(chan *datatypes.PipelineItem),
		nil,
		make(chan struct{}),
		filterFunc,
	}
}

func (m *GenericFilterModule) Duplicate(specificId string) (base_modules.Module,
	error) {
	return nil, fmt.Errorf("generic filter module can not be duplicated")
}

func (m *GenericFilterModule) GetInputChannel() chan<- *datatypes.PipelineItem {
	return m.inputChannel
}

func (m *GenericFilterModule) SetOutputChannel(
	inputChannel chan<- *datatypes.PipelineItem) error {
	if inputChannel == nil {
		return fmt.Errorf("can't set output to a nil channel")
	}

	m.outputChannel = inputChannel
	return nil
}

func (m *GenericFilterModule) Start(waitGroup *sync.WaitGroup) error {
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

	go m.doWork(waitGroup)

	return nil
}

func (m *GenericFilterModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *GenericFilterModule) SetFilterFunc(
	filterFunc func(*datatypes.PipelineItem) bool) {
	m.filterFunc = filterFunc
}

func (m *GenericFilterModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
L:
	for {
		select {
		case item, ok := <-m.inputChannel:
			if ok {
				filtered := m.filterFunc(item)
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

