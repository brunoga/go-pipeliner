package modules

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
)

type GenericOutputModule struct {
	*base_modules.GenericModule

	inputChannel chan *datatypes.PipelineItem
	quitChannel  chan struct{}

	consumerFunc func(*datatypes.PipelineItem) bool
}

func NewGenericOutputModule(name, version, genericId, specificId string,
	consumerFunc func(*datatypes.PipelineItem) bool) *GenericOutputModule {
	return &GenericOutputModule{
		base_modules.NewGenericModule(name, version, genericId,
			specificId, "pipeliner-output"),
		make(chan *datatypes.PipelineItem),
		make(chan struct{}),
		consumerFunc,
	}
}

func (m *GenericOutputModule) Duplicate(specificId string) (base_modules.Module,
	error) {
	return nil, fmt.Errorf("generic output module can not be duplicated")
}

func (m *GenericOutputModule) GetInputChannel() chan<- *datatypes.PipelineItem {
	return m.inputChannel
}

func (m *GenericOutputModule) Start(waitGroup *sync.WaitGroup) error {
	if m.inputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("input channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *GenericOutputModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *GenericOutputModule) SetConsumerFunc(
	consumerFunc func(*datatypes.PipelineItem) bool) {
	m.consumerFunc = consumerFunc
}

func (m *GenericOutputModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	consumerChannel := make(chan *datatypes.PipelineItem)
	consumerControlChannel := make(chan struct{})

	go m.itemHandler(consumerChannel, consumerControlChannel)
L:
	for {
		select {
		case item, ok := <-m.inputChannel:
			if ok {
				consumerChannel <- item
			} else {
				close(consumerControlChannel)
				m.inputChannel = nil
				break L
			}
		case <-m.quitChannel:
			close(consumerControlChannel)
			break L
		}
	}
}

func (m *GenericOutputModule) itemHandler(
	consumerChannel <-chan *datatypes.PipelineItem,
        consumerControlChannel <-chan struct{}) {
L:
	for {
		select {
		case _, ok := <-consumerControlChannel:
			if !ok {
				break L
			}
		case pipelineItem := <-consumerChannel:
			// TODO(bga): We might do something with the returned
			//boolean.
			_ = m.consumerFunc(pipelineItem)
		}
	}
}

