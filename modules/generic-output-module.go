package modules

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
)

type GenericOutputModule struct {
	*GenericPipelineModule

	inputChannel chan *datatypes.PipelineItem

	consumerFunc func(<-chan *datatypes.PipelineItem, *sync.WaitGroup)
}

func NewGenericOutputModule(name, version, genericId, specificId string,
	consumerFunc func(<-chan *datatypes.PipelineItem,
		*sync.WaitGroup)) *GenericOutputModule {
	return &GenericOutputModule{
		NewGenericPipelineModule(name, version, genericId, specificId,
			"pipeliner-output"),
		make(chan *datatypes.PipelineItem),
		consumerFunc,
	}
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

func (m *GenericOutputModule) SetConsumerFunc(
	consumerFunc func(<-chan *datatypes.PipelineItem, *sync.WaitGroup)) {
	m.consumerFunc = consumerFunc
}

func (m *GenericOutputModule) doWork(waitGroup *sync.WaitGroup) {
	consumerChannel := make(chan *datatypes.PipelineItem)

	go m.consumerFunc(consumerChannel, waitGroup)
L:
	for {
		select {
		case pipelineItem, ok := <-m.inputChannel:
			if ok {
				consumerChannel <- pipelineItem
			} else {
				close(consumerChannel)
				m.inputChannel = nil
				break L
			}
		case <-m.quitChannel:
			close(consumerChannel)
			break L
		}
	}
}
