package modules

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
)

type GenericConsumerModule struct {
	*GenericPipelineModule

	inputChannel chan *datatypes.PipelineItem

	consumerFunc func(<-chan *datatypes.PipelineItem, *sync.WaitGroup)
}

func NewGenericConsumerModule(name, version, genericId, specificId string,
	consumerFunc func(<-chan *datatypes.PipelineItem,
		*sync.WaitGroup)) *GenericConsumerModule {
	return &GenericConsumerModule{
		NewGenericPipelineModule(name, version, genericId, specificId,
			"pipeliner-consumer"),
		make(chan *datatypes.PipelineItem),
		consumerFunc,
	}
}

func (m *GenericConsumerModule) GetInputChannel() chan<- *datatypes.PipelineItem {
	return m.inputChannel
}

func (m *GenericConsumerModule) Start(waitGroup *sync.WaitGroup) error {
	if m.inputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("input channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *GenericConsumerModule) SetConsumerFunc(
	consumerFunc func(<-chan *datatypes.PipelineItem, *sync.WaitGroup)) {
	m.consumerFunc = consumerFunc
}

func (m *GenericConsumerModule) doWork(waitGroup *sync.WaitGroup) {
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
