package modules

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
)

type GenericProducerModule struct {
	*GenericPipelineModule

	outputChannel chan<- *datatypes.PipelineItem

	producerFunc func(chan<- *datatypes.PipelineItem, <-chan struct{})
}

func NewGenericProducerModule(name, version, genericId, specificId string,
	producerFunc func(chan<- *datatypes.PipelineItem,
		<-chan struct{})) *GenericProducerModule {
	return &GenericProducerModule{
		NewGenericPipelineModule(name, version, genericId, specificId,
			"pipeliner-producer"),
		nil,
		producerFunc,
	}
}

func (m *GenericProducerModule) SetOutputChannel(
	outputChannel chan<- *datatypes.PipelineItem) error {
	if outputChannel == nil {
		return fmt.Errorf("can't set output to a nil channel")
	}

	m.outputChannel = outputChannel

	return nil
}

func (m *GenericProducerModule) Start(waitGroup *sync.WaitGroup) error {
	if !m.Ready() {
		waitGroup.Done()
		return fmt.Errorf("not ready")
	}

	if m.outputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("output channel not connected")
	}

	if m.producerFunc == nil {
		waitGroup.Done()
		return fmt.Errorf("producer function must not be nil")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *GenericProducerModule) SetProducerFunc(
	producerFunc func(chan<- *datatypes.PipelineItem, <-chan struct{})) error {
	if producerFunc == nil {
		return fmt.Errorf("producer function must not be nil")
	}

	m.producerFunc = producerFunc

	return nil
}

func (m *GenericProducerModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	producerChannel := make(chan *datatypes.PipelineItem)
	producerControlChannel := make(chan struct{})

	go m.producerFunc(producerChannel, producerControlChannel)

L:
	for {
		select {
		case item, ok := <-producerChannel:
			if ok {
				m.outputChannel <- item
			} else {
				close(m.outputChannel)
				break L
			}
		case <-m.quitChannel:
			close(producerControlChannel)
			break L
		}
	}
}
