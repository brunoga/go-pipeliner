package pipeline

import (
	"fmt"
	"sync"

	base_modules "github.com/brunoga/go-modules"
)

type demultiplexerModule struct {
	*base_modules.GenericModule

	input   chan interface{}
	outputs []chan<- interface{}
	quit    chan interface{}
}

func newDemultiplexerModule(specificId string) *demultiplexerModule {
	return &demultiplexerModule{
		base_modules.NewGenericModule("Demultiplexer Module",
			"1.0.0", "demultiplexer", specificId, "pipeline"),
		nil,
		nil,
		make(chan interface{}),
	}
}

func (m *demultiplexerModule) GetInputChannel() chan<- interface{} {
	if m.input == nil {
		m.input = make(chan interface{})
	}
	return m.input
}

func (m *demultiplexerModule) SetOutputChannel(output chan<- interface{}) error {
	if output == nil {
		return fmt.Errorf("can´t use nil channel as output")
	}

	m.outputs = append(m.outputs, output)
	return nil
}

func (m *demultiplexerModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := newDemultiplexerModule(specificId)
	err := base_modules.RegisterModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *demultiplexerModule) Start(waitGroup *sync.WaitGroup) error {
	if m.input == nil {
		waitGroup.Done()
		return fmt.Errorf("no input set")
	}

	if m.outputs == nil {
		waitGroup.Done()
		return fmt.Errorf("no output(s) set")
	}

	go m.doWork(waitGroup)
	return nil
}

func (m *demultiplexerModule) Stop() {
	close(m.quit)
	m.quit = make(chan interface{})
}

func (m *demultiplexerModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
L:
	for {
		select {
		case data, ok := <-m.input:
			if ok {
				for _, destChan := range m.outputs {
					destChan <- data
				}
			} else {
				for _, destChan := range m.outputs {
					close(destChan)
				}
				m.outputs = nil
				break L
			}
		case <-m.quit:
			break L
		}
	}
}

func init() {
	base_modules.RegisterModule(newDemultiplexerModule(""))
}
