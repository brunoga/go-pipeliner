package pipeline

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
	"github.com/brunoga/go-pipeliner/log"

	base_modules "github.com/brunoga/go-modules"
)

type demultiplexerModule struct {
	*base_modules.GenericModule

	input   chan *datatypes.PipelineItem
	outputs []chan<- *datatypes.PipelineItem
	quit    chan struct{}
	logChannel chan<- *log.LogEntry
}

func newDemultiplexerModule(specificId string) *demultiplexerModule {
	return &demultiplexerModule{
		base_modules.NewGenericModule("Demultiplexer Module",
			"1.0.0", "demultiplexer", specificId, "pipeline"),
		nil,
		nil,
		make(chan struct{}),
		nil,
	}
}

func (m *demultiplexerModule) GetInputChannel() chan<- *datatypes.PipelineItem {
	if m.input == nil {
		m.input = make(chan *datatypes.PipelineItem)
	}
	return m.input
}

func (m *demultiplexerModule) SetOutputChannel(output chan<- *datatypes.PipelineItem) error {
	if output == nil {
		return fmt.Errorf("can´t use nil channel as output")
	}

	m.outputs = append(m.outputs, output)
	return nil
}

func (m *demultiplexerModule) SetLogChannel(logChannel chan<- *log.LogEntry) {
	m.logChannel = logChannel
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
	m.quit = make(chan struct{})
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
