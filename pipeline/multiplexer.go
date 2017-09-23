package pipeline

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
	"github.com/brunoga/go-pipeliner/log"

	base_modules "gopkg.in/brunoga/go-modules.v1"
)

type multiplexerModule struct {
	*base_modules.GenericModule

	inputChannels []<-chan *datatypes.PipelineItem
	outputChannel chan<- *datatypes.PipelineItem
	quitChannel   chan struct{}
	logChannel    chan<- *log.LogEntry
}

func newMultiplexerModule(specificId string) *multiplexerModule {
	return &multiplexerModule{
		base_modules.NewGenericModule("Multiplexer Module",
			"1.0.0", "multiplexer", specificId, "pipeline"),
		nil,
		nil,
		make(chan struct{}),
		nil,
	}
}

func (m *multiplexerModule) GetInputChannel() chan<- *datatypes.PipelineItem {
	inputChannel := make(chan *datatypes.PipelineItem)
	m.inputChannels = append(m.inputChannels, inputChannel)
	return inputChannel
}

func (m *multiplexerModule) SetOutputChannel(outputChannel chan<- *datatypes.PipelineItem) error {
	if outputChannel == nil {
		return fmt.Errorf("can´t use nil channel as output")
	}

	m.outputChannel = outputChannel
	return nil
}

func (m *multiplexerModule) SetLogChannel(logChannel chan<- *log.LogEntry) {
	m.logChannel = logChannel
}

func (m *multiplexerModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := newMultiplexerModule(specificId)
	err := base_modules.RegisterModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *multiplexerModule) Start(waitGroup *sync.WaitGroup) error {
	if m.outputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("no output set")
	}
	if m.inputChannels == nil {
		waitGroup.Done()
		return fmt.Errorf("no input(s) set")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *multiplexerModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *multiplexerModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	var wg sync.WaitGroup
	for _, inputChannel := range m.inputChannels {
		wg.Add(1)
		go inputHandler(inputChannel, m.outputChannel, m.quitChannel, &wg)
	}

	wg.Wait()

	// TODO(bga): It Stop() was called, we should not close the output channel.
	close(m.outputChannel)
}

func inputHandler(inputChannel <-chan *datatypes.PipelineItem,
	outputChannel chan<- *datatypes.PipelineItem,
	quitChannel chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
L:
	for {
		select {
		case data, ok := <-inputChannel:
			if ok {
				outputChannel <- data
			} else {
				break L
			}
		case <-quitChannel:
			break L
		}
	}
}

func init() {
	base_modules.RegisterModule(newMultiplexerModule(""))
}
