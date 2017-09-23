package pipeline

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
	"github.com/brunoga/go-pipeliner/log"

	base_modules "gopkg.in/brunoga/go-modules.v1"
)

type Starter interface {
	Start(*sync.WaitGroup) error
}

type Stopper interface {
	Stop()
}

type InputChannelGetter interface {
	GetInputChannel() chan<- *datatypes.PipelineItem
}

type OutputChannelSetter interface {
	SetOutputChannel(chan<- *datatypes.PipelineItem) error
}

type ProducerNode interface {
	Starter
	Stopper
	OutputChannelSetter
	log.Logger
}

type ProcessorNode interface {
	Starter
	Stopper
	OutputChannelSetter
	InputChannelGetter
	log.Logger
}

type ConsumerNode interface {
	Starter
	Stopper
	InputChannelGetter
	log.Logger
}

type Pipeline struct {
	name string

	producerNodes  []ProducerNode
	processorNodes []ProcessorNode
	consumerNodes  []ConsumerNode

	multiplexer   *multiplexerModule
	demultiplexer *demultiplexerModule

	waitGroup    *sync.WaitGroup
	logWaitGroup *sync.WaitGroup

	logChannel chan *log.LogEntry
}

func New(name string) *Pipeline {
	return &Pipeline{
		name: name,

		producerNodes:  nil,
		processorNodes: nil,
		consumerNodes:  nil,

		multiplexer:   nil,
		demultiplexer: nil,

		waitGroup:    nil,
		logWaitGroup: nil,

		logChannel: make(chan *log.LogEntry),
	}
}

func (p *Pipeline) AddProducerNode(producerNode ProducerNode) error {
	if producerNode == nil {
		return fmt.Errorf("can't add a nil producer node")
	}

	_, ok := producerNode.(ProcessorNode)
	if ok {
		return fmt.Errorf("tried to add a processor node as a producer node")
	}

	producerNode.SetLogChannel(p.logChannel)

	p.producerNodes = append(p.producerNodes, producerNode)

	return nil
}

func (p *Pipeline) AddProcessorNode(processorNode ProcessorNode) error {
	if processorNode == nil {
		return fmt.Errorf("can't add a nil processor node")
	}

	processorNode.SetLogChannel(p.logChannel)

	p.processorNodes = append(p.processorNodes, processorNode)

	return nil
}

func (p *Pipeline) AddConsumerNode(consumerNode ConsumerNode) error {
	if consumerNode == nil {
		return fmt.Errorf("can't add a nil consumer node")
	}

	_, ok := consumerNode.(ProcessorNode)
	if ok {
		return fmt.Errorf("tried to add a processor node as a consumer node")
	}

	consumerNode.SetLogChannel(p.logChannel)

	p.consumerNodes = append(p.consumerNodes, consumerNode)

	return nil
}

func (p *Pipeline) Start() error {
	err := p.connectPipeline()
	if err != nil {
		return err
	}

	// Start log task.
	p.logWaitGroup = new(sync.WaitGroup)
	p.logWaitGroup.Add(1)
	go p.logTask()

	p.waitGroup = new(sync.WaitGroup)

	// Start all producers.
	for _, producerNode := range p.producerNodes {
		p.waitGroup.Add(1)
		producerNode.Start(p.waitGroup)
	}

	// Start multiplexer if we have one.
	if p.multiplexer != nil {
		p.waitGroup.Add(1)
		p.multiplexer.Start(p.waitGroup)
	}

	// Start all processors.
	for _, processorNode := range p.processorNodes {
		p.waitGroup.Add(1)
		processorNode.Start(p.waitGroup)
	}

	// Start demultiplexer if we have one.
	if p.demultiplexer != nil {
		p.waitGroup.Add(1)
		p.demultiplexer.Start(p.waitGroup)
	}

	// Start all consumers.
	for _, consumerNode := range p.consumerNodes {
		p.waitGroup.Add(1)
		consumerNode.Start(p.waitGroup)
	}

	return nil
}

func (p *Pipeline) Stop() {
	// Stop all producers.
	for _, producerNode := range p.producerNodes {
		producerNode.Stop()
	}

	// Stop multiplexer if we have one.
	if p.multiplexer != nil {
		p.multiplexer.Stop()
	}

	// Stop all processors.
	for _, processorNode := range p.processorNodes {
		processorNode.Stop()
	}

	// Stop demultiplexer if we have one.
	if p.demultiplexer != nil {
		p.demultiplexer.Stop()
	}

	// Stop all consumers.
	for _, consumerNode := range p.consumerNodes {
		consumerNode.Stop()
	}

	// Stop log task.
	close(p.logChannel)
}

func (p *Pipeline) Wait() {
	p.waitGroup.Wait()
	close(p.logChannel)
	p.logWaitGroup.Wait()
}

func (p *Pipeline) String() string {
	return p.name
}

func (p *Pipeline) Dump() {
	fmt.Printf("\n** Pipeline %q configured with %d nodes:\n", p.name, len(p.producerNodes)+
		len(p.processorNodes)+len(p.consumerNodes))
	fmt.Println("\n--- Producers  ---")
	for _, node := range p.producerNodes {
		stringer := node.(fmt.Stringer)
		fmt.Println(stringer)
	}
	fmt.Println("\n--- Processors ---")
	for _, node := range p.processorNodes {
		stringer := node.(fmt.Stringer)
		fmt.Println(stringer)
	}
	fmt.Println("\n--- Consumers ---")
	for _, node := range p.consumerNodes {
		stringer := node.(fmt.Stringer)
		fmt.Println(stringer)
	}
}

func (p *Pipeline) checkPipeline() error {
	if len(p.producerNodes) == 0 {
		return fmt.Errorf("no producer nodes added to the pipeline")
	}

	if len(p.consumerNodes) == 0 {
		return fmt.Errorf("no consumer nodes added to the pipeline")
	}

	return nil
}

func (p *Pipeline) connectPipeline() error {
	err := p.checkPipeline()
	if err != nil {
		return err
	}

	var lastNode ConsumerNode = nil

	// Connect consumers to the pipeline.
	if len(p.consumerNodes) > 1 {
		demultiplexer := base_modules.GetDefaultModuleByGenericId("demultiplexer")
		if demultiplexer == nil {
			return fmt.Errorf("no demultiplexer module available")
		}

		module, err := demultiplexer.Duplicate(p.name)
		if err != nil {
			return err
		}

		p.demultiplexer = module.(*demultiplexerModule)

		p.demultiplexer.SetLogChannel(p.logChannel)

		// Connect demultiplexer to all consumer nodes.
		for _, consumerNode := range p.consumerNodes {
			p.demultiplexer.SetOutputChannel(consumerNode.GetInputChannel())
		}

		lastNode = p.demultiplexer
	} else {
		lastNode = p.consumerNodes[0]
	}

	// Connect processors to the pipeline.
	if len(p.processorNodes) > 0 {
		for i := 0; i < len(p.processorNodes)-1; i++ {
			currentProcessorNode := p.processorNodes[i]
			nextProcessorNode := p.processorNodes[i+1]
			currentProcessorNode.SetOutputChannel(nextProcessorNode.GetInputChannel())
		}
		lastProcessorNode := p.processorNodes[len(p.processorNodes)-1]
		lastProcessorNode.SetOutputChannel(lastNode.GetInputChannel())
		lastNode = p.processorNodes[0]
	}

	// Connect inputs to the pipeline.
	if len(p.producerNodes) > 1 {
		multiplexer := base_modules.GetDefaultModuleByGenericId("multiplexer")
		if multiplexer == nil {
			return fmt.Errorf("no multiplexer module available")
		}

		module, err := multiplexer.Duplicate(p.name)
		if err != nil {
			return err
		}

		p.multiplexer = module.(*multiplexerModule)

		p.multiplexer.SetLogChannel(p.logChannel)

		// Connect multiplexer to last node.
		p.multiplexer.SetOutputChannel(lastNode.GetInputChannel())

		// Connect all producers to multiplexer.
		for _, producerNode := range p.producerNodes {
			producerNode.SetOutputChannel(p.multiplexer.GetInputChannel())
		}
	} else {
		p.producerNodes[0].SetOutputChannel(lastNode.GetInputChannel())
	}

	return nil
}

func (p *Pipeline) logTask() {
	defer p.logWaitGroup.Done()
	for logEntry := range p.logChannel {
		fmt.Printf("%s/%s : %v\n", logEntry.Module.GenericId(),
			logEntry.Module.SpecificId(), logEntry.Err)
	}
}
