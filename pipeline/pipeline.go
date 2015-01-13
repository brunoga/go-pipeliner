package pipeline

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"
	"github.com/brunoga/go-pipeliner/log"

	base_modules "github.com/brunoga/go-modules"
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

type FilterNode interface {
	Starter
	Stopper
	OutputChannelSetter
	InputChannelGetter
	log.Logger
}

type OutputNode interface {
	Starter
	Stopper
	InputChannelGetter
	log.Logger
}

type Pipeline struct {
	name string

	producerNodes []ProducerNode
	filterNodes   []FilterNode
	outputNodes   []OutputNode

	multiplexer   *multiplexerModule
	demultiplexer *demultiplexerModule

	waitGroup    *sync.WaitGroup
	logWaitGroup *sync.WaitGroup

	logChannel chan *log.LogEntry
}

func New(name string) *Pipeline {
	return &Pipeline{
		name: name,

		producerNodes: nil,
		filterNodes:   nil,
		outputNodes:   nil,

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

	_, ok := producerNode.(FilterNode)
	if ok {
		return fmt.Errorf("tried to add a filter node as a producer node")
	}

	producerNode.SetLogChannel(p.logChannel)

	p.producerNodes = append(p.producerNodes, producerNode)

	return nil
}

func (p *Pipeline) AddFilterNode(filterNode FilterNode) error {
	if filterNode == nil {
		return fmt.Errorf("can't add a nil filter node")
	}

	filterNode.SetLogChannel(p.logChannel)

	p.filterNodes = append(p.filterNodes, filterNode)

	return nil
}

func (p *Pipeline) AddOutputNode(outputNode OutputNode) error {
	if outputNode == nil {
		return fmt.Errorf("can't add a nil output node")
	}

	_, ok := outputNode.(FilterNode)
	if ok {
		return fmt.Errorf("tried to add a filter node as an output node")
	}

	outputNode.SetLogChannel(p.logChannel)

	p.outputNodes = append(p.outputNodes, outputNode)

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

	// Start all inputs.
	for _, producerNode := range p.producerNodes {
		p.waitGroup.Add(1)
		producerNode.Start(p.waitGroup)
	}

	// Start multiplexer if we have one.
	if p.multiplexer != nil {
		p.waitGroup.Add(1)
		p.multiplexer.Start(p.waitGroup)
	}

	// Start all filters.
	for _, filterNode := range p.filterNodes {
		p.waitGroup.Add(1)
		filterNode.Start(p.waitGroup)
	}

	// Start demultiplexer if we have one.
	if p.demultiplexer != nil {
		p.waitGroup.Add(1)
		p.demultiplexer.Start(p.waitGroup)
	}

	// Start all outputs.
	for _, outputNode := range p.outputNodes {
		p.waitGroup.Add(1)
		outputNode.Start(p.waitGroup)
	}

	return nil
}

func (p *Pipeline) Stop() {
	// Stop all inputs.
	for _, producerNode := range p.producerNodes {
		producerNode.Stop()
	}

	// Stop multiplexer if we have one.
	if p.multiplexer != nil {
		p.multiplexer.Stop()
	}

	// Stop all filters.
	for _, filterNode := range p.filterNodes {
		filterNode.Stop()
	}

	// Stop demultiplexer if we have one.
	if p.demultiplexer != nil {
		p.demultiplexer.Stop()
	}

	// Stop all outputs.
	for _, outputNode := range p.outputNodes {
		outputNode.Stop()
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
		len(p.filterNodes)+len(p.outputNodes))
	fmt.Println("\n--- Producers  ---")
	for _, node := range p.producerNodes {
		stringer := node.(fmt.Stringer)
		fmt.Println(stringer)
	}
	fmt.Println("\n--- Filters ---")
	for _, node := range p.filterNodes {
		stringer := node.(fmt.Stringer)
		fmt.Println(stringer)
	}
	fmt.Println("\n--- Outputs ---")
	for _, node := range p.outputNodes {
		stringer := node.(fmt.Stringer)
		fmt.Println(stringer)
	}
}

func (p *Pipeline) checkPipeline() error {
	if len(p.producerNodes) == 0 {
		return fmt.Errorf("no producer nodes added to the pipeline")
	}

	if len(p.outputNodes) == 0 {
		return fmt.Errorf("no output nodes added to the pipeline")
	}

	return nil
}

func (p *Pipeline) connectPipeline() error {
	err := p.checkPipeline()
	if err != nil {
		return err
	}

	var lastNode OutputNode = nil

	// Connect outputs to the pipeline.
	if len(p.outputNodes) > 1 {
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

		// Connect demultiplexer to all output nodes.
		for _, outputNode := range p.outputNodes {
			p.demultiplexer.SetOutputChannel(outputNode.GetInputChannel())
		}

		lastNode = p.demultiplexer
	} else {
		lastNode = p.outputNodes[0]
	}

	// Connect filters to the pipeline.
	if len(p.filterNodes) > 0 {
		for i := 0; i < len(p.filterNodes)-1; i++ {
			currentFilterNode := p.filterNodes[i]
			nextFilterNode := p.filterNodes[i+1]
			currentFilterNode.SetOutputChannel(nextFilterNode.GetInputChannel())
		}
		lastFilter := p.filterNodes[len(p.filterNodes)-1]
		lastFilter.SetOutputChannel(lastNode.GetInputChannel())
		lastNode = p.filterNodes[0]
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
