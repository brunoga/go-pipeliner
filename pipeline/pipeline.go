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

type InputNode interface {
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

	inputNodes  []InputNode
	filterNodes []FilterNode
	outputNodes []OutputNode

	multiplexer   *multiplexerModule
	demultiplexer *demultiplexerModule

	waitGroup *sync.WaitGroup

	logChannel chan *log.LogEntry
}

func New(name string) *Pipeline {
	return &Pipeline{
		name: name,

		inputNodes:  nil,
		filterNodes: nil,
		outputNodes: nil,

		multiplexer:   nil,
		demultiplexer: nil,

		waitGroup: nil,

		logChannel: make(chan *log.LogEntry),
	}
}

func (p *Pipeline) AddInputNode(inputNode InputNode) error {
	if inputNode == nil {
		return fmt.Errorf("can't add a nil input node")
	}

	_, ok := inputNode.(FilterNode)
	if ok {
		return fmt.Errorf("tried to add a filter node as an input node")
	}

	inputNode.SetLogChannel(p.logChannel)

	p.inputNodes = append(p.inputNodes, inputNode)

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

	p.waitGroup = new(sync.WaitGroup)

	// Start log task.
	p.waitGroup.Add(1)
	go p.logTask()

	// Start all inputs.
	for _, inputNode := range p.inputNodes {
		p.waitGroup.Add(1)
		inputNode.Start(p.waitGroup)
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
	for _, inputNode := range p.inputNodes {
		inputNode.Stop()
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
}

func (p *Pipeline) String() string {
	return p.name
}

func (p *Pipeline) Dump() {
	fmt.Printf("\n** Pipeline %q configured with %d nodes:\n", p.name, len(p.inputNodes)+
		len(p.filterNodes)+len(p.outputNodes))
	fmt.Println("\n--- Inputs  ---")
	for _, node := range p.inputNodes {
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
	if len(p.inputNodes) == 0 {
		return fmt.Errorf("no input nodes added to the pipeline")
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
	if len(p.inputNodes) > 1 {
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

		// Connect all inputs to multiplexer.
		for _, inputNode := range p.inputNodes {
			inputNode.SetOutputChannel(p.multiplexer.GetInputChannel())
		}
	} else {
		p.inputNodes[0].SetOutputChannel(lastNode.GetInputChannel())
	}

	return nil
}

func (p *Pipeline) logTask() {
	defer p.waitGroup.Done()
	for logEntry := range p.logChannel {
		fmt.Printf("%s/%s : %v\n", logEntry.Module.GenericId(),
			logEntry.Module.SpecificId(), logEntry.Err)
	}
}

