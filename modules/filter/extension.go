package input

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type ExtensionFilterModule struct {
	*base_modules.GenericModule

	inputChannel  chan interface{}
	outputChannel chan<- interface{}
	quitChannel   chan interface{}

	extension string
}

func NewExtensionFilterModule(specificId string) *ExtensionFilterModule {
	return &ExtensionFilterModule{
		base_modules.NewGenericModule("Extension Filter Module",
			"1.0.0", "extension", specificId, "pipeliner-filter"),
		make(chan interface{}),
		nil,
		make(chan interface{}),
		"",
	}
}

func (m *ExtensionFilterModule) Configure(params *base_modules.ParameterMap) error {
	extensionParam, ok := (*params)["extension"]
	if !ok || extensionParam == "" {
		return fmt.Errorf("required extension parameter not found")
	}

	if !strings.HasPrefix(extensionParam, ".") {
		return fmt.Errorf("extension parameter must start with a dot (.)")
	}

	m.extension = extensionParam

	m.SetReady(true)

	return nil
}

func (m *ExtensionFilterModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"extension": "",
	}
}

func (m *ExtensionFilterModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := NewExtensionFilterModule(specificId)
	err := pipeliner_modules.RegisterPipelinerFilterModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *ExtensionFilterModule) GetInputChannel() chan<- interface{} {
	return m.inputChannel
}

func (m *ExtensionFilterModule) SetOutputChannel(inputChannel chan<- interface{}) error {
	m.outputChannel = inputChannel

	return nil
}

func (m *ExtensionFilterModule) Start(waitGroup *sync.WaitGroup) error {
	if !m.Ready() {
		waitGroup.Done()
		return fmt.Errorf("not ready")
	}

	if m.inputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("input channel not connected")
	}

	if m.outputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("output channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *ExtensionFilterModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan interface{})
}

func (m *ExtensionFilterModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
L:
	for {
		select {
		case item, ok := <-m.inputChannel:
			if ok {
				m.checkItem(item)
			} else {
				close(m.outputChannel)
				break L
			}
		case <-m.quitChannel:
			break L
		}
	}
}

func (m *ExtensionFilterModule) checkItem(item interface{}) {
	checkedUrl, ok := item.(*url.URL)
	if !ok {
		// Ignore non-URL entries.
		return
	}

	if strings.HasSuffix(checkedUrl.Path, m.extension) {
		m.outputChannel <- checkedUrl
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerFilterModule(NewExtensionFilterModule(""))
}
