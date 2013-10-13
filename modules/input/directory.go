package input

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type DirectoryInputModule struct {
	*base_modules.GenericModule

	outputChannel chan<- *datatypes.PipelineItem
	quitChannel   chan struct{}

	path      string
	recursive bool
}

type internalItem struct {
	filePath *string
	fileInfo *os.FileInfo
}

func NewDirectoryInputModule(specificId string) *DirectoryInputModule {
	return &DirectoryInputModule{
		base_modules.NewGenericModule("Directory Input Module", "1.0.0",
			"directory", specificId, "pipeliner-input"),
		nil,
		make(chan struct{}),
		"",
		false,
	}
}

func (m *DirectoryInputModule) Configure(params *base_modules.ParameterMap) error {
	var ok bool

	pathParam, ok := (*params)["path"]
	if !ok || pathParam == "" {
		return fmt.Errorf("required path parameter not found")
	}

	processedPath, err := filepath.Abs(filepath.Clean(pathParam))
	if err != nil {
		return fmt.Errorf("error processing path : %v", err)
	}

	m.path = processedPath

	recursiveParam, ok := (*params)["recursive"]
	if !ok {
		// Non-recursive is the default.
		m.recursive = false
	} else {
		if recursiveParam == "true" {
			m.recursive = true
		} else if recursiveParam == "false" {
			m.recursive = false
		} else {
			return fmt.Errorf("invalid recursive parameter")
		}
	}

	m.SetReady(true)

	return nil
}

func (m *DirectoryInputModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"path":      "",
		"recursive": "false",
	}
}

func (m *DirectoryInputModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := NewDirectoryInputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerInputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *DirectoryInputModule) SetOutputChannel(outputChannel chan<- *datatypes.PipelineItem) error {
	if outputChannel == nil {
		return fmt.Errorf("can't set output to a nil channel")
	}

	m.outputChannel = outputChannel
	return nil
}

func (m *DirectoryInputModule) Start(waitGroup *sync.WaitGroup) error {
	if !m.Ready() {
		waitGroup.Done()
		return fmt.Errorf("not ready")
	}

	if m.outputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("output channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *DirectoryInputModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *DirectoryInputModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	itemChannel := make(chan *internalItem)
	itemControlChannel := make(chan struct{})
	go setupReadDirectory(m.path, m.recursive, itemChannel, itemControlChannel)

L:
	for {
		select {
		case item, ok := <-itemChannel:
			if ok {
				fileUrl, err := url.Parse("file:///" + filepath.Join(
					*item.filePath, (*item.fileInfo).Name()))
				if err != nil {
					// TODO(bga): Log error.
					continue
				}

				pipelineItem := datatypes.NewPipelineItem(m.GenericId())
				_ = pipelineItem.AddUrl(fileUrl)
				pipelineItem.SetName(fileUrl.Path)
				pipelineItem.AddPayload("directory", item.fileInfo)
				m.outputChannel <- pipelineItem
			} else {
				close(m.outputChannel)
				break L
			}
		case <-m.quitChannel:
			close(itemControlChannel)
			break L
		}
	}
}

func setupReadDirectory(path string, recursive bool,
	itemChannel chan<- *internalItem, itemControlChannel <-chan struct{}) {
	defer close(itemChannel)
	readDirectory(path, recursive, itemChannel, itemControlChannel)
}

func readDirectory(path string, recursive bool, itemChannel chan<- *internalItem,
	itemControlChannel <-chan struct{}) {
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}

L:
	for _, file := range fileInfos {
		if file.IsDir() && recursive {
			readDirectory(filepath.Join(path,
				file.Name()), true, itemChannel, itemControlChannel)
		} else if !file.IsDir() {
			select {
			case _, ok := <-itemControlChannel:
				if !ok {
					break L
				}
			case itemChannel <- &internalItem{&path, &file}:
				// Do nothing.
			}
		}
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerInputModule(NewDirectoryInputModule(""))
}
