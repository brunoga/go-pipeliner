package input

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"

	"github.com/brunoga/go-pipeliner/datatypes"

	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	base_modules "gopkg.in/brunoga/go-modules.v1"
)

type DirectoryProducerModule struct {
	*pipeliner_modules.GenericProducerModule

	path      string
	recursive bool
}

func NewDirectoryProducerModule(specificId string) *DirectoryProducerModule {
	directoryProducerModule := &DirectoryProducerModule{
		pipeliner_modules.NewGenericProducerModule(
			"Directory Producer Module", "1.0.0",
			"directory", specificId, nil),
		"",
		false,
	}
	directoryProducerModule.SetProducerFunc(
		directoryProducerModule.setupReadDirectory)

	return directoryProducerModule
}

func (m *DirectoryProducerModule) Configure(
	params *base_modules.ParameterMap) error {
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

func (m *DirectoryProducerModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"path":      "",
		"recursive": "false",
	}
}

func (m *DirectoryProducerModule) Duplicate(
	specificId string) (base_modules.Module, error) {
	duplicate := NewDirectoryProducerModule(specificId)
	err := pipeliner_modules.RegisterPipelinerProducerModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *DirectoryProducerModule) setupReadDirectory(
	producerChannel chan<- *datatypes.PipelineItem,
	producerControlChannel <-chan struct{}) {
	defer close(producerChannel)

	readDirectory(m.GenericId(), m.path, m.recursive, producerChannel,
		producerControlChannel)
}

func readDirectory(genericId, path string, recursive bool,
	producerChannel chan<- *datatypes.PipelineItem,
	producerControlChannel <-chan struct{}) {
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		// TODO(bga): Log error.
		return
	}

L:
	for _, file := range fileInfos {
		if file.IsDir() && recursive {
			readDirectory(genericId, filepath.Join(path,
				file.Name()), true, producerChannel,
				producerControlChannel)
		} else if !file.IsDir() {
			fileUrl, err := url.Parse("file://" + filepath.Join(
				path, file.Name()))
			if err != nil {
				// TODO(bga): Log error.
				continue
			}

			pipelineItem := datatypes.NewPipelineItem(genericId)
			_ = pipelineItem.AddUrl(fileUrl)
			pipelineItem.SetName(fileUrl.Path)
			pipelineItem.AddPayload("directory", file)

			select {
			case _, ok := <-producerControlChannel:
				if !ok {
					break L
				}
			case producerChannel <- pipelineItem:
				// Do nothing.
			}
		}
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerProducerModule(
		NewDirectoryProducerModule(""))
}
