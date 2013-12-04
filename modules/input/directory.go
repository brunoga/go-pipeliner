package input

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type DirectoryInputModule struct {
	*pipeliner_modules.GenericInputModule

	path      string
	recursive bool
}

func NewDirectoryInputModule(specificId string) *DirectoryInputModule {
	directoryInputModule := &DirectoryInputModule{
		pipeliner_modules.NewGenericInputModule(
			"Directory Input Module", "1.0.0",
			"directory", specificId, nil),
		"",
		false,
	}
	directoryInputModule.SetGeneratorFunc(
		directoryInputModule.setupReadDirectory)

	return directoryInputModule
}

func (m *DirectoryInputModule) Configure(
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

func (m *DirectoryInputModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"path":      "",
		"recursive": "false",
	}
}

func (m *DirectoryInputModule) Duplicate(
	specificId string) (base_modules.Module, error) {
	duplicate := NewDirectoryInputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerInputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *DirectoryInputModule) setupReadDirectory(
	generatorChannel chan<- *datatypes.PipelineItem,
	generatorControlChannel <-chan struct{}) {
	defer close(generatorChannel)

	readDirectory(m.GenericId(), m.path, m.recursive, generatorChannel,
		generatorControlChannel)
}

func readDirectory(genericId, path string, recursive bool,
	generatorChannel chan<- *datatypes.PipelineItem,
	generatorControlChannel <-chan struct{}) {
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		// TODO(bga): Log error.
		return
	}

L:
	for _, file := range fileInfos {
		if file.IsDir() && recursive {
			readDirectory(genericId, filepath.Join(path,
				file.Name()), true, generatorChannel,
				generatorControlChannel)
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
			case _, ok := <-generatorControlChannel:
				if !ok {
					break L
				}
			case generatorChannel <- pipelineItem:
				// Do nothing.
			}
		}
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerInputModule(
		NewDirectoryInputModule(""))
}
