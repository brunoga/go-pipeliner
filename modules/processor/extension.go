package input

import (
	"fmt"
	"strings"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type ExtensionProcessorModule struct {
	*pipeliner_modules.GenericProcessorModule

	extension string
}

func NewExtensionProcessorModule(specificId string) *ExtensionProcessorModule {
	extensionProcessorModule := &ExtensionProcessorModule{
		pipeliner_modules.NewGenericProcessorModule(
			"Extension Processor Module", "1.0.0", "extension",
			specificId, nil),
		"",
	}
	extensionProcessorModule.SetProcessorFunc(
		extensionProcessorModule.filterExtension)

	return extensionProcessorModule
}

func (m *ExtensionProcessorModule) Configure(params *base_modules.ParameterMap) error {
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

func (m *ExtensionProcessorModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"extension": "",
	}
}

func (m *ExtensionProcessorModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := NewExtensionProcessorModule(specificId)
	err := pipeliner_modules.RegisterPipelinerProcessorModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *ExtensionProcessorModule) filterExtension(
	item *datatypes.PipelineItem) bool {
	checkedUrl, err := item.GetUrl(0)
	if err != nil {
		// TODO(bga): Log error.
		return true
	}

	if !strings.HasSuffix(checkedUrl.Path, m.extension) {
		return true
	}

	return false
}

func init() {
	pipeliner_modules.RegisterPipelinerProcessorModule(
		NewExtensionProcessorModule(""))
}
