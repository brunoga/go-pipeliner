package input

import (
	"fmt"
	"strings"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type ExtensionFilterModule struct {
	*pipeliner_modules.GenericFilterModule

	extension string
}

func NewExtensionFilterModule(specificId string) *ExtensionFilterModule {
	extensionFilterModule := &ExtensionFilterModule{
		pipeliner_modules.NewGenericFilterModule(
			"Extension Filter Module", "1.0.0", "extension",
			specificId, nil),
		"",
	}
	extensionFilterModule.SetFilterFunc(
		extensionFilterModule.filterExtension)

	return extensionFilterModule
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

func (m *ExtensionFilterModule) filterExtension(
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
	pipeliner_modules.RegisterPipelinerFilterModule(
		NewExtensionFilterModule(""))
}

