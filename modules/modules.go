package modules

import (
	"github.com/brunoga/go-pipeliner/pipeline"

	base_modules "github.com/brunoga/go-modules"
)

type PipelinerProducerModule interface {
	// Include methods from the base module interface.
	base_modules.Module

	// Include methods required by pipeline input nodes.
	pipeline.ProducerNode
}

type PipelinerFilterModule interface {
	// Include methods from the base module interface.
	base_modules.Module

	// Include methods required by pipeline filter nodes.
	pipeline.FilterNode
}

type PipelinerOutputModule interface {
	// Include methods from the base module interface.
	base_modules.Module

	// include methods required by pipeline output nodes.
	pipeline.OutputNode
}

// RegisterPipelinerInputModule registers a Pipeliner producer module.
func RegisterPipelinerProducerModule(module PipelinerProducerModule) error {
	return base_modules.RegisterModule(module)
}

// RegisterPipelinerFilterModule registers a Pipeliner filter module.
func RegisterPipelinerFilterModule(module PipelinerFilterModule) error {
	return base_modules.RegisterModule(module)
}

// RegisterPipelineroutputModule registers a Pipeliner output module.
func RegisterPipelinerOutputModule(module PipelinerOutputModule) error {
	return base_modules.RegisterModule(module)
}
