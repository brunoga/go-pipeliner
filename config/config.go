// config project config.go
package config

import (
	"fmt"

	"github.com/brunoga/go-pipeliner/pipeline"
	"github.com/kylelemons/go-gypsy/yaml"

	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	modules_base "gopkg.in/brunoga/go-modules.v1"

	_ "github.com/brunoga/go-pipeliner/modules/consumer"
	_ "github.com/brunoga/go-pipeliner/modules/processor"
	_ "github.com/brunoga/go-pipeliner/modules/producer"
)

type Config struct {
	yamlFile *yaml.File

	pipelines []*pipeline.Pipeline
}

func New(path string) (*Config, error) {
	yamlFile, err := yaml.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := &Config{
		yamlFile:  yamlFile,
		pipelines: nil,
	}

	err = config.process()
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (c *Config) StartPipelines() error {
	for _, pipeline := range c.pipelines {
		err := pipeline.Start()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Config) WaitPipelines() {
	for _, pipeline := range c.pipelines {
		pipeline.Wait()
	}
}

func (c *Config) Dump() {
	for _, pipeline := range c.pipelines {
		pipeline.Dump()
		fmt.Println("")
	}
}

func (c *Config) process() error {
	return processListOrMapNode(c.yamlFile.Root, true, func(node yaml.Node, key string) error {
		pipeline, err := validatePipeline(node, key)
		if err != nil {
			return err
		}

		c.pipelines = append(c.pipelines, pipeline)

		return nil
	})
}

func configureModule(node yaml.Node, module modules_base.Module) error {
	nodeMap, ok := node.(yaml.Map)
	if !ok {
		return fmt.Errorf("unexpected node type")
	}

	parameters := module.Parameters()

	for key, configValueNode := range nodeMap {
		if key == "name" {
			continue
		}

		_, ok := (*parameters)[key]
		if !ok {
			return fmt.Errorf("unknown parameter %q", key)
		}

		configValue, ok := configValueNode.(yaml.Scalar)
		if !ok {
			return fmt.Errorf("node has parameter field with invalid type")
		}

		(*parameters)[key] = configValue.String()
	}

	err := module.Configure(parameters)
	if err != nil {
		return err
	}

	if !module.Ready() {
		return fmt.Errorf("module not ready after configuration")
	}

	return nil
}

func setupModule(node yaml.Node, key string) (modules_base.Module, error) {
	nameNode, err := yaml.Child(node, ".name")
	if err != nil || nameNode == nil {
		fmt.Println(node)
		return nil, fmt.Errorf("node has no name field")
	}

	nameField, ok := nameNode.(yaml.Scalar)
	if !ok {
		return nil, fmt.Errorf("node has name field with invalid type")
	}

	name := nameField.String()

	defaultModule := modules_base.GetDefaultModuleByGenericId(key)
	if defaultModule == nil {
		return nil, fmt.Errorf("no modules with generic id %q", key)
	}

	specificModule := modules_base.GetModuleById(key, name)
	if specificModule != nil {
		return nil, fmt.Errorf("module with name %q already exists", name)
	}

	module, err := defaultModule.Duplicate(name)
	if err != nil {
		return nil, err
	}

	err = configureModule(node, module)
	if err != nil {
		return nil, err
	}

	return module, nil
}

func processProducerNode(producerNode yaml.Node, pipeline *pipeline.Pipeline) error {
	return processListOrMapNode(producerNode, true, func(node yaml.Node, key string) error {
		module, err := setupModule(node, key)
		if err != nil {
			return err
		}

		if module.Type() != "pipeliner-producer" {
			return fmt.Errorf("%s is not a pipeliner producer module",
				module.GenericId())
		}

		pipeline.AddProducerNode(module.(pipeliner_modules.PipelinerProducerModule))

		return nil
	})
}

func processProcessorNode(processorNode yaml.Node, pipeline *pipeline.Pipeline) error {
	return processListOrMapNode(processorNode, true, func(node yaml.Node, key string) error {
		module, err := setupModule(node, key)
		if err != nil {
			return err
		}

		if module.Type() != "pipeliner-processor" {
			return fmt.Errorf("%s is not a pipeliner processor module",
				module.GenericId())
		}

		pipeline.AddProcessorNode(module.(pipeliner_modules.PipelinerProcessorModule))

		return nil
	})
}

func processConsumerNode(consumerNode yaml.Node, pipeline *pipeline.Pipeline) error {
	return processListOrMapNode(consumerNode, true, func(node yaml.Node, key string) error {
		module, err := setupModule(node, key)
		if err != nil {
			return err
		}

		if module.Type() != "pipeliner-consumer" {
			return fmt.Errorf("%s is not a pipeliner consumer module",
				module.GenericId())
		}

		pipeline.AddConsumerNode(module.(pipeliner_modules.PipelinerConsumerModule))

		return nil
	})
}

func validatePipeline(pipelineNode yaml.Node, key string) (*pipeline.Pipeline, error) {
	if key != "pipeline" {
		return nil, fmt.Errorf("Expected \"pipeline\" node. Got \"%s\".", key)
	}

	nameNode, err := yaml.Child(pipelineNode, ".name")
	if err != nil {
		return nil, err
	}
	if nameNode == nil {
		return nil, fmt.Errorf("Missing name field in pipeline.")
	}

	pipeline := pipeline.New(nameNode.(yaml.Scalar).String())

	producerNode, err := yaml.Child(pipelineNode, ".producer")
	if err != nil {
		return nil, err
	}
	if producerNode == nil {
		return nil, fmt.Errorf("Missing producer field in pipeline.")
	}

	err = processProducerNode(producerNode, pipeline)
	if err != nil {
		return nil, err
	}

	// Processor nodes are optional.
	processorNode, err := yaml.Child(pipelineNode, ".processor")
	if err != nil {
		if _, ok := err.(*yaml.NodeNotFound); !ok {
			return nil, err
		}

	}
	if processorNode != nil {
		err = processProcessorNode(processorNode, pipeline)
		if err != nil {
			return nil, err
		}
	}

	consumerNode, err := yaml.Child(pipelineNode, ".consumer")
	if err != nil {
		return nil, err
	}
	if consumerNode == nil {
		return nil, fmt.Errorf("Missing consumer field in pipeline.")
	}

	err = processConsumerNode(consumerNode, pipeline)
	if err != nil {
		return nil, err
	}

	return pipeline, nil
}

func processListOrMapNode(node yaml.Node, requireList bool,
	mapFunc func(yaml.Node, string) error) error {
	switch checkedNode := node.(type) {
	case yaml.List:
		if !requireList {
			return fmt.Errorf("found list node. Expected map node")
		}
		for _, subNode := range checkedNode {
			err := processListOrMapNode(subNode, false, mapFunc)
			if err != nil {
				return err
			}
		}
	case yaml.Map:
		if requireList {
			return fmt.Errorf("found map node. Expected list node")
		}
		for key, subNode := range checkedNode {
			err := mapFunc(subNode, key)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("Invalid node type %T. Expected List or Map.", checkedNode)
	}

	return nil
}
