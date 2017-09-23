package modules

import (
	"github.com/brunoga/go-pipeliner/log"

	base_modules "gopkg.in/brunoga/go-modules.v1"
)

type GenericPipelineModule struct {
	*base_modules.GenericModule

	quitChannel chan struct{}
	logChannel  chan<- *log.LogEntry
}

func NewGenericPipelineModule(name, version, genericId, specificId,
	moduleType string) *GenericPipelineModule {
	return &GenericPipelineModule{
		base_modules.NewGenericModule(name, version,
			genericId, specificId, moduleType),
		make(chan struct{}),
		nil,
	}
}

func (m *GenericPipelineModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *GenericPipelineModule) SetLogChannel(
	logChannel chan<- *log.LogEntry) {
	m.logChannel = logChannel
}

func (m *GenericPipelineModule) Log(err error) {
	if m.logChannel != nil {
		m.logChannel <- log.NewLogEntry(m, err)
	}
}
