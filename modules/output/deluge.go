package input

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
	deluge "github.com/brunoga/go-deluge"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type DelugeOutputModule struct {
	*base_modules.GenericModule

	inputChannel chan *datatypes.PipelineItem
	quitChannel  chan struct{}

	delugeClient *deluge.Deluge
}

func NewDelugeOutputModule(specificId string) *DelugeOutputModule {
	return &DelugeOutputModule{
		base_modules.NewGenericModule("Deluge Output Module", "1.0.0",
			"deluge", specificId, "pipeliner-output"),
		make(chan *datatypes.PipelineItem),
		make(chan struct{}),
		nil,
	}
}

func (m *DelugeOutputModule) Configure(params *base_modules.ParameterMap) error {
	serverParam, ok := (*params)["server"]
	if !ok || len(serverParam) == 0 {
		return fmt.Errorf("required server parameter not found")
	}

	passwordParam, ok := (*params)["password"]
	if !ok || len(passwordParam) == 0 {
		return fmt.Errorf("required password parameter not found")
	}

	delugeClient, err := deluge.New(serverParam, passwordParam)
	if err != nil {
		return err
	}

	m.delugeClient = delugeClient

	m.SetReady(true)

	return nil
}

func (m *DelugeOutputModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"server": "",
		"password": "",
	}
}


func (m *DelugeOutputModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := NewDelugeOutputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerOutputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *DelugeOutputModule) GetInputChannel() chan<- *datatypes.PipelineItem {
	return m.inputChannel
}

func (m *DelugeOutputModule) Start(waitGroup *sync.WaitGroup) error {
	if !m.Ready() {
		waitGroup.Done()
		return fmt.Errorf("not ready")
	}

	if m.inputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("input channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *DelugeOutputModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *DelugeOutputModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
L:
	for {
		select {
		case item, ok := <-m.inputChannel:
			if ok {
				m.sendToDeluge(item)
			} else {
				m.inputChannel = nil
				break L
			}
		case <-m.quitChannel:
			break L
		}
	}
}

func (m *DelugeOutputModule) sendToDeluge(item *datatypes.PipelineItem) {
	// Use first URL available.
	// TODO(bga): Allow user to define preferred hosts or URL types (e.g.
	// prefer magnet links).
	torrentUrl, err := item.GetUrl(0)
	if err != nil {
		// TODO(bga): Log error.
		return
	}

	// TODO(bga): Empty configuration for now.
	options := map[string]interface{}{}

	// TODO(bga): Handle errors when addding torrent.
	switch torrentUrl.Scheme {
	case "magnet":
		m.delugeClient.CoreAddTorrentMagnet(
			torrentUrl.String(), options)
	case "http":
		m.delugeClient.CoreAddTorrentUrl(
			torrentUrl.String(), options)
	default:
		// TODO(bga): Add handling of other types.
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerOutputModule(NewDelugeOutputModule(""))
}

