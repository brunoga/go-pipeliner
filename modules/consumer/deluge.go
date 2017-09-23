package input

import (
	"fmt"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"

	deluge "github.com/brunoga/go-deluge"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	base_modules "gopkg.in/brunoga/go-modules.v1"
)

type DelugeConsumerModule struct {
	*pipeliner_modules.GenericConsumerModule

	delugeClient *deluge.Deluge
}

func NewDelugeConsumerModule(specificId string) *DelugeConsumerModule {
	delugeConsumerModule := &DelugeConsumerModule{
		pipeliner_modules.NewGenericConsumerModule("Deluge Consumer Module",
			"1.0.0", "deluge", specificId, nil),
		nil,
	}
	delugeConsumerModule.SetConsumerFunc(delugeConsumerModule.sendItemToDeluge)

	return delugeConsumerModule
}

func (m *DelugeConsumerModule) Configure(
	params *base_modules.ParameterMap) error {
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

func (m *DelugeConsumerModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"server":   "",
		"password": "",
	}
}

func (m *DelugeConsumerModule) Duplicate(specificId string) (base_modules.Module,
	error) {
	duplicate := NewDelugeConsumerModule(specificId)
	err := pipeliner_modules.RegisterPipelinerConsumerModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *DelugeConsumerModule) sendItemToDeluge(
	consumerChannel <-chan *datatypes.PipelineItem,
	waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	for pipelineItem := range consumerChannel {
		// Use first URL available.
		// TODO(bga): Allow user to define preferred hosts or URL types
		// (e.g. prefer magnet links).
		torrentUrl, err := pipelineItem.GetUrl(0)
		if err != nil {
			// TODO(bga): Log error.
			continue
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
			continue
		}
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerConsumerModule(
		NewDelugeConsumerModule(""))
}
