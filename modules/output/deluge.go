package input

import (
	"fmt"

	"github.com/brunoga/go-pipeliner/datatypes"

	deluge "github.com/brunoga/go-deluge"
	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type DelugeOutputModule struct {
	*pipeliner_modules.GenericOutputModule

	delugeClient *deluge.Deluge
}

func NewDelugeOutputModule(specificId string) *DelugeOutputModule {
	delugeOutputModule := &DelugeOutputModule{
		pipeliner_modules.NewGenericOutputModule("Deluge Output Module",
			"1.0.0", "deluge", specificId, nil),
		nil,
	}
	delugeOutputModule.SetConsumerFunc(delugeOutputModule.sendItemToDeluge)

	return delugeOutputModule
}

func (m *DelugeOutputModule) Configure(
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

func (m *DelugeOutputModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"server":   "",
		"password": "",
	}
}

func (m *DelugeOutputModule) Duplicate(specificId string) (base_modules.Module,
	error) {
	duplicate := NewDelugeOutputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerOutputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *DelugeOutputModule) sendItemToDeluge(
	item *datatypes.PipelineItem) bool {
	// Use first URL available.
	// TODO(bga): Allow user to define preferred hosts or URL types (e.g.
	// prefer magnet links).
	torrentUrl, err := item.GetUrl(0)
	if err != nil {
		// TODO(bga): Log error.
		return false
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
		return false
	}

	return true
}

func init() {
	pipeliner_modules.RegisterPipelinerOutputModule(
		NewDelugeOutputModule(""))
}
