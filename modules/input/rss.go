package input

import (
	"fmt"
	"net/url"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	rss "github.com/jteeuwen/go-pkg-rss"
)

type RssInputModule struct {
	*pipeliner_modules.GenericInputModule

	rssUrl *url.URL
}

func NewRssInputModule(specificId string) *RssInputModule {
	rssInputModule := &RssInputModule{
		pipeliner_modules.NewGenericInputModule("RSS Input Module",
			"1.0.0", "rss", specificId, nil),
		nil,
	}
	rssInputModule.SetGeneratorFunc(rssInputModule.setupReadRss)

	return rssInputModule
}

func (m *RssInputModule) Configure(params *base_modules.ParameterMap) error {
	var ok bool

	urlParamStr, ok := (*params)["url"]
	if !ok || len(urlParamStr) == 0 {
		return fmt.Errorf("required url parameter not found")
	}

	parsedUrlParam, err := url.Parse(urlParamStr)
	if err != nil {
		return fmt.Errorf("error processing url : %v", err)
	}

	m.rssUrl = parsedUrlParam

	m.SetReady(true)

	return nil
}

func (m *RssInputModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"url": "",
	}
}

func (m *RssInputModule) Duplicate(specificId string) (base_modules.Module,
	error) {
	duplicate := NewRssInputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerInputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *RssInputModule) setupReadRss(
	generatorChannel chan<- *datatypes.PipelineItem,
	generatorControlChannel <-chan struct{}) {
	defer close(generatorChannel)

	readRss(m.GenericId(), m.rssUrl, generatorChannel,
		generatorControlChannel)
}

func readRss(genericId string, rssUrl *url.URL,
	generatorChannel chan<- *datatypes.PipelineItem,
	generatorControlChannel <-chan struct{}) {
	feed := rss.New(5, true, nil,
		func(feed *rss.Feed, ch *rss.Channel, newItems []*rss.Item) {
		L:
			for _, item := range newItems {
				pipelineItem := datatypes.NewPipelineItem(
					genericId)
				pipelineItem.SetName(item.Title)
				for _, itemUrl := range item.Links {
					_, err := pipelineItem.AddUrlString(
						itemUrl.Href)
					if err != nil {
						// TODO(bga): Log error.
						continue
					}
				}
				pipelineItem.AddPayload("rss", item)

				select {
				case _, ok := <-generatorControlChannel:
					if !ok {
						break L
					}
				case generatorChannel <- pipelineItem:
					// Do nothing.
				}
			}
		})
	feed.Fetch(rssUrl.String(), nil)
}

func init() {
	pipeliner_modules.RegisterPipelinerInputModule(NewRssInputModule(""))
}
