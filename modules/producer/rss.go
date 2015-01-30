package input

import (
	"fmt"
	"net/url"

	"github.com/brunoga/go-pipeliner/datatypes"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	rss "github.com/jteeuwen/go-pkg-rss"
)

type RssProducerModule struct {
	*pipeliner_modules.GenericProducerModule

	rssUrl *url.URL
}

func NewRssProducerModule(specificId string) *RssProducerModule {
	rssProducerModule := &RssProducerModule{
		pipeliner_modules.NewGenericProducerModule("RSS Producer Module",
			"1.0.0", "rss", specificId, nil),
		nil,
	}
	rssProducerModule.SetProducerFunc(rssProducerModule.setupReadRss)

	return rssProducerModule
}

func (m *RssProducerModule) Configure(params *base_modules.ParameterMap) error {
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

func (m *RssProducerModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"url": "",
	}
}

func (m *RssProducerModule) Duplicate(specificId string) (base_modules.Module,
	error) {
	duplicate := NewRssProducerModule(specificId)
	err := pipeliner_modules.RegisterPipelinerProducerModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *RssProducerModule) setupReadRss(
	producerChannel chan<- *datatypes.PipelineItem,
	producerControlChannel <-chan struct{}) {
	defer close(producerChannel)

	readRss(m.GenericId(), m.rssUrl, producerChannel,
		producerControlChannel)
}

func readRss(genericId string, rssUrl *url.URL,
	producerChannel chan<- *datatypes.PipelineItem,
	producerControlChannel <-chan struct{}) {
	feed := rss.New(5, true, nil,
		func(feed *rss.Feed, ch *rss.Channel, newItems []*rss.Item) {
		L:
			for _, item := range newItems {
				pipelineItem := datatypes.NewPipelineItem(
					genericId)
				pipelineItem.SetName(item.Title)
				pipelineItem.SetDescription(item.Description)

				itemDate, err := item.ParsedPubDate()
				if err != nil {
					pipelineItem.SetDate(itemDate)
				}

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
				case _, ok := <-producerControlChannel:
					if !ok {
						break L
					}
				case producerChannel <- pipelineItem:
					// Do nothing.
				}
			}
		})
	feed.Fetch(rssUrl.String(), nil)
}

func init() {
	pipeliner_modules.RegisterPipelinerProducerModule(NewRssProducerModule(""))
}
