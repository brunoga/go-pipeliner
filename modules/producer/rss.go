package input

import (
	"fmt"
	"net/url"

	"github.com/brunoga/go-pipeliner/datatypes"
	"github.com/mmcdole/gofeed"

	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	base_modules "gopkg.in/brunoga/go-modules.v1"
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
	rssProducerModule.SetProducerFunc(rssProducerModule.readRss)

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

func (m *RssProducerModule) readRss(
	producerChannel chan<- *datatypes.PipelineItem,
	producerControlChannel <-chan struct{}) {
	defer close(producerChannel)

	fp := gofeed.NewParser()
	feed, err := fp.ParseURL(m.rssUrl.String())
	if err != nil {
		// TODO(bga): Handle errors.
		return
	}

	for _, item := range feed.Items {
		pipelineItem := datatypes.NewPipelineItem(m.GenericId())
		pipelineItem.SetName(item.Title)
		pipelineItem.SetDescription(item.Description)
		pipelineItem.SetDate(*item.PublishedParsed)
		pipelineItem.AddUrlString(item.Link)
		pipelineItem.AddPayload("rss", item)
		select {
		case _, ok := <-producerControlChannel:
			if !ok {
				break
			}
		case producerChannel <- pipelineItem:
			// Do nothing.
		}

	}
}

func init() {
	pipeliner_modules.RegisterPipelinerProducerModule(NewRssProducerModule(""))
}
