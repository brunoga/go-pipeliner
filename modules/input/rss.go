package input

import (
	"fmt"
	"net/url"
	"sync"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	rss "github.com/jteeuwen/go-pkg-rss"
)

type RssInputModule struct {
	*base_modules.GenericModule

	outputChannel chan<- interface{}
	quitChannel   chan struct{}

	rssUrl *url.URL
}

func NewRssInputModule(specificId string) *RssInputModule {
	return &RssInputModule{
		base_modules.NewGenericModule("RSS Input Module", "1.0.0",
			"rss", specificId, "pipeliner-input"),
		nil,
		make(chan struct{}),
		nil,
	}
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

func (m *RssInputModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := NewRssInputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerInputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *RssInputModule) SetOutputChannel(outputChannel chan<- interface{}) error {
	if outputChannel == nil {
		return fmt.Errorf("can't set output to a nil channel")
	}

	m.outputChannel = outputChannel
	return nil
}

func (m *RssInputModule) Start(waitGroup *sync.WaitGroup) error {
	if !m.Ready() {
		waitGroup.Done()
		return fmt.Errorf("not ready")
	}

	if m.outputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("output channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *RssInputModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *RssInputModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	itemChannel := make(chan interface{})
	itemControlChannel := make(chan struct{})
	go setupReadRss(m.rssUrl, itemChannel, itemControlChannel)

L:
	for {
		select {
		case item, ok := <-itemChannel:
			if ok {
				m.outputChannel <- item
			} else {
				close(m.outputChannel)
				break L
			}
		case <-m.quitChannel:
			close(itemControlChannel)
			break L
		}
	}
}

func setupReadRss(rssUrl *url.URL, itemChannel chan<- interface{},
	itemControlChannel <-chan struct{}) {
	defer close(itemChannel)
	readRss(rssUrl, itemChannel, itemControlChannel)
}

func readRss(rssUrl *url.URL, itemChannel chan<- interface{},
	itemControlChannel <-chan struct{}) {
	feed := rss.New(5, true, nil, func(feed *rss.Feed, ch *rss.Channel, newItems []*rss.Item) {
	L:
		for _, item := range newItems {
			select {
			case _, ok := <-itemControlChannel:
				if !ok {
					break L
				}
			case itemChannel <- item.Links[0].Href:
				// Do nothing.
			}
		}
	})
	feed.Fetch(rssUrl.String(), nil)
}

func init() {
	pipeliner_modules.RegisterPipelinerInputModule(NewRssInputModule(""))
}
