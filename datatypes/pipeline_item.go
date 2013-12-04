package datatypes

import (
	"fmt"
	"net/url"
)

// PayloadMap is a container for module-specific data that can be added to a
// PipelineItem while it traverses the pipeline.
type PayloadMap map[string]interface{}

// PipelineItem represents an item that is traversing the pipeline.
type PipelineItem struct {
	inputGenericId string

	name string
	urls []*url.URL

	payload PayloadMap
}

// NewPipelineItem creates a new item with the given inputGenericId (i.e. the
// generic id of the plugin that inserted it in the pipeline). The returned
// PipelineItem has all fields initialized with default values.
func NewPipelineItem(inputGenericId string) *PipelineItem {
	return &PipelineItem{
		inputGenericId,
		"",
		make([]*url.URL, 0),
		make(PayloadMap),
	}
}

// GetInputGenericId returns the generic id for the input that created this
// item.
func (i *PipelineItem) GetInputGenericId() string {
	return i.inputGenericId
}

func (i *PipelineItem) AddUrl(itemUrl *url.URL) int {
	i.urls = append(i.urls, itemUrl)
	return len(i.urls) - 1
}

func (i *PipelineItem) AddUrlString(itemUrlString string) (int, error) {
	parsedUrl, err := url.Parse(itemUrlString)
	if err != nil {
		return -1, err
	}

	return i.AddUrl(parsedUrl), nil
}

func (i *PipelineItem) GetUrl(index int) (*url.URL, error) {
	if index > (len(i.urls)-1) || index < 0 {
		return nil, fmt.Errorf("index out of bounds")
	}

	return i.urls[index], nil
}

func (i *PipelineItem) SetName(itemName string) {
	i.name = itemName
}

func (i *PipelineItem) GetName() string {
	return i.name
}

func (i *PipelineItem) AddPayload(payloadId string, payload interface{}) error {
	_, ok := i.payload[payloadId]
	if ok {
		return fmt.Errorf("payload with id %q already exists", payloadId)
	}

	i.payload[payloadId] = payload

	return nil
}

func (i *PipelineItem) GetPayload(payloadId string) (interface{}, error) {
	data, ok := i.payload[payloadId]
	if !ok {
		return nil, fmt.Errorf("payload with id %q does not exist", payloadId)
	}

	return data, nil
}
