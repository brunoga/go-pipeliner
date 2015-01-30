package datatypes

import (
	"fmt"
	"net/url"
	"time"
)

// PayloadMap is a container for module-specific data that can be added to a
// PipelineItem while it traverses the pipeline.
type PayloadMap map[string]interface{}

// PipelineItem represents an item that is traversing the pipeline.
type PipelineItem struct {
	inputGenericId string

	name        string
	description string

	date time.Time

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
		"",
		time.Now(),
		make([]*url.URL, 0),
		make(PayloadMap),
	}
}

// GetInputGenericId returns the generic id for the input that created this
// item.
func (i *PipelineItem) GetInputGenericId() string {
	return i.inputGenericId
}

// AddUrl adds the given URL pointer to the list of URLs associated with this
// item, returning the index of the item just added.
func (i *PipelineItem) AddUrl(itemUrl *url.URL) int {
	i.urls = append(i.urls, itemUrl)
	// TODO(bga): This is race condition prone.
	return len(i.urls) - 1
}

// AddUrlString parses the given string as a URL and returns the index for the
// item just added and a nil error in case of success or a non-nil error in
// case of failure.
func (i *PipelineItem) AddUrlString(itemUrlString string) (int, error) {
	parsedUrl, err := url.Parse(itemUrlString)
	if err != nil {
		return -1, err
	}

	return i.AddUrl(parsedUrl), nil
}

// GetUrl returns the URL at given index, or an error in case the index is not
// valid.
func (i *PipelineItem) GetUrl(index int) (*url.URL, error) {
	if index > (len(i.urls)-1) || index < 0 {
		return nil, fmt.Errorf("index out of bounds")
	}

	return i.urls[index], nil
}

// SetName sets the name for the item.
func (i *PipelineItem) SetName(itemName string) {
	i.name = itemName
}

// GetName returns the name for this item.
func (i *PipelineItem) GetName() string {
	return i.name
}

// SetDescription sets the description for the item.
func (i *PipelineItem) SetDescription(itemDescription string) {
	i.description = itemDescription
}

// GetDescription returns the description for this item.
func (i *PipelineItem) GetDescription() string {
	return i.description
}

// SetDate sets the date for the item.
func (i *PipelineItem) SetDate(itemDate time.Time) {
	i.date = itemDate
}

// GetDate returns the date for this item.
func (i *PipelineItem) GetDate() time.Time {
	return i.date
}

// AddPayload adds the given payload and associates it with the given payloadId.
// It returns a nil error in case of success or a non-nil error otherwise.
func (i *PipelineItem) AddPayload(payloadId string, payload interface{}) error {
	_, ok := i.payload[payloadId]
	if ok {
		return fmt.Errorf("payload with id %q already exists", payloadId)
	}

	i.payload[payloadId] = payload

	return nil
}

// GetPayload returns the payload associated with the given payloadId on success
// or a non-nil error in the case of failure.
func (i *PipelineItem) GetPayload(payloadId string) (interface{}, error) {
	data, ok := i.payload[payloadId]
	if !ok {
		return nil, fmt.Errorf("payload with id %q does not exist", payloadId)
	}

	return data, nil
}

// String returns a string representation of the item. This satisfies the
// fmt.Stringer interface.
func (i *PipelineItem) String() string {
	return fmt.Sprintf("%s : %s : %s : %v", i.name, i.date, i.description, i.urls)
}
