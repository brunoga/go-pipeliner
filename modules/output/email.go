package input

import (
	"fmt"
	"net/smtp"
	"strings"
	"sync"

	base_modules "github.com/brunoga/go-modules"
	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
)

type EmailOutputModule struct {
	*base_modules.GenericModule

	inputChannel chan interface{}
	quitChannel  chan struct{}

	authUser     string
	authPassword string
	smtpServer   string
	from         string
	to           string
	subject      string

	emailItems []interface{}
}

func NewEmailOutputModule(specificId string) *EmailOutputModule {
	return &EmailOutputModule{
		base_modules.NewGenericModule("E-Mail Output Module", "1.0.0",
			"email", specificId, "pipeliner-output"),
		make(chan interface{}),
		make(chan struct{}),
		"",
		"",
		"",
		"",
		"",
		"",
		nil,
	}
}

func (m *EmailOutputModule) Configure(params *base_modules.ParameterMap) error {
	var ok bool

	authUserParam, ok := (*params)["auth_user"]
	if !ok || authUserParam == "" {
		return fmt.Errorf("required auth_user parameter not found")
	}

	m.authUser = authUserParam

	authPasswordParam, ok := (*params)["auth_password"]
	if !ok || authPasswordParam == "" {
		return fmt.Errorf("required auth_password parameter not found")
	}

	m.authPassword = authPasswordParam

	smtpServerParam, ok := (*params)["smtp_server"]
	if !ok || smtpServerParam == "" {
		return fmt.Errorf("required smtp_server parameter not found")
	}

	m.smtpServer = smtpServerParam

	fromParam, ok := (*params)["from"]
	if !ok || fromParam == "" {
		return fmt.Errorf("required from parameter not found")
	}

	m.from = fromParam

	toParam, ok := (*params)["to"]
	if !ok || toParam == "" {
		return fmt.Errorf("required to parameter not found")
	}

	m.to = toParam

	subjectParam, ok := (*params)["subject"]
	if !ok || subjectParam == "" {
		return fmt.Errorf("required subject parameter not found")
	}

	m.subject = subjectParam

	m.SetReady(true)

	return nil
}

func (m *EmailOutputModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"auth_user":     "",
		"auth_password": "",
		"smtp_server":   "",
		"from":          "",
		"to":            "",
		"subject":       "Go Pipeliner Output",
	}
}

func (m *EmailOutputModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := NewEmailOutputModule(specificId)
	err := pipeliner_modules.RegisterPipelinerOutputModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *EmailOutputModule) GetInputChannel() chan<- interface{} {
	return m.inputChannel
}

func (m *EmailOutputModule) Ready() bool {
	return true
}

func (m *EmailOutputModule) Start(waitGroup *sync.WaitGroup) error {
	if m.inputChannel == nil {
		waitGroup.Done()
		return fmt.Errorf("input channel not connected")
	}

	go m.doWork(waitGroup)

	return nil
}

func (m *EmailOutputModule) Stop() {
	close(m.quitChannel)
	m.quitChannel = make(chan struct{})
}

func (m *EmailOutputModule) doWork(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
L:
	for {
		select {
		case item, ok := <-m.inputChannel:
			if ok {
				m.emailItems = append(m.emailItems, item)
			} else {
				m.inputChannel = nil
				break L
			}
		case <-m.quitChannel:
			break L
		}
	}
	body := "To: " + m.to + "\r\nSubject: " + m.subject + "\r\n\r\n"
	for i, emailItem := range m.emailItems {
		body += fmt.Sprintf("%d : %v\r\n", i, emailItem)
	}
	smtp.SendMail(m.smtpServer, smtp.PlainAuth("", m.authUser,
		m.authPassword, strings.Split(m.smtpServer, ":")[0]), m.from,
		[]string{m.to}, []byte(body))
}

func init() {
	pipeliner_modules.RegisterPipelinerOutputModule(NewEmailOutputModule(""))
}
