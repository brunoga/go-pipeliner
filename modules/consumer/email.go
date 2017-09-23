package input

import (
	"fmt"
	"net/smtp"
	"strings"
	"sync"

	"github.com/brunoga/go-pipeliner/datatypes"

	pipeliner_modules "github.com/brunoga/go-pipeliner/modules"
	base_modules "gopkg.in/brunoga/go-modules.v1"
)

type EmailConsumerModule struct {
	*pipeliner_modules.GenericConsumerModule

	authUser     string
	authPassword string
	smtpServer   string
	from         string
	to           string
	subject      string
}

func NewEmailConsumerModule(specificId string) *EmailConsumerModule {
	emailConsumerModule := &EmailConsumerModule{
		pipeliner_modules.NewGenericConsumerModule("E-Mail Consumer Module",
			"1.0.0", "email", specificId, nil),
		"",
		"",
		"",
		"",
		"",
		"",
	}
	emailConsumerModule.SetConsumerFunc(emailConsumerModule.sendEmail)

	return emailConsumerModule
}

func (m *EmailConsumerModule) Configure(params *base_modules.ParameterMap) error {
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

func (m *EmailConsumerModule) Parameters() *base_modules.ParameterMap {
	return &base_modules.ParameterMap{
		"auth_user":     "",
		"auth_password": "",
		"smtp_server":   "",
		"from":          "",
		"to":            "",
		"subject":       "Go Pipeliner Consumer",
	}
}

func (m *EmailConsumerModule) Duplicate(specificId string) (base_modules.Module, error) {
	duplicate := NewEmailConsumerModule(specificId)
	err := pipeliner_modules.RegisterPipelinerConsumerModule(duplicate)
	if err != nil {
		return nil, err
	}

	return duplicate, nil
}

func (m *EmailConsumerModule) sendEmail(
	consumerChannel <-chan *datatypes.PipelineItem,
	waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	// Setup body.
	body := "To: " + m.to + "\r\nSubject: " + m.subject + "\r\n\r\n"

	// Add items to body.
	i := 1
	for pipelineItem := range consumerChannel {
		body += fmt.Sprintf("%d : %v\r\n", i, pipelineItem)
		i++
	}

	// Send email.
	err := smtp.SendMail(m.smtpServer, smtp.PlainAuth("", m.authUser,
		m.authPassword, strings.Split(m.smtpServer, ":")[0]), m.from,
		[]string{m.to}, []byte(body))
	if err != nil {
		// TODO(bga): Log error.
	}
}

func init() {
	pipeliner_modules.RegisterPipelinerConsumerModule(
		NewEmailConsumerModule(""))
}
