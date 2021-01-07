package nsq

import (
	"net"
	"time"

	"github.com/herb-go/messagequeue"
	nsq "github.com/nsqio/go-nsq"
)

const DefaultChannel = "herb-go-default"

type Driver struct {
	Addr       string
	Channel    string
	LookupAddr string
	Config     *nsq.Config
}

func NewDriver() *Driver {
	return &Driver{
		Config: nsq.NewConfig(),
	}
}
func (d *Driver) SubscribeTopic(topic string, h messagequeue.MessageHandler) (messagequeue.Subscription, error) {
	var err error

	consumer, err := nsq.NewConsumer(topic, d.Channel, d.Config)
	if err != nil {
		return nil, err
	}
	s := &Subscription{
		Consumer:       consumer,
		MessageHandler: h,
	}
	consumer.AddHandler(nsq.HandlerFunc(s.hanlder))
	if d.LookupAddr != "" {
		err = consumer.ConnectToNSQLookupd(d.LookupAddr)
	} else {
		err = consumer.ConnectToNSQD(d.Addr)
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}
func (d *Driver) NewTopicPublisher(topic string) (messagequeue.Publisher, error) {
	nsqp, err := nsq.NewProducer(d.Addr, d.Config)
	if err != nil {
		return nil, err
	}
	p := &Publisher{
		Producer: nsqp,
		Topic:    topic,
	}
	return p, nil
}
func (d *Driver) Close() error {
	return nil
}

type Config struct {
	Addr                            string
	Channel                         string
	LookupAddr                      string
	LocalAddr                       string
	DialTimeoutInSecond             int64
	ReadTimeoutInSecond             int64
	WriteTimeoutInSecond            int64
	LookupdPollIntervalInSecond     int64
	LookupdPollJitter               float64
	MaxRequeueDelayInSecond         int64
	DefaultRequeueDelayInSecond     int64
	MaxBackoffDurationInSecond      int64
	BackoffMultiplierInSecond       int64
	MaxAttempts                     uint16
	LowRdyIdleTimeoutInSecond       int64
	LowRdyTimeoutInSecond           int64
	RDYRedistributeIntervalInSecond int64
	ClientID                        string
	Hostname                        string
	UserAgent                       string
	HeartbeatIntervalInSecond       int64
	SampleRate                      int32
	Deflate                         bool
	DeflateLevel                    int
	Snappy                          bool
	OutputBufferSize                int64
	OutputBufferTimeoutInSecond     int64
	MaxInFlight                     int
	MsgTimeoutInSecond              int64
	AuthSecret                      string
}

//ApplyTo apply config to queue
func (c *Config) ApplyTo(d *Driver) error {
	d.Addr = c.Addr
	d.LookupAddr = c.LookupAddr
	d.Channel = c.Channel
	if d.Channel == "" {
		d.Channel = DefaultChannel
	}
	if c.LocalAddr != "" {
		addr, err := net.ResolveTCPAddr("tcp", c.LocalAddr)
		if err != nil {
			return err
		}
		d.Config.LocalAddr = addr
	}
	if c.DialTimeoutInSecond != 0 {
		d.Config.DialTimeout = time.Duration(c.DialTimeoutInSecond) * time.Second
	}
	if c.ReadTimeoutInSecond != 0 {
		d.Config.ReadTimeout = time.Duration(c.ReadTimeoutInSecond) * time.Second
	}
	if c.WriteTimeoutInSecond != 0 {
		d.Config.WriteTimeout = time.Duration(c.WriteTimeoutInSecond) * time.Second
	}
	if c.LookupdPollIntervalInSecond != 0 {
		d.Config.LookupdPollInterval = time.Duration(c.LookupdPollIntervalInSecond) * time.Second
	}
	if c.LookupdPollJitter != 0 {
		d.Config.LookupdPollJitter = c.LookupdPollJitter
	}
	if c.MaxRequeueDelayInSecond != 0 {
		d.Config.MaxRequeueDelay = time.Duration(c.MaxRequeueDelayInSecond) * time.Second
	}
	if c.DefaultRequeueDelayInSecond != 0 {
		d.Config.DefaultRequeueDelay = time.Duration(c.DefaultRequeueDelayInSecond) * time.Second
	}
	if c.MaxBackoffDurationInSecond != 0 {
		d.Config.MaxBackoffDuration = time.Duration(c.MaxBackoffDurationInSecond) * time.Second
	}
	if c.BackoffMultiplierInSecond != 0 {
		d.Config.BackoffMultiplier = time.Duration(c.BackoffMultiplierInSecond) * time.Second
	}
	if c.MaxAttempts != 0 {
		d.Config.MaxAttempts = c.MaxAttempts
	}
	if c.LowRdyIdleTimeoutInSecond != 0 {
		d.Config.LowRdyIdleTimeout = time.Duration(c.LowRdyIdleTimeoutInSecond) * time.Second
	}
	if c.LowRdyTimeoutInSecond != 0 {
		d.Config.LowRdyTimeout = time.Duration(c.LowRdyTimeoutInSecond) * time.Second
	}
	if c.RDYRedistributeIntervalInSecond != 0 {
		d.Config.RDYRedistributeInterval = time.Duration(c.RDYRedistributeIntervalInSecond) * time.Second
	}
	if c.ClientID != "" {
		d.Config.ClientID = c.ClientID
	}
	if c.Hostname != "" {
		d.Config.Hostname = c.Hostname
	}
	if c.UserAgent != "" {
		d.Config.UserAgent = c.UserAgent
	}
	if c.HeartbeatIntervalInSecond != 0 {
		d.Config.HeartbeatInterval = time.Duration(c.HeartbeatIntervalInSecond) * time.Second
	}
	if c.SampleRate != 0 {
		d.Config.SampleRate = c.SampleRate
	}
	if c.Deflate != false {
		d.Config.Deflate = c.Deflate
	}
	if c.DeflateLevel != 0 {
		d.Config.DeflateLevel = c.DeflateLevel
	}
	if c.Snappy != false {
		d.Config.Snappy = c.Snappy
	}
	if c.OutputBufferSize != 0 {
		d.Config.OutputBufferSize = c.OutputBufferSize
	}
	if c.OutputBufferTimeoutInSecond != 0 {
		d.Config.OutputBufferTimeout = time.Duration(c.OutputBufferTimeoutInSecond) * time.Second
	}
	if c.MaxInFlight != 0 {
		d.Config.MaxInFlight = c.MaxInFlight
	}
	if c.MsgTimeoutInSecond != 0 {
		d.Config.MsgTimeout = time.Duration(c.MsgTimeoutInSecond) * time.Second
	}
	if c.AuthSecret != "" {
		d.Config.AuthSecret = c.AuthSecret
	}
	return nil
}

func NewConfig() *Config {
	return &Config{}
}

type Publisher struct {
	Topic    string
	Producer *nsq.Producer
}

func (p *Publisher) Publish(message []byte) error {
	return p.Producer.Publish(p.Topic, message)
}

func (p *Publisher) Close() error {
	p.Producer.Stop()
	return nil
}

type Subscription struct {
	Consumer       *nsq.Consumer
	MessageHandler messagequeue.MessageHandler
}

func (s *Subscription) hanlder(msg *nsq.Message) error {
	m := messagequeue.NewMessage().
		WithID(string(msg.ID[0:16])).
		WithData(msg.Body)
	messagequeue.HandleMesage(s.MessageHandler, m)
	return nil
}
func (s *Subscription) Unsubscribe() error {
	s.Consumer.Stop()
	return nil
}

func Factory(loader func(interface{}) error) (messagequeue.Driver, error) {
	c := NewConfig()
	var err error
	err = loader(c)
	if err != nil {
		return nil, err
	}

	q := NewDriver()
	err = c.ApplyTo(q)
	if err != nil {
		return nil, err
	}
	return q, nil
}

func init() {
	messagequeue.Register("nsq", Factory)
}
