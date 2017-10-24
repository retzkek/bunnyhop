package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

var (
	publisherStarts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bunnyhop",
		Subsystem: "publisher",
		Name:      "starts_total",
		Help:      "number of times publisher has been (re-)started",
	})
	publisherMessages = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "bunnyhop",
			Subsystem: "publisher",
			Name:      "messages_total",
			Help:      "number of messages publisher has processed",
		},
		[]string{"filter"},
	)
	publisherAcks = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bunnyhop",
		Subsystem: "publisher",
		Name:      "acks_total",
		Help:      "number of messages broker has acknowledged",
	})
	publisherNacks = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bunnyhop",
		Subsystem: "publisher",
		Name:      "nacks_total",
		Help:      "number of messages broker has rejected",
	})
	publisherReturns = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bunnyhop",
		Subsystem: "publisher",
		Name:      "returns_total",
		Help:      "number of messages broker has returned",
	})
)

func init() {
	prometheus.MustRegister(publisherStarts)
	prometheus.MustRegister(publisherMessages)
	prometheus.MustRegister(publisherAcks)
	prometheus.MustRegister(publisherNacks)
	prometheus.MustRegister(publisherReturns)
}

type Filter struct {
	Field   string
	Pattern string
}

type Publisher struct {
	ConnectionConfig
	outbox       chan Message
	confirm      chan uint64
	reject       chan uint64
	messages     map[uint64]uint64
	messageCount uint64
	filters      []Filter
}

func startPublisher(ctx context.Context, wg *sync.WaitGroup, outbox chan Message, confirm, reject chan uint64) error {
	pub := Publisher{
		ConnectionConfig: ConnectionConfig{
			Host:       viper.GetString("destination.host"),
			Port:       viper.GetString("destination.port"),
			Scheme:     viper.GetString("destination.scheme"),
			Vhost:      viper.GetString("destination.vhost"),
			User:       viper.GetString("destination.user"),
			Password:   viper.GetString("destination.password"),
			SkipVerify: viper.GetBool("destination.skipVerify"),
			MinRetry:   viper.GetString("destination.minRetry"),
			MaxRetry:   viper.GetString("destination.maxRetry"),
		},
		outbox:   outbox,
		confirm:  confirm,
		reject:   reject,
		messages: make(map[uint64]uint64),
	}
	if err := pub.Check(); err != nil {
		return err
	}
	if err := viper.UnmarshalKey("filters", &pub.filters); err != nil {
		return fmt.Errorf("error loading filters: %s", err.Error())
	}
	log.Debugf("loaded filters: %s", pub.filters)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			log.Info("starting publisher")
			publisherStarts.Inc()
			retry := pub.BackoffRetry()
			if err := pub.Run(ctx); err != nil {
				log.WithFields(log.Fields{
					"retry":  retry.String(),
					"reason": err.Error(),
				}).Warning("publisher exited")
			}
			select {
			case <-ctx.Done():
				log.Infof("publisher stopped: %s", ctx.Err())
				return
			case <-time.After(retry):
			}
		}
	}()
	return nil
}

func (p *Publisher) Run(ctx context.Context) error {
	log.WithFields(log.Fields{
		"user":  p.User,
		"host":  p.Host,
		"vhost": p.Vhost,
		"port":  p.Port,
	}).Info("publisher: connecting to RabbitMQ")
	conn, err := amqp.DialTLS(p.URI(), &tls.Config{InsecureSkipVerify: p.SkipVerify})
	if err != nil {
		log.Debug(err)
		return NewAMQPError("unable to connect to destination RabbitMQ")
	}
	defer conn.Close()
	connClosing := conn.NotifyClose(make(chan *amqp.Error))
	blockings := conn.NotifyBlocked(make(chan amqp.Blocking))

	cch, err := conn.Channel()
	if err != nil {
		log.Debug(err)
		return NewAMQPError("unable to open channel to destination RabbitMQ")
	}
	defer cch.Close()
	chanClosing := cch.NotifyClose(make(chan *amqp.Error, 1))
	returns := cch.NotifyReturn(make(chan amqp.Return))
	if err = cch.Confirm(false); err != nil {
		log.Debug(err)
		return NewAMQPError("RabbitMQ channel could not be put into confirm mode")
	}
	confirms := cch.NotifyPublish(make(chan amqp.Confirmation))

	if err := setupPublisherExchange(cch); err != nil {
		return err
	}

	// reset after successful connection
	p.ResetRetry()

publisherLoop:
	for {
		select {
		case <-ctx.Done():
			log.Infof("closing publisher: %s", ctx.Err())
			break publisherLoop
		case cl := <-connClosing:
			log.WithFields(log.Fields{
				"code":             cl.Code,
				"reason":           cl.Reason,
				"server-initiated": cl.Server,
				"can-recover":      cl.Recover,
			}).Debug("closing publisher: connection closed")
			return NewAMQPError("connection closed")
		case cl := <-chanClosing:
			log.WithFields(log.Fields{
				"code":             cl.Code,
				"reason":           cl.Reason,
				"server-initiated": cl.Server,
				"can-recover":      cl.Recover,
			}).Debug("closing publisher: channel closed")
			return NewAMQPError("channel closed")
		case b := <-blockings:
			if b.Active {
				log.WithField("reason", b.Reason).Warning("publisher: TCP blocked")
			} else {
				log.Infof("publisher: TCP unblocked")
			}
		case r := <-returns:
			log.WithFields(log.Fields{
				"code":   r.ReplyCode,
				"reason": r.ReplyText,
				"id":     r.MessageId,
			}).Warning("publisher: record returned")
			publisherReturns.Inc()
			tag, err := strconv.ParseUint(r.MessageId, 36, 64)
			if err != nil {
				log.Debug(err)
			} else {
				p.reject <- p.messages[tag]
				delete(p.messages, tag)
			}
		case cf := <-confirms:
			log.WithFields(log.Fields{
				"tag": cf.DeliveryTag,
				"ack": cf.Ack,
			}).Debug("publisher: confirm")
			originTag, found := p.messages[cf.DeliveryTag]
			if found {
				if cf.Ack {
					log.WithFields(log.Fields{
						"consumerTag":  originTag,
						"publisherTag": cf.DeliveryTag,
					}).Debug("publisher: got ack")
					publisherAcks.Inc()
					p.confirm <- originTag
				} else {
					log.WithFields(log.Fields{
						"consumerTag":  originTag,
						"publisherTag": cf.DeliveryTag,
					}).Warning("publisher: got nack")
					publisherNacks.Inc()
					p.reject <- originTag
				}
				delete(p.messages, cf.DeliveryTag)
			} else {
				log.WithFields(log.Fields{
					"publisherTag": cf.DeliveryTag,
				}).Debug("publisher: couldn't find consumer tag")
			}
		case r := <-p.outbox:
			log.Debugf("publisher: got record: %s", string(r.Body))
			if p.filterPass(r) {
				log.Debug("publisher: publishing record")
				publisherMessages.With(prometheus.Labels{"filter": "pass"}).Inc()
				if err := cch.Publish(
					viper.GetString("destination.exchange.name"),
					viper.GetString("destination.exchange.routingKey"),
					true,  // mandatory
					false, // immediate
					amqp.Publishing{
						ContentType:     r.ContentType,
						ContentEncoding: r.ContentEncoding,
						Body:            r.Body,
						DeliveryMode:    2, // persistent
						MessageId:       strconv.FormatUint(p.messageCount+1, 36),
					}); err != nil {
					log.Debug(err)
					p.reject <- r.DeliveryTag
				} else {
					p.messageCount += 1
					p.messages[p.messageCount] = r.DeliveryTag
				}
			} else {
				log.Debug("publisher: discarding record")
				publisherMessages.With(prometheus.Labels{"filter": "drop"}).Inc()
				p.confirm <- r.DeliveryTag
			}
		}
	}
	return nil
}

func setupPublisherExchange(cch *amqp.Channel) error {
	if err := cch.ExchangeDeclarePassive(
		viper.GetString("destination.exchange.name"),
		viper.GetString("destination.exchange.type"),
		viper.GetBool("destination.exchange.durable"),
		viper.GetBool("destination.exchange.autoDelete"),
		viper.GetBool("destination.exchange.internal"),
		false, nil); err != nil {
		log.Warningf("exchange %s not found or not properly configured in destination RabbitMQ, trying to declare", viper.GetString("destination.exchange.name"))
		if err := cch.ExchangeDeclare(
			viper.GetString("destination.exchange.name"),
			viper.GetString("destination.exchange.type"),
			viper.GetBool("destination.exchange.durable"),
			viper.GetBool("destination.exchange.autoDelete"),
			viper.GetBool("destination.exchange.internal"),
			false, nil); err != nil {
			log.Debug(err)
			return NewAMQPError(fmt.Sprintf("unable to declare exchange %s in destination RabbitMQ", viper.GetString("destination.exchange.name")))
		}
	}
	return nil
}

func (p *Publisher) filterPass(m Message) bool {
	// short-circuit with empty filter list
	if len(p.filters) == 0 {
		return true
	}
	var r map[string]interface{}
	if err := json.Unmarshal(m.Body, &r); err != nil {
		log.Debugf("error unmarshalling record: %s", err.Error())
		log.Debug(r)
		return false
	}
	for _, f := range p.filters {
		val, found := r[f.Field]
		if found {
			var sval string
			switch val.(type) {
			case string:
				sval = val.(string)
			default:
				log.Errorf("error matching filter: value of field %s is not a string", f.Field)
				continue
			}
			if matched, err := regexp.MatchString(f.Pattern, sval); matched && err == nil {
				log.WithFields(log.Fields{
					"field":   f.Field,
					"val":     sval,
					"pattern": f.Pattern,
				}).Debug("filter matched")
				return true
			} else if err != nil {
				log.Errorf("error matching filter [%s=%s]: %s", f.Field, f.Pattern, err.Error())
				continue
			}
		}

	}
	return false
}
