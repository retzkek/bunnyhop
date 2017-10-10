package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
)

type Publisher struct {
	ConnectionConfig
}

func startPublisher(ctx context.Context, outbox chan Message, wg *sync.WaitGroup) error {
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
	}
	if err := pub.Check(); err != nil {
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			log.Info("starting publisher")
			retry := pub.BackoffRetry()
			if err := pub.Run(ctx, outbox); err != nil {
				log.WithFields(log.Fields{
					"retry":  retry.String(),
					"reason": err.Error(),
				}).Warning("publisher exited")
			}
			select {
			case <-ctx.Done():
				log.Infof("stopping publisher: %s", ctx.Err())
				return
			case <-time.After(retry):
			}
		}
	}()
	return nil
}

func (p *Publisher) Run(ctx context.Context, outbox chan Message) error {
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

	if err := setupPublisherExchange(cch); err != nil {
		return err
	}

	// reset after successful connection
	p.ResetRetry()

	for {
		select {
		case <-ctx.Done():
			log.Infof("closing publisher: %s", ctx.Err())
			return nil
		case cl := <-connClosing:
			log.WithFields(log.Fields{
				"code":             cl.Code,
				"reason":           cl.Reason,
				"server-initiated": cl.Server,
				"can-recover":      cl.Recover,
			}).Debug("closing consumer: connection closed")
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
		case r := <-outbox:
			log.Debugf("publishing record")
			if err := cch.Publish(
				viper.GetString("destination.exchange.name"),
				viper.GetString("destination.exchange.routingKey"),
				true,  // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType:     r.ContentType,
					ContentEncoding: r.ContentEncoding,
					Body:            r.Body,
				}); err != nil {
				log.Debug(err)
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
