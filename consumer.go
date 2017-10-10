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

type Consumer struct {
	ConnectionConfig
}

func startConsumer(ctx context.Context, outbox chan Message, wg *sync.WaitGroup) error {
	con := Consumer{
		ConnectionConfig: ConnectionConfig{
			Host:       viper.GetString("origin.host"),
			Port:       viper.GetString("origin.port"),
			Scheme:     viper.GetString("origin.scheme"),
			Vhost:      viper.GetString("origin.vhost"),
			User:       viper.GetString("origin.user"),
			Password:   viper.GetString("origin.password"),
			SkipVerify: viper.GetBool("origin.skipVerify"),
			MinRetry:   viper.GetString("origin.minRetry"),
			MaxRetry:   viper.GetString("origin.maxRetry"),
		},
	}
	if err := con.Check(); err != nil {
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			log.Info("starting consumer")
			retry := con.BackoffRetry()
			if err := con.Run(ctx, outbox); err != nil {
				log.WithFields(log.Fields{
					"retry":  retry.String(),
					"reason": err.Error(),
				}).Warning("consumer exited")
			}
			select {
			case <-ctx.Done():
				log.Infof("stopping consumer: %s", ctx.Err())
				return
			case <-time.After(retry):
			}
		}
	}()
	return nil
}

func (c *Consumer) Run(ctx context.Context, outbox chan Message) error {
	log.WithFields(log.Fields{
		"user":  c.User,
		"host":  c.Host,
		"vhost": c.Vhost,
		"port":  c.Port,
	}).Infof("consumer: connecting to RabbitMQ")
	conn, err := amqp.DialTLS(c.URI(), &tls.Config{InsecureSkipVerify: c.SkipVerify})
	if err != nil {
		log.Debug(err)
		return NewAMQPError("unable to connect to origin RabbitMQ")
	}
	defer conn.Close()
	connClosing := conn.NotifyClose(make(chan *amqp.Error))

	cch, err := conn.Channel()
	if err != nil {
		log.Debug(err)
		return NewAMQPError("unable to open channel to origin RabbitMQ")
	}
	defer cch.Close()
	chanClosing := cch.NotifyClose(make(chan *amqp.Error, 1))

	if err := setupConsumerQueue(cch); err != nil {
		return err
	}

	inbox, err := cch.Consume(viper.GetString("origin.queue.name"),
		"", false, false, true, false, nil)
	if err != nil {
		log.Debug(err)
		return NewAMQPError("unable to start consumer in origin RabbitMQ")
	}

	// reset after successful connection
	c.ResetRetry()

	for {
		select {
		case <-ctx.Done():
			log.Infof("closing consumer: %s", ctx.Err())
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
			}).Debug("closing consumer: channel closed")
			return NewAMQPError("channel closed")
		case r := <-inbox:
			log.Debugf("got record %d", r.DeliveryTag)
			outbox <- Message{
				ContentType:     r.ContentType,
				ContentEncoding: r.ContentEncoding,
				Body:            r.Body,
			}
			r.Ack(false)
		}
	}
	return nil
}

func setupConsumerQueue(cch *amqp.Channel) error {
	if err := cch.ExchangeDeclarePassive(
		viper.GetString("origin.exchange.name"),
		viper.GetString("origin.exchange.type"),
		viper.GetBool("origin.exchange.durable"),
		viper.GetBool("origin.exchange.autoDelete"),
		viper.GetBool("origin.exchange.internal"),
		false, nil); err != nil {
		log.Debug(err)
		return NewAMQPError(fmt.Sprintf("exchange %s not found or not properly configured in origin RabbitMQ", viper.GetString("origin.exchange.name")))
	}
	if _, err := cch.QueueDeclare(
		viper.GetString("origin.queue.name"),
		viper.GetBool("origin.queue.durable"),
		viper.GetBool("origin.queue.autoDelete"),
		viper.GetBool("origin.queue.exclusive"),
		false, nil); err != nil {
		log.Debug(err)
		return NewAMQPError("unable to declare queue in origin RabbitMQ")
	}
	if err := cch.QueueBind(
		viper.GetString("origin.queue.name"),
		viper.GetString("origin.queue.routingKey"),
		viper.GetString("origin.exchange.name"),
		false, nil); err != nil {
		log.Debug(err)
		return NewAMQPError("unable to bind queue in origin RabbitMQ")
	}
	return nil
}
