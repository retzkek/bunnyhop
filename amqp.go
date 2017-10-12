package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	amqpErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bunnyhop",
		Subsystem: "amqp",
		Name:      "errors_total",
		Help:      "number of AMQP errors",
	})
)

func init() {
	prometheus.MustRegister(amqpErrors)
}

// AMQPError represents an error communicating with the AMQP broker.
type AMQPError struct {
	Message string
}

func NewAMQPError(msg string) AMQPError {
	amqpErrors.Inc()
	return AMQPError{Message: msg}
}

func (e AMQPError) Error() string {
	return e.Message
}

// Message contains the basic components of an AMQP message.
type Message struct {
	ContentType     string // MIME content type
	ContentEncoding string // MIME content encoding
	Body            []byte
	DeliveryTag     uint64
	MessageId       string
}

func (m Message) String() string {
	return string(m.Body)
}

// ConnectionConfig stores and verifies AMQP connection information.
type ConnectionConfig struct {
	Host              string
	Port              string
	Scheme            string
	Vhost             string
	User              string
	Password          string
	SkipVerify        bool
	MinRetry          string
	minRetryDuration  time.Duration
	MaxRetry          string
	maxRetryDuration  time.Duration
	lastRetryDuration time.Duration
	PrefetchCount     int
}

// Check provided configuration, and replace null config values
// with defaults that should work with default RabbitMQ config.
func (c *ConnectionConfig) Check() error {
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port == "" {
		c.Port = "5672"
	}
	if c.Scheme == "" {
		c.Scheme = "amqp"
	}
	if c.User == "" {
		c.User = "guest"
	}
	if c.Password == "" {
		c.Password = "guest"
	}
	var err error
	if c.MinRetry == "" {
		c.MinRetry = "1s"
	}
	c.minRetryDuration, err = time.ParseDuration(c.MinRetry)
	if err != nil {
		return fmt.Errorf("error parsing MinRetry: %s", err)
	}
	if c.MaxRetry == "" {
		c.MaxRetry = "1h"
	}
	c.maxRetryDuration, err = time.ParseDuration(c.MaxRetry)
	if err != nil {
		return fmt.Errorf("error parsing MaxRetry: %s", err)
	}
	return nil
}

func (c *ConnectionConfig) URI() string {
	return c.Scheme + "://" + c.User + ":" + c.Password + "@" +
		c.Host + ":" + c.Port + "/" + c.Vhost
}

// BackoffRetry computes the next retry duration, using "Decorrelated Jitter" method.
// https://www.awsarchitectureblog.com/2015/03/backoff.html
func (c *ConnectionConfig) BackoffRetry() time.Duration {
	if c.lastRetryDuration < c.minRetryDuration {
		c.lastRetryDuration = c.minRetryDuration
	}
	var sleep time.Duration
	sleep = c.minRetryDuration + time.Duration(rand.Int63n(int64(c.lastRetryDuration)*3-int64(c.minRetryDuration)))
	if sleep > c.maxRetryDuration {
		sleep = c.maxRetryDuration
	}
	c.lastRetryDuration = sleep
	return sleep
}

// ResetRetry resets the retry duration.
func (c *ConnectionConfig) ResetRetry() {
	c.lastRetryDuration = c.minRetryDuration
}
