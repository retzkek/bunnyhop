package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/graphite"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

func init() {
	viper.SetDefault("origin.host", "localhost")
	viper.SetDefault("origin.port", "5672")
	viper.SetDefault("origin.scheme", "amqp")
	viper.SetDefault("origin.vhost", "")
	viper.SetDefault("origin.user", "guest")
	viper.SetDefault("origin.password", "guest")
	viper.SetDefault("origin.skipVerify", false)
	viper.SetDefault("origin.minRetry", "1s")
	viper.SetDefault("origin.maxRetry", "1h")
	viper.SetDefault("origin.exchange.name", "gracc")
	viper.SetDefault("origin.exchange.type", "fanout")
	viper.SetDefault("origin.exchange.durable", true)
	viper.SetDefault("origin.exchange.autoDelete", false)
	viper.SetDefault("origin.exchange.internal", false)
	viper.SetDefault("origin.queue.name", "gracc.forward")
	viper.SetDefault("origin.queue.routingKey", "#")
	viper.SetDefault("origin.queue.durable", true)
	viper.SetDefault("origin.queue.autoDelete", false)
	viper.SetDefault("origin.queue.exclusive", true)

	viper.SetDefault("destination.filter", map[string]string{"ProbeName": ".*"})
	viper.SetDefault("destination.host", "localhost")
	viper.SetDefault("destination.port", "5672")
	viper.SetDefault("destination.scheme", "amqp")
	viper.SetDefault("destination.vhost", "")
	viper.SetDefault("destination.user", "guest")
	viper.SetDefault("destination.password", "guest")
	viper.SetDefault("destination.minRetry", "1s")
	viper.SetDefault("destination.maxRetry", "1h")
	viper.SetDefault("destination.skipVerify", false)
	viper.SetDefault("destination.exchange.name", "amq.direct")
	viper.SetDefault("destination.exchange.type", "direct")
	viper.SetDefault("destination.exchange.durable", true)
	viper.SetDefault("destination.exchange.autoDelete", false)
	viper.SetDefault("destination.exchange.internal", false)
	viper.SetDefault("destination.exchange.routingKey", "gracc")

	viper.SetDefault("app.log.level", "debug")
	viper.SetDefault("app.metrics.publish.enabled", true)
	viper.SetDefault("app.metrics.publish.address", "localhost:8080")
	viper.SetDefault("app.metrics.graphite.enabled", true)
	viper.SetDefault("app.metrics.graphite.url", "localhost:2003")
	viper.SetDefault("app.metrics.graphite.prefix", "")
	viper.SetDefault("app.metrics.graphite.interval", "1m")
	viper.SetDefault("app.metrics.graphite.timeout", "10s")

	viper.SetConfigName("gracc-forward")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	viper.SetEnvPrefix("GRACC")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
}

func main() {
	// start logging
	switch viper.GetString("app.log.level") {
	case "debug":
		log.SetLevel(log.DebugLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	// Global context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start monitoring
	if viper.GetBool("app.metrics.publish.enabled") {
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(viper.GetString("app.metrics.publish.address"), nil)
	}
	if viper.GetBool("app.metrics.graphite.enabled") {
		if err := startGraphite(
			ctx,
			viper.GetString("app.metrics.graphite.url"),
			viper.GetString("app.metrics.graphite.prefix"),
			viper.GetString("app.metrics.graphite.interval"),
			viper.GetString("app.metrics.graphite.timeout"),
		); err != nil {
			log.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	inbox, err := startConsumer(ctx, &wg)
	if err != nil {
		log.Fatal(err)
	}

	// start publisher
	// TODO

	// forward records
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
mainloop:
	for {
		select {
		case r := <-inbox:
			log.Debug(string(r))
			// TODO
		case s := <-signals:
			log.WithField("signal", s).Debug("got signal")
			switch s {
			case syscall.SIGINT:
				cancel()
				wg.Wait()
				break mainloop
			}
		}

	}
}

func startConsumer(ctx context.Context, wg *sync.WaitGroup) (chan []byte, error) {
	outbox := make(chan []byte)
	var err error
	conn := Connection{
		Host:       viper.GetString("origin.host"),
		Port:       viper.GetString("origin.port"),
		Scheme:     viper.GetString("origin.scheme"),
		Vhost:      viper.GetString("origin.vhost"),
		User:       viper.GetString("origin.user"),
		Password:   viper.GetString("origin.password"),
		SkipVerify: viper.GetBool("origin.skipVerify"),
		MinRetry:   viper.GetString("origin.minRetry"),
		MaxRetry:   viper.GetString("origin.maxRetry"),
	}
	if err := conn.Connect(ctx); err != nil {
		log.Error("unable to connect to origin RabbitMQ")
		return outbox, err
	}
	cch, err := conn.OpenChannel()
	if err != nil {
		log.Error("unable to open channel to origin RabbitMQ")
		return outbox, err
	}
	if err := cch.ExchangeDeclarePassive(
		viper.GetString("origin.exchange.name"),
		viper.GetString("origin.exchange.type"),
		viper.GetBool("origin.exchange.durable"),
		viper.GetBool("origin.exchange.autoDelete"),
		viper.GetBool("origin.exchange.internal"),
		false, nil); err != nil {
		log.Errorf("exchange %s not found or not properly configured in origin RabbitMQ", viper.GetString("origin.exchange.name"))
		return outbox, err
	}
	if _, err := cch.QueueDeclare(
		viper.GetString("origin.queue.name"),
		viper.GetBool("origin.queue.durable"),
		viper.GetBool("origin.queue.autoDelete"),
		viper.GetBool("origin.queue.exclusive"),
		false, nil); err != nil {
		log.Errorf("unable to declare queue in origin RabbitMQ")
		return outbox, err
	}
	if err := cch.QueueBind(
		viper.GetString("origin.queue.name"),
		viper.GetString("origin.queue.routingKey"),
		viper.GetString("origin.exchange.name"),
		false, nil); err != nil {
		log.Errorf("unable to bind queue in origin RabbitMQ")
		return outbox, err
	}
	inbox, err := cch.Consume(viper.GetString("origin.queue.name"),
		"", false, false, true, false, nil)
	if err != nil {
		log.Errorf("unable to start consumer in origin RabbitMQ")
		return outbox, err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Infof("closing consumer: %s", ctx.Err())
				cch.Close()
				return
			case r := <-inbox:
				log.Debugf("got record %d", r.DeliveryTag)
				outbox <- r.Body
				r.Ack(false)
			}
		}
	}()
	return outbox, nil
}

func startGraphite(ctx context.Context, url, prefix, interval, timeout string) error {
	intervald, err := time.ParseDuration(interval)
	if err != nil {
		return fmt.Errorf("invalid graphite interval: %s", intervald)
	}
	timeoutd, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("invalid graphite timeout: %s", timeoutd)
	}
	log.WithFields(log.Fields{
		"URL":      url,
		"Prefix":   prefix,
		"Interval": intervald,
	}).Info("Starting graphite metrics.")
	b, err := graphite.NewBridge(&graphite.Config{
		URL:           url,
		Gatherer:      prometheus.DefaultGatherer,
		Prefix:        prefix,
		Interval:      intervald,
		Timeout:       timeoutd,
		ErrorHandling: graphite.AbortOnError,
		Logger:        log.StandardLogger(),
	})
	if err != nil {
		return err
	}

	// Push initial metrics to Graphite. Fail fast if the push fails.
	if err := b.Push(); err != nil {
		return err
	}

	// Start pushing metrics to Graphite in the Run() loop.
	go b.Run(ctx)
	return nil
}
