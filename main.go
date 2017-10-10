package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
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
	viper.SetDefault("origin.qos.prefetchCount", 1)

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
	viper.SetDefault("app.metrics.publish.timeout", "30s")
	viper.SetDefault("app.metrics.graphite.enabled", true)
	viper.SetDefault("app.metrics.graphite.url", "localhost:2003")
	viper.SetDefault("app.metrics.graphite.prefix", "gracc-forward")
	viper.SetDefault("app.metrics.graphite.interval", "1m")
	viper.SetDefault("app.metrics.graphite.timeout", "10s")
	viper.SetDefault("app.pprof.enabled", false)
	viper.SetDefault("app.pprof.address", "localhost:6060")

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

	// wait group to track goroutines
	var wg sync.WaitGroup

	// monitoring
	if viper.GetBool("app.metrics.publish.enabled") {
		if err := startMetricsServer(
			ctx,
			viper.GetString("app.metrics.publish.address"),
			viper.GetString("app.metrics.publish.timeout"),
			&wg,
		); err != nil {
			log.Fatal(err)
		}
	}
	if viper.GetBool("app.metrics.graphite.enabled") {
		if err := startGraphite(
			ctx,
			viper.GetString("app.metrics.graphite.url"),
			viper.GetString("app.metrics.graphite.prefix"),
			viper.GetString("app.metrics.graphite.interval"),
			viper.GetString("app.metrics.graphite.timeout"),
			&wg,
		); err != nil {
			log.Fatal(err)
		}
	}
	if viper.GetBool("app.pprof.enabled") {
		go http.ListenAndServe(viper.GetString("app.pprof.address"), nil)
		log.Infof("pprof available at %s/debug/pprof/", viper.GetString("app.pprof.address"))
	}

	// size channels to hold maximum number of in-flight records
	inbox := make(chan Message, viper.GetInt("origin.qos.prefetchCount"))
	confirm := make(chan uint64, viper.GetInt("origin.qos.prefetchCount"))
	reject := make(chan uint64, viper.GetInt("origin.qos.prefetchCount"))
	if err := startConsumer(ctx, &wg, inbox, confirm, reject); err != nil {
		log.Fatal(err)
	}
	if err := startPublisher(ctx, &wg, inbox, confirm, reject); err != nil {
		log.Fatal(err)
	}

	// run until exit signalled
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
mainloop:
	for {
		select {
		case s := <-signals:
			log.WithField("signal", s).Debug("got signal")
			switch s {
			case syscall.SIGINT:
				log.Info("exiting (SIGINT)...")
				break mainloop
			}
		}
	}

	cancel()
	wg.Wait()
}

func startMetricsServer(ctx context.Context, address, timeout string, wg *sync.WaitGroup) error {
	timeoutd, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("invalid metrics server timeout duration %s", timeout)
	}
	metricSrv := &http.Server{Addr: address}
	log.WithField("address", address).Info("starting metrics server")
	http.Handle("/metrics", promhttp.Handler())
	wg.Add(2)
	go func() {
		defer wg.Done()
		log.Debug(metricSrv.ListenAndServe())
	}()
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.WithField("timeout", timeout).Info("stopping metrics server...")
		ctx, cancel := context.WithTimeout(context.Background(), timeoutd)
		if err := metricSrv.Shutdown(ctx); err != nil {
			log.Warningf("error when stopping metrics server: %s", err)
		}
		cancel()
		log.Info("metrics server stopped")
	}()
	return nil
}

func startGraphite(ctx context.Context, url, prefix, interval, timeout string, wg *sync.WaitGroup) error {
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
	}).Info("starting graphite metrics")
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Run(ctx)
	}()
	return nil
}
