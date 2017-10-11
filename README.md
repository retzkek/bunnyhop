# bunnyhop

🐇.='''=.🐇

bunnyhop is an AMQP 0.9.1 (e.g. RabbitMQ) message relay, that simply reads from
a queue bound to an exchange in one broker and writes to an exchange in another.
It aims for reliability and preventing message loss.

It optionally can filter JSON messages by matching the value of top-level keys 
to a regex pattern. If filter rules are provided, they define a whitelist, where
a message that matches any rule is forwarded. If *no* rules are defined, *all*
messages are forwarded (and bunnyhop skips the overhead of unmarshaling the message from JSON).

## Installation

The recommended method of running bunnyhop is with Docker:

```
docker pull retzkek/bunnyhop
```

Binary releases may be available on [GitHub](https://github.com/retzkek/bunnyhop).

## Configuration

bunnyhop supports configuration via JSON, TOML, YAML, HCL, or Java properties formats.
See the `bunnyhop.yml` file for a complete configuration example with description and defaults.
It will look for files named `bunnyhop` with the appropriate extension, e.g. `bunnyhop.yml`,
in the following locations:

* /etc
* /etc/bunnyhop
* .

Parameters can also be set by environment variable that is prefixed with "BH\_", and
follows dot notation, except underscore-delimited and in all caps. e.g.:

* BH\_APP\_LOG\_LEVEL=debug
* BH\_ORIGIN\_EXCHANGE\_NAME=myexchange

## Monitoring

bunnyhop by default will publish various runtime metrics  in the 
[Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/)
at http://localhost:8080/metrics. It can also push metrics in the Graphite "plain" format at regular 
intervals.

Additionally, it can expose [pprof](https://golang.org/pkg/net/http/pprof/) profiling information 
on a separate port (default http://localhost:6060).
