BUILD=`date -u +%Y-%m-%dT%H:%M:%SZ`
build:
	go build -ldflags="-X main.BUILD=${BUILD}"

docker:
	docker build -t retzkek/bunnyhop .
