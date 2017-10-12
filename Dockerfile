FROM golang:1.8

WORKDIR /go/src/app
COPY . .

RUN go-wrapper download   # "go get -d -v ./..."
RUN go-wrapper install -ldflags="-X main.BUILD=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

CMD ["go-wrapper", "run"] # ["app"]
