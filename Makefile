GOFILES_NOVENDOR = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

all: fmt vet benchmark example

fmt:
	gofmt -l -w ${GOFILES_NOVENDOR}

vet:
	go vet ${GOFILES_NOVENDOR}

benchmark:
	go build -o bin/publisher benchmarks/publisher.go
	go build -o bin/publisher_sync benchmarks/publisher_sync.go
	go build -o bin/publisher_func benchmarks/publisher_func.go
	go build -o bin/sarama_publisher benchmarks/sarama_publisher.go

example:
	go build -o bin/worker examples/worker.go

clean:
	rm bin/*

