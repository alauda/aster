
all: benchmark example

benchmark:
	go build -o bin/publisher benchmarks/publisher.go
	go build -o bin/publisher_sync benchmarks/publisher_sync.go
	go build -o bin/publisher_func benchmarks/publisher_func.go
	go build -o bin/sarama_publisher benchmarks/sarama_publisher.go

example:
	go build -o bin/worker examples/worker.go

clean:
	rm bin/*

