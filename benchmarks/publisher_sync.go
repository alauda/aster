package main

import (
	_ "expvar"
	"net/http"
	"os"
	"sync"
	//"github.com/fukata/golang-stats-api-handler"
	//"time"
	"fmt"
	"github.com/alauda/aster/logs"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strconv"
	"time"
)

func main() {
	logger, _ := logs.SetupLogger(logs.LogConfig{Stdout: true})
	topic := os.Args[1]
	s := os.Args[2]
	var message_size int64
	var err error
	if message_size, err = strconv.ParseInt(s, 10, 64); err != nil {
		logger.Fatalf("message_size error %s", err)
	}

	go func() {
		//http.HandleFunc("/stats", stats_api.Handler)
		http.ListenAndServe(":8081", nil)

	}()

	// size 16 (2 ^ 4)
	var payload = "0123456789abcdef"
	for i := 0; i < 13; i++ {
		payload = payload + payload
	}
	// should be 1k
	logger.Infof("Message payload size %dk", len(payload)/1024)

	wg := &sync.WaitGroup{}

	var i int64 = 0
	wg.Add(1)
	run := true
	go func() {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":      "localhost:9092",
			"go.delivery.reports":    false,
			"go.batch.producer":      true,
			"queue.buffering.max.ms": 1,
			//"queue.buffering.max.messages":1,
		})

		if err != nil {
			fmt.Printf("Failed to create producer: %s\n", err)
			os.Exit(1)
		}

		logger.Info("Created Producer %v", p)

		// Optional delivery channel, if not specified the Producer object's
		logger.Info("publisher started!")
		for run {
			i++

			// .Events channel is used.
			deliveryChan := make(chan kafka.Event)

			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				}, Value: []byte(payload)}, deliveryChan)
			start := time.Now()
			e := <-deliveryChan
			elapsed := time.Since(start)
			logger.Infof("Pooling from channel took %s", elapsed)

			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				logger.Errorf("Delivery failed: %v", m.TopicPartition.Error)
			} else {
				//logger.Debugf("Delivered message to topic %s [%d] at offset %v",
				//	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}

			close(deliveryChan)

			if i%100 == 0 {
				logger.Infof("%dk messages sent(%d)", (i / 1000), i)
			}

			if i == message_size {
				run = false
			}
		}
		wg.Done()
		logger.Info("publisch goroutine stopped!")
	}()

	wg.Wait()

	logger.Infof("message sent %d", i)
	logger.Info("app will exist!")
}
