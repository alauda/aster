package main

import (
	_ "expvar"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	//"github.com/fukata/golang-stats-api-handler"
	//"time"
	"github.com/alauda/aster/logs"
	"github.com/alauda/aster/producer"
	"strconv"
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

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	publisher, _ := producer.NewSaramaPublisher("localhost:9092", topic, logger)

	var i int64 = 0
	run := true
	wg.Add(1)
	go func() {
		logger.Info("publisher started!")
		for run {
			i++
			//publisher.Publish(payload)
			publisher.Publish(payload)
			if i%1000 == 0 {
				logger.Infof("%dk messages sent(%d)", (i / 1000), i)
				// cleanup
				//logger.Debugf("closing publisher")
				// publisher.Close()
				//logger.Debugf("publisher closed")
				//publisher ,_= lich.NewPublisher("localhost:9092", "test-2", logger)
				//time.Sleep(time.Second * 10)
			}
			if i == message_size {
				run = false
			}
		}
		wg.Done()
		logger.Info("publisch goroutine stopped!")
	}()

	//for run {
	//	select {
	//	case sig := <-sigChan:
	//		logger.Infof("Caught signal %v: terminating", sig)
	//		// stop producer
	//		run = false
	//	}
	//}

	wg.Wait()

	// cleanup
	logger.Debugf("closing publisher at end")
	publisher.Close()
	logger.Debugf("publisher closed at end")
	logger.Infof("message sent %d", i)
	logger.Info("app will exist!")
}
